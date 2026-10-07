package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3client "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/suhwr/blobkit"
)

// Driver implements provider.Driver backed by AWS SDK v2, supporting Cloudflare R2,
// AWS S3, MinIO, Wasabi, and other S3-compatible endpoints.
type Driver struct {
	cfg           Config
	client        *s3client.Client
	presignClient *s3client.PresignClient
	chunkPool     sync.Pool
}

// NewDriver initializes a high-performance S3 driver with persistent connection pooling.
func NewDriver(cfg Config) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Transport: NewPooledHTTPTransport(),
			Timeout:   0, // Streaming requests manage their own context timeouts
		}
	}

	awsCfg := aws.Config{
		Region:           cfg.Region,
		HTTPClient:       httpClient,
		RetryMaxAttempts: cfg.MaxRetries,
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			cfg.SessionToken,
		)
	}

	client := s3client.NewFromConfig(awsCfg, func(o *s3client.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	presignClient := s3client.NewPresignClient(client)

	d := &Driver{
		cfg:           cfg,
		client:        client,
		presignClient: presignClient,
	}
	d.chunkPool.New = func() any {
		b := make([]byte, cfg.MultipartPartSize)
		return &b
	}

	return d, nil
}

func (d *Driver) getChunkBuffer() *[]byte {
	bufPtr, ok := d.chunkPool.Get().(*[]byte)
	if !ok || bufPtr == nil || int64(cap(*bufPtr)) < d.cfg.MultipartPartSize {
		b := make([]byte, d.cfg.MultipartPartSize)
		return &b
	}
	*bufPtr = (*bufPtr)[:d.cfg.MultipartPartSize]
	return bufPtr
}

func (d *Driver) putChunkBuffer(bufPtr *[]byte) {
	if bufPtr != nil && int64(cap(*bufPtr)) >= d.cfg.MultipartPartSize {
		d.chunkPool.Put(bufPtr)
	}
}

func (d *Driver) Name() string {
	return d.cfg.Name
}

func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
		blobkit.CapMultipartPut |
		blobkit.CapPresignGet |
		blobkit.CapPresignPut |
		blobkit.CapBatchDelete |
		blobkit.CapByteRangeGet |
		blobkit.CapCopy |
		blobkit.CapMultipartSession
}

func (d *Driver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if r == nil {
		return nil, blobkit.ErrNilReader
	}

	// Use single-part PutObject if content length is known and below the multipart threshold
	if opts.Size > 0 && opts.Size < d.cfg.MultipartThreshold {
		input := &s3client.PutObjectInput{
			Bucket:        aws.String(d.cfg.Bucket),
			Key:           aws.String(obj.Key),
			Body:          r,
			ContentLength: aws.Int64(opts.Size),
			ContentType:   aws.String(obj.ContentType),
		}
		if len(opts.Metadata) > 0 {
			input.Metadata = opts.Metadata
		}
		if opts.ContentDisposition != "" {
			input.ContentDisposition = aws.String(opts.ContentDisposition)
		}
		if opts.CacheControl != "" {
			input.CacheControl = aws.String(opts.CacheControl)
		}

		resp, err := d.client.PutObject(ctx, input)
		if err != nil {
			return nil, d.wrapError("put", obj.Key, err)
		}

		now := time.Now().UTC()
		stored := *obj
		stored.Bucket = d.cfg.Bucket
		stored.Size = opts.Size
		if resp.ETag != nil {
			stored.ETag = *resp.ETag
		}
		stored.UpdatedAt = now
		if stored.CreatedAt.IsZero() {
			stored.CreatedAt = now
		}
		stored.Provider = d.cfg.Name
		return &stored, nil
	}

	// Use chunked multipart streaming with bounded memory budget
	return d.uploadMultipart(ctx, obj, r, opts)
}

func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	input := &s3client.GetObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(key),
	}

	if opts.Range != "" {
		input.Range = aws.String(opts.Range)
	}
	if opts.IfMatch != "" {
		input.IfMatch = aws.String(opts.IfMatch)
	}
	if opts.IfNoneMatch != "" {
		input.IfNoneMatch = aws.String(opts.IfNoneMatch)
	}
	if opts.IfModifiedSince != nil {
		input.IfModifiedSince = opts.IfModifiedSince
	}
	if opts.IfUnmodifiedSince != nil {
		input.IfUnmodifiedSince = opts.IfUnmodifiedSince
	}

	resp, err := d.client.GetObject(ctx, input)
	if err != nil {
		return nil, d.wrapError("get", key, err)
	}

	var size int64
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	var contentType string
	if resp.ContentType != nil {
		contentType = *resp.ContentType
	}
	var etag string
	if resp.ETag != nil {
		etag = *resp.ETag
	}
	var lastModified time.Time
	if resp.LastModified != nil {
		lastModified = *resp.LastModified
	}

	obj := blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Bucket,
		Size:        size,
		ContentType: contentType,
		ETag:        etag,
		Metadata:    resp.Metadata,
		UpdatedAt:   lastModified,
		Provider:    d.cfg.Name,
	}

	return &blobkit.ObjectReader{
		Object: obj,
		Body:   resp.Body,
	}, nil
}

func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	resp, err := d.client.HeadObject(ctx, &s3client.HeadObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, d.wrapError("head", key, err)
	}

	var size int64
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	var contentType string
	if resp.ContentType != nil {
		contentType = *resp.ContentType
	}
	var etag string
	if resp.ETag != nil {
		etag = *resp.ETag
	}
	var lastModified time.Time
	if resp.LastModified != nil {
		lastModified = *resp.LastModified
	}

	return &blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Bucket,
		Size:        size,
		ContentType: contentType,
		ETag:        etag,
		Metadata:    resp.Metadata,
		UpdatedAt:   lastModified,
		Provider:    d.cfg.Name,
	}, nil
}

func (d *Driver) Delete(ctx context.Context, key string) error {
	_, err := d.client.DeleteObject(ctx, &s3client.DeleteObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return d.wrapError("delete", key, err)
	}
	return nil
}

func (d *Driver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	const batchLimit = 1000
	var deletedKeys []string

	for i := 0; i < len(keys); i += batchLimit {
		end := i + batchLimit
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[i:end]

		objIDs := make([]types.ObjectIdentifier, len(chunk))
		for j, k := range chunk {
			objIDs[j] = types.ObjectIdentifier{Key: aws.String(k)}
		}

		resp, err := d.client.DeleteObjects(ctx, &s3client.DeleteObjectsInput{
			Bucket: aws.String(d.cfg.Bucket),
			Delete: &types.Delete{
				Objects: objIDs,
				Quiet:   aws.Bool(false),
			},
		})
		if err != nil {
			return deletedKeys, d.wrapError("delete_batch", "", err)
		}

		for _, dObj := range resp.Deleted {
			if dObj.Key != nil {
				deletedKeys = append(deletedKeys, *dObj.Key)
			}
		}

		if len(resp.Errors) > 0 {
			var errMsgs []string
			for _, e := range resp.Errors {
				keyName := ""
				if e.Key != nil {
					keyName = *e.Key
				}
				msg := ""
				if e.Message != nil {
					msg = *e.Message
				}
				errMsgs = append(errMsgs, fmt.Sprintf("%s: %s", keyName, msg))
			}
			return deletedKeys, d.wrapError("delete_batch", "", fmt.Errorf("partial deletion failure (%d errors): %s", len(resp.Errors), strings.Join(errMsgs, "; ")))
		}
	}

	return deletedKeys, nil
}

func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	input := &s3client.ListObjectsV2Input{
		Bucket: aws.String(d.cfg.Bucket),
	}
	if opts.Prefix != "" {
		input.Prefix = aws.String(opts.Prefix)
	}
	if opts.Delimiter != "" {
		input.Delimiter = aws.String(opts.Delimiter)
	}
	if opts.Cursor != "" {
		input.ContinuationToken = aws.String(opts.Cursor)
	}
	if opts.Limit > 0 {
		input.MaxKeys = aws.Int32(int32(opts.Limit))
	}

	resp, err := d.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, d.wrapError("list", opts.Prefix, err)
	}

	result := &blobkit.ListResult{
		Objects:     make([]blobkit.Object, len(resp.Contents)),
		IsTruncated: resp.IsTruncated != nil && *resp.IsTruncated,
	}

	if resp.NextContinuationToken != nil {
		result.NextCursor = *resp.NextContinuationToken
	}

	for _, cp := range resp.CommonPrefixes {
		if cp.Prefix != nil {
			result.CommonPrefixes = append(result.CommonPrefixes, *cp.Prefix)
		}
	}

	for i, c := range resp.Contents {
		var size int64
		if c.Size != nil {
			size = *c.Size
		}
		var etag string
		if c.ETag != nil {
			etag = *c.ETag
		}
		var lastMod time.Time
		if c.LastModified != nil {
			lastMod = *c.LastModified
		}
		var keyStr string
		if c.Key != nil {
			keyStr = *c.Key
		}

		result.Objects[i] = blobkit.Object{
			Key:       keyStr,
			Bucket:    d.cfg.Bucket,
			Size:      size,
			ETag:      etag,
			UpdatedAt: lastMod,
			Provider:  d.cfg.Name,
		}
	}

	return result, nil
}

func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	expiry := opts.Expiry
	if expiry <= 0 {
		expiry = blobkit.DefaultPresignExpiry
	}

	req, err := d.presignClient.PresignGetObject(ctx, &s3client.GetObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(key),
	}, s3client.WithPresignExpires(expiry))
	if err != nil {
		return nil, d.wrapError("presign_get", key, err)
	}

	signedHeaders := make(map[string]string)
	for k, v := range req.SignedHeader {
		if len(v) > 0 {
			signedHeaders[k] = v[0]
		}
	}

	return &blobkit.PresignedURL{
		URL:           req.URL,
		Method:        req.Method,
		ExpiresAt:     time.Now().Add(expiry),
		SignedHeaders: signedHeaders,
	}, nil
}

func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	expiry := opts.Expiry
	if expiry <= 0 {
		expiry = blobkit.DefaultPresignExpiry
	}

	input := &s3client.PutObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(key),
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	if opts.ContentDisposition != "" {
		input.ContentDisposition = aws.String(opts.ContentDisposition)
	}

	req, err := d.presignClient.PresignPutObject(ctx, input, s3client.WithPresignExpires(expiry))
	if err != nil {
		return nil, d.wrapError("presign_put", key, err)
	}

	signedHeaders := make(map[string]string)
	for k, v := range req.SignedHeader {
		if len(v) > 0 {
			signedHeaders[k] = v[0]
		}
	}

	return &blobkit.PresignedURL{
		URL:           req.URL,
		Method:        req.Method,
		ExpiresAt:     time.Now().Add(expiry),
		SignedHeaders: signedHeaders,
	}, nil
}

func (d *Driver) ResolveURL(key string) (string, error) {
	cleanKey := strings.TrimLeft(key, "/")
	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", d.cfg.PublicBaseURL, cleanKey), nil
	}
	if d.cfg.Endpoint != "" {
		if d.cfg.UsePathStyle {
			return fmt.Sprintf("%s/%s/%s", strings.TrimRight(d.cfg.Endpoint, "/"), d.cfg.Bucket, cleanKey), nil
		}
		// Virtual-hosted style
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.Endpoint, "/"), cleanKey), nil
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", d.cfg.Bucket, d.cfg.Region, cleanKey), nil
}

func (d *Driver) Copy(ctx context.Context, srcKey, dstKey string) error {
	source := url.PathEscape(d.cfg.Bucket + "/" + strings.TrimLeft(srcKey, "/"))
	_, err := d.client.CopyObject(ctx, &s3client.CopyObjectInput{
		Bucket:     aws.String(d.cfg.Bucket),
		Key:        aws.String(strings.TrimLeft(dstKey, "/")),
		CopySource: aws.String(source),
	})
	if err != nil {
		return d.wrapError("copy", srcKey, err)
	}
	return nil
}

func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	input := &s3client.CreateMultipartUploadInput{
		Bucket:      aws.String(d.cfg.Bucket),
		Key:         aws.String(obj.Key),
		ContentType: aws.String(obj.ContentType),
	}
	if len(opts.Metadata) > 0 {
		input.Metadata = opts.Metadata
	}
	if opts.ContentDisposition != "" {
		input.ContentDisposition = aws.String(opts.ContentDisposition)
	}
	if opts.CacheControl != "" {
		input.CacheControl = aws.String(opts.CacheControl)
	}
	resp, err := d.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", d.wrapError("create_multipart", obj.Key, err)
	}
	return *resp.UploadId, nil
}

func (d *Driver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	resp, err := d.client.UploadPart(ctx, &s3client.UploadPartInput{
		Bucket:        aws.String(d.cfg.Bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(partNumber),
		Body:          r,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return "", d.wrapError("upload_part", key, err)
	}
	if resp.ETag != nil {
		return *resp.ETag, nil
	}
	return "", nil
}

func (d *Driver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	completed := make([]types.CompletedPart, len(parts))
	var totalSize int64
	for i, p := range parts {
		completed[i] = types.CompletedPart{
			PartNumber: aws.Int32(p.PartNumber),
			ETag:       aws.String(p.ETag),
		}
		totalSize += p.Size
	}
	sort.Slice(completed, func(i, j int) bool {
		return *completed[i].PartNumber < *completed[j].PartNumber
	})

	resp, err := d.client.CompleteMultipartUpload(ctx, &s3client.CompleteMultipartUploadInput{
		Bucket:   aws.String(d.cfg.Bucket),
		Key:      aws.String(obj.Key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		return nil, d.wrapError("complete_multipart", obj.Key, err)
	}

	stored := *obj
	stored.Bucket = d.cfg.Bucket
	stored.Size = totalSize
	if resp.ETag != nil {
		stored.ETag = *resp.ETag
	}
	stored.UpdatedAt = time.Now().UTC()
	stored.Provider = d.cfg.Name
	return &stored, nil
}

func (d *Driver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	_, err := d.client.AbortMultipartUpload(ctx, &s3client.AbortMultipartUploadInput{
		Bucket:   aws.String(d.cfg.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return d.wrapError("abort_multipart", key, err)
	}
	return nil
}

func (d *Driver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	resp, err := d.client.ListParts(ctx, &s3client.ListPartsInput{
		Bucket:   aws.String(d.cfg.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return nil, d.wrapError("list_parts", key, err)
	}
	var parts []blobkit.CompletedPart
	for _, p := range resp.Parts {
		var num int32
		if p.PartNumber != nil {
			num = *p.PartNumber
		}
		var etag string
		if p.ETag != nil {
			etag = *p.ETag
		}
		var size int64
		if p.Size != nil {
			size = *p.Size
		}
		parts = append(parts, blobkit.CompletedPart{
			PartNumber: num,
			ETag:       etag,
			Size:       size,
		})
	}
	return parts, nil
}

func (d *Driver) Close() error {
	if d.cfg.GlobalMemoryLimiter != nil {
		d.cfg.GlobalMemoryLimiter.Close()
	}
	return nil
}

// wrapError translates raw AWS/Smithy SDK errors into domain sentinel errors and sanitized messages.
func (d *Driver) wrapError(op, key string, err error) error {
	if err == nil {
		return nil
	}

	// Check for specific AWS S3 error codes
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return blobkit.WrapError(op, key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	var nsb *types.NoSuchBucket
	if errors.As(err, &nsb) {
		return blobkit.WrapError(op, key, d.cfg.Name, blobkit.ErrBucketNotFound)
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return blobkit.WrapError(op, key, d.cfg.Name, blobkit.ErrObjectNotFound)
		case "NoSuchBucket":
			return blobkit.WrapError(op, key, d.cfg.Name, blobkit.ErrBucketNotFound)
		case "PreconditionFailed", "AtLeastOneConditionFailed":
			return blobkit.WrapError(op, key, d.cfg.Name, blobkit.ErrPreconditionFailed)
		case "SlowDown", "RequestTimeout", "ServiceUnavailable", "503", "500":
			return blobkit.WrapError(op, key, d.cfg.Name, blobkit.ErrProviderUnavailable)
		}
	}

	return blobkit.WrapError(op, key, d.cfg.Name, err)
}
