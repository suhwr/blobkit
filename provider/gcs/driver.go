package gcs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// Ensure Driver implements blobkit.Driver at compile time.
var _ blobkit.Driver = (*Driver)(nil)

// Driver implements the blobkit.Driver interface for Google Cloud Storage using standard JSON APIs.
type Driver struct {
	cfg        Config
	httpClient *http.Client
	sessionsMu sync.RWMutex
	sessions   map[string]*multipartSessionState
}

// NewDriver constructs and validates a new Google Cloud Storage driver.
func NewDriver(cfg Config) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Driver{
		cfg:        cfg,
		httpClient: cfg.HTTPClient,
		sessions:   make(map[string]*multipartSessionState),
	}, nil
}

// Name returns the driver identifier.
func (d *Driver) Name() string {
	return d.cfg.Name
}

// Capabilities returns the feature bitmask supported by this driver.
func (d *Driver) Capabilities() blobkit.Capability {
	caps := blobkit.CapDirectPut |
		blobkit.CapMultipartPut |
		blobkit.CapByteRangeGet |
		blobkit.CapCopy |
		blobkit.CapBatchDelete |
		blobkit.CapMultipartSession

	// V4 Signed URLs are supported when RSA signing key is provided
	if d.cfg.parsedPrivateKey != nil || d.cfg.SignBytesFunc != nil {
		caps |= blobkit.CapPresignGet | blobkit.CapPresignPut
	}

	return caps
}

func resolveMetadata(obj *blobkit.Object, opts blobkit.PutOptions) map[string]string {
	if len(opts.Metadata) > 0 {
		return opts.Metadata
	}
	if obj != nil && len(obj.Metadata) > 0 {
		return obj.Metadata
	}
	return nil
}

func validateKey(key string) error {
	return blobkit.ValidateKey(key)
}

func readErrorBody(body io.Reader) []byte {
	if body == nil {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(body, 64*1024))
	return b
}

// Put uploads an object stream directly to Google Cloud Storage.
func (d *Driver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("put", "", d.cfg.Name, err)
	}
	if r == nil {
		return nil, blobkit.ErrNilReader
	}
	if obj == nil {
		return nil, blobkit.ErrInvalidKey
	}
	if err := validateKey(obj.Key); err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	cleanKey := obj.Key
	contentType := obj.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	meta := resolveMetadata(obj, opts)

	payloadReader, payloadSize, isExact, err := blobkit.ResolvePayload(r, opts)
	if err != nil {
		return nil, blobkit.WrapError("put", cleanKey, d.cfg.Name, err)
	}

	// If size is unknown (streaming), exceeds MultipartThreshold, or has custom metadata,
	// use GCS Resumable Upload protocol to keep memory bounded to ChunkSize.
	if (!isExact && payloadSize <= 0) || (payloadSize > d.cfg.MultipartThreshold) || (len(meta) > 0 && payloadSize > 0) {
		return d.uploadStreamResumable(ctx, obj, payloadReader, contentType, meta, opts)
	}

	// Single-shot direct streaming upload without full in-memory buffering (supports 0-byte uploads)
	endpoint := fmt.Sprintf("%s/b/%s/o?uploadType=media&name=%s",
		d.cfg.UploadAPIBaseURL,
		url.PathEscape(d.cfg.Bucket),
		url.QueryEscape(cleanKey),
	)

	h := sha256.New()
	var bodyReader io.Reader = payloadReader
	tee := io.TeeReader(bodyReader, h)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, tee)
	if err != nil {
		return nil, blobkit.WrapError("put", cleanKey, d.cfg.Name, err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Content-Length", strconv.FormatInt(payloadSize, 10))

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		if sr, ok := payloadReader.(*blobkit.SizeReader); ok && sr != nil {
			if sr.Verify() != nil || sr.TotalRead() != payloadSize {
				delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_ = d.Delete(delCtx, cleanKey)
				cancel()
				return nil, blobkit.WrapError("put", cleanKey, d.cfg.Name, blobkit.ErrSizeMismatch)
			}
		}
		return nil, parseGCSError("put", cleanKey, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	bodyBytes := readErrorBody(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, parseGCSError("put", cleanKey, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	var res gcsObjectResource
	if err := json.Unmarshal(bodyBytes, &res); err != nil {
		return nil, blobkit.WrapError("put", cleanKey, d.cfg.Name, err)
	}

	if sr, ok := payloadReader.(*blobkit.SizeReader); ok {
		if verifyErr := sr.Verify(); verifyErr != nil {
			delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = d.Delete(delCtx, cleanKey)
			cancel()
			return nil, blobkit.WrapError("put", cleanKey, d.cfg.Name, verifyErr)
		}
	}

	result := d.mapObjectResource(cleanKey, &res)
	result.ChecksumSHA256 = hex.EncodeToString(h.Sum(nil))
	result.Metadata = meta
	result.Provider = d.cfg.Name
	result.Status = blobkit.StateCommitted

	return result, nil
}

// uploadStreamResumable streams an io.Reader into GCS via Resumable Upload protocol in chunks of ChunkSize.
func (d *Driver) uploadStreamResumable(ctx context.Context, obj *blobkit.Object, r io.Reader, contentType string, meta map[string]string, opts blobkit.PutOptions) (*blobkit.Object, error) {
	targetObj := *obj
	targetObj.ContentType = contentType
	targetObj.Metadata = meta

	putOpts := opts
	putOpts.Metadata = meta

	uploadID, err := d.CreateMultipart(ctx, &targetObj, putOpts)
	if err != nil {
		return nil, err
	}

	chunkSize := d.cfg.ChunkSize
	buf := make([]byte, chunkSize)
	var parts []blobkit.CompletedPart
	var partNum int32 = 1

	hSHA := sha256.New()

	for {
		if err := ctx.Err(); err != nil {
			_ = d.AbortMultipart(context.Background(), obj.Key, uploadID)
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
		}

		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
			hSHA.Write(chunk)
			etag, partErr := d.UploadPart(ctx, obj.Key, uploadID, partNum, bytes.NewReader(chunk), int64(n))
			if partErr != nil {
				_ = d.AbortMultipart(context.Background(), obj.Key, uploadID)
				return nil, partErr
			}

			parts = append(parts, blobkit.CompletedPart{
				PartNumber: partNum,
				ETag:       etag,
				Size:       int64(n),
			})
			partNum++
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			_ = d.AbortMultipart(context.Background(), obj.Key, uploadID)
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, readErr)
		}
	}

	res, err := d.CompleteMultipart(ctx, &targetObj, uploadID, parts)
	if err != nil {
		_ = d.AbortMultipart(context.Background(), obj.Key, uploadID)
		return nil, err
	}

	res.ChecksumSHA256 = hex.EncodeToString(hSHA.Sum(nil))
	res.Metadata = meta
	return res, nil
}

// Get retrieves an object stream and its metadata from Google Cloud Storage.
func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}
	endpoint := fmt.Sprintf("%s/b/%s/o/%s?alt=media",
		d.cfg.StorageAPIBaseURL,
		url.PathEscape(d.cfg.Bucket),
		url.PathEscape(key),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	if opts.Range != "" {
		req.Header.Set("Range", opts.Range)
	}
	if opts.IfMatch != "" {
		req.Header.Set("If-Match", opts.IfMatch)
	}
	if opts.IfNoneMatch != "" {
		req.Header.Set("If-None-Match", opts.IfNoneMatch)
	}
	if opts.IfModifiedSince != nil {
		req.Header.Set("If-Modified-Since", opts.IfModifiedSince.UTC().Format(http.TimeFormat))
	}
	if opts.IfUnmodifiedSince != nil {
		req.Header.Set("If-Unmodified-Since", opts.IfUnmodifiedSince.UTC().Format(http.TimeFormat))
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, parseGCSError("get", key, d.cfg.Name, 0, nil, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		bodyBytes := readErrorBody(resp.Body)
		resp.Body.Close()
		return nil, parseGCSError("get", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	var size int64
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if s, err := strconv.ParseInt(cl, 10, 64); err == nil {
			size = s
		}
	}

	var lastMod time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := time.Parse(http.TimeFormat, lm); err == nil {
			lastMod = t.UTC()
		}
	}

	meta := make(map[string]string)
	for k, v := range resp.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-goog-meta-") {
			metaName := strings.TrimPrefix(lower, "x-goog-meta-")
			meta[metaName] = strings.Join(v, ",")
		}
	}

	obj := blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Bucket,
		Size:        size,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        resp.Header.Get("ETag"),
		Metadata:    meta,
		UpdatedAt:   lastMod,
		Provider:    d.cfg.Name,
		Status:      blobkit.StateCommitted,
	}

	return &blobkit.ObjectReader{
		Object: obj,
		Body:   resp.Body,
	}, nil
}

// Head inspects a GCS object and returns its metadata without downloading the payload body.
func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}
	endpoint := fmt.Sprintf("%s/b/%s/o/%s",
		d.cfg.StorageAPIBaseURL,
		url.PathEscape(d.cfg.Bucket),
		url.PathEscape(key),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, parseGCSError("head", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	bodyBytes := readErrorBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, parseGCSError("head", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	var res gcsObjectResource
	if err := json.Unmarshal(bodyBytes, &res); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	obj := d.mapObjectResource(key, &res)
	return obj, nil
}

// Delete permanently removes an object from Google Cloud Storage.
func (d *Driver) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}
	endpoint := fmt.Sprintf("%s/b/%s/o/%s",
		d.cfg.StorageAPIBaseURL,
		url.PathEscape(d.cfg.Bucket),
		url.PathEscape(key),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return parseGCSError("delete", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		bodyBytes := readErrorBody(resp.Body)
		return parseGCSError("delete", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	return nil
}

// DeleteBatch removes multiple keys concurrently using a bounded worker pool.
func (d *Driver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("delete_batch", "", d.cfg.Name, err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	for _, k := range keys {
		if err := validateKey(k); err != nil {
			return nil, blobkit.WrapError("delete_batch", k, d.cfg.Name, err)
		}
	}

	type delResult struct {
		key string
		err error
	}

	concurrency := 16
	if len(keys) < concurrency {
		concurrency = len(keys)
	}

	keyChan := make(chan string, len(keys))
	for _, k := range keys {
		keyChan <- k
	}
	close(keyChan)

	resChan := make(chan delResult, len(keys))
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keyChan {
				err := d.Delete(ctx, k)
				resChan <- delResult{key: k, err: err}
			}
		}()
	}

	wg.Wait()
	close(resChan)

	var deleted []string
	var errMsgs []string

	for res := range resChan {
		if res.err == nil {
			deleted = append(deleted, res.key)
		} else {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", res.key, res.err))
		}
	}

	if len(errMsgs) > 0 {
		return deleted, blobkit.WrapError("delete_batch", "", d.cfg.Name, fmt.Errorf("errors during batch delete: %s", strings.Join(errMsgs, "; ")))
	}

	return deleted, nil
}

// Copy duplicates an object server-side using Google Cloud Storage rewrite API.
func (d *Driver) Copy(ctx context.Context, srcKey, dstKey string) error {
	if err := ctx.Err(); err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}
	if err := validateKey(srcKey); err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}
	if err := validateKey(dstKey); err != nil {
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}
	rewriteToken := ""

	for {
		endpoint := fmt.Sprintf("%s/b/%s/o/%s/rewriteTo/b/%s/o/%s",
			d.cfg.StorageAPIBaseURL,
			url.PathEscape(d.cfg.Bucket),
			url.PathEscape(srcKey),
			url.PathEscape(d.cfg.Bucket),
			url.PathEscape(dstKey),
		)

		if rewriteToken != "" {
			endpoint += fmt.Sprintf("?rewriteToken=%s", url.QueryEscape(rewriteToken))
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
		}

		if err := d.authorizeRequest(ctx, req); err != nil {
			return err
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			return parseGCSError("copy", srcKey, d.cfg.Name, 0, nil, err)
		}

		bodyBytes := readErrorBody(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return parseGCSError("copy", srcKey, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
		}

		type rewriteResponse struct {
			Done         bool   `json:"done"`
			RewriteToken string `json:"rewriteToken"`
		}

		var rw rewriteResponse
		if err := json.Unmarshal(bodyBytes, &rw); err != nil {
			return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
		}

		if rw.Done {
			return nil
		}
		rewriteToken = rw.RewriteToken
	}
}

// List queries objects in the GCS bucket matching prefix and pagination options.
func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("list", "", d.cfg.Name, err)
	}
	query := make(url.Values)
	if opts.Prefix != "" {
		query.Set("prefix", opts.Prefix)
	}
	if opts.Delimiter != "" {
		query.Set("delimiter", opts.Delimiter)
	}
	if opts.Cursor != "" {
		query.Set("pageToken", opts.Cursor)
	}
	if opts.Limit > 0 {
		query.Set("maxResults", strconv.Itoa(opts.Limit))
	}

	endpoint := fmt.Sprintf("%s/b/%s/o?%s",
		d.cfg.StorageAPIBaseURL,
		url.PathEscape(d.cfg.Bucket),
		query.Encode(),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("list", "", d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, parseGCSError("list", "", d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	bodyBytes := readErrorBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, parseGCSError("list", "", d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	type listResponse struct {
		NextPageToken string              `json:"nextPageToken"`
		Prefixes      []string            `json:"prefixes"`
		Items         []gcsObjectResource `json:"items"`
	}

	var lr listResponse
	if err := json.Unmarshal(bodyBytes, &lr); err != nil {
		return nil, blobkit.WrapError("list", "", d.cfg.Name, err)
	}

	var objects []blobkit.Object
	for _, it := range lr.Items {
		obj := d.mapObjectResource(it.Name, &it)
		objects = append(objects, *obj)
	}

	return &blobkit.ListResult{
		Objects:        objects,
		CommonPrefixes: lr.Prefixes,
		NextCursor:     lr.NextPageToken,
		IsTruncated:    lr.NextPageToken != "",
	}, nil
}

// PresignGet creates a temporary V4 Signed URL for downloading an object.
func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, err)
	}
	return d.buildV4SignedURL(ctx, http.MethodGet, key, opts)
}

// PresignPut creates a temporary V4 Signed URL for uploading an object.
func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, err)
	}
	return d.buildV4SignedURL(ctx, http.MethodPut, key, opts)
}

// ResolveURL builds a public access or CDN URL for the given key.
func (d *Driver) ResolveURL(key string) (string, error) {
	escapedKey := blobkit.EscapeURLPath(key)
	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.PublicBaseURL, "/"), escapedKey), nil
	}
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", d.cfg.Bucket, escapedKey), nil
}

// Close gracefully terminates open HTTP idle connections and clears session state.
func (d *Driver) Close() error {
	d.sessionsMu.Lock()
	d.sessions = make(map[string]*multipartSessionState)
	d.sessionsMu.Unlock()

	if d.httpClient != nil {
		if tr, ok := d.httpClient.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	return nil
}
