package azure

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// Driver implements blobkit.Driver for Azure Blob Storage.
type Driver struct {
	cfg        Config
	httpClient *http.Client

	sessionsMu sync.RWMutex
	sessions   map[string]string // uploadID -> key
}

// NewDriver constructs and initializes a new Azure Blob Storage driver.
func NewDriver(cfg Config) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0, // Streaming relies on context timeouts
		}
	}

	return &Driver{
		cfg:        cfg,
		httpClient: httpClient,
		sessions:   make(map[string]string),
	}, nil
}

// Name returns the driver identifier.
func (d *Driver) Name() string {
	return d.cfg.Name
}

// Capabilities declares features supported by Azure Blob Storage.
func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
		blobkit.CapMultipartPut |
		blobkit.CapMultipartSession |
		blobkit.CapPresignGet |
		blobkit.CapPresignPut |
		blobkit.CapByteRangeGet |
		blobkit.CapCopy |
		blobkit.CapBatchDelete
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

// Put uploads an object stream as an Azure Block Blob.
// For objects within MultipartThreshold with known size (including 0-byte objects), it streams directly using single Put Blob.
// For objects exceeding MultipartThreshold or unknown/streaming sizes, it stages chunked blocks and commits them.
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

	contentType := obj.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(obj.Key))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	meta := resolveMetadata(obj, opts)

	payloadReader, payloadSize, isExact, err := blobkit.ResolvePayload(r, opts)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	// If size is unknown (streaming) or exceeds MultipartThreshold, use chunked block staging
	if (!isExact && payloadSize <= 0) || (payloadSize > d.cfg.MultipartThreshold) {
		return d.uploadStreamMultipart(ctx, obj, payloadReader, contentType, meta, opts)
	}

	// Single-shot direct streaming upload without whole-body buffering
	urlStr := d.blobURL(obj.Key)
	var bodyReader io.Reader = payloadReader

	hMD5 := md5.New()
	hSHA := sha256.New()
	tee := io.TeeReader(bodyReader, io.MultiWriter(hMD5, hSHA))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, urlStr, tee)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	req.ContentLength = payloadSize
	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-ms-blob-content-type", contentType)

	for k, v := range meta {
		req.Header.Set(fmt.Sprintf("x-ms-meta-%s", k), v)
	}

	if err := d.authorize(req); err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes := readErrorBody(resp.Body)
		return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	if sr, ok := payloadReader.(*blobkit.SizeReader); ok {
		if err := sr.Verify(); err != nil {
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
		}
	}

	now := time.Now().UTC()
	etag := resp.Header.Get("ETag")
	if etag == "" {
		etag = fmt.Sprintf("\"%s\"", hex.EncodeToString(hMD5.Sum(nil)))
	}
	shaHex := hex.EncodeToString(hSHA.Sum(nil))

	stored := *obj
	stored.Bucket = d.cfg.Container
	stored.Size = opts.Size
	stored.ContentType = contentType
	stored.ETag = etag
	stored.Metadata = meta
	stored.ChecksumSHA256 = shaHex
	stored.UpdatedAt = now
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.Provider = d.cfg.Name
	stored.Status = blobkit.StateCommitted

	return &stored, nil
}

// uploadStreamMultipart streams an io.Reader of arbitrary/unknown size into staged blocks and commits them.
func (d *Driver) uploadStreamMultipart(ctx context.Context, obj *blobkit.Object, r io.Reader, contentType string, meta map[string]string, opts blobkit.PutOptions) (*blobkit.Object, error) {
	uploadID, err := d.CreateMultipart(ctx, obj, opts)
	if err != nil {
		return nil, err
	}

	partSize := d.cfg.PartSize
	buf := make([]byte, partSize)
	var parts []blobkit.CompletedPart
	var partNum int32 = 1
	var totalSize int64

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
			totalSize += int64(n)
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

	targetObj := *obj
	targetObj.ContentType = contentType
	targetObj.Metadata = meta

	res, err := d.CompleteMultipart(ctx, &targetObj, uploadID, parts)
	if err != nil {
		_ = d.AbortMultipart(context.Background(), obj.Key, uploadID)
		return nil, err
	}

	res.ChecksumSHA256 = hex.EncodeToString(hSHA.Sum(nil))
	res.Metadata = meta
	return res, nil
}

// Get retrieves an object stream and its metadata from Azure Blob Storage.
func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}
	urlStr := d.blobURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
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

	if err := d.authorize(req); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("get", key, d.cfg.Name, 0, nil, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		bodyBytes := readErrorBody(resp.Body)
		resp.Body.Close()
		return nil, wrapHTTPError("get", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
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
		if strings.HasPrefix(lower, "x-ms-meta-") {
			metaName := strings.TrimPrefix(lower, "x-ms-meta-")
			meta[metaName] = strings.Join(v, ",")
		}
	}

	obj := blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Container,
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

// Head inspects a blob and returns its metadata without downloading the body.
func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}
	urlStr := d.blobURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, urlStr, nil)
	if err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	if err := d.authorize(req); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("head", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, wrapHTTPError("head", key, d.cfg.Name, resp.StatusCode, nil, nil)
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
		if strings.HasPrefix(lower, "x-ms-meta-") {
			metaName := strings.TrimPrefix(lower, "x-ms-meta-")
			meta[metaName] = strings.Join(v, ",")
		}
	}

	return &blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Container,
		Size:        size,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        resp.Header.Get("ETag"),
		Metadata:    meta,
		UpdatedAt:   lastMod,
		Provider:    d.cfg.Name,
		Status:      blobkit.StateCommitted,
	}, nil
}

// Delete removes a blob from Azure Blob Storage.
func (d *Driver) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}
	urlStr := d.blobURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, urlStr, nil)
	if err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	if err := d.authorize(req); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("delete", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		bodyBytes := readErrorBody(resp.Body)
		return wrapHTTPError("delete", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
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
		return deleted, blobkit.WrapError("delete_batch", "", d.cfg.Name, fmt.Errorf("batch delete errors: %s", strings.Join(errMsgs, "; ")))
	}

	return deleted, nil
}

// Copy duplicates a blob within Azure Storage via x-ms-copy-source.
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
	srcURL := d.blobURL(srcKey)
	dstURL := d.blobURL(dstKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, dstURL, nil)
	if err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}

	req.Header.Set("x-ms-copy-source", srcURL)

	if err := d.authorize(req); err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("copy", srcKey, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		bodyBytes := readErrorBody(resp.Body)
		return wrapHTTPError("copy", srcKey, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	return nil
}

// List queries blobs in the container matching criteria (GET /container?restype=container&comp=list).
func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}
	params := url.Values{}
	params.Set("restype", "container")
	params.Set("comp", "list")

	if opts.Prefix != "" {
		params.Set("prefix", opts.Prefix)
	}
	if opts.Delimiter != "" {
		params.Set("delimiter", opts.Delimiter)
	}
	if opts.Cursor != "" {
		params.Set("marker", opts.Cursor)
	}

	limit := opts.Limit
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	params.Set("maxresults", strconv.Itoa(limit))

	urlStr := fmt.Sprintf("%s/%s?%s", d.cfg.EndpointURL(), d.cfg.Container, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}

	if err := d.authorize(req); err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("list", opts.Prefix, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	bodyBytes := readErrorBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, wrapHTTPError("list", opts.Prefix, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	type azureListResponse struct {
		XMLName    xml.Name `xml:"EnumerationResults"`
		Prefix     string   `xml:"Prefix"`
		Marker     string   `xml:"Marker"`
		NextMarker string   `xml:"NextMarker"`
		Blobs      struct {
			Blob []struct {
				Name       string `xml:"Name"`
				Properties struct {
					LastModified  string `xml:"Last-Modified"`
					ETag          string `xml:"Etag"`
					ContentLength int64  `xml:"Content-Length"`
					ContentType   string `xml:"Content-Type"`
				} `xml:"Properties"`
			} `xml:"Blob"`
			BlobPrefix []struct {
				Name string `xml:"Name"`
			} `xml:"BlobPrefix"`
		} `xml:"Blobs"`
	}

	var lr azureListResponse
	if err := xml.Unmarshal(bodyBytes, &lr); err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}

	var objects []blobkit.Object
	for _, b := range lr.Blobs.Blob {
		var lastMod time.Time
		if t, err := time.Parse(http.TimeFormat, b.Properties.LastModified); err == nil {
			lastMod = t.UTC()
		}

		objects = append(objects, blobkit.Object{
			Key:         b.Name,
			Bucket:      d.cfg.Container,
			Size:        b.Properties.ContentLength,
			ContentType: b.Properties.ContentType,
			ETag:        b.Properties.ETag,
			UpdatedAt:   lastMod,
			Provider:    d.cfg.Name,
			Status:      blobkit.StateCommitted,
		})
	}

	var commonPrefixes []string
	for _, bp := range lr.Blobs.BlobPrefix {
		commonPrefixes = append(commonPrefixes, bp.Name)
	}

	return &blobkit.ListResult{
		Objects:        objects,
		NextCursor:     lr.NextMarker,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    lr.NextMarker != "",
	}, nil
}

// PresignGet generates a time-limited SAS download URL (sp=r).
func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, err)
	}
	if d.cfg.AccountKey == "" {
		return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, fmt.Errorf("AccountKey required for SAS generation"))
	}

	expiryDuration := opts.Expiry
	if expiryDuration <= 0 {
		expiryDuration = 15 * time.Minute
	}
	expiry := time.Now().Add(expiryDuration)

	sas, err := generateBlobSAS(d.cfg.AccountName, d.cfg.AccountKey, d.cfg.Container, key, "r", d.cfg.APIVersion, expiry)
	if err != nil {
		return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, err)
	}

	fullURL := fmt.Sprintf("%s?%s", d.blobURL(key), sas)
	return &blobkit.PresignedURL{
		URL:       fullURL,
		Method:    http.MethodGet,
		ExpiresAt: expiry,
	}, nil
}

// PresignPut generates a time-limited SAS upload URL (sp=w) with x-ms-blob-type: BlockBlob.
func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, err)
	}
	if d.cfg.AccountKey == "" {
		return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, fmt.Errorf("AccountKey required for SAS generation"))
	}

	expiryDuration := opts.Expiry
	if expiryDuration <= 0 {
		expiryDuration = 15 * time.Minute
	}
	expiry := time.Now().Add(expiryDuration)

	sas, err := generateBlobSAS(d.cfg.AccountName, d.cfg.AccountKey, d.cfg.Container, key, "w", d.cfg.APIVersion, expiry)
	if err != nil {
		return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, err)
	}

	fullURL := fmt.Sprintf("%s?%s", d.blobURL(key), sas)
	headers := map[string]string{
		"x-ms-blob-type": "BlockBlob",
	}

	return &blobkit.PresignedURL{
		URL:           fullURL,
		Method:        http.MethodPut,
		SignedHeaders: headers,
		ExpiresAt:     expiry,
	}, nil
}

// ResolveURL builds a public access or CDN URL for the given key.
func (d *Driver) ResolveURL(key string) (string, error) {
	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.PublicBaseURL, "/"), strings.TrimLeft(key, "/")), nil
	}
	return d.blobURL(key), nil
}

// Close gracefully closes idle connections and clears sessions.
func (d *Driver) Close() error {
	d.sessionsMu.Lock()
	d.sessions = make(map[string]string)
	d.sessionsMu.Unlock()

	if d.httpClient != nil {
		if tr, ok := d.httpClient.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	return nil
}

// blobURL returns the fully qualified URL for a blob resource.
func (d *Driver) blobURL(key string) string {
	cleanKey := path.Clean("/" + strings.TrimSpace(key))
	cleanKey = strings.TrimPrefix(cleanKey, "/")
	parts := strings.Split(cleanKey, "/")
	escapedParts := make([]string, len(parts))
	for i, p := range parts {
		escapedParts[i] = url.PathEscape(p)
	}
	return fmt.Sprintf("%s/%s/%s", d.cfg.EndpointURL(), d.cfg.Container, strings.Join(escapedParts, "/"))
}

// authorize adds SharedKey authorization or attaches SAS token query parameters.
func (d *Driver) authorize(req *http.Request) error {
	if d.cfg.SASToken != "" {
		q := req.URL.Query()
		sasParams, _ := url.ParseQuery(d.cfg.SASToken)
		for k, v := range sasParams {
			for _, val := range v {
				q.Add(k, val)
			}
		}
		req.URL.RawQuery = q.Encode()
		return nil
	}

	if d.cfg.AccountKey != "" {
		return signRequest(req, d.cfg.AccountName, d.cfg.AccountKey, d.cfg.APIVersion)
	}

	return nil
}
