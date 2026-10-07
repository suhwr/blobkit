package webdav

import (
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// Driver implements blobkit.Driver for RFC 4918-compliant WebDAV servers.
type Driver struct {
	cfg        Config
	httpClient *http.Client

	collMu     sync.RWMutex
	knownColls map[string]bool
}

// NewDriver constructs and initializes a new WebDAV storage driver.
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
		knownColls: make(map[string]bool),
	}, nil
}

// Name returns the driver identifier.
func (d *Driver) Name() string {
	return d.cfg.Name
}

// Capabilities declares features natively supported by the WebDAV driver.
func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
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

const maxErrorBodyBytes = 64 * 1024

func readErrorBody(body io.Reader) []byte {
	if body == nil {
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes))
	return data
}

// Put uploads an object stream to the WebDAV server via HTTP PUT.
func (d *Driver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if r == nil {
		return nil, blobkit.ErrNilReader
	}
	if obj == nil {
		return nil, blobkit.ErrInvalidKey
	}
	if err := blobkit.ValidateKey(obj.Key); err != nil {
		return nil, err
	}

	// Ensure parent collections (directories) exist on WebDAV
	if err := d.ensureParentCollections(ctx, obj.Key); err != nil {
		return nil, err
	}

	endpoint := d.objectURL(obj.Key)

	resolvedR, effectiveSize, hasExplicitSize, err := blobkit.ResolvePayload(r, opts)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	sizeReader := blobkit.NewSizeReader(resolvedR, effectiveSize, hasExplicitSize)

	// Hash stream and count bytes while reading
	md5Hash := md5.New()
	shaHash := sha256.New()
	cw := &countWriter{w: io.MultiWriter(md5Hash, shaHash)}
	teeReader := io.TeeReader(sizeReader, cw)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, teeReader)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	d.authorize(req)

	contentType := obj.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(obj.Key))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	req.Header.Set("Content-Type", contentType)

	if effectiveSize >= 0 {
		req.ContentLength = effectiveSize
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		if sizeReader.Verify() != nil || (effectiveSize >= 0 && sizeReader.TotalRead() != effectiveSize) {
			delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = d.Delete(delCtx, obj.Key)
			cancel()
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, blobkit.ErrSizeMismatch)
		}
		return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, 0, err)
	}
	defer resp.Body.Close()

	if err := sizeReader.Verify(); err != nil {
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = d.Delete(delCtx, obj.Key)
		cancel()
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		readErrorBody(resp.Body)
		return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, resp.StatusCode, nil)
	}

	now := time.Now().UTC()
	etag := resp.Header.Get("ETag")
	if etag == "" {
		etag = fmt.Sprintf("\"%s\"", hex.EncodeToString(md5Hash.Sum(nil)))
	}
	shaHex := hex.EncodeToString(shaHash.Sum(nil))

	size := cw.n

	stored := *obj
	stored.Bucket = d.cfg.Endpoint
	stored.Size = size
	stored.ContentType = contentType
	stored.ETag = etag
	stored.Metadata = resolveMetadata(obj, opts)
	stored.ChecksumSHA256 = shaHex
	stored.UpdatedAt = now
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.Provider = d.cfg.Name
	stored.Status = blobkit.StateCommitted

	return &stored, nil
}

// Get retrieves an object stream and its metadata from the WebDAV server.
func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}

	endpoint := d.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	d.authorize(req)

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

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("get", key, d.cfg.Name, 0, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		readErrorBody(resp.Body)
		resp.Body.Close()
		return nil, wrapHTTPError("get", key, d.cfg.Name, resp.StatusCode, nil)
	}

	var size int64
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if s, err := strconv.ParseInt(cl, 10, 64); err == nil {
			size = s
		}
	}

	var lastMod time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		lastMod = parseWebDAVTime(lm)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(key))
	}

	obj := blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Endpoint,
		Size:        size,
		ContentType: contentType,
		ETag:        resp.Header.Get("ETag"),
		UpdatedAt:   lastMod,
		Provider:    d.cfg.Name,
		Status:      blobkit.StateCommitted,
	}

	return &blobkit.ObjectReader{
		Object: obj,
		Body:   resp.Body,
	}, nil
}

// Head inspects an object and returns its metadata using PROPFIND (Depth: 0) or HEAD.
func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}

	endpoint := d.objectURL(key)

	// First attempt standard HEAD request
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}
	d.authorize(req)

	resp, err := d.httpClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var size int64
			if cl := resp.Header.Get("Content-Length"); cl != "" {
				if s, err := strconv.ParseInt(cl, 10, 64); err == nil {
					size = s
				}
			}

			var lastMod time.Time
			if lm := resp.Header.Get("Last-Modified"); lm != "" {
				lastMod = parseWebDAVTime(lm)
			}

			contentType := resp.Header.Get("Content-Type")
			if contentType == "" {
				contentType = mime.TypeByExtension(filepath.Ext(key))
			}

			return &blobkit.Object{
				Key:         key,
				Bucket:      d.cfg.Endpoint,
				Size:        size,
				ContentType: contentType,
				ETag:        resp.Header.Get("ETag"),
				UpdatedAt:   lastMod,
				Provider:    d.cfg.Name,
				Status:      blobkit.StateCommitted,
			}, nil
		} else if resp.StatusCode == http.StatusNotFound {
			return nil, blobkit.WrapError("head", key, d.cfg.Name, blobkit.ErrObjectNotFound)
		}
	}

	// Fallback to PROPFIND with Depth: 0
	return d.propfindResource(ctx, key, 0)
}

// Delete removes an object from WebDAV permanently.
func (d *Driver) Delete(ctx context.Context, key string) error {
	if err := blobkit.ValidateKey(key); err != nil {
		return err
	}

	endpoint := d.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	d.authorize(req)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("delete", key, d.cfg.Name, 0, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		readErrorBody(resp.Body)
		return wrapHTTPError("delete", key, d.cfg.Name, resp.StatusCode, nil)
	}

	return nil
}

// DeleteBatch removes multiple keys concurrently using a bounded worker pool.
func (d *Driver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	for _, k := range keys {
		if err := blobkit.ValidateKey(k); err != nil {
			return nil, err
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

// Copy duplicates an object using WebDAV standard RFC 4918 COPY method.
func (d *Driver) Copy(ctx context.Context, srcKey, dstKey string) error {
	if err := blobkit.ValidateKey(srcKey); err != nil {
		return err
	}
	if err := blobkit.ValidateKey(dstKey); err != nil {
		return err
	}

	if err := d.ensureParentCollections(ctx, dstKey); err != nil {
		return err
	}

	srcURL := d.objectURL(srcKey)
	dstURL := d.objectURL(dstKey)

	req, err := http.NewRequestWithContext(ctx, "COPY", srcURL, nil)
	if err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}

	d.authorize(req)
	req.Header.Set("Destination", dstURL)
	req.Header.Set("Overwrite", "T")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("copy", srcKey, d.cfg.Name, 0, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		readErrorBody(resp.Body)
		return wrapHTTPError("copy", srcKey, d.cfg.Name, resp.StatusCode, nil)
	}

	return nil
}

// List queries resources under the given prefix using WebDAV PROPFIND.
func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	prefix := strings.Trim(opts.Prefix, "/")
	reqURL := d.objectURL(prefix)
	if prefix == "" {
		reqURL = d.cfg.Endpoint + "/"
	}

	propfindXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:getcontentlength/>
    <D:getcontenttype/>
    <D:getetag/>
    <D:getlastmodified/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>`

	req, err := http.NewRequestWithContext(ctx, "PROPFIND", reqURL, strings.NewReader(propfindXML))
	if err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}

	d.authorize(req)
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("list", opts.Prefix, d.cfg.Name, 0, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 207 && resp.StatusCode != http.StatusOK {
		readErrorBody(resp.Body)
		return nil, wrapHTTPError("list", opts.Prefix, d.cfg.Name, resp.StatusCode, nil)
	}

	var ms multistatusXML
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}

	basePath := "/"
	if u, err := url.Parse(d.cfg.Endpoint); err == nil {
		basePath = u.Path
	}
	basePath = strings.TrimRight(basePath, "/") + "/"

	var objects []blobkit.Object
	var commonPrefixes []string
	seenPrefixes := make(map[string]bool)

	for _, r := range ms.Responses {
		hrefPath := r.Href
		if u, err := url.Parse(r.Href); err == nil {
			hrefPath = u.Path
		}

		relKey := strings.TrimPrefix(hrefPath, basePath)
		relKey = strings.Trim(relKey, "/")

		if relKey == "" || relKey == prefix {
			continue // Skip root or self
		}

		var prop *propXML
		for _, ps := range r.Propstat {
			if strings.Contains(ps.Status, "200") {
				prop = &ps.Prop
				break
			}
		}

		if prop == nil {
			continue
		}

		// Handle directory collection
		if prop.isDir() {
			dirPrefix := relKey + "/"
			if opts.Prefix == "" || strings.HasPrefix(dirPrefix, opts.Prefix) {
				if opts.Delimiter != "" && !seenPrefixes[dirPrefix] {
					seenPrefixes[dirPrefix] = true
					commonPrefixes = append(commonPrefixes, dirPrefix)
				}
			}
			continue
		}

		// Prefix filtering
		if opts.Prefix != "" && !strings.HasPrefix(relKey, opts.Prefix) {
			continue
		}

		// Delimiter grouping
		if opts.Delimiter != "" {
			sub := strings.TrimPrefix(relKey, opts.Prefix)
			idx := strings.Index(sub, opts.Delimiter)
			if idx >= 0 {
				p := opts.Prefix + sub[:idx+len(opts.Delimiter)]
				if !seenPrefixes[p] {
					seenPrefixes[p] = true
					commonPrefixes = append(commonPrefixes, p)
				}
				continue
			}
		}

		objects = append(objects, blobkit.Object{
			Key:         relKey,
			Bucket:      d.cfg.Endpoint,
			Size:        prop.ContentLength,
			ContentType: prop.ContentType,
			ETag:        prop.ETag,
			UpdatedAt:   parseWebDAVTime(prop.LastModified),
			Provider:    d.cfg.Name,
			Status:      blobkit.StateCommitted,
		})
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})
	sort.Strings(commonPrefixes)

	// Apply Cursor pagination
	startIdx := 0
	if opts.Cursor != "" {
		for i, o := range objects {
			if o.Key > opts.Cursor {
				startIdx = i
				break
			}
		}
	}
	objects = objects[startIdx:]

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	isTruncated := false
	var nextCursor string
	if len(objects) > limit {
		isTruncated = true
		nextCursor = objects[limit-1].Key
		objects = objects[:limit]
	}

	return &blobkit.ListResult{
		Objects:        objects,
		NextCursor:     nextCursor,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}, nil
}

// PresignGet is unsupported directly by WebDAV protocol.
func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// PresignPut is unsupported directly by WebDAV protocol.
func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// ResolveURL builds a public access or endpoint URL for the given key.
func (d *Driver) ResolveURL(key string) (string, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return "", err
	}
	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.PublicBaseURL, "/"), strings.TrimLeft(key, "/")), nil
	}
	return d.objectURL(key), nil
}

// Close gracefully closes idle connections.
func (d *Driver) Close() error {
	d.collMu.Lock()
	d.knownColls = make(map[string]bool)
	d.collMu.Unlock()

	if d.httpClient != nil {
		if tr, ok := d.httpClient.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	return nil
}

// ensureParentCollections recursively creates parent WebDAV collections via MKCOL if missing.
func (d *Driver) ensureParentCollections(ctx context.Context, key string) error {
	dir := path.Dir(key)
	if dir == "." || dir == "/" || dir == "" {
		return nil
	}

	segments := strings.Split(strings.Trim(dir, "/"), "/")
	current := ""

	for _, seg := range segments {
		if current == "" {
			current = seg
		} else {
			current = current + "/" + seg
		}

		d.collMu.RLock()
		known := d.knownColls[current]
		d.collMu.RUnlock()

		if known {
			continue
		}

		collURL := d.objectURL(current)
		req, err := http.NewRequestWithContext(ctx, "MKCOL", collURL, nil)
		if err != nil {
			return blobkit.WrapError("mkcol", current, d.cfg.Name, err)
		}

		d.authorize(req)

		resp, err := d.httpClient.Do(req)
		if err != nil {
			return wrapHTTPError("mkcol", current, d.cfg.Name, 0, err)
		}
		resp.Body.Close()

		// 201 Created or 405 Method Not Allowed (already exists) or 301/200 indicate success
		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusOK {
			d.collMu.Lock()
			d.knownColls[current] = true
			d.collMu.Unlock()
		} else {
			return wrapHTTPError("mkcol", current, d.cfg.Name, resp.StatusCode, nil)
		}
	}

	return nil
}

// propfindResource queries metadata for a single resource with specified depth.
func (d *Driver) propfindResource(ctx context.Context, key string, depth int) (*blobkit.Object, error) {
	endpoint := d.objectURL(key)
	propfindXML := `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:getcontentlength/>
    <D:getcontenttype/>
    <D:getetag/>
    <D:getlastmodified/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>`

	req, err := http.NewRequestWithContext(ctx, "PROPFIND", endpoint, strings.NewReader(propfindXML))
	if err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	d.authorize(req)
	req.Header.Set("Depth", strconv.Itoa(depth))
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("head", key, d.cfg.Name, 0, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 207 && resp.StatusCode != http.StatusOK {
		readErrorBody(resp.Body)
		return nil, wrapHTTPError("head", key, d.cfg.Name, resp.StatusCode, nil)
	}

	var ms multistatusXML
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	if len(ms.Responses) == 0 {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	var prop *propXML
	for _, ps := range ms.Responses[0].Propstat {
		if strings.Contains(ps.Status, "200") {
			prop = &ps.Prop
			break
		}
	}

	if prop == nil || prop.isDir() {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	contentType := prop.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(key))
	}

	return &blobkit.Object{
		Key:         key,
		Bucket:      d.cfg.Endpoint,
		Size:        prop.ContentLength,
		ContentType: contentType,
		ETag:        prop.ETag,
		UpdatedAt:   parseWebDAVTime(prop.LastModified),
		Provider:    d.cfg.Name,
		Status:      blobkit.StateCommitted,
	}, nil
}

// objectURL constructs the complete URL for a given object key.
func (d *Driver) objectURL(key string) string {
	cleanKey := path.Clean("/" + strings.TrimSpace(key))
	cleanKey = strings.TrimPrefix(cleanKey, "/")
	parts := strings.Split(cleanKey, "/")
	escapedParts := make([]string, len(parts))
	for i, p := range parts {
		escapedParts[i] = url.PathEscape(p)
	}
	return fmt.Sprintf("%s/%s", d.cfg.Endpoint, strings.Join(escapedParts, "/"))
}

// authorize adds HTTP Basic or Bearer authentication headers to the request.
func (d *Driver) authorize(req *http.Request) {
	if d.cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+d.cfg.BearerToken)
		return
	}
	if d.cfg.Username != "" || d.cfg.Password != "" {
		req.SetBasicAuth(d.cfg.Username, d.cfg.Password)
	}
}

// CreateMultipart, UploadPart, CompleteMultipart, AbortMultipart, ListParts
// WebDAV doesn't support S3 multipart natively; stub to return unsupported operation.
func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	if obj == nil {
		return "", blobkit.ErrInvalidKey
	}
	if err := blobkit.ValidateKey(obj.Key); err != nil {
		return "", err
	}
	return "", blobkit.WrapError("create_multipart", obj.Key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

func (d *Driver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return "", err
	}
	return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

func (d *Driver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	if obj == nil {
		return nil, blobkit.ErrInvalidKey
	}
	if err := blobkit.ValidateKey(obj.Key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

func (d *Driver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	if err := blobkit.ValidateKey(key); err != nil {
		return err
	}
	return blobkit.WrapError("abort_multipart", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

func (d *Driver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// countWriter tracks total bytes written through it.
type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
