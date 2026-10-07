package gdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// Driver implements blobkit.Driver for Google Drive REST API v3.
type Driver struct {
	cfg        Config
	httpClient *http.Client
	cache      *KeyCache

	sessionsMu sync.RWMutex
	sessions   map[string]*multipartSessionState
}

// NewDriver constructs and initializes a new Google Drive storage driver.
func NewDriver(cfg Config) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0, // Streaming uploads/downloads rely on context timeouts rather than client timeouts
		}
	}

	d := &Driver{
		cfg:        cfg,
		httpClient: httpClient,
		cache:      NewKeyCache(cfg.KeyCacheCapacity),
		sessions:   make(map[string]*multipartSessionState),
	}

	return d, nil
}

// Name returns the driver identifier.
func (d *Driver) Name() string {
	return d.cfg.Name
}

// Capabilities declares features natively supported by Google Drive.
func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
		blobkit.CapMultipartPut |
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

// Put uploads an object stream to Google Drive, preserving S3-compatible overwrite semantics.
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

	resolvedR, effectiveSize, hasExplicitSize, err := blobkit.ResolvePayload(r, opts)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	sizeReader := blobkit.NewSizeReader(resolvedR, effectiveSize, hasExplicitSize)

	// Check if a file with the same key already exists to perform in-place overwrite
	existingFileID, _ := d.lookupFileID(ctx, obj.Key)

	optsCopy := opts
	if effectiveSize >= 0 {
		optsCopy.Size = effectiveSize
		optsCopy.ExplicitSize = true
	} else {
		optsCopy.Size = blobkit.SizeUnknown
	}

	return d.uploadStreamResumable(ctx, obj, sizeReader, optsCopy, existingFileID)
}

// Get retrieves an object stream and its metadata from Google Drive.
func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}

	fileID, err := d.resolveFileID(ctx, key)
	if err != nil {
		return nil, err
	}

	// Fetch metadata first to populate the returned Object
	metaObj, err := d.Head(ctx, key)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("alt", "media")
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
	}

	endpoint := fmt.Sprintf("%s/files/%s?%s", d.cfg.DriveAPIBaseURL, url.PathEscape(fileID), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	// Handle conditional headers and Byte-Range requests
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
		return nil, wrapHTTPError("get", key, d.cfg.Name, 0, nil, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		bodyBytes := readErrorBody(resp.Body)
		resp.Body.Close()
		return nil, wrapHTTPError("get", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	// If partial content was returned, adjust object size based on Content-Length header
	retObj := *metaObj
	if resp.StatusCode == http.StatusPartialContent {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if l, err := strconv.ParseInt(cl, 10, 64); err == nil {
				retObj.Size = l
			}
		}
	}

	return &blobkit.ObjectReader{
		Object: retObj,
		Body:   resp.Body,
	}, nil
}

// Head inspects an object and returns its metadata without downloading the body.
func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}

	fileID, err := d.resolveFileID(ctx, key)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("fields", "id,name,size,mimeType,md5Checksum,modifiedTime,createdTime,appProperties,trashed")
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
	}

	endpoint := fmt.Sprintf("%s/files/%s?%s", d.cfg.DriveAPIBaseURL, url.PathEscape(fileID), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("head", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes := readErrorBody(resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			d.cache.Delete(key)
		}
		return nil, wrapHTTPError("head", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var fileResp driveFileResponse
	if err := json.Unmarshal(bodyBytes, &fileResp); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	if fileResp.Trashed {
		d.cache.Delete(key)
		return nil, blobkit.WrapError("head", key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	return d.mapDriveFileToObject(key, &fileResp), nil
}

// Delete removes an object from Google Drive permanently.
func (d *Driver) Delete(ctx context.Context, key string) error {
	if err := blobkit.ValidateKey(key); err != nil {
		return err
	}

	fileID, err := d.resolveFileID(ctx, key)
	if err != nil {
		if errors.Is(err, blobkit.ErrObjectNotFound) {
			return nil // Idempotent deletion
		}
		return err
	}

	params := url.Values{}
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
	}

	endpoint := fmt.Sprintf("%s/files/%s?%s", d.cfg.DriveAPIBaseURL, url.PathEscape(fileID), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("delete", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	d.cache.Delete(key)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		bodyBytes := readErrorBody(resp.Body)
		return wrapHTTPError("delete", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
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

	type deleteResult struct {
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

	resChan := make(chan deleteResult, len(keys))
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keyChan {
				err := d.Delete(ctx, k)
				resChan <- deleteResult{key: k, err: err}
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

// Copy duplicates an object within Google Drive and updates the destination key.
func (d *Driver) Copy(ctx context.Context, srcKey, dstKey string) error {
	if err := blobkit.ValidateKey(srcKey); err != nil {
		return err
	}
	if err := blobkit.ValidateKey(dstKey); err != nil {
		return err
	}

	srcFileID, err := d.resolveFileID(ctx, srcKey)
	if err != nil {
		return err
	}

	metadata := map[string]interface{}{
		"name":    path.Base(dstKey),
		"parents": []string{d.cfg.FolderID},
		"appProperties": map[string]string{
			"blobkit_key": dstKey,
		},
	}

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}

	params := url.Values{}
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
	}

	endpoint := fmt.Sprintf("%s/files/%s/copy?%s", d.cfg.DriveAPIBaseURL, url.PathEscape(srcFileID), params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(metaBytes))
	if err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("copy", srcKey, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes := readErrorBody(resp.Body)
		return wrapHTTPError("copy", srcKey, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var fileResp driveFileResponse
	if err := json.Unmarshal(bodyBytes, &fileResp); err == nil {
		d.cache.Set(dstKey, fileResp.ID)
	}

	return nil
}

// List lists objects in Google Drive matching the provided criteria.
func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	params := url.Values{}
	query := fmt.Sprintf("'%s' in parents and trashed = false", escapeQueryParam(d.cfg.FolderID))
	params.Set("q", query)
	params.Set("fields", "nextPageToken,files(id,name,size,mimeType,md5Checksum,modifiedTime,createdTime,appProperties)")

	limit := opts.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	params.Set("pageSize", strconv.Itoa(limit))

	if opts.Cursor != "" {
		params.Set("pageToken", opts.Cursor)
	}
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
		params.Set("includeItemsFromAllDrives", "true")
	}

	endpoint := fmt.Sprintf("%s/files?%s", d.cfg.DriveAPIBaseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, blobkit.WrapError("list", "", d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, blobkit.WrapError("list", "", d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("list", "", d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes := readErrorBody(resp.Body)
		return nil, wrapHTTPError("list", "", d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))

	type listResponse struct {
		NextPageToken string              `json:"nextPageToken"`
		Files         []driveFileResponse `json:"files"`
	}

	var lr listResponse
	if err := json.Unmarshal(bodyBytes, &lr); err != nil {
		return nil, blobkit.WrapError("list", "", d.cfg.Name, err)
	}

	var objects []blobkit.Object
	var commonPrefixes []string
	seenPrefixes := make(map[string]bool)

	for _, f := range lr.Files {
		key := f.Name
		if k, ok := f.AppProperties["blobkit_key"]; ok && k != "" {
			key = k
		}

		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			continue
		}

		if opts.Delimiter != "" {
			sub := strings.TrimPrefix(key, opts.Prefix)
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

		d.cache.Set(key, f.ID)
		objects = append(objects, *d.mapDriveFileToObject(key, &f))
	}

	sort.Strings(commonPrefixes)

	return &blobkit.ListResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		NextCursor:     lr.NextPageToken,
		IsTruncated:    lr.NextPageToken != "",
	}, nil
}

// PresignGet is not natively supported by Google Drive REST API.
func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// PresignPut is not natively supported by Google Drive REST API.
func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// ResolveURL builds a public access or CDN URL for the given key.
func (d *Driver) ResolveURL(key string) (string, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return "", err
	}
	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.PublicBaseURL, "/"), strings.TrimLeft(key, "/")), nil
	}

	fileID, found := d.cache.Get(key)
	if found {
		return fmt.Sprintf("https://drive.google.com/uc?id=%s&export=download", url.QueryEscape(fileID)), nil
	}

	return "", blobkit.WrapError("resolve_url", key, d.cfg.Name, fmt.Errorf("file ID not cached for URL resolution"))
}

// Close gracefully closes the driver and clears active caches and sessions.
func (d *Driver) Close() error {
	d.cache.Clear()

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

// lookupFileID checks the LRU cache and Google Drive files.list API to discover an existing file's ID.
func (d *Driver) lookupFileID(ctx context.Context, key string) (string, error) {
	if fileID, ok := d.cache.Get(key); ok {
		return fileID, nil
	}

	params := url.Values{}
	q := fmt.Sprintf("'%s' in parents and appProperties has { key='blobkit_key' and value='%s' } and trashed = false",
		escapeQueryParam(d.cfg.FolderID),
		escapeQueryParam(key))
	params.Set("q", q)
	params.Set("fields", "files(id,name,appProperties)")
	params.Set("pageSize", "1")
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
		params.Set("includeItemsFromAllDrives", "true")
	}

	endpoint := fmt.Sprintf("%s/files?%s", d.cfg.DriveAPIBaseURL, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", blobkit.WrapError("lookup_file_id", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return "", blobkit.WrapError("lookup_file_id", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", wrapHTTPError("lookup_file_id", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes := readErrorBody(resp.Body)
		return "", wrapHTTPError("lookup_file_id", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	type searchResponse struct {
		Files []struct {
			ID string `json:"id"`
		} `json:"files"`
	}

	var sr searchResponse
	if err := json.Unmarshal(bodyBytes, &sr); err != nil {
		return "", blobkit.WrapError("lookup_file_id", key, d.cfg.Name, err)
	}

	if len(sr.Files) == 0 {
		return "", blobkit.WrapError("lookup_file_id", key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	fileID := sr.Files[0].ID
	d.cache.Set(key, fileID)
	return fileID, nil
}

// resolveFileID returns the file ID for a key or returns ErrObjectNotFound.
func (d *Driver) resolveFileID(ctx context.Context, key string) (string, error) {
	return d.lookupFileID(ctx, key)
}

// authorizeRequest sets OAuth2 bearer authorization on outgoing requests.
func (d *Driver) authorizeRequest(ctx context.Context, req *http.Request) error {
	if d.cfg.TokenFunc != nil {
		tok, err := d.cfg.TokenFunc(ctx)
		if err != nil {
			return fmt.Errorf("failed to obtain oauth token: %w", err)
		}
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		return nil
	}

	if d.cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+d.cfg.BearerToken)
	}

	return nil
}

// mapDriveFileToObject transforms a Google Drive File API JSON response into a blobkit.Object.
func (d *Driver) mapDriveFileToObject(key string, file *driveFileResponse) *blobkit.Object {
	var size int64
	if s, err := file.Size.Int64(); err == nil {
		size = s
	}

	var updatedAt time.Time
	if file.ModifiedTime != "" {
		if t, err := time.Parse(time.RFC3339Nano, file.ModifiedTime); err == nil {
			updatedAt = t.UTC()
		} else if t, err := time.Parse(time.RFC3339, file.ModifiedTime); err == nil {
			updatedAt = t.UTC()
		}
	}

	var createdAt time.Time
	if file.CreatedTime != "" {
		if t, err := time.Parse(time.RFC3339Nano, file.CreatedTime); err == nil {
			createdAt = t.UTC()
		} else if t, err := time.Parse(time.RFC3339, file.CreatedTime); err == nil {
			createdAt = t.UTC()
		}
	}

	meta := make(map[string]string)
	for k, v := range file.AppProperties {
		if k != "blobkit_key" {
			meta[k] = v
		}
	}

	return &blobkit.Object{
		ID:          file.ID,
		Key:         key,
		Bucket:      d.cfg.FolderID,
		Size:        size,
		ContentType: file.MimeType,
		ETag:        file.MD5Checksum,
		Metadata:    meta,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		Provider:    d.cfg.Name,
		Status:      blobkit.StateCommitted,
	}
}

// escapeQueryParam escapes single quotes in values for Google Drive 'q' search queries.
func escapeQueryParam(val string) string {
	return strings.ReplaceAll(val, "'", "\\'")
}
