package gcs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// gcsObjectResource represents the subset of fields in a GCS Object Resource.
type gcsObjectResource struct {
	Kind        string            `json:"kind"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Bucket      string            `json:"bucket"`
	Generation  string            `json:"generation"`
	ContentType string            `json:"contentType"`
	Size        json.Number       `json:"size"`
	MD5Hash     string            `json:"md5Hash"`
	ETag        string            `json:"etag"`
	Updated     string            `json:"updated"`
	TimeCreated string            `json:"timeCreated"`
	Metadata    map[string]string `json:"metadata"`
}

// multipartSessionState tracks in-flight chunked resumable upload sessions.
type multipartSessionState struct {
	mu           sync.Mutex
	uploadURI    string
	key          string
	contentType  string
	totalSize    int64 // -1 if unknown / streaming
	uploadedSize int64
	completed    bool
	resultObj    *blobkit.Object
	parts        map[int32]blobkit.CompletedPart
}

// CreateMultipart initiates a resumable upload session on Google Cloud Storage.
// Implements the official GCS Resumable Upload protocol.
func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	if obj == nil {
		return "", blobkit.ErrInvalidKey
	}
	if err := blobkit.ValidateKey(obj.Key); err != nil {
		return "", err
	}

	cleanKey := obj.Key
	contentType := obj.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	endpoint := fmt.Sprintf("%s/b/%s/o?uploadType=resumable&name=%s",
		d.cfg.UploadAPIBaseURL,
		url.PathEscape(d.cfg.Bucket),
		url.QueryEscape(cleanKey),
	)

	var reqBody io.Reader
	if len(opts.Metadata) > 0 {
		metaBody := map[string]interface{}{
			"metadata": opts.Metadata,
		}
		data, err := json.Marshal(metaBody)
		if err != nil {
			return "", blobkit.WrapError("create_multipart", cleanKey, d.cfg.Name, err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, reqBody)
	if err != nil {
		return "", blobkit.WrapError("create_multipart", cleanKey, d.cfg.Name, err)
	}

	req.Header.Set("X-Upload-Content-Type", contentType)
	if opts.Size > 0 {
		req.Header.Set("X-Upload-Content-Length", strconv.FormatInt(opts.Size, 10))
	}
	if len(opts.Metadata) > 0 {
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return "", err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", parseGCSError("create_multipart", cleanKey, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes := readErrorBody(resp.Body)
		return "", parseGCSError("create_multipart", cleanKey, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	sessionURI := resp.Header.Get("Location")
	if sessionURI == "" {
		return "", blobkit.WrapError("create_multipart", cleanKey, d.cfg.Name, fmt.Errorf("missing Location header in GCS response"))
	}

	session := &multipartSessionState{
		uploadURI:    sessionURI,
		key:          cleanKey,
		contentType:  contentType,
		totalSize:    opts.Size,
		uploadedSize: 0,
		parts:        make(map[int32]blobkit.CompletedPart),
	}

	d.sessionsMu.Lock()
	d.sessions[sessionURI] = session
	d.sessionsMu.Unlock()

	return sessionURI, nil
}

// UploadPart streams an individual chunk of data to the Google Cloud Storage resumable upload session.
func (d *Driver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return "", err
	}
	if uploadID == "" {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrInvalidID)
	}
	if r == nil {
		return "", blobkit.ErrNilReader
	}
	if size < 0 {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrSizeMismatch)
	}

	d.sessionsMu.RLock()
	session, found := d.sessions[uploadID]
	d.sessionsMu.RUnlock()

	if !found || session.key != key {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.completed {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, fmt.Errorf("session already completed"))
	}

	// Buffer part data
	buf := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}
	if int64(n) != size {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrSizeMismatch)
	}
	actualSize := int64(n)

	// Compute MD5 for part identification
	h := md5.Sum(buf)
	etag := hex.EncodeToString(h[:])

	rangeStart := session.uploadedSize
	rangeEnd := rangeStart + actualSize - 1

	var totalStr string
	if session.totalSize > 0 {
		totalStr = strconv.FormatInt(session.totalSize, 10)
	} else {
		totalStr = "*"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.uploadURI, bytes.NewReader(buf))
	if err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}

	req.Header.Set("Content-Length", strconv.FormatInt(actualSize, 10))
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%s", rangeStart, rangeEnd, totalStr))
	if session.contentType != "" {
		req.Header.Set("Content-Type", session.contentType)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return "", err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", parseGCSError("upload_part", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	// 308 Resume Incomplete indicates chunk accepted and upload still incomplete
	if resp.StatusCode == 308 {
		session.uploadedSize = rangeEnd + 1
		session.parts[partNumber] = blobkit.CompletedPart{
			PartNumber: partNumber,
			Size:       actualSize,
			ETag:       etag,
		}
		return etag, nil
	}

	// 200 OK or 201 Created indicates the final chunk completed the object
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		session.uploadedSize = rangeEnd + 1
		session.parts[partNumber] = blobkit.CompletedPart{
			PartNumber: partNumber,
			Size:       actualSize,
			ETag:       etag,
		}

		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var res gcsObjectResource
		if err := json.Unmarshal(bodyBytes, &res); err == nil {
			session.resultObj = d.mapObjectResource(key, &res)
			session.completed = true
		}
		return etag, nil
	}

	bodyBytes := readErrorBody(resp.Body)
	return "", parseGCSError("upload_part", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
}

// CompleteMultipart finalizes the resumable upload session on Google Cloud Storage.
func (d *Driver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	if obj == nil {
		return nil, blobkit.ErrInvalidKey
	}
	if err := blobkit.ValidateKey(obj.Key); err != nil {
		return nil, err
	}
	if uploadID == "" {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrInvalidID)
	}

	d.sessionsMu.RLock()
	session, found := d.sessions[uploadID]
	d.sessionsMu.RUnlock()

	if !found || session.key != obj.Key {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.completed && session.resultObj != nil {
		d.sessionsMu.Lock()
		delete(d.sessions, uploadID)
		d.sessionsMu.Unlock()
		return session.resultObj, nil
	}

	// If upload has not completed yet, finalize by sending 0 bytes with the known total size
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.uploadURI, nil)
	if err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	req.Header.Set("Content-Length", "0")
	req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", session.uploadedSize))

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, parseGCSError("complete_multipart", obj.Key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes := readErrorBody(resp.Body)
		return nil, parseGCSError("complete_multipart", obj.Key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var res gcsObjectResource
	if err := json.Unmarshal(bodyBytes, &res); err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	resultObj := d.mapObjectResource(obj.Key, &res)

	d.sessionsMu.Lock()
	delete(d.sessions, uploadID)
	d.sessionsMu.Unlock()

	return resultObj, nil
}

// AbortMultipart cancels a resumable upload session and cleans up resources on GCS.
func (d *Driver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	if err := blobkit.ValidateKey(key); err != nil {
		return err
	}
	if uploadID == "" {
		return nil
	}

	d.sessionsMu.Lock()
	session, found := d.sessions[uploadID]
	if found && session.key == key {
		delete(d.sessions, uploadID)
	}
	d.sessionsMu.Unlock()

	if !found || session.key != key {
		return nil // Idempotent abort
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, session.uploadURI, nil)
	if err != nil {
		return blobkit.WrapError("abort_multipart", key, d.cfg.Name, err)
	}
	req.Header.Set("Content-Length", "0")

	if err := d.authorizeRequest(ctx, req); err != nil {
		return err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return parseGCSError("abort_multipart", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	// GCS responds with 499 (Client Closed Request) or 200/204 when successfully aborted
	if resp.StatusCode != 499 && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes := readErrorBody(resp.Body)
		return parseGCSError("abort_multipart", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	return nil
}

// ListParts returns the list of parts recorded for an active upload session.
func (d *Driver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	if uploadID == "" {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, blobkit.ErrInvalidID)
	}

	d.sessionsMu.RLock()
	session, found := d.sessions[uploadID]
	d.sessionsMu.RUnlock()

	if !found || session.key != key {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	parts := make([]blobkit.CompletedPart, 0, len(session.parts))
	for _, p := range session.parts {
		parts = append(parts, p)
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// mapObjectResource translates a GCS Object Resource to a canonical blobkit.Object.
func (d *Driver) mapObjectResource(key string, res *gcsObjectResource) *blobkit.Object {
	if res == nil {
		return nil
	}

	size, _ := res.Size.Int64()

	var updatedAt time.Time
	if res.Updated != "" {
		updatedAt, _ = time.Parse(time.RFC3339, res.Updated)
	} else if res.TimeCreated != "" {
		updatedAt, _ = time.Parse(time.RFC3339, res.TimeCreated)
	}
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	meta := make(map[string]string)
	for k, v := range res.Metadata {
		meta[k] = v
	}

	return &blobkit.Object{
		ID:          res.ID,
		Key:         key,
		Size:        size,
		ContentType: res.ContentType,
		ETag:        res.ETag,
		Metadata:    meta,
		Provider:    d.cfg.Name,
		Bucket:      res.Bucket,
		UpdatedAt:   updatedAt,
	}
}
