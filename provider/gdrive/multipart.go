package gdrive

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"sync"

	"github.com/suhwr/blobkit"
)

// driveFileResponse represents the subset of fields returned by Google Drive API v3 for a File resource.
type driveFileResponse struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	MimeType      string            `json:"mimeType"`
	Size          json.Number       `json:"size"`
	MD5Checksum   string            `json:"md5Checksum"`
	ModifiedTime  string            `json:"modifiedTime"`
	CreatedTime   string            `json:"createdTime"`
	AppProperties map[string]string `json:"appProperties"`
	Trashed       bool              `json:"trashed"`
}

// multipartSessionState tracks in-flight chunked resumable upload sessions.
type multipartSessionState struct {
	mu           sync.Mutex
	uploadURI    string
	key          string
	mimeType     string
	totalSize    int64 // -1 if unknown / streaming
	uploadedSize int64
	completed    bool
	resultObj    *blobkit.Object
	parts        map[int32]blobkit.CompletedPart
}

// CreateMultipart initiates a resumable upload session on Google Drive.
// Conforms to the Resumable Upload protocol specified in Drive API v3.
func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	if obj == nil {
		return "", blobkit.ErrInvalidKey
	}
	if err := blobkit.ValidateKey(obj.Key); err != nil {
		return "", err
	}

	meta := resolveMetadata(obj, opts)
	session, err := d.initiateResumableSession(ctx, obj.Key, obj.ContentType, opts.Size, "", meta)
	if err != nil {
		return "", err
	}

	d.sessionsMu.Lock()
	d.sessions[session.uploadURI] = session
	d.sessionsMu.Unlock()

	return session.uploadURI, nil
}

// UploadPart streams an individual chunk of data to the Google Drive resumable session URI.
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
	if partNumber <= 0 {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, fmt.Errorf("part number must be >= 1"))
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

	sr := blobkit.NewSizeReader(r, size, true)
	h := md5.New()
	tr := io.TeeReader(sr, h)

	rangeStart := session.uploadedSize
	rangeEnd := rangeStart + size - 1

	var contentRange string
	if session.totalSize > 0 {
		contentRange = fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, session.totalSize)
	} else {
		contentRange = fmt.Sprintf("bytes %d-%d/*", rangeStart, rangeEnd)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.uploadURI, tr)
	if err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}

	req.ContentLength = size
	req.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	req.Header.Set("Content-Range", contentRange)
	if session.mimeType != "" {
		req.Header.Set("Content-Type", session.mimeType)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", wrapHTTPError("upload_part", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if err := sr.Verify(); err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}
	actualSize := sr.TotalRead()
	etag := hex.EncodeToString(h.Sum(nil))

	// 308 Resume Incomplete = chunk accepted, awaiting next chunks
	// 200 OK / 201 Created = upload completed
	if resp.StatusCode == 308 {
		session.uploadedSize += actualSize
		part := blobkit.CompletedPart{
			PartNumber: partNumber,
			ETag:       etag,
			Size:       actualSize,
		}
		session.parts[partNumber] = part
		return etag, nil
	} else if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		session.uploadedSize += actualSize
		session.completed = true

		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var fileResp driveFileResponse
		if err := json.Unmarshal(bodyBytes, &fileResp); err == nil {
			resultObj := d.mapDriveFileToObject(key, &fileResp)
			session.resultObj = resultObj
			d.cache.Set(key, fileResp.ID)
		}

		part := blobkit.CompletedPart{
			PartNumber: partNumber,
			ETag:       etag,
			Size:       actualSize,
		}
		session.parts[partNumber] = part
		return etag, nil
	}

	bodyBytes := readErrorBody(resp.Body)
	return "", wrapHTTPError("upload_part", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
}

// CompleteMultipart finalizes a resumable upload session on Google Drive.
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

	seenParts := make(map[int32]bool, len(parts))
	for _, p := range parts {
		if p.PartNumber <= 0 || seenParts[p.PartNumber] {
			return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrMultipartInvalidState)
		}
		seenParts[p.PartNumber] = true
	}

	d.sessionsMu.RLock()
	session, found := d.sessions[uploadID]
	d.sessionsMu.RUnlock()

	if !found || session.key != obj.Key {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	// Cross-check caller parts against session parts if recorded
	if len(session.parts) > 0 {
		for _, p := range parts {
			sessPart, exists := session.parts[p.PartNumber]
			if !exists {
				return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, fmt.Errorf("%w: part %d not found in recorded session", blobkit.ErrMultipartInvalidState, p.PartNumber))
			}
			if p.ETag != "" && sessPart.ETag != "" && p.ETag != sessPart.ETag {
				return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, fmt.Errorf("%w: part %d ETag mismatch", blobkit.ErrMultipartInvalidState, p.PartNumber))
			}
		}
	}

	if session.completed && session.resultObj != nil {
		res := *session.resultObj
		d.sessionsMu.Lock()
		delete(d.sessions, uploadID)
		d.sessionsMu.Unlock()
		return &res, nil
	}

	// If not already completed, send a status inquiry with 0 bytes to verify server state
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.uploadURI, bytes.NewReader(nil))
	if err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	req.Header.Set("Content-Length", "0")
	if session.totalSize > 0 {
		req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", session.totalSize))
	} else {
		req.Header.Set("Content-Range", "bytes */*")
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("complete_multipart", obj.Key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var fileResp driveFileResponse
		if err := json.Unmarshal(bodyBytes, &fileResp); err != nil {
			return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
		}
		res := d.mapDriveFileToObject(obj.Key, &fileResp)
		d.cache.Set(obj.Key, fileResp.ID)

		d.sessionsMu.Lock()
		delete(d.sessions, uploadID)
		d.sessionsMu.Unlock()

		return res, nil
	}

	bodyBytes := readErrorBody(resp.Body)
	return nil, wrapHTTPError("complete_multipart", obj.Key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
}

// AbortMultipart cancels a resumable upload session on Google Drive.
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

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, uploadID, nil)
	if err != nil {
		return blobkit.WrapError("abort_multipart", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return wrapHTTPError("abort_multipart", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	return nil
}

// ListParts returns the list of parts recorded for an active session.
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

// initiateResumableSession initiates a Resumable Upload session (POST for create, PATCH for update).
func (d *Driver) initiateResumableSession(ctx context.Context, key, mimeType string, totalSize int64, existingFileID string, meta map[string]string) (*multipartSessionState, error) {
	var endpoint string
	var method string

	params := url.Values{}
	params.Set("uploadType", "resumable")
	if d.cfg.SupportsAllDrives {
		params.Set("supportsAllDrives", "true")
	}

	appProps := map[string]string{
		"blobkit_key": key,
	}
	for k, v := range meta {
		if k != "blobkit_key" {
			appProps[k] = v
		}
	}

	metadata := map[string]interface{}{
		"name":          path.Base(key),
		"appProperties": appProps,
	}

	if existingFileID != "" {
		method = http.MethodPatch
		endpoint = fmt.Sprintf("%s/files/%s?%s", d.cfg.UploadAPIBaseURL, url.PathEscape(existingFileID), params.Encode())
	} else {
		method = http.MethodPost
		metadata["parents"] = []string{d.cfg.FolderID}
		endpoint = fmt.Sprintf("%s/files?%s", d.cfg.UploadAPIBaseURL, params.Encode())
	}

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, blobkit.WrapError("initiate_session", key, d.cfg.Name, err)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(metaBytes))
	if err != nil {
		return nil, blobkit.WrapError("initiate_session", key, d.cfg.Name, err)
	}

	if err := d.authorizeRequest(ctx, req); err != nil {
		return nil, blobkit.WrapError("initiate_session", key, d.cfg.Name, err)
	}

	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	if mimeType != "" {
		req.Header.Set("X-Upload-Content-Type", mimeType)
	}
	if totalSize > 0 {
		req.Header.Set("X-Upload-Content-Length", strconv.FormatInt(totalSize, 10))
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("initiate_session", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes := readErrorBody(resp.Body)
		return nil, wrapHTTPError("initiate_session", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	uploadURI := resp.Header.Get("Location")
	if uploadURI == "" {
		return nil, blobkit.WrapError("initiate_session", key, d.cfg.Name, fmt.Errorf("missing Location header in session response"))
	}

	return &multipartSessionState{
		uploadURI: uploadURI,
		key:       key,
		mimeType:  mimeType,
		totalSize: totalSize,
		parts:     make(map[int32]blobkit.CompletedPart),
	}, nil
}

// uploadStreamResumable streams an io.Reader of arbitrary size through chunks of MinChunkSize multiples.
func (d *Driver) uploadStreamResumable(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions, existingFileID string) (*blobkit.Object, error) {
	meta := resolveMetadata(obj, opts)
	session, err := d.initiateResumableSession(ctx, obj.Key, obj.ContentType, opts.Size, existingFileID, meta)
	if err != nil {
		if existingFileID != "" && errors.Is(err, blobkit.ErrObjectNotFound) {
			d.cache.Delete(obj.Key)
			session, err = d.initiateResumableSession(ctx, obj.Key, obj.ContentType, opts.Size, "", meta)
		}
		if err != nil {
			return nil, err
		}
	}

	chunkSize := d.cfg.ChunkSize
	buf := make([]byte, chunkSize)
	var uploaded int64
	var partNum int32 = 1

	for {
		select {
		case <-ctx.Done():
			_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
			return nil, ctx.Err()
		default:
		}

		n, readErr := io.ReadFull(r, buf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, readErr)
		}
		if n > 0 {
			chunk := buf[:n]
			isLastChunk := (readErr == io.EOF || readErr == io.ErrUnexpectedEOF)
			chunkLen := int64(n)

			rangeStart := uploaded
			rangeEnd := uploaded + chunkLen - 1

			var contentRange string
			if isLastChunk {
				total := uploaded + chunkLen
				contentRange = fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, total)
			} else if opts.Size > 0 {
				contentRange = fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeEnd, opts.Size)
			} else {
				contentRange = fmt.Sprintf("bytes %d-%d/*", rangeStart, rangeEnd)
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.uploadURI, bytes.NewReader(chunk))
			if err != nil {
				_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
				return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
			}

			req.Header.Set("Content-Length", strconv.FormatInt(chunkLen, 10))
			req.Header.Set("Content-Range", contentRange)
			if obj.ContentType != "" {
				req.Header.Set("Content-Type", obj.ContentType)
			}

			resp, err := d.httpClient.Do(req)
			if err != nil {
				_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
				return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, 0, nil, err)
			}

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
				if sr, ok := r.(*blobkit.SizeReader); ok {
					if err := sr.Verify(); err != nil {
						resp.Body.Close()
						_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
						_ = d.Delete(context.Background(), obj.Key)
						return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
					}
				}
				bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
				resp.Body.Close()

				var fileResp driveFileResponse
				if err := json.Unmarshal(bodyBytes, &fileResp); err != nil {
					return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
				}
				res := d.mapDriveFileToObject(obj.Key, &fileResp)
				d.cache.Set(obj.Key, fileResp.ID)
				return res, nil
			} else if resp.StatusCode == 308 {
				uploaded += chunkLen
				partNum++
				resp.Body.Close()
			} else {
				bodyBytes := readErrorBody(resp.Body)
				resp.Body.Close()
				_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
				return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
			}
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, readErr)
		}
	}

	// Finalize upload if not yet completed (e.g. empty file or stream ending on chunk boundary)
	if sr, ok := r.(*blobkit.SizeReader); ok {
		if err := sr.Verify(); err != nil {
			_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.uploadURI, bytes.NewReader(nil))
	if err != nil {
		_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}
	req.Header.Set("Content-Length", "0")
	req.Header.Set("Content-Range", fmt.Sprintf("bytes */%d", uploaded))

	resp, err := d.httpClient.Do(req)
	if err != nil {
		_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
		return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var fileResp driveFileResponse
		if err := json.Unmarshal(bodyBytes, &fileResp); err != nil {
			return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
		}
		res := d.mapDriveFileToObject(obj.Key, &fileResp)
		d.cache.Set(obj.Key, fileResp.ID)
		return res, nil
	}
	bodyBytes := readErrorBody(resp.Body)
	_ = d.AbortMultipart(context.Background(), obj.Key, session.uploadURI)
	return nil, wrapHTTPError("put", obj.Key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
}
