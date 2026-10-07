package gdrive_test

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
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/gdrive"
)

type mockFile struct {
	ID            string
	Name          string
	MimeType      string
	Data          []byte
	MD5Checksum   string
	CreatedTime   time.Time
	ModifiedTime  time.Time
	AppProperties map[string]string
	Trashed       bool
}

type mockSession struct {
	ID           string
	Key          string
	Name         string
	MimeType     string
	TotalSize    int64
	Buffer       []byte
	Completed    bool
	FileID       string
	IsUpdate     bool
	TargetFileID string
}

type mockDriveServer struct {
	mu       sync.Mutex
	files    map[string]*mockFile
	sessions map[string]*mockSession
	nextID   int
}

func newMockDriveServer() *mockDriveServer {
	return &mockDriveServer{
		files:    make(map[string]*mockFile),
		sessions: make(map[string]*mockSession),
	}
}

func (s *mockDriveServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Resumable Upload Session PUT /upload_session/{sessionID}
	if strings.HasPrefix(r.URL.Path, "/upload_session/") {
		sessionID := strings.TrimPrefix(r.URL.Path, "/upload_session/")
		session, ok := s.sessions[sessionID]
		if !ok {
			http.Error(w, `{"error":{"code":404,"message":"Session not found"}}`, http.StatusNotFound)
			return
		}

		if r.Method == http.MethodDelete {
			delete(s.sessions, sessionID)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if r.Method == http.MethodPut {
			contentRange := r.Header.Get("Content-Range")
			body, _ := io.ReadAll(r.Body)

			// Finalize 0-byte upload
			if contentRange == "bytes */0" {
				session.Completed = true
				h := md5.Sum(session.Buffer)
				checksum := hex.EncodeToString(h[:])

				var file *mockFile
				if session.IsUpdate && session.TargetFileID != "" {
					file = s.files[session.TargetFileID]
					if file != nil {
						file.Data = session.Buffer
						file.MD5Checksum = checksum
						file.ModifiedTime = time.Now().UTC()
					}
				}

				if file == nil {
					s.nextID++
					fileID := fmt.Sprintf("file_%d", s.nextID)
					file = &mockFile{
						ID:           fileID,
						Name:         session.Name,
						MimeType:     session.MimeType,
						Data:         session.Buffer,
						MD5Checksum:  checksum,
						CreatedTime:  time.Now().UTC(),
						ModifiedTime: time.Now().UTC(),
						AppProperties: map[string]string{
							"blobkit_key": session.Key,
						},
					}
					s.files[fileID] = file
				}

				session.FileID = file.ID
				s.writeFileResponse(w, file)
				return
			}

			// Status inquiry with 0 bytes
			if len(body) == 0 && (contentRange == "bytes */*" || strings.HasPrefix(contentRange, "bytes */")) {
				if session.Completed {
					file := s.files[session.FileID]
					s.writeFileResponse(w, file)
					return
				}
				w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(session.Buffer)-1))
				w.WriteHeader(308)
				return
			}

			// Validate chunk alignment: if not last chunk, len(body) must be multiple of 256 KiB
			var rangeEnd, totalSize int64
			if contentRange != "" {
				parts := strings.Split(contentRange, " ")
				if len(parts) == 2 {
					sub := strings.Split(parts[1], "/")
					if len(sub) == 2 {
						byteRange := strings.Split(sub[0], "-")
						if len(byteRange) == 2 {
							rangeEnd, _ = strconv.ParseInt(byteRange[1], 10, 64)
						}
						if sub[1] != "*" {
							totalSize, _ = strconv.ParseInt(sub[1], 10, 64)
						}
					}
				}
			}

			isLast := (totalSize > 0 && rangeEnd+1 >= totalSize)
			if !isLast && len(body)%gdrive.MinChunkSize != 0 {
				http.Error(w, `{"error":{"code":400,"message":"Chunk size must be a multiple of 256 KiB"}}`, http.StatusBadRequest)
				return
			}

			session.Buffer = append(session.Buffer, body...)

			if isLast || (totalSize > 0 && int64(len(session.Buffer)) >= totalSize) {
				session.Completed = true
				h := md5.Sum(session.Buffer)
				checksum := hex.EncodeToString(h[:])

				var file *mockFile
				if session.IsUpdate && session.TargetFileID != "" {
					file = s.files[session.TargetFileID]
					if file != nil {
						file.Data = session.Buffer
						file.MD5Checksum = checksum
						file.ModifiedTime = time.Now().UTC()
					}
				}

				if file == nil {
					s.nextID++
					fileID := fmt.Sprintf("file_%d", s.nextID)
					file = &mockFile{
						ID:           fileID,
						Name:         session.Name,
						MimeType:     session.MimeType,
						Data:         session.Buffer,
						MD5Checksum:  checksum,
						CreatedTime:  time.Now().UTC(),
						ModifiedTime: time.Now().UTC(),
						AppProperties: map[string]string{
							"blobkit_key": session.Key,
						},
					}
					s.files[fileID] = file
				}

				session.FileID = file.ID
				s.writeFileResponse(w, file)
				return
			}

			// Incomplete chunk
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(session.Buffer)-1))
			w.WriteHeader(308)
			return
		}
	}

	// 2. Resumable Session Initiation: POST /upload/drive/v3/files or PATCH /upload/drive/v3/files/{id}
	if strings.HasPrefix(r.URL.Path, "/upload/drive/v3/files") {
		uploadType := r.URL.Query().Get("uploadType")
		if uploadType != "resumable" {
			http.Error(w, "unsupported uploadType", http.StatusBadRequest)
			return
		}

		var meta struct {
			Name          string            `json:"name"`
			Parents       []string          `json:"parents"`
			AppProperties map[string]string `json:"appProperties"`
		}
		_ = json.NewDecoder(r.Body).Decode(&meta)

		s.nextID++
		sessionID := fmt.Sprintf("sess_%d", s.nextID)

		var isUpdate bool
		var targetFileID string
		if r.Method == http.MethodPatch {
			isUpdate = true
			targetFileID = strings.TrimPrefix(r.URL.Path, "/upload/drive/v3/files/")
		}

		var totalSize int64 = -1
		if cl := r.Header.Get("X-Upload-Content-Length"); cl != "" {
			totalSize, _ = strconv.ParseInt(cl, 10, 64)
		}

		s.sessions[sessionID] = &mockSession{
			ID:           sessionID,
			Key:          meta.AppProperties["blobkit_key"],
			Name:         meta.Name,
			MimeType:     r.Header.Get("X-Upload-Content-Type"),
			TotalSize:    totalSize,
			IsUpdate:     isUpdate,
			TargetFileID: targetFileID,
		}

		sessionURI := fmt.Sprintf("http://%s/upload_session/%s", r.Host, sessionID)
		w.Header().Set("Location", sessionURI)
		w.WriteHeader(http.StatusOK)
		return
	}

	// 3. Search / List / Query: GET /drive/v3/files
	if r.URL.Path == "/drive/v3/files" && r.Method == http.MethodGet {
		q := r.URL.Query().Get("q")
		var matched []*mockFile

		for _, f := range s.files {
			if f.Trashed {
				continue
			}

			if strings.Contains(q, "appProperties has") {
				// Parse key='blobkit_key' and value='...'
				idx := strings.Index(q, "value='")
				if idx != -1 {
					tail := q[idx+len("value='"):]
					endQuote := strings.Index(tail, "'")
					if endQuote != -1 {
						searchKey := strings.ReplaceAll(tail[:endQuote], "\\'", "'")
						if f.AppProperties["blobkit_key"] == searchKey {
							matched = append(matched, f)
						}
					}
				}
			} else {
				matched = append(matched, f)
			}
		}

		resp := map[string]interface{}{}
		var fileList []map[string]interface{}
		for _, f := range matched {
			fileList = append(fileList, s.fileToMap(f))
		}
		resp["files"] = fileList
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// 4. File Operations: /drive/v3/files/{id}
	if strings.HasPrefix(r.URL.Path, "/drive/v3/files/") {
		sub := strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")

		// Copy: POST /drive/v3/files/{id}/copy
		if strings.HasSuffix(sub, "/copy") && r.Method == http.MethodPost {
			srcID := strings.TrimSuffix(sub, "/copy")
			srcFile, ok := s.files[srcID]
			if !ok || srcFile.Trashed {
				http.Error(w, `{"error":{"code":404,"message":"Source not found"}}`, http.StatusNotFound)
				return
			}

			var meta struct {
				Name          string            `json:"name"`
				AppProperties map[string]string `json:"appProperties"`
			}
			_ = json.NewDecoder(r.Body).Decode(&meta)

			s.nextID++
			dstID := fmt.Sprintf("file_%d", s.nextID)
			dstFile := &mockFile{
				ID:           dstID,
				Name:         meta.Name,
				MimeType:     srcFile.MimeType,
				Data:         srcFile.Data,
				MD5Checksum:  srcFile.MD5Checksum,
				CreatedTime:  time.Now().UTC(),
				ModifiedTime: time.Now().UTC(),
				AppProperties: map[string]string{
					"blobkit_key": meta.AppProperties["blobkit_key"],
				},
			}
			s.files[dstID] = dstFile
			s.writeFileResponse(w, dstFile)
			return
		}

		fileID := sub
		file, ok := s.files[fileID]
		if !ok || file.Trashed {
			http.Error(w, `{"error":{"code":404,"message":"File not found"}}`, http.StatusNotFound)
			return
		}

		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("alt") == "media" {
				// Streaming download, handle Range
				rangeHeader := r.Header.Get("Range")
				data := file.Data

				if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
					parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
					start, _ := strconv.ParseInt(parts[0], 10, 64)
					end := int64(len(data) - 1)
					if len(parts) > 1 && parts[1] != "" {
						end, _ = strconv.ParseInt(parts[1], 10, 64)
					}
					if start < 0 || start > int64(len(data)) || end < start {
						w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
						return
					}
					if end >= int64(len(data)) {
						end = int64(len(data) - 1)
					}
					chunk := data[start : end+1]
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
					w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(chunk)
					return
				}

				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(data)
				return
			}

			// Head / Metadata
			s.writeFileResponse(w, file)
			return

		case http.MethodDelete:
			delete(s.files, fileID)
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	http.NotFound(w, r)
}

func (s *mockDriveServer) writeFileResponse(w http.ResponseWriter, file *mockFile) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(s.fileToMap(file))
}

func (s *mockDriveServer) fileToMap(file *mockFile) map[string]interface{} {
	return map[string]interface{}{
		"id":            file.ID,
		"name":          file.Name,
		"mimeType":      file.MimeType,
		"size":          strconv.Itoa(len(file.Data)),
		"md5Checksum":   file.MD5Checksum,
		"createdTime":   file.CreatedTime.Format(time.RFC3339Nano),
		"modifiedTime":  file.ModifiedTime.Format(time.RFC3339Nano),
		"appProperties": file.AppProperties,
		"trashed":       file.Trashed,
	}
}

func setupTestDriver(t *testing.T, handler http.Handler) (*gdrive.Driver, *httptest.Server) {
	server := httptest.NewServer(handler)

	cfg := gdrive.Config{
		Name:             "gdrive-test",
		FolderID:         "test_folder_id",
		BearerToken:      "mock-secret-bearer-token-12345",
		DriveAPIBaseURL:  server.URL + "/drive/v3",
		UploadAPIBaseURL: server.URL + "/upload/drive/v3",
		HTTPClient:       server.Client(),
		ChunkSize:        gdrive.MinChunkSize, // 256 KiB
		KeyCacheCapacity: 100,
	}

	driver, err := gdrive.NewDriver(cfg)
	if err != nil {
		server.Close()
		t.Fatalf("failed to create driver: %v", err)
	}

	return driver, server
}

func TestConfig_Validation(t *testing.T) {
	// Missing folder ID
	cfg := gdrive.Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing FolderID")
	}

	// Chunk size not multiple of 256 KiB
	cfg = gdrive.Config{
		FolderID:  "folder_123",
		ChunkSize: 300 * 1024,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when ChunkSize is not multiple of 256 KiB")
	}

	// Valid config with defaults applied
	cfg = gdrive.Config{
		FolderID: "folder_123",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ChunkSize != gdrive.DefaultChunkSize {
		t.Fatalf("expected DefaultChunkSize %d, got %d", gdrive.DefaultChunkSize, cfg.ChunkSize)
	}
	if cfg.KeyCacheCapacity != gdrive.DefaultKeyCacheCapacity {
		t.Fatalf("expected DefaultKeyCacheCapacity, got %d", cfg.KeyCacheCapacity)
	}
}

func TestDriver_Capabilities(t *testing.T) {
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	caps := driver.Capabilities()
	if caps&blobkit.CapDirectPut == 0 {
		t.Fatal("expected CapDirectPut")
	}
	if caps&blobkit.CapMultipartPut == 0 {
		t.Fatal("expected CapMultipartPut")
	}
	if caps&blobkit.CapByteRangeGet == 0 {
		t.Fatal("expected CapByteRangeGet")
	}
	if caps&blobkit.CapCopy == 0 {
		t.Fatal("expected CapCopy")
	}
	if caps&blobkit.CapBatchDelete == 0 {
		t.Fatal("expected CapBatchDelete")
	}
	if caps&blobkit.CapPresignGet != 0 || caps&blobkit.CapPresignPut != 0 {
		t.Fatal("presign capabilities should NOT be set for Google Drive")
	}
}

func TestDriver_CRUD_And_ByteRange(t *testing.T) {
	ctx := context.Background()
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	payload := bytes.Repeat([]byte("BlobKitGDriveData!"), 50000) // ~900 KB
	obj := &blobkit.Object{
		Key:         "documents/report.pdf",
		ContentType: "application/pdf",
	}

	// 1. Put
	saved, err := driver.Put(ctx, obj, bytes.NewReader(payload), blobkit.PutOptions{
		Size: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if saved.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), saved.Size)
	}
	if saved.ContentType != "application/pdf" {
		t.Fatalf("expected ContentType application/pdf, got %s", saved.ContentType)
	}
	if saved.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// 2. Head
	head, err := driver.Head(ctx, "documents/report.pdf")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if head.Size != int64(len(payload)) {
		t.Fatalf("head size mismatch: %d != %d", head.Size, len(payload))
	}
	if head.ETag != saved.ETag {
		t.Fatalf("head etag mismatch: %s != %s", head.ETag, saved.ETag)
	}

	// 3. Get Full
	reader, err := driver.Get(ctx, "documents/report.pdf", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	downloaded, err := io.ReadAll(reader.Body)
	reader.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(downloaded, payload) {
		t.Fatal("downloaded payload does not match original")
	}

	// 4. Get with Byte Range (Partial)
	offset := int64(100)
	length := int64(250)
	rangeReader, err := driver.Get(ctx, "documents/report.pdf", blobkit.GetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", offset, offset+length-1),
	})
	if err != nil {
		t.Fatalf("Get with Range failed: %v", err)
	}
	rangeData, err := io.ReadAll(rangeReader.Body)
	rangeReader.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll range failed: %v", err)
	}
	if int64(len(rangeData)) != length {
		t.Fatalf("expected range length %d, got %d", length, len(rangeData))
	}
	expectedRange := payload[offset : offset+length]
	if !bytes.Equal(rangeData, expectedRange) {
		t.Fatal("range data does not match slice")
	}

	// 5. Delete
	err = driver.Delete(ctx, "documents/report.pdf")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Head after Delete should return ErrObjectNotFound
	_, err = driver.Head(ctx, "documents/report.pdf")
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	// Delete again should be idempotent (no error)
	err = driver.Delete(ctx, "documents/report.pdf")
	if err != nil {
		t.Fatalf("expected idempotent Delete, got error: %v", err)
	}
}

func TestDriver_OverwriteSemantics(t *testing.T) {
	ctx := context.Background()
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	key := "config/settings.json"
	v1 := []byte(`{"version": 1}`)
	v2 := []byte(`{"version": 2, "updated": true}`)

	// Put version 1
	obj := &blobkit.Object{Key: key, ContentType: "application/json"}
	saved1, err := driver.Put(ctx, obj, bytes.NewReader(v1), blobkit.PutOptions{Size: int64(len(v1))})
	if err != nil {
		t.Fatalf("Put v1 failed: %v", err)
	}

	// Put version 2 (overwrite)
	saved2, err := driver.Put(ctx, obj, bytes.NewReader(v2), blobkit.PutOptions{Size: int64(len(v2))})
	if err != nil {
		t.Fatalf("Put v2 overwrite failed: %v", err)
	}

	// File ID should be preserved during in-place overwrite
	if saved1.ID != saved2.ID {
		t.Fatalf("expected preserved File ID during overwrite, got %s and %s", saved1.ID, saved2.ID)
	}

	// Verify content matches v2
	r, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get after overwrite failed: %v", err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()

	if !bytes.Equal(body, v2) {
		t.Fatalf("expected %s, got %s", string(v2), string(body))
	}
}

func TestDriver_ResumableMultipart_Lifecycle(t *testing.T) {
	ctx := context.Background()
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	key := "large/archive.tar"
	obj := &blobkit.Object{Key: key, ContentType: "application/x-tar"}

	chunk1 := bytes.Repeat([]byte("A"), gdrive.MinChunkSize) // 256 KiB
	chunk2 := bytes.Repeat([]byte("B"), 1000)                // Final chunk (remainder)
	totalSize := int64(len(chunk1) + len(chunk2))

	// 1. CreateMultipart
	uploadID, err := driver.CreateMultipart(ctx, obj, blobkit.PutOptions{Size: totalSize})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}
	if uploadID == "" {
		t.Fatal("expected non-empty uploadID")
	}

	// 2. UploadPart 1
	etag1, err := driver.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(chunk1), int64(len(chunk1)))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}
	if etag1 == "" {
		t.Fatal("expected non-empty etag1")
	}

	// 3. UploadPart 2 (completes)
	etag2, err := driver.UploadPart(ctx, key, uploadID, 2, bytes.NewReader(chunk2), int64(len(chunk2)))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}
	if etag2 == "" {
		t.Fatal("expected non-empty etag2")
	}

	// 4. CompleteMultipart
	parts := []blobkit.CompletedPart{
		{PartNumber: 1, ETag: etag1, Size: int64(len(chunk1))},
		{PartNumber: 2, ETag: etag2, Size: int64(len(chunk2))},
	}
	completed, err := driver.CompleteMultipart(ctx, obj, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}
	if completed.Size != totalSize {
		t.Fatalf("expected total size %d, got %d", totalSize, completed.Size)
	}

	// 5. Verify payload
	r, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()

	expected := append(chunk1, chunk2...)
	if !bytes.Equal(body, expected) {
		t.Fatal("uploaded multipart stream does not match expected payload")
	}
}

func TestDriver_Copy_And_BatchDelete(t *testing.T) {
	ctx := context.Background()
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	data := []byte("Original file contents to be copied")
	srcKey := "photos/vacation.jpg"
	dstKey := "photos/vacation_backup.jpg"

	_, err := driver.Put(ctx, &blobkit.Object{Key: srcKey, ContentType: "image/jpeg"}, bytes.NewReader(data), blobkit.PutOptions{Size: int64(len(data))})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Copy
	err = driver.Copy(ctx, srcKey, dstKey)
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Verify dstKey exists and has identical data
	r, err := driver.Get(ctx, dstKey, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get copy destination failed: %v", err)
	}
	dstData, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !bytes.Equal(dstData, data) {
		t.Fatal("copied data does not match source")
	}

	// Batch Delete
	deleted, err := driver.DeleteBatch(ctx, []string{srcKey, dstKey})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted items, got %d", len(deleted))
	}

	// Both should be gone
	_, err = driver.Head(ctx, srcKey)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected srcKey not found, got %v", err)
	}
	_, err = driver.Head(ctx, dstKey)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected dstKey not found, got %v", err)
	}
}

func TestDriver_ErrorScrubbing_And_Mapping(t *testing.T) {
	// Custom error server returning 403 quota exceeded and 503
	errHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqURL := r.URL.String()
		if strings.Contains(reqURL, "quota-fail") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"The user's Drive storage quota has been exceeded","errors":[{"domain":"usageLimits","reason":"storageQuotaExceeded"}]}}`))
			return
		}
		if strings.Contains(reqURL, "server-fail") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Backend unavailable with Bearer secret-token-123"))
			return
		}
		http.NotFound(w, r)
	})

	server := httptest.NewServer(errHandler)
	defer server.Close()

	cfg := gdrive.Config{
		FolderID:         "test_folder",
		BearerToken:      "super-secret-bearer-token",
		DriveAPIBaseURL:  server.URL + "/drive/v3",
		UploadAPIBaseURL: server.URL + "/upload/drive/v3",
		HTTPClient:       server.Client(),
	}
	driver, err := gdrive.NewDriver(cfg)
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	// 1. Quota Exceeded mapping
	_, err = driver.Head(context.Background(), "quota-fail")
	if !errors.Is(err, blobkit.ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got: %v", err)
	}

	// 2. Provider Unavailable mapping & token scrubbing
	_, err = driver.Head(context.Background(), "server-fail")
	if !errors.Is(err, blobkit.ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable, got: %v", err)
	}
	if strings.Contains(err.Error(), "secret-token-123") {
		t.Fatalf("error leaked secret token: %v", err)
	}
}

func TestDriver_ContextCancellation(t *testing.T) {
	// Slow server hanging indefinitely
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer slowServer.Close()

	cfg := gdrive.Config{
		FolderID:         "test_folder",
		BearerToken:      "mock-token",
		DriveAPIBaseURL:  slowServer.URL + "/drive/v3",
		UploadAPIBaseURL: slowServer.URL + "/upload/drive/v3",
		HTTPClient:       slowServer.Client(),
	}
	driver, err := gdrive.NewDriver(cfg)
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = driver.Put(ctx, &blobkit.Object{Key: "test.txt"}, strings.NewReader("hello"), blobkit.PutOptions{})
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
}

func TestDriver_Concurrency(t *testing.T) {
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	concurrency := 10
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("concurrent/worker_%d.txt", idx)
			payload := []byte(fmt.Sprintf("worker data %d", idx))

			// Put
			_, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(payload), blobkit.PutOptions{Size: int64(len(payload))})
			if err != nil {
				t.Errorf("worker %d Put failed: %v", idx, err)
				return
			}

			// Head
			_, err = driver.Head(ctx, key)
			if err != nil {
				t.Errorf("worker %d Head failed: %v", idx, err)
				return
			}

			// Get
			r, err := driver.Get(ctx, key, blobkit.GetOptions{})
			if err != nil {
				t.Errorf("worker %d Get failed: %v", idx, err)
				return
			}
			data, _ := io.ReadAll(r.Body)
			r.Body.Close()
			if !bytes.Equal(data, payload) {
				t.Errorf("worker %d payload mismatch", idx)
			}
		}()
	}

	wg.Wait()
}

func TestDriver_List(t *testing.T) {
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()

	// Upload 3 objects
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "images/img1.png"}, strings.NewReader("1"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "images/img2.png"}, strings.NewReader("2"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "docs/readme.txt"}, strings.NewReader("3"), blobkit.PutOptions{})

	// List with prefix "images/"
	res, err := driver.List(ctx, blobkit.ListOptions{Prefix: "images/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(res.Objects) != 2 {
		t.Fatalf("expected 2 objects with prefix images/, got %d", len(res.Objects))
	}
}

func TestDriver_PresignUnsupported(t *testing.T) {
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()

	_, err := driver.PresignGet(ctx, "test.txt", blobkit.PresignOptions{})
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for PresignGet, got: %v", err)
	}

	_, err = driver.PresignPut(ctx, "test.txt", blobkit.PresignOptions{})
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for PresignPut, got: %v", err)
	}
}

func TestDriver_ResolveURL(t *testing.T) {
	mock := newMockDriveServer()
	server := httptest.NewServer(mock)
	defer server.Close()

	// 1. With PublicBaseURL
	driver1, err := gdrive.NewDriver(gdrive.Config{
		FolderID:         "f1",
		PublicBaseURL:    "https://cdn.example.com/assets",
		DriveAPIBaseURL:  server.URL + "/drive/v3",
		UploadAPIBaseURL: server.URL + "/upload/drive/v3",
		HTTPClient:       server.Client(),
	})
	if err != nil {
		t.Fatalf("failed to init driver: %v", err)
	}
	defer driver1.Close()

	u1, err := driver1.ResolveURL("images/avatar.png")
	if err != nil {
		t.Fatalf("ResolveURL failed: %v", err)
	}
	if u1 != "https://cdn.example.com/assets/images/avatar.png" {
		t.Fatalf("unexpected URL: %s", u1)
	}

	// 2. Without PublicBaseURL (returns drive export URL if cached)
	driver2, err := gdrive.NewDriver(gdrive.Config{
		FolderID:         "f1",
		DriveAPIBaseURL:  server.URL + "/drive/v3",
		UploadAPIBaseURL: server.URL + "/upload/drive/v3",
		HTTPClient:       server.Client(),
	})
	if err != nil {
		t.Fatalf("failed to init driver: %v", err)
	}
	defer driver2.Close()

	// Before put -> not cached
	_, err = driver2.ResolveURL("photos/test.jpg")
	if err == nil {
		t.Fatal("expected error before put when fileID is not cached")
	}

	// After put -> cached
	_, err = driver2.Put(context.Background(), &blobkit.Object{Key: "photos/test.jpg"}, strings.NewReader("data"), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	u2, err := driver2.ResolveURL("photos/test.jpg")
	if err != nil {
		t.Fatalf("ResolveURL failed after Put: %v", err)
	}
	if !strings.HasPrefix(u2, "https://drive.google.com/uc?id=") {
		t.Fatalf("unexpected drive download URL: %s", u2)
	}
}

func TestDriver_IntegrationWithBlobKitClient(t *testing.T) {
	mock := newMockDriveServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init blobkit client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	content := []byte("Client Integration Test Payload for Google Drive")

	// 1. Client.Put
	obj, err := client.Put(ctx, bytes.NewReader(content), blobkit.PutOptions{
		Namespace: "integration",
		Filename:  "test_file.txt",
	})
	if err != nil {
		t.Fatalf("Client.Put failed: %v", err)
	}
	if obj.Provider != "gdrive-test" {
		t.Fatalf("expected provider gdrive-test, got %s", obj.Provider)
	}
	if obj.Size != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), obj.Size)
	}

	// 2. Client.Get
	r, err := client.Get(ctx, obj.Key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Client.Get failed: %v", err)
	}
	downloaded, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(downloaded, content) {
		t.Fatal("downloaded bytes mismatch")
	}

	// 3. Client.Head
	headObj, err := client.Head(ctx, obj.Key)
	if err != nil {
		t.Fatalf("Client.Head failed: %v", err)
	}
	if headObj.Size != int64(len(content)) {
		t.Fatalf("head size mismatch: %d != %d", headObj.Size, len(content))
	}

	// 4. Client.PermanentDelete
	err = client.PermanentDelete(ctx, obj.Key)
	if err != nil {
		t.Fatalf("Client.PermanentDelete failed: %v", err)
	}

	// 5. Verify deleted
	_, err = client.Head(ctx, obj.Key)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}
