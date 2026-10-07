package gcs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
)

// mockGCSObject holds in-memory representation of a stored object.
type mockGCSObject struct {
	Name        string
	Bucket      string
	Data        []byte
	ContentType string
	Metadata    map[string]string
	Updated     time.Time
	ETag        string
	MD5         string
}

// mockGCSServer simulates the Google Cloud Storage JSON API v1 and Resumable Upload protocol.
type mockGCSServer struct {
	mu        sync.RWMutex
	bucket    string
	objects   map[string]*mockGCSObject
	sessions  map[string]*mockSession
	sessionSeq int
}

type mockSession struct {
	Name        string
	ContentType string
	Metadata    map[string]string
	Data        []byte
	TotalSize   int64
}

func newMockGCSServer(bucket string) *mockGCSServer {
	return &mockGCSServer{
		bucket:   bucket,
		objects:  make(map[string]*mockGCSObject),
		sessions: make(map[string]*mockSession),
	}
}

func (m *mockGCSServer) handler(serverURL *string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Resumable Upload Session endpoints: /resumable-session/{id}
		if strings.HasPrefix(path, "/resumable-session/") {
			sessionID := strings.TrimPrefix(path, "/resumable-session/")
			m.handleResumableSession(w, r, sessionID)
			return
		}

		// Storage API Upload endpoints: /upload/storage/v1/b/{bucket}/o
		if strings.HasPrefix(path, "/upload/storage/v1/b/") {
			m.handleUploadAPI(w, r, serverURL)
			return
		}

		// Storage API endpoints: /storage/v1/b/{bucket}/o...
		if strings.HasPrefix(path, "/storage/v1/b/") {
			m.handleStorageAPI(w, r)
			return
		}

		http.NotFound(w, r)
	}
}

func (m *mockGCSServer) handleUploadAPI(w http.ResponseWriter, r *http.Request, serverURL *string) {
	uploadType := r.URL.Query().Get("uploadType")
	key := r.URL.Query().Get("name")

	switch uploadType {
	case "media":
		data, _ := io.ReadAll(r.Body)
		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		m.mu.Lock()
		obj := &mockGCSObject{
			Name:        key,
			Bucket:      m.bucket,
			Data:        data,
			ContentType: contentType,
			Updated:     time.Now().UTC(),
			ETag:        `"mock-etag"`,
			MD5:         "mock-md5",
		}
		m.objects[key] = obj
		m.mu.Unlock()

		res := m.objectToResource(obj)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)

	case "multipart":
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mr := multipart.NewReader(r.Body, params["boundary"])
		var metaPart map[string]interface{}
		var dataPart []byte
		var contentType string

		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			pContentType := part.Header.Get("Content-Type")
			if strings.Contains(pContentType, "application/json") {
				_ = json.NewDecoder(part).Decode(&metaPart)
			} else {
				dataPart, _ = io.ReadAll(part)
				contentType = pContentType
			}
		}

		var objName string
		customMeta := make(map[string]string)
		if metaPart != nil {
			if n, ok := metaPart["name"].(string); ok {
				objName = n
			}
			if c, ok := metaPart["contentType"].(string); ok && contentType == "" {
				contentType = c
			}
			if mRaw, ok := metaPart["metadata"].(map[string]interface{}); ok {
				for k, v := range mRaw {
					if vs, ok := v.(string); ok {
						customMeta[k] = vs
					}
				}
			}
		}
		if objName == "" {
			objName = key
		}

		m.mu.Lock()
		obj := &mockGCSObject{
			Name:        objName,
			Bucket:      m.bucket,
			Data:        dataPart,
			ContentType: contentType,
			Metadata:    customMeta,
			Updated:     time.Now().UTC(),
			ETag:        `"mock-etag"`,
			MD5:         "mock-md5",
		}
		m.objects[objName] = obj
		m.mu.Unlock()

		res := m.objectToResource(obj)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)

	case "resumable":
		m.mu.Lock()
		m.sessionSeq++
		sessionID := strconv.Itoa(m.sessionSeq)

		var customMeta map[string]string
		if r.Header.Get("Content-Type") == "application/json; charset=UTF-8" {
			var bodyMap struct {
				Metadata map[string]string `json:"metadata"`
			}
			_ = json.NewDecoder(r.Body).Decode(&bodyMap)
			customMeta = bodyMap.Metadata
		}

		totalSize, _ := strconv.ParseInt(r.Header.Get("X-Upload-Content-Length"), 10, 64)

		m.sessions[sessionID] = &mockSession{
			Name:        key,
			ContentType: r.Header.Get("X-Upload-Content-Type"),
			Metadata:    customMeta,
			TotalSize:   totalSize,
		}
		m.mu.Unlock()

		sessionURI := fmt.Sprintf("%s/resumable-session/%s", *serverURL, sessionID)
		w.Header().Set("Location", sessionURI)
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (m *mockGCSServer) handleResumableSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	m.mu.Lock()
	session, found := m.sessions[sessionID]
	m.mu.Unlock()

	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if r.Method == http.MethodDelete {
		m.mu.Lock()
		delete(m.sessions, sessionID)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method == http.MethodPut {
		chunkData, _ := io.ReadAll(r.Body)
		contentRange := r.Header.Get("Content-Range")

		m.mu.Lock()
		session.Data = append(session.Data, chunkData...)
		currentLen := int64(len(session.Data))

		// Check if upload is completed
		isComplete := false
		if session.TotalSize > 0 && currentLen >= session.TotalSize {
			isComplete = true
		} else if strings.Contains(contentRange, "/") {
			parts := strings.Split(contentRange, "/")
			if len(parts) == 2 && parts[1] != "*" {
				if total, err := strconv.ParseInt(parts[1], 10, 64); err == nil && currentLen >= total {
					isComplete = true
				}
			}
		}

		if isComplete {
			obj := &mockGCSObject{
				Name:        session.Name,
				Bucket:      m.bucket,
				Data:        session.Data,
				ContentType: session.ContentType,
				Metadata:    session.Metadata,
				Updated:     time.Now().UTC(),
				ETag:        `"resumable-etag"`,
				MD5:         "resumable-md5",
			}
			m.objects[session.Name] = obj
			delete(m.sessions, sessionID)
			m.mu.Unlock()

			res := m.objectToResource(obj)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(res)
			return
		}

		m.mu.Unlock()

		// 308 Resume Incomplete
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", currentLen-1))
		w.WriteHeader(308)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (m *mockGCSServer) handleStorageAPI(w http.ResponseWriter, r *http.Request) {
	// Storage API prefix: /storage/v1/b/{bucket}/o
	prefix := fmt.Sprintf("/storage/v1/b/%s/o", m.bucket)
	if !strings.HasPrefix(r.URL.Path, prefix) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	subPath := strings.TrimPrefix(r.URL.Path, prefix)
	subPath = strings.TrimPrefix(subPath, "/")

	// Copy/Rewrite check: {srcKey}/rewriteTo/b/{bucket}/o/{dstKey}
	if strings.Contains(subPath, "/rewriteTo/b/") {
		parts := strings.Split(subPath, "/rewriteTo/b/")
		srcKey, _ := url.PathUnescape(parts[0])
		rest := parts[1]
		dstParts := strings.Split(rest, "/o/")
		dstKey, _ := url.PathUnescape(dstParts[1])

		m.mu.Lock()
		srcObj, exists := m.objects[srcKey]
		if !exists {
			m.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}

		dstObj := &mockGCSObject{
			Name:        dstKey,
			Bucket:      m.bucket,
			Data:        srcObj.Data,
			ContentType: srcObj.ContentType,
			Metadata:    srcObj.Metadata,
			Updated:     time.Now().UTC(),
			ETag:        `"copy-etag"`,
			MD5:         srcObj.MD5,
		}
		m.objects[dstKey] = dstObj
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"kind":"storage#rewriteResponse","done":true,"totalBytesRewritten":"100","objectSize":"100"}`))
		return
	}

	// List objects: subPath is empty
	if subPath == "" {
		m.mu.RLock()
		queryPrefix := r.URL.Query().Get("prefix")
		delimiter := r.URL.Query().Get("delimiter")

		var items []gcsObjectResource
		prefixesMap := make(map[string]bool)

		for _, obj := range m.objects {
			if queryPrefix != "" && !strings.HasPrefix(obj.Name, queryPrefix) {
				continue
			}

			if delimiter != "" {
				rel := strings.TrimPrefix(obj.Name, queryPrefix)
				if idx := strings.Index(rel, delimiter); idx >= 0 {
					commonPrefix := queryPrefix + rel[:idx+len(delimiter)]
					prefixesMap[commonPrefix] = true
					continue
				}
			}

			items = append(items, *m.objectToResource(obj))
		}
		m.mu.RUnlock()

		var commonPrefixes []string
		for p := range prefixesMap {
			commonPrefixes = append(commonPrefixes, p)
		}

		res := struct {
			Kind     string              `json:"kind"`
			Items    []gcsObjectResource `json:"items"`
			Prefixes []string            `json:"prefixes"`
		}{
			Kind:     "storage#objects",
			Items:    items,
			Prefixes: commonPrefixes,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
		return
	}

	// Single object operations: subPath is key
	key, _ := url.PathUnescape(subPath)

	m.mu.RLock()
	obj, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not Found","errors":[{"reason":"notFound"}]}}`))
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Download with alt=media
		if r.URL.Query().Get("alt") == "media" {
			data := obj.Data
			rangeHeader := r.Header.Get("Range")

			for k, v := range obj.Metadata {
				w.Header().Set("x-goog-meta-"+k, v)
			}
			w.Header().Set("ETag", obj.ETag)
			w.Header().Set("Last-Modified", obj.Updated.Format(http.TimeFormat))

			if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
				rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
				parts := strings.Split(rangeSpec, "-")
				start, _ := strconv.ParseInt(parts[0], 10, 64)
				end := int64(len(data) - 1)
				if len(parts) > 1 && parts[1] != "" {
					end, _ = strconv.ParseInt(parts[1], 10, 64)
				}
				if end >= int64(len(data)) {
					end = int64(len(data) - 1)
				}

				slice := data[start : end+1]
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(slice)
				return
			}

			w.Header().Set("Content-Type", obj.ContentType)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}

		// Head / Metadata inspection
		res := m.objectToResource(obj)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)

	case http.MethodDelete:
		m.mu.Lock()
		delete(m.objects, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (m *mockGCSServer) objectToResource(obj *mockGCSObject) *gcsObjectResource {
	return &gcsObjectResource{
		Kind:        "storage#object",
		ID:          fmt.Sprintf("%s/%s", obj.Bucket, obj.Name),
		Name:        obj.Name,
		Bucket:      obj.Bucket,
		ContentType: obj.ContentType,
		Size:        json.Number(strconv.Itoa(len(obj.Data))),
		ETag:        obj.ETag,
		MD5Hash:     obj.MD5,
		Updated:     obj.Updated.Format(time.RFC3339),
		Metadata:    obj.Metadata,
	}
}

func setupTestDriver(t *testing.T, mock *mockGCSServer) (*Driver, *httptest.Server) {
	var serverURL string
	server := httptest.NewServer(mock.handler(&serverURL))
	serverURL = server.URL

	cfg := Config{
		Name:              "gcs-test",
		Bucket:            mock.bucket,
		BearerToken:       "test-bearer-token",
		StorageAPIBaseURL: serverURL + "/storage/v1",
		UploadAPIBaseURL:  serverURL + "/upload/storage/v1",
		PublicBaseURL:     serverURL + "/public",
		HTTPClient:        server.Client(),
	}

	driver, err := NewDriver(cfg)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	return driver, server
}

// -------------------------------------------------------------
// Unit and Integration Tests
// -------------------------------------------------------------

func TestConfig_Validation(t *testing.T) {
	// 1. Missing bucket
	cfg := Config{
		BearerToken: "token",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing bucket")
	}

	// 2. Missing auth
	cfg = Config{
		Bucket: "my-bucket",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing auth")
	}

	// 3. Invalid chunk size
	cfg = Config{
		Bucket:      "my-bucket",
		BearerToken: "token",
		ChunkSize:   100, // not multiple of 256 KiB
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on unaligned chunk size")
	}

	// 4. Valid config with defaults
	cfg = Config{
		Bucket:      "my-bucket",
		BearerToken: "token",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if cfg.Name != "gcs" {
		t.Errorf("expected name 'gcs', got %s", cfg.Name)
	}
	if cfg.ChunkSize != DefaultChunkSize {
		t.Errorf("expected chunk size %d, got %d", DefaultChunkSize, cfg.ChunkSize)
	}
}

func TestDriver_Capabilities(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	caps := driver.Capabilities()
	if caps&blobkit.CapDirectPut == 0 {
		t.Error("missing CapDirectPut")
	}
	if caps&blobkit.CapByteRangeGet == 0 {
		t.Error("missing CapByteRangeGet")
	}
	if caps&blobkit.CapCopy == 0 {
		t.Error("missing CapCopy")
	}
	if caps&blobkit.CapBatchDelete == 0 {
		t.Error("missing CapBatchDelete")
	}
	if caps&blobkit.CapMultipartSession == 0 {
		t.Error("missing CapMultipartSession")
	}
	if caps&blobkit.CapPresignGet != 0 {
		t.Error("CapPresignGet should not be set without RSA private key")
	}
}

func TestDriver_CRUD_And_ByteRange(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	key := "docs/readme.txt"
	content := []byte("Hello Google Cloud Storage World!")

	// 1. Put
	putObj, err := driver.Put(ctx, &blobkit.Object{
		Key:         key,
		ContentType: "text/plain",
	}, bytes.NewReader(content), blobkit.PutOptions{
		Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if putObj.Key != key || putObj.Size != int64(len(content)) {
		t.Fatalf("unexpected object: %+v", putObj)
	}

	// 2. Head
	headObj, err := driver.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if headObj.Key != key || headObj.Size != int64(len(content)) {
		t.Fatalf("unexpected head object: %+v", headObj)
	}

	// 3. Get Full
	reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != string(content) {
		t.Fatalf("expected %q, got %q", string(content), string(body))
	}

	// 4. Get Byte Range (bytes 0-4 -> "Hello")
	rangeReader, err := driver.Get(ctx, key, blobkit.GetOptions{
		Range: "bytes=0-4",
	})
	if err != nil {
		t.Fatalf("Get range failed: %v", err)
	}
	defer rangeReader.Close()

	rangeBody, err := io.ReadAll(rangeReader)
	if err != nil {
		t.Fatalf("failed to read range body: %v", err)
	}
	if string(rangeBody) != "Hello" {
		t.Fatalf("expected 'Hello', got %q", string(rangeBody))
	}

	// 5. Delete
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 6. Head 404
	_, err = driver.Head(ctx, key)
	if err == nil || !blobkit.IsNotFound(err) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestDriver_MultipartWithMetadata(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	key := "images/banner.png"
	content := []byte("\x89PNG\r\n\x1a\nfake-image-bytes")

	meta := map[string]string{
		"author":      "Alice",
		"environment": "staging",
	}

	putObj, err := driver.Put(ctx, &blobkit.Object{
		Key:         key,
		ContentType: "image/png",
	}, bytes.NewReader(content), blobkit.PutOptions{
		Size:     int64(len(content)),
		Metadata: meta,
	})
	if err != nil {
		t.Fatalf("Put with metadata failed: %v", err)
	}
	if putObj.Metadata["author"] != "Alice" {
		t.Fatalf("expected author Alice, got %v", putObj.Metadata)
	}

	headObj, err := driver.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if headObj.Metadata["environment"] != "staging" {
		t.Fatalf("expected staging environment, got %v", headObj.Metadata)
	}
}

func TestDriver_ResumableMultipart(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	key := "large/archive.bin"

	part1 := make([]byte, MinChunkSize) // 256 KiB
	for i := range part1 {
		part1[i] = 'A'
	}
	part2 := make([]byte, 1024) // 1 KiB final
	for i := range part2 {
		part2[i] = 'B'
	}
	totalSize := int64(len(part1) + len(part2))

	// 1. Create Multipart session
	uploadID, err := driver.CreateMultipart(ctx, &blobkit.Object{
		Key:         key,
		ContentType: "application/octet-stream",
	}, blobkit.PutOptions{
		Size: totalSize,
	})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}

	// 2. Upload Part 1
	etag1, err := driver.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(part1), int64(len(part1)))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}
	if etag1 == "" {
		t.Fatal("expected non-empty etag for part 1")
	}

	// 3. List Parts
	parts, err := driver.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}
	if len(parts) != 1 || parts[0].PartNumber != 1 {
		t.Fatalf("unexpected parts list: %+v", parts)
	}

	// 4. Upload Part 2 (completes upload)
	etag2, err := driver.UploadPart(ctx, key, uploadID, 2, bytes.NewReader(part2), int64(len(part2)))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}
	if etag2 == "" {
		t.Fatal("expected non-empty etag for part 2")
	}

	// 5. Complete Multipart
	completedObj, err := driver.CompleteMultipart(ctx, &blobkit.Object{Key: key}, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}
	if completedObj.Size != totalSize {
		t.Fatalf("expected total size %d, got %d", totalSize, completedObj.Size)
	}

	// 6. Verify object via Get
	reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get after multipart failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if int64(len(data)) != totalSize {
		t.Fatalf("expected downloaded size %d, got %d", totalSize, len(data))
	}
}

func TestDriver_AbortMultipart(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	key := "cancel/job.bin"

	uploadID, err := driver.CreateMultipart(ctx, &blobkit.Object{Key: key}, blobkit.PutOptions{Size: 1000})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}

	if err := driver.AbortMultipart(ctx, key, uploadID); err != nil {
		t.Fatalf("AbortMultipart failed: %v", err)
	}

	// List parts should now fail with invalid ID
	_, err = driver.ListParts(ctx, key, uploadID)
	if err == nil {
		t.Fatal("expected error listing parts of aborted session")
	}
}

func TestDriver_Copy_And_BatchDelete(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	srcKey := "data/original.csv"
	dstKey := "data/backup.csv"
	data := []byte("id,name\n1,Alice\n2,Bob")

	_, err := driver.Put(ctx, &blobkit.Object{Key: srcKey}, bytes.NewReader(data), blobkit.PutOptions{
		Size: int64(len(data)),
	})
	if err != nil {
		t.Fatalf("Put original failed: %v", err)
	}

	// Copy
	if err := driver.Copy(ctx, srcKey, dstKey); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Verify copied object
	copyHead, err := driver.Head(ctx, dstKey)
	if err != nil {
		t.Fatalf("Head copied object failed: %v", err)
	}
	if copyHead.Size != int64(len(data)) {
		t.Fatalf("expected size %d, got %d", len(data), copyHead.Size)
	}

	// Batch Delete
	deleted, err := driver.DeleteBatch(ctx, []string{srcKey, dstKey, "non-existent.txt"})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 3 {
		t.Fatalf("expected 3 deleted items, got %d", len(deleted))
	}
}

func TestDriver_List(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	keys := []string{
		"media/photos/a.jpg",
		"media/photos/b.jpg",
		"media/videos/movie.mp4",
		"root.txt",
	}

	for _, k := range keys {
		_, _ = driver.Put(ctx, &blobkit.Object{Key: k}, bytes.NewReader([]byte("sample")), blobkit.PutOptions{
			Size: 6,
		})
	}

	// 1. List with prefix
	res, err := driver.List(ctx, blobkit.ListOptions{
		Prefix: "media/photos/",
	})
	if err != nil {
		t.Fatalf("List prefix failed: %v", err)
	}
	if len(res.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(res.Objects))
	}

	// 2. List with delimiter
	delRes, err := driver.List(ctx, blobkit.ListOptions{
		Prefix:    "media/",
		Delimiter: "/",
	})
	if err != nil {
		t.Fatalf("List delimiter failed: %v", err)
	}
	if len(delRes.CommonPrefixes) != 2 {
		t.Fatalf("expected 2 common prefixes, got %v", delRes.CommonPrefixes)
	}
}

func TestDriver_Presign_V4(t *testing.T) {
	// Generate an RSA test key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	cfg := Config{
		Bucket:              "secure-bucket",
		BearerToken:         "dummy",
		ServiceAccountEmail: "sa-test@project.iam.gserviceaccount.com",
		PrivateKeyPEM:       privPEM,
	}

	driver, err := NewDriver(cfg)
	if err != nil {
		t.Fatalf("failed to create driver with signing key: %v", err)
	}
	defer driver.Close()

	ctx := context.Background()

	// 1. PresignGet
	presignGet, err := driver.PresignGet(ctx, "reports/q3.pdf", blobkit.PresignOptions{
		Expiry: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignGet failed: %v", err)
	}

	if !strings.Contains(presignGet.URL, "X-Goog-Algorithm=GOOG4-RSA-SHA256") {
		t.Fatalf("missing algorithm in URL: %s", presignGet.URL)
	}
	if !strings.Contains(presignGet.URL, "X-Goog-Signature=") {
		t.Fatalf("missing signature in URL: %s", presignGet.URL)
	}
	if presignGet.SignedHeaders["host"] != "storage.googleapis.com" {
		t.Fatalf("expected host header, got %v", presignGet.SignedHeaders)
	}

	// 2. PresignPut
	presignPut, err := driver.PresignPut(ctx, "uploads/avatar.png", blobkit.PresignOptions{
		Expiry:      10 * time.Minute,
		ContentType: "image/png",
	})
	if err != nil {
		t.Fatalf("PresignPut failed: %v", err)
	}
	if !strings.Contains(presignPut.URL, "X-Goog-Signature=") {
		t.Fatalf("missing signature in Put URL: %s", presignPut.URL)
	}
	if presignPut.SignedHeaders["content-type"] != "image/png" {
		t.Fatalf("expected signed content-type header, got %v", presignPut.SignedHeaders)
	}

	// Verify capabilities now include Presign
	caps := driver.Capabilities()
	if caps&blobkit.CapPresignGet == 0 || caps&blobkit.CapPresignPut == 0 {
		t.Fatal("expected Presign capabilities enabled")
	}
}

func TestDriver_ErrorScrubbing_And_Mapping(t *testing.T) {
	errHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "server-error") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":503,"message":"Busy with Bearer ya29.secret_token and X-Goog-Signature=abcdef123456"}}`))
			return
		}
		if strings.Contains(r.URL.Path, "precondition") {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`{"error":{"code":412,"message":"conditionNotMet"}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":404,"message":"notFound"}}`))
	})

	server := httptest.NewServer(errHandler)
	defer server.Close()

	cfg := Config{
		Bucket:            "test-bucket",
		BearerToken:       "ya29.test-secret-token",
		StorageAPIBaseURL: server.URL + "/storage/v1",
		UploadAPIBaseURL:  server.URL + "/upload/storage/v1",
		HTTPClient:        server.Client(),
	}

	driver, _ := NewDriver(cfg)
	defer driver.Close()

	ctx := context.Background()

	// 1. 404
	err := driver.Delete(ctx, "missing.txt")
	// Delete is idempotent on 404, returns nil
	if err != nil {
		t.Fatalf("expected nil for idempotent delete on 404, got %v", err)
	}

	_, err = driver.Head(ctx, "missing.txt")
	if !blobkit.IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}

	// 2. 412 Precondition Failed
	_, err = driver.Head(ctx, "precondition")
	if err == nil || !blobkit.IsPreconditionFailed(err) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	// 3. 503 Server Unavailable + Scrubbing
	_, err = driver.Head(ctx, "server-error")
	if err == nil || !blobkit.IsProviderUnavailable(err) {
		t.Fatalf("expected ErrProviderUnavailable, got %v", err)
	}
	errStr := err.Error()
	if strings.Contains(errStr, "secret_token") || strings.Contains(errStr, "abcdef123456") {
		t.Fatalf("secret leaked in error message: %s", errStr)
	}
	if !strings.Contains(errStr, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] in error message: %s", errStr)
	}
}

func TestDriver_Concurrency(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	workers := 20

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent/%d.txt", id)
			data := []byte(fmt.Sprintf("worker data %d", id))

			// Put
			_, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(data), blobkit.PutOptions{
				Size: int64(len(data)),
			})
			if err != nil {
				t.Errorf("worker %d Put failed: %v", id, err)
				return
			}

			// Head
			head, err := driver.Head(ctx, key)
			if err != nil {
				t.Errorf("worker %d Head failed: %v", id, err)
				return
			}
			if head.Size != int64(len(data)) {
				t.Errorf("worker %d size mismatch", id)
			}

			// Get
			r, err := driver.Get(ctx, key, blobkit.GetOptions{})
			if err != nil {
				t.Errorf("worker %d Get failed: %v", id, err)
				return
			}
			defer r.Close()
			readBytes, _ := io.ReadAll(r)
			if string(readBytes) != string(data) {
				t.Errorf("worker %d content mismatch", id)
			}

			// Delete
			if err := driver.Delete(ctx, key); err != nil {
				t.Errorf("worker %d Delete failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestDriver_IntegrationWithBlobKitClient(t *testing.T) {
	mock := newMockGCSServer("client-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := []byte("Client Integration with GCS Driver")

	// 1. Client.Put
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "gcs_client",
		Filename:  "sample.txt",
	})
	if err != nil {
		t.Fatalf("Client.Put failed: %v", err)
	}
	if obj.Provider != "gcs-test" {
		t.Fatalf("expected provider gcs-test, got %s", obj.Provider)
	}
	if obj.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), obj.Size)
	}

	// 2. Client.Get
	r, err := client.Get(ctx, obj.Key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Client.Get failed: %v", err)
	}
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !bytes.Equal(data, payload) {
		t.Fatal("downloaded bytes mismatch")
	}

	// 3. Client.PermanentDelete
	if err := client.PermanentDelete(ctx, obj.Key); err != nil {
		t.Fatalf("Client.PermanentDelete failed: %v", err)
	}

	// 4. Verify deleted
	_, err = client.Head(ctx, obj.Key)
	if !blobkit.IsNotFound(err) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}

func TestDriver_StreamingPut_AutoResumable_And_Security(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	ctx := context.Background()

	// Configure tiny threshold so upload switches to resumable upload protocol
	driver.cfg.MultipartThreshold = 256 * 1024 // 256 KiB
	driver.cfg.ChunkSize = 256 * 1024          // 256 KiB (MinChunkSize)

	payload := bytes.Repeat([]byte("gcs-resumable-chunk-data-"), 20000) // ~500 KiB > 256 KiB
	key := "autoresumable/sample.dat"

	putObj, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(payload), blobkit.PutOptions{
		Size:     int64(len(payload)),
		Metadata: map[string]string{"env": "prod", "tier": "gold"},
	})
	if err != nil {
		t.Fatalf("Put with autoresumable failed: %v", err)
	}

	if putObj.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), putObj.Size)
	}
	if putObj.Metadata["tier"] != "gold" {
		t.Fatalf("expected gold tier metadata, got: %v", putObj.Metadata)
	}

	// Verify download matches
	reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get autoresumable blob failed: %v", err)
	}
	readBytes, err := io.ReadAll(reader.Body)
	reader.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(readBytes, payload) {
		t.Fatal("autoresumable content mismatch")
	}

	// Path traversal rejection
	_, err = driver.Put(ctx, &blobkit.Object{Key: "../../../etc/shadow"}, strings.NewReader("bad"), blobkit.PutOptions{})
	if !errors.Is(err, blobkit.ErrSecurityViolation) {
		t.Fatalf("expected ErrSecurityViolation for path traversal, got: %v", err)
	}
}

