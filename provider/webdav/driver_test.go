package webdav_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/webdav"
)

type mockResource struct {
	isDir       bool
	data        []byte
	contentType string
	etag        string
	modTime     time.Time
}

type mockWebDAVServer struct {
	mu        sync.Mutex
	resources map[string]*mockResource
}

func parseHTTPTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(http.TimeFormat, s)
	if err != nil {
		return nil
	}
	return &t
}

func newMockWebDAVServer() *mockWebDAVServer {
	return &mockWebDAVServer{
		resources: map[string]*mockResource{
			"/": {isDir: true, modTime: time.Now().UTC()},
		},
	}
}

func (s *mockWebDAVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqPath := strings.TrimRight(r.URL.Path, "/")
	if reqPath == "" {
		reqPath = "/"
	}

	switch r.Method {
	case "MKCOL":
		if _, exists := s.resources[reqPath]; exists {
			w.WriteHeader(http.StatusMethodNotAllowed) // 405 already exists
			return
		}
		s.resources[reqPath] = &mockResource{
			isDir:   true,
			modTime: time.Now().UTC(),
		}
		w.WriteHeader(http.StatusCreated)
		return

	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		h := md5.Sum(body)
		etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(h[:]))

		ct := r.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}

		s.resources[reqPath] = &mockResource{
			isDir:       false,
			data:        body,
			contentType: ct,
			etag:        etag,
			modTime:     time.Now().UTC(),
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusCreated)
		return

	case http.MethodGet:
		res, exists := s.resources[reqPath]
		if !exists || res.isDir {
			http.NotFound(w, r)
			return
		}

		if err := blobkit.CheckPreconditions(res.etag, res.modTime, blobkit.GetOptions{
			IfMatch:           r.Header.Get("If-Match"),
			IfNoneMatch:       r.Header.Get("If-None-Match"),
			IfModifiedSince:   parseHTTPTime(r.Header.Get("If-Modified-Since")),
			IfUnmodifiedSince: parseHTTPTime(r.Header.Get("If-Unmodified-Since")),
		}); err != nil {
			if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
				w.WriteHeader(http.StatusNotModified)
			} else {
				w.WriteHeader(http.StatusPreconditionFailed)
			}
			return
		}

		w.Header().Set("Content-Type", res.contentType)
		w.Header().Set("ETag", res.etag)
		w.Header().Set("Last-Modified", res.modTime.Format(http.TimeFormat))

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
			parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			var end int64 = int64(len(res.data) - 1)
			if len(parts) > 1 && parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}

			if start < 0 || start > int64(len(res.data)) || end < start {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= int64(len(res.data)) {
				end = int64(len(res.data) - 1)
			}

			chunk := res.data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(res.data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(chunk)
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(res.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(res.data)
		return

	case http.MethodHead:
		res, exists := s.resources[reqPath]
		if !exists || res.isDir {
			http.NotFound(w, r)
			return
		}

		if err := blobkit.CheckPreconditions(res.etag, res.modTime, blobkit.GetOptions{
			IfMatch:           r.Header.Get("If-Match"),
			IfNoneMatch:       r.Header.Get("If-None-Match"),
			IfModifiedSince:   parseHTTPTime(r.Header.Get("If-Modified-Since")),
			IfUnmodifiedSince: parseHTTPTime(r.Header.Get("If-Unmodified-Since")),
		}); err != nil {
			if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
				w.WriteHeader(http.StatusNotModified)
			} else {
				w.WriteHeader(http.StatusPreconditionFailed)
			}
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(res.data)))
		w.Header().Set("Content-Type", res.contentType)
		w.Header().Set("ETag", res.etag)
		w.Header().Set("Last-Modified", res.modTime.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		return

	case http.MethodDelete:
		if _, exists := s.resources[reqPath]; !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		delete(s.resources, reqPath)
		w.WriteHeader(http.StatusNoContent)
		return

	case "COPY":
		res, exists := s.resources[reqPath]
		if !exists {
			http.NotFound(w, r)
			return
		}

		destHeader := r.Header.Get("Destination")
		destURL, err := url.Parse(destHeader)
		if err != nil {
			http.Error(w, "invalid destination", http.StatusBadRequest)
			return
		}

		dstPath := strings.TrimRight(destURL.Path, "/")
		s.resources[dstPath] = &mockResource{
			isDir:       res.isDir,
			data:        res.data,
			contentType: res.contentType,
			etag:        res.etag,
			modTime:     time.Now().UTC(),
		}
		w.WriteHeader(http.StatusCreated)
		return

	case "PROPFIND":
		depth := r.Header.Get("Depth")
		var xmlBuf bytes.Buffer
		xmlBuf.WriteString(`<?xml version="1.0" encoding="utf-8" ?><D:multistatus xmlns:D="DAV:">`)

		if depth == "0" {
			res, exists := s.resources[reqPath]
			if !exists {
				http.NotFound(w, r)
				return
			}
			s.writePropResponse(&xmlBuf, reqPath, res)
		} else {
			// Depth 1
			for path, res := range s.resources {
				if path == reqPath || strings.HasPrefix(path, reqPath+"/") {
					s.writePropResponse(&xmlBuf, path, res)
				}
			}
		}

		xmlBuf.WriteString(`</D:multistatus>`)
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(207)
		_, _ = w.Write(xmlBuf.Bytes())
		return
	}

	http.NotFound(w, r)
}

func (s *mockWebDAVServer) writePropResponse(buf *bytes.Buffer, p string, res *mockResource) {
	buf.WriteString("<D:response><D:href>")
	buf.WriteString(p)
	buf.WriteString("</D:href><D:propstat><D:prop>")
	if res.isDir {
		buf.WriteString("<D:resourcetype><D:collection/></D:resourcetype>")
	} else {
		buf.WriteString("<D:resourcetype/>")
		buf.WriteString(fmt.Sprintf("<D:getcontentlength>%d</D:getcontentlength>", len(res.data)))
		buf.WriteString(fmt.Sprintf("<D:getcontenttype>%s</D:getcontenttype>", res.contentType))
		buf.WriteString(fmt.Sprintf("<D:getetag>%s</D:getetag>", res.etag))
	}
	buf.WriteString(fmt.Sprintf("<D:getlastmodified>%s</D:getlastmodified>", res.modTime.Format(http.TimeFormat)))
	buf.WriteString("</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>")
}

func setupTestDriver(t *testing.T, handler http.Handler) (*webdav.Driver, *httptest.Server) {
	server := httptest.NewServer(handler)

	cfg := webdav.Config{
		Name:          "webdav-test",
		Endpoint:      server.URL,
		Username:      "mockuser",
		Password:      "mockpass123",
		PublicBaseURL: "https://cdn.example.com",
		HTTPClient:    server.Client(),
	}

	driver, err := webdav.NewDriver(cfg)
	if err != nil {
		server.Close()
		t.Fatalf("failed to create driver: %v", err)
	}

	return driver, server
}

func TestConfig_Validation(t *testing.T) {
	// Missing Endpoint
	cfg := webdav.Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing Endpoint")
	}

	// Invalid URL scheme
	cfg = webdav.Config{
		Endpoint: "ftp://example.com/dav",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on non-http(s) scheme")
	}

	// Valid config with defaults
	cfg = webdav.Config{
		Endpoint: "https://cloud.example.com/remote.php/webdav/",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Name != "webdav" {
		t.Fatalf("expected default Name 'webdav', got %s", cfg.Name)
	}
	if cfg.Endpoint != "https://cloud.example.com/remote.php/webdav" {
		t.Fatalf("expected trimmed endpoint, got %s", cfg.Endpoint)
	}
}

func TestDriver_Capabilities(t *testing.T) {
	mock := newMockWebDAVServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()
	payload := []byte("webdav capability behavioral execution")

	// 1. Behavioral execution of CapDirectPut
	obj, err := driver.Put(ctx, &blobkit.Object{Key: "cap-webdav.txt"}, bytes.NewReader(payload), blobkit.PutOptions{Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("CapDirectPut execution failed: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Fatalf("CapDirectPut size mismatch: %d", obj.Size)
	}

	// 2. Behavioral execution of CapByteRangeGet
	rReader, err := driver.Get(ctx, "cap-webdav.txt", blobkit.GetOptions{Range: "bytes=0-5"})
	if err != nil {
		t.Fatalf("CapByteRangeGet execution failed: %v", err)
	}
	sub, _ := io.ReadAll(rReader)
	rReader.Close()
	if string(sub) != "webdav" {
		t.Fatalf("CapByteRangeGet slice mismatch: %q", string(sub))
	}

	// 3. Behavioral execution of CapCopy
	if err := driver.Copy(ctx, "cap-webdav.txt", "cap-webdav-copy.txt"); err != nil {
		t.Fatalf("CapCopy execution failed: %v", err)
	}
	copyHead, err := driver.Head(ctx, "cap-webdav-copy.txt")
	if err != nil || copyHead.Size != int64(len(payload)) {
		t.Fatalf("CapCopy destination verification failed: %v", err)
	}

	// 4. Negative path: Unadvertised CapPresignGet MUST return ErrUnsupportedOperation
	_, pErr := driver.PresignGet(ctx, "cap-webdav.txt", blobkit.PresignOptions{})
	if !errors.Is(pErr, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for unadvertised PresignGet, got: %v", pErr)
	}
}

func TestDriver_CRUD_And_ByteRange(t *testing.T) {
	ctx := context.Background()
	mock := newMockWebDAVServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	payload := bytes.Repeat([]byte("BlobKitWebDAVTestData!"), 500) // 11,000 bytes
	key := "documents/2026/annual_report.pdf"
	obj := &blobkit.Object{
		Key:         key,
		ContentType: "application/pdf",
	}

	// 1. Put (triggers auto-MKCOL for documents and documents/2026)
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
		t.Fatalf("expected application/pdf, got %s", saved.ContentType)
	}
	if saved.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// 2. Head
	head, err := driver.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if head.Size != int64(len(payload)) {
		t.Fatalf("head size mismatch: %d != %d", head.Size, len(payload))
	}
	if head.ETag != saved.ETag {
		t.Fatalf("head ETag mismatch: %s != %s", head.ETag, saved.ETag)
	}

	// 3. Get Full
	reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, err := io.ReadAll(reader.Body)
	reader.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("downloaded payload mismatch")
	}

	// 4. Get with Byte Range
	offset := int64(50)
	length := int64(200)
	rangeReader, err := driver.Get(ctx, key, blobkit.GetOptions{
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
	expectedSlice := payload[offset : offset+length]
	if !bytes.Equal(rangeData, expectedSlice) {
		t.Fatal("range data mismatch")
	}

	// 5. Delete
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Head after Delete should return ErrObjectNotFound
	_, err = driver.Head(ctx, key)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	// Delete again should be idempotent
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("expected idempotent Delete, got: %v", err)
	}
}

func TestDriver_COPY_And_BatchDelete(t *testing.T) {
	ctx := context.Background()
	mock := newMockWebDAVServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	payload := []byte("WebDAV copy payload")
	srcKey := "photos/vacation.jpg"
	dstKey := "photos/vacation_backup.jpg"

	_, err := driver.Put(ctx, &blobkit.Object{Key: srcKey, ContentType: "image/jpeg"}, bytes.NewReader(payload), blobkit.PutOptions{Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Copy
	if err := driver.Copy(ctx, srcKey, dstKey); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Verify dstKey
	dstHead, err := driver.Head(ctx, dstKey)
	if err != nil {
		t.Fatalf("Head dstKey failed: %v", err)
	}
	if dstHead.Size != int64(len(payload)) {
		t.Fatalf("copied size mismatch: %d != %d", dstHead.Size, len(payload))
	}

	// Batch Delete
	deleted, err := driver.DeleteBatch(ctx, []string{srcKey, dstKey})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted items, got %d", len(deleted))
	}

	_, err = driver.Head(ctx, srcKey)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected srcKey deleted, got %v", err)
	}
}

func TestDriver_List(t *testing.T) {
	ctx := context.Background()
	mock := newMockWebDAVServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	// Upload items
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "media/music/track1.mp3"}, strings.NewReader("1"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "media/music/track2.mp3"}, strings.NewReader("2"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "media/videos/clip.mp4"}, strings.NewReader("3"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "docs/readme.txt"}, strings.NewReader("4"), blobkit.PutOptions{})

	// List with Prefix
	res, err := driver.List(ctx, blobkit.ListOptions{Prefix: "media/music/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(res.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(res.Objects))
	}

	// List with Delimiter
	delRes, err := driver.List(ctx, blobkit.ListOptions{Prefix: "media/", Delimiter: "/"})
	if err != nil {
		t.Fatalf("List with delimiter failed: %v", err)
	}
	if len(delRes.CommonPrefixes) != 2 {
		t.Fatalf("expected 2 common prefixes (music/, videos/), got %v", delRes.CommonPrefixes)
	}
}

func TestDriver_ResolveURL_And_Presign(t *testing.T) {
	mock := newMockWebDAVServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()

	// ResolveURL
	u, err := driver.ResolveURL("images/pic.png")
	if err != nil {
		t.Fatalf("ResolveURL failed: %v", err)
	}
	if u != "https://cdn.example.com/images/pic.png" {
		t.Fatalf("unexpected URL: %s", u)
	}

	// Presign unsupported
	_, err = driver.PresignGet(ctx, "test.txt", blobkit.PresignOptions{})
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for PresignGet, got: %v", err)
	}

	_, err = driver.PresignPut(ctx, "test.txt", blobkit.PresignOptions{})
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for PresignPut, got: %v", err)
	}
}

func TestDriver_CredentialScrubbing_And_Errors(t *testing.T) {
	errHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "unauth") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if strings.Contains(r.URL.Path, "server-down") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Service unavailable with Basic bW9ja3VzZXI6cGFzc3dvcmQ="))
			return
		}
		http.NotFound(w, r)
	})

	server := httptest.NewServer(errHandler)
	defer server.Close()

	cfg := webdav.Config{
		Endpoint:   server.URL,
		Username:   "secretuser",
		Password:   "secretpass123",
		HTTPClient: server.Client(),
	}
	driver, err := webdav.NewDriver(cfg)
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	// 1. Unauthorized
	_, err = driver.Head(context.Background(), "unauth")
	if err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected unauthorized error, got: %v", err)
	}

	// 2. Unavailable & Scrubbing
	_, err = driver.Head(context.Background(), "server-down")
	if !errors.Is(err, blobkit.ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable, got: %v", err)
	}
	if strings.Contains(err.Error(), "secretpass123") || strings.Contains(err.Error(), "bW9ja3VzZXI6cGFzc3dvcmQ=") {
		t.Fatalf("error leaked credentials: %v", err)
	}
}

func TestDriver_Concurrency(t *testing.T) {
	mock := newMockWebDAVServer()
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
			key := fmt.Sprintf("concurrent/worker_%02d.txt", idx)
			payload := []byte(fmt.Sprintf("worker data %02d", idx))

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

func TestDriver_IntegrationWithBlobKitClient(t *testing.T) {
	mock := newMockWebDAVServer()
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := []byte("Client Integration with WebDAV Storage Driver")

	// 1. Client.Put
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "webdav_client",
		Filename:  "sample.txt",
	})
	if err != nil {
		t.Fatalf("Client.Put failed: %v", err)
	}
	if obj.Provider != "webdav-test" {
		t.Fatalf("expected provider webdav-test, got %s", obj.Provider)
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
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}
