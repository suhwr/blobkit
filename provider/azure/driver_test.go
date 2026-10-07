package azure_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
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
	"github.com/suhwr/blobkit/provider/azure"
)

type mockBlob struct {
	data        []byte
	contentType string
	etag        string
	modTime     time.Time
	metadata    map[string]string
}

type mockAzureServer struct {
	mu          sync.Mutex
	container   string
	blobs       map[string]*mockBlob
	stagedParts map[string]map[string][]byte // blobKey -> blockID -> data
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

func newMockAzureServer(container string) *mockAzureServer {
	return &mockAzureServer{
		container:   container,
		blobs:       make(map[string]*mockBlob),
		stagedParts: make(map[string]map[string][]byte),
	}
}

func (s *mockAzureServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cleanPath := strings.Trim(r.URL.Path, "/")
	parts := strings.SplitN(cleanPath, "/", 2)

	if len(parts) == 0 || parts[0] != s.container {
		http.NotFound(w, r)
		return
	}

	// Container level operation: GET /container?restype=container&comp=list
	if len(parts) == 1 {
		if r.URL.Query().Get("comp") == "list" && r.Method == http.MethodGet {
			prefix := r.URL.Query().Get("prefix")
			delimiter := r.URL.Query().Get("delimiter")

			var xmlBuf bytes.Buffer
			xmlBuf.WriteString(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>`)

			seenPrefixes := make(map[string]bool)
			for k, b := range s.blobs {
				if prefix != "" && !strings.HasPrefix(k, prefix) {
					continue
				}

				if delimiter != "" {
					sub := strings.TrimPrefix(k, prefix)
					idx := strings.Index(sub, delimiter)
					if idx >= 0 {
						p := prefix + sub[:idx+len(delimiter)]
						if !seenPrefixes[p] {
							seenPrefixes[p] = true
							xmlBuf.WriteString(fmt.Sprintf("<BlobPrefix><Name>%s</Name></BlobPrefix>", p))
						}
						continue
					}
				}

				xmlBuf.WriteString(fmt.Sprintf("<Blob><Name>%s</Name><Properties><Content-Length>%d</Content-Length><Content-Type>%s</Content-Type><Etag>%s</Etag><Last-Modified>%s</Last-Modified></Properties></Blob>",
					k, len(b.data), b.contentType, b.etag, b.modTime.Format(http.TimeFormat)))
			}

			xmlBuf.WriteString(`</Blobs></EnumerationResults>`)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(xmlBuf.Bytes())
			return
		}

		http.NotFound(w, r)
		return
	}

	blobKey := parts[1]

	switch r.Method {
	case http.MethodPut:
		comp := r.URL.Query().Get("comp")

		// 1. Stage Block: PUT ?comp=block&blockid=...
		if comp == "block" {
			blockID := r.URL.Query().Get("blockid")
			body, _ := io.ReadAll(r.Body)

			if s.stagedParts[blobKey] == nil {
				s.stagedParts[blobKey] = make(map[string][]byte)
			}
			s.stagedParts[blobKey][blockID] = body
			w.WriteHeader(http.StatusCreated)
			return
		}

		// 2. Commit Block List: PUT ?comp=blocklist
		if comp == "blocklist" {
			type blockListReq struct {
				XMLName xml.Name `xml:"BlockList"`
				Latest  []string `xml:"Latest"`
			}
			var bl blockListReq
			body, _ := io.ReadAll(r.Body)
			_ = xml.Unmarshal(body, &bl)

			var assembled []byte
			for _, id := range bl.Latest {
				partData := s.stagedParts[blobKey][id]
				assembled = append(assembled, partData...)
			}

			h := md5.Sum(assembled)
			etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(h[:]))

			meta := make(map[string]string)
			for k, v := range r.Header {
				lower := strings.ToLower(k)
				if strings.HasPrefix(lower, "x-ms-meta-") {
					meta[strings.TrimPrefix(lower, "x-ms-meta-")] = strings.Join(v, ",")
				}
			}

			ct := r.Header.Get("x-ms-blob-content-type")
			if ct == "" {
				ct = "application/octet-stream"
			}

			s.blobs[blobKey] = &mockBlob{
				data:        assembled,
				contentType: ct,
				etag:        etag,
				modTime:     time.Now().UTC(),
				metadata:    meta,
			}
			delete(s.stagedParts, blobKey)

			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusCreated)
			return
		}

		// 3. Server-side Copy: PUT with x-ms-copy-source
		if copySrc := r.Header.Get("x-ms-copy-source"); copySrc != "" {
			u, _ := url.Parse(copySrc)
			srcParts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
			if len(srcParts) == 2 {
				srcBlob := s.blobs[srcParts[1]]
				if srcBlob == nil {
					http.NotFound(w, r)
					return
				}
				s.blobs[blobKey] = &mockBlob{
					data:        srcBlob.data,
					contentType: srcBlob.contentType,
					etag:        srcBlob.etag,
					modTime:     time.Now().UTC(),
					metadata:    srcBlob.metadata,
				}
				w.WriteHeader(http.StatusAccepted)
				return
			}
		}

		// 4. Direct Put BlockBlob
		body, _ := io.ReadAll(r.Body)
		h := md5.Sum(body)
		etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(h[:]))

		meta := make(map[string]string)
		for k, v := range r.Header {
			lower := strings.ToLower(k)
			if strings.HasPrefix(lower, "x-ms-meta-") {
				meta[strings.TrimPrefix(lower, "x-ms-meta-")] = strings.Join(v, ",")
			}
		}

		ct := r.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}

		s.blobs[blobKey] = &mockBlob{
			data:        body,
			contentType: ct,
			etag:        etag,
			modTime:     time.Now().UTC(),
			metadata:    meta,
		}

		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusCreated)
		return

	case http.MethodGet:
		// 1. List Blocks: GET ?comp=blocklist
		if r.URL.Query().Get("comp") == "blocklist" {
			var xmlBuf bytes.Buffer
			xmlBuf.WriteString(`<?xml version="1.0" encoding="utf-8"?><BlockList><UncommittedBlocks>`)
			for id, data := range s.stagedParts[blobKey] {
				xmlBuf.WriteString(fmt.Sprintf("<Block><Name>%s</Name><Size>%d</Size></Block>", id, len(data)))
			}
			xmlBuf.WriteString(`</UncommittedBlocks></BlockList>`)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(xmlBuf.Bytes())
			return
		}

		b, ok := s.blobs[blobKey]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><Error><Code>BlobNotFound</Code></Error>`))
			return
		}

		if err := blobkit.CheckPreconditions(b.etag, b.modTime, blobkit.GetOptions{
			IfMatch:           r.Header.Get("If-Match"),
			IfNoneMatch:       r.Header.Get("If-None-Match"),
			IfModifiedSince:   parseHTTPTime(r.Header.Get("If-Modified-Since")),
			IfUnmodifiedSince: parseHTTPTime(r.Header.Get("If-Unmodified-Since")),
		}); err != nil {
			if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
				w.WriteHeader(http.StatusNotModified)
			} else {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><Error><Code>ConditionNotMet</Code></Error>`))
			}
			return
		}

		w.Header().Set("Content-Type", b.contentType)
		w.Header().Set("ETag", b.etag)
		w.Header().Set("Last-Modified", b.modTime.Format(http.TimeFormat))
		for k, v := range b.metadata {
			w.Header().Set(fmt.Sprintf("x-ms-meta-%s", k), v)
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
			parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			var end int64 = int64(len(b.data) - 1)
			if len(parts) > 1 && parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}

			if start < 0 || start > int64(len(b.data)) || end < start {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= int64(len(b.data)) {
				end = int64(len(b.data) - 1)
			}

			chunk := b.data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b.data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(chunk)
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(b.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b.data)
		return

	case http.MethodHead:
		b, ok := s.blobs[blobKey]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if err := blobkit.CheckPreconditions(b.etag, b.modTime, blobkit.GetOptions{
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

		w.Header().Set("Content-Length", strconv.Itoa(len(b.data)))
		w.Header().Set("Content-Type", b.contentType)
		w.Header().Set("ETag", b.etag)
		w.Header().Set("Last-Modified", b.modTime.Format(http.TimeFormat))
		for k, v := range b.metadata {
			w.Header().Set(fmt.Sprintf("x-ms-meta-%s", k), v)
		}
		w.WriteHeader(http.StatusOK)
		return

	case http.MethodDelete:
		if _, ok := s.blobs[blobKey]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		delete(s.blobs, blobKey)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	http.NotFound(w, r)
}

func setupTestDriver(t *testing.T, handler http.Handler) (*azure.Driver, *httptest.Server) {
	server := httptest.NewServer(handler)

	// Mock valid base64 key: 32 bytes
	mockKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("A"), 32))

	cfg := azure.Config{
		Name:           "azure-test",
		AccountName:    "mockstorageaccount",
		AccountKey:     mockKey,
		Container:      "blobs",
		CustomEndpoint: server.URL,
		PublicBaseURL:  "https://cdn.example.com",
		HTTPClient:     server.Client(),
	}

	driver, err := azure.NewDriver(cfg)
	if err != nil {
		server.Close()
		t.Fatalf("failed to create driver: %v", err)
	}

	return driver, server
}

func TestConfig_Validation(t *testing.T) {
	// Missing account
	cfg := azure.Config{Container: "blobs"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing AccountName")
	}

	// Missing container
	cfg = azure.Config{AccountName: "acc"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing Container")
	}

	// Valid config
	cfg = azure.Config{
		AccountName: "myaccount",
		Container:   "mycontainer",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.EndpointURL() != "https://myaccount.blob.core.windows.net" {
		t.Fatalf("unexpected default endpoint: %s", cfg.EndpointURL())
	}
}

func TestDriver_Capabilities(t *testing.T) {
	mock := newMockAzureServer("blobs")
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
	if caps&blobkit.CapPresignGet == 0 {
		t.Fatal("expected CapPresignGet")
	}
	if caps&blobkit.CapPresignPut == 0 {
		t.Fatal("expected CapPresignPut")
	}
	if caps&blobkit.CapCopy == 0 {
		t.Fatal("expected CapCopy")
	}
	if caps&blobkit.CapBatchDelete == 0 {
		t.Fatal("expected CapBatchDelete")
	}
}

func TestDriver_CRUD_And_ByteRange(t *testing.T) {
	ctx := context.Background()
	mock := newMockAzureServer("blobs")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	payload := bytes.Repeat([]byte("BlobKitAzureBlockBlobData!"), 500)
	key := "reports/2026/q3_summary.pdf"
	obj := &blobkit.Object{
		Key:         key,
		ContentType: "application/pdf",
		Metadata: map[string]string{
			"dept": "finance",
		},
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
		t.Fatalf("expected application/pdf, got %s", saved.ContentType)
	}

	// 2. Head
	head, err := driver.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if head.Size != int64(len(payload)) {
		t.Fatalf("head size mismatch: %d != %d", head.Size, len(payload))
	}
	if head.Metadata["dept"] != "finance" {
		t.Fatalf("metadata mismatch: %v", head.Metadata)
	}

	// 3. Get Full
	r, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("downloaded bytes mismatch")
	}

	// 4. Get with Byte Range
	offset := int64(100)
	length := int64(300)
	rangeR, err := driver.Get(ctx, key, blobkit.GetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", offset, offset+length-1),
	})
	if err != nil {
		t.Fatalf("Get with Range failed: %v", err)
	}
	rangeData, err := io.ReadAll(rangeR.Body)
	rangeR.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll range failed: %v", err)
	}
	if int64(len(rangeData)) != length {
		t.Fatalf("expected length %d, got %d", length, len(rangeData))
	}
	if !bytes.Equal(rangeData, payload[offset:offset+length]) {
		t.Fatal("range data mismatch")
	}

	// 5. Delete
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Head after Delete returns ErrObjectNotFound
	_, err = driver.Head(ctx, key)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	// Delete again should be idempotent
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("expected idempotent delete, got: %v", err)
	}
}

func TestDriver_BlockBlobMultipart(t *testing.T) {
	ctx := context.Background()
	mock := newMockAzureServer("blobs")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	key := "large/dataset.parquet"
	obj := &blobkit.Object{
		Key:         key,
		ContentType: "application/octet-stream",
	}

	chunk1 := bytes.Repeat([]byte("A"), 4000)
	chunk2 := bytes.Repeat([]byte("B"), 6000)
	totalExpected := append(chunk1, chunk2...)

	// 1. CreateMultipart
	uploadID, err := driver.CreateMultipart(ctx, obj, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}

	// 2. UploadPart 1 & 2
	etag1, err := driver.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(chunk1), int64(len(chunk1)))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}
	etag2, err := driver.UploadPart(ctx, key, uploadID, 2, bytes.NewReader(chunk2), int64(len(chunk2)))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	// 3. ListParts
	parts, err := driver.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}

	// 4. CompleteMultipart
	completedParts := []blobkit.CompletedPart{
		{PartNumber: 1, ETag: etag1, Size: int64(len(chunk1))},
		{PartNumber: 2, ETag: etag2, Size: int64(len(chunk2))},
	}
	completed, err := driver.CompleteMultipart(ctx, obj, uploadID, completedParts)
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}
	if completed.Size != int64(len(totalExpected)) {
		t.Fatalf("expected size %d, got %d", len(totalExpected), completed.Size)
	}

	// 5. Verify payload
	r, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get merged blob failed: %v", err)
	}
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !bytes.Equal(data, totalExpected) {
		t.Fatal("merged blob content mismatch")
	}
}

func TestDriver_COPY_And_BatchDelete(t *testing.T) {
	ctx := context.Background()
	mock := newMockAzureServer("blobs")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	payload := []byte("Azure copy content")
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
	head, err := driver.Head(ctx, dstKey)
	if err != nil {
		t.Fatalf("Head dstKey failed: %v", err)
	}
	if head.Size != int64(len(payload)) {
		t.Fatalf("copy size mismatch: %d != %d", head.Size, len(payload))
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
		t.Fatalf("expected srcKey deleted, got: %v", err)
	}
}

func TestDriver_List(t *testing.T) {
	ctx := context.Background()
	mock := newMockAzureServer("blobs")
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
		t.Fatalf("expected 2 common prefixes, got %v", delRes.CommonPrefixes)
	}
}

func TestDriver_Presign_SAS(t *testing.T) {
	mock := newMockAzureServer("blobs")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	ctx := context.Background()

	// 1. PresignGet
	presignGet, err := driver.PresignGet(ctx, "data/file.csv", blobkit.PresignOptions{
		Expiry: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignGet failed: %v", err)
	}
	if !strings.Contains(presignGet.URL, "sp=r") || !strings.Contains(presignGet.URL, "sig=") {
		t.Fatalf("invalid SAS read URL: %s", presignGet.URL)
	}

	// 2. PresignPut
	presignPut, err := driver.PresignPut(ctx, "data/upload.csv", blobkit.PresignOptions{
		Expiry: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPut failed: %v", err)
	}
	if !strings.Contains(presignPut.URL, "sp=w") || !strings.Contains(presignPut.URL, "sig=") {
		t.Fatalf("invalid SAS write URL: %s", presignPut.URL)
	}
	if presignPut.SignedHeaders["x-ms-blob-type"] != "BlockBlob" {
		t.Fatalf("expected header x-ms-blob-type: BlockBlob, got: %v", presignPut.SignedHeaders)
	}
}

func TestDriver_ErrorScrubbing_And_Mapping(t *testing.T) {
	errHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "server-error") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><Error><Code>ServerBusy</Code><Message>Busy with sig=super_secret_sig</Message></Error>`))
			return
		}
		http.NotFound(w, r)
	})

	server := httptest.NewServer(errHandler)
	defer server.Close()

	cfg := azure.Config{
		AccountName:    "mockacc",
		AccountKey:     base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("B"), 32)),
		Container:      "blobs",
		CustomEndpoint: server.URL,
		HTTPClient:     server.Client(),
	}
	driver, err := azure.NewDriver(cfg)
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	// 1. Not Found
	_, err = driver.Head(context.Background(), "missing")
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	// 2. Server Busy mapping & signature scrubbing
	_, err = driver.Head(context.Background(), "server-error")
	if !errors.Is(err, blobkit.ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable, got: %v", err)
	}
	if strings.Contains(err.Error(), "super_secret_sig") {
		t.Fatalf("error leaked secret signature: %v", err)
	}
}

func TestDriver_Concurrency(t *testing.T) {
	mock := newMockAzureServer("blobs")
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
			payload := []byte(fmt.Sprintf("azure data %02d", idx))

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
	mock := newMockAzureServer("blobs")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := []byte("Client Integration with Azure Blob Storage Driver")

	// 1. Client.Put
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "azure_client",
		Filename:  "sample.txt",
	})
	if err != nil {
		t.Fatalf("Client.Put failed: %v", err)
	}
	if obj.Provider != "azure-test" {
		t.Fatalf("expected provider azure-test, got %s", obj.Provider)
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

func TestDriver_StreamingPut_AutoChunking_And_Security(t *testing.T) {
	mock := newMockAzureServer("blobs")
	server := httptest.NewServer(mock)
	defer server.Close()
	ctx := context.Background()

	mockKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("A"), 32))
	cfg := azure.Config{
		Name:               "azure-test",
		AccountName:        "mockstorageaccount",
		AccountKey:         mockKey,
		Container:          "blobs",
		CustomEndpoint:     server.URL,
		MultipartThreshold: 256,
		PartSize:           128,
		HTTPClient:         server.Client(),
	}
	driver, err := azure.NewDriver(cfg)
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}

	payload := bytes.Repeat([]byte("multipart-chunk-data-"), 50) // ~1050 bytes > 256
	key := "autochunk/test.bin"

	putObj, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(payload), blobkit.PutOptions{
		Size:     int64(len(payload)),
		Metadata: map[string]string{"uploader": "chunk-bot"},
	})
	if err != nil {
		t.Fatalf("Put with autochunk failed: %v", err)
	}

	if putObj.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), putObj.Size)
	}
	if putObj.Metadata["uploader"] != "chunk-bot" {
		t.Fatalf("expected uploader metadata chunk-bot, got: %v", putObj.Metadata)
	}

	// Verify download matches
	reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get autochunk blob failed: %v", err)
	}
	readBytes, err := io.ReadAll(reader.Body)
	reader.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(readBytes, payload) {
		t.Fatal("autochunk content mismatch")
	}

	// Path traversal attack rejection
	_, err = driver.Put(ctx, &blobkit.Object{Key: "../../etc/passwd"}, strings.NewReader("bad"), blobkit.PutOptions{})
	if !errors.Is(err, blobkit.ErrSecurityViolation) {
		t.Fatalf("expected ErrSecurityViolation for traversal, got: %v", err)
	}
}
