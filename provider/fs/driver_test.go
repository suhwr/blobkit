package fs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/fs"
)

func newTestDriver(t *testing.T) (*fs.Driver, string) {
	tempDir := t.TempDir()
	driver, err := fs.NewDriver(fs.Config{
		Name:              "fs-test",
		RootDir:           tempDir,
		EnableSidecarMeta: true,
	})
	if err != nil {
		t.Fatalf("failed to create fs driver: %v", err)
	}
	return driver, tempDir
}

func TestConfig_Validation(t *testing.T) {
	// Missing RootDir
	cfg := fs.Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing RootDir")
	}

	// Valid config with auto-created dir
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "auto_created_blobs")
	cfg = fs.Config{
		RootDir: targetDir,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		t.Fatal("expected RootDir to be created automatically")
	}
	if cfg.DirMode != 0755 || cfg.FileMode != 0644 {
		t.Fatalf("expected default modes 0755/0644, got %v/%v", cfg.DirMode, cfg.FileMode)
	}
}

func TestDriver_PathTraversalSecurity(t *testing.T) {
	driver, _ := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	maliciousKeys := []string{
		"../escape.txt",
		"../../etc/passwd",
		"foo/../../bar.txt",
		"/absolute/path.txt",
		"nested/../../../root.txt",
		"null\x00byte.txt",
		".staging/exploit.txt",
		"target.meta.json",
	}

	for _, k := range maliciousKeys {
		_, err := driver.Put(ctx, &blobkit.Object{Key: k}, strings.NewReader("bad"), blobkit.PutOptions{})
		if !errors.Is(err, blobkit.ErrSecurityViolation) && !errors.Is(err, blobkit.ErrInvalidKey) {
			t.Fatalf("expected ErrSecurityViolation or ErrInvalidKey for malicious key %q, got: %v", k, err)
		}

		_, err = driver.Get(ctx, k, blobkit.GetOptions{})
		if !errors.Is(err, blobkit.ErrSecurityViolation) && !errors.Is(err, blobkit.ErrInvalidKey) {
			t.Fatalf("expected ErrSecurityViolation or ErrInvalidKey for malicious key %q, got: %v", k, err)
		}

		err = driver.Delete(ctx, k)
		if !errors.Is(err, blobkit.ErrSecurityViolation) && !errors.Is(err, blobkit.ErrInvalidKey) {
			t.Fatalf("expected ErrSecurityViolation or ErrInvalidKey for malicious key %q, got: %v", k, err)
		}
	}
}

func TestDriver_CRUD_And_ByteRange(t *testing.T) {
	driver, _ := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	payload := bytes.Repeat([]byte("BlobKitLocalFSTestData!"), 1000) // 23,000 bytes
	key := "documents/2026/report.pdf"
	obj := &blobkit.Object{
		Key:         key,
		ContentType: "application/pdf",
		Metadata: map[string]string{
			"author": "shiroine",
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
	if head.Metadata["author"] != "shiroine" {
		t.Fatalf("head metadata mismatch: %v", head.Metadata)
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
		t.Fatal("downloaded data mismatch")
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

	// Verify Head returns ErrObjectNotFound
	_, err = driver.Head(ctx, key)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}

	// Delete again should be idempotent
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("expected idempotent delete, got: %v", err)
	}
}

func TestDriver_AtomicOverwrite(t *testing.T) {
	driver, _ := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	key := "config/app.json"
	v1 := []byte(`{"version": 1}`)
	v2 := []byte(`{"version": 2, "active": true}`)

	obj := &blobkit.Object{Key: key, ContentType: "application/json"}
	_, err := driver.Put(ctx, obj, bytes.NewReader(v1), blobkit.PutOptions{Size: int64(len(v1))})
	if err != nil {
		t.Fatalf("Put v1 failed: %v", err)
	}

	// Overwrite with v2
	_, err = driver.Put(ctx, obj, bytes.NewReader(v2), blobkit.PutOptions{Size: int64(len(v2))})
	if err != nil {
		t.Fatalf("Put v2 failed: %v", err)
	}

	r, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	content, _ := io.ReadAll(r.Body)
	r.Body.Close()

	if !bytes.Equal(content, v2) {
		t.Fatalf("expected %s, got %s", string(v2), string(content))
	}
}

func TestDriver_MultipartLifecycle(t *testing.T) {
	driver, rootDir := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	key := "large/archive.bin"
	obj := &blobkit.Object{Key: key, ContentType: "application/octet-stream"}

	chunk1 := bytes.Repeat([]byte("A"), 5000)
	chunk2 := bytes.Repeat([]byte("B"), 8000)
	chunk3 := bytes.Repeat([]byte("C"), 3000)
	totalExpected := append(append(chunk1, chunk2...), chunk3...)

	// 1. CreateMultipart
	uploadID, err := driver.CreateMultipart(ctx, obj, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}
	if uploadID == "" {
		t.Fatal("expected non-empty uploadID")
	}

	// 2. UploadPart 1, 2, 3
	etag1, err := driver.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(chunk1), int64(len(chunk1)))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}
	etag2, err := driver.UploadPart(ctx, key, uploadID, 2, bytes.NewReader(chunk2), int64(len(chunk2)))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}
	etag3, err := driver.UploadPart(ctx, key, uploadID, 3, bytes.NewReader(chunk3), int64(len(chunk3)))
	if err != nil {
		t.Fatalf("UploadPart 3 failed: %v", err)
	}

	// 3. ListParts
	parts, err := driver.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// 4. CompleteMultipart
	completedParts := []blobkit.CompletedPart{
		{PartNumber: 1, ETag: etag1, Size: int64(len(chunk1))},
		{PartNumber: 2, ETag: etag2, Size: int64(len(chunk2))},
		{PartNumber: 3, ETag: etag3, Size: int64(len(chunk3))},
	}
	finalObj, err := driver.CompleteMultipart(ctx, obj, uploadID, completedParts)
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}
	if finalObj.Size != int64(len(totalExpected)) {
		t.Fatalf("expected final size %d, got %d", len(totalExpected), finalObj.Size)
	}

	// Staging dir should be deleted
	stagingPath := filepath.Join(rootDir, ".staging", uploadID)
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Fatal("expected staging directory to be cleaned up after completion")
	}

	// Verify merged file content
	r, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get merged file failed: %v", err)
	}
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !bytes.Equal(data, totalExpected) {
		t.Fatal("merged file content mismatch")
	}

	// 5. AbortMultipart test
	abortUploadID, err := driver.CreateMultipart(ctx, obj, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("CreateMultipart for abort failed: %v", err)
	}
	_, _ = driver.UploadPart(ctx, key, abortUploadID, 1, strings.NewReader("temp"), 4)
	if err := driver.AbortMultipart(ctx, key, abortUploadID); err != nil {
		t.Fatalf("AbortMultipart failed: %v", err)
	}
	abortStaging := filepath.Join(rootDir, ".staging", abortUploadID)
	if _, err := os.Stat(abortStaging); !os.IsNotExist(err) {
		t.Fatal("expected abort to remove staging directory")
	}
}

func TestDriver_Copy_And_BatchDelete(t *testing.T) {
	driver, _ := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	payload := []byte("Filesystem copy test content")
	srcKey := "photos/image.jpg"
	dstKey := "photos/image_backup.jpg"

	_, err := driver.Put(ctx, &blobkit.Object{
		Key:         srcKey,
		ContentType: "image/jpeg",
		Metadata:    map[string]string{"tag": "travel"},
	}, bytes.NewReader(payload), blobkit.PutOptions{Size: int64(len(payload))})
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
	if dstHead.Size != int64(len(payload)) || dstHead.Metadata["tag"] != "travel" {
		t.Fatalf("copied metadata mismatch: %v", dstHead)
	}

	// Batch Delete
	deleted, err := driver.DeleteBatch(ctx, []string{srcKey, dstKey})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted keys, got %d", len(deleted))
	}

	_, err = driver.Head(ctx, srcKey)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected srcKey deleted, got %v", err)
	}
}

func TestDriver_List(t *testing.T) {
	driver, _ := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	// Upload objects
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "media/music/song1.mp3"}, strings.NewReader("1"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "media/music/song2.mp3"}, strings.NewReader("2"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "media/videos/clip.mp4"}, strings.NewReader("3"), blobkit.PutOptions{})
	_, _ = driver.Put(ctx, &blobkit.Object{Key: "docs/readme.txt"}, strings.NewReader("4"), blobkit.PutOptions{})

	// 1. List with Prefix
	res, err := driver.List(ctx, blobkit.ListOptions{Prefix: "media/music/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(res.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(res.Objects))
	}

	// 2. List with Delimiter
	delRes, err := driver.List(ctx, blobkit.ListOptions{Prefix: "media/", Delimiter: "/"})
	if err != nil {
		t.Fatalf("List with delimiter failed: %v", err)
	}
	if len(delRes.CommonPrefixes) != 2 {
		t.Fatalf("expected 2 common prefixes (music/, videos/), got %v", delRes.CommonPrefixes)
	}
}

func TestDriver_ResolveURL_And_Presign(t *testing.T) {
	tempDir := t.TempDir()
	driver, _ := fs.NewDriver(fs.Config{
		RootDir:       tempDir,
		PublicBaseURL: "http://localhost:8080/storage",
	})
	defer driver.Close()

	ctx := context.Background()

	// ResolveURL with base URL
	u, err := driver.ResolveURL("images/pic.png")
	if err != nil {
		t.Fatalf("ResolveURL failed: %v", err)
	}
	if u != "http://localhost:8080/storage/images/pic.png" {
		t.Fatalf("unexpected URL: %s", u)
	}

	// Presign operations must return ErrUnsupportedOperation
	_, err = driver.PresignGet(ctx, "test.txt", blobkit.PresignOptions{})
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for PresignGet, got: %v", err)
	}
	_, err = driver.PresignPut(ctx, "test.txt", blobkit.PresignOptions{})
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for PresignPut, got: %v", err)
	}
}

func TestDriver_Concurrency(t *testing.T) {
	driver, _ := newTestDriver(t)
	defer driver.Close()
	ctx := context.Background()

	concurrency := 12
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("concurrent/worker_%02d.bin", idx)
			payload := bytes.Repeat([]byte(fmt.Sprintf("%02d", idx)), 200)

			// Put
			_, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(payload), blobkit.PutOptions{Size: int64(len(payload))})
			if err != nil {
				t.Errorf("worker %d Put failed: %v", idx, err)
				return
			}

			// Head
			head, err := driver.Head(ctx, key)
			if err != nil {
				t.Errorf("worker %d Head failed: %v", idx, err)
				return
			}
			if head.Size != int64(len(payload)) {
				t.Errorf("worker %d size mismatch", idx)
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
	driver, _ := newTestDriver(t)
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := []byte("Client Integration with Local POSIX Filesystem Driver")

	// 1. Client.Put
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "fs_integration",
		Filename:  "notes.txt",
	})
	if err != nil {
		t.Fatalf("Client.Put failed: %v", err)
	}
	if obj.Provider != "fs-test" {
		t.Fatalf("expected provider fs-test, got %s", obj.Provider)
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
		t.Fatal("downloaded payload mismatch")
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
