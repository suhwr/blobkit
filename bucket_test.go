package blobkit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
)

func TestBucket_BasicCRUD(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test-bucket"})
	bucket, err := blobkit.NewBucket(driver)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	if bucket.Name() != driver.Name() {
		t.Fatalf("expected name %s, got %s", driver.Name(), bucket.Name())
	}
	if bucket.Driver() != driver {
		t.Fatalf("expected driver to match")
	}

	// 1. Put and Head
	key := "docs/readme.txt"
	payload := []byte("Hello, low-level BlobKit!")
	obj, err := bucket.Put(ctx, key, bytes.NewReader(payload), blobkit.PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if obj.Key != key || obj.Size != int64(len(payload)) {
		t.Fatalf("unexpected object: %+v", obj)
	}

	// 2. Exists & Head
	exists, err := bucket.Exists(ctx, key)
	if err != nil || !exists {
		t.Fatalf("expected key to exist, got %v (err: %v)", exists, err)
	}

	headObj, err := bucket.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if headObj.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), headObj.Size)
	}

	// 3. Get and read body
	reader, err := bucket.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("expected content %q, got %q", payload, data)
	}

	// 4. PutBytes and GetBytes
	binKey := "images/avatar.bin"
	binData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_, err = bucket.PutBytes(ctx, binKey, binData, blobkit.PutOptions{ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	readBin, _, err := bucket.GetBytes(ctx, binKey, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if !bytes.Equal(readBin, binData) {
		t.Fatalf("expected %v, got %v", binData, readBin)
	}

	// 5. Copy
	dstKey := "images/avatar-copy.bin"
	if err := bucket.Copy(ctx, binKey, dstKey); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	existsCopy, err := bucket.Exists(ctx, dstKey)
	if err != nil || !existsCopy {
		t.Fatalf("expected copied key to exist")
	}

	// 6. List
	listRes, err := bucket.List(ctx, blobkit.ListOptions{Prefix: "images/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(listRes.Objects) != 2 {
		t.Fatalf("expected 2 objects under images/, got %d", len(listRes.Objects))
	}

	// 7. DeleteBatch
	deleted, err := bucket.DeleteBatch(ctx, []string{binKey, dstKey})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted keys, got %d", len(deleted))
	}

	// 8. Delete
	if err := bucket.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	existsAfter, err := bucket.Exists(ctx, key)
	if err != nil || existsAfter {
		t.Fatalf("expected key to no longer exist, got %v", existsAfter)
	}
}

func TestBucket_StreamingWriter(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test-bucket"})
	bucket, err := blobkit.NewBucket(driver)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	key := "logs/app.log"
	w, err := bucket.NewWriter(ctx, key, blobkit.PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	chunks := [][]byte{
		[]byte("Line 1: Starting server...\n"),
		[]byte("Line 2: Connected to database.\n"),
		[]byte("Line 3: Ready for traffic.\n"),
	}

	var expected []byte
	for _, chunk := range chunks {
		expected = append(expected, chunk...)
		n, writeErr := w.Write(chunk)
		if writeErr != nil || n != len(chunk) {
			t.Fatalf("Write failed: n=%d err=%v", n, writeErr)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify content
	data, _, err := bucket.GetBytes(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if !bytes.Equal(data, expected) {
		t.Fatalf("expected %q, got %q", expected, data)
	}
}

func TestBucket_SecurityAndValidation(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test-bucket"})
	bucket, _ := blobkit.NewBucket(driver)

	// Nil driver
	if _, err := blobkit.NewBucket(nil); err == nil {
		t.Fatal("expected error on nil driver")
	}

	// Path traversal attempt
	maliciousKeys := []string{
		"../escape.txt",
		"/absolute/path.txt",
		"folder/../../root.txt",
		"evil\x00null.txt",
	}

	for _, badKey := range maliciousKeys {
		if _, err := bucket.Put(ctx, badKey, bytes.NewReader([]byte("test")), blobkit.PutOptions{}); err == nil {
			t.Fatalf("expected security error on bad key %q", badKey)
		}
		if _, err := bucket.Get(ctx, badKey, blobkit.GetOptions{}); err == nil {
			t.Fatalf("expected security error on bad key %q in Get", badKey)
		}
		if _, err := bucket.Head(ctx, badKey); err == nil {
			t.Fatalf("expected security error on bad key %q in Head", badKey)
		}
		if err := bucket.Delete(ctx, badKey); err == nil {
			t.Fatalf("expected security error on bad key %q in Delete", badKey)
		}
	}

	// Nil reader
	if _, err := bucket.Put(ctx, "valid.txt", nil, blobkit.PutOptions{}); !errors.Is(err, blobkit.ErrNilReader) {
		t.Fatalf("expected ErrNilReader, got %v", err)
	}
}

func TestClient_BucketBridge(t *testing.T) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{Bucket: "bridge-bucket"})
	client, err := blobkit.New(blobkit.WithDriver(memDriver))
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	defer client.Close()

	bucket, err := client.Bucket(ctx, "")
	if err != nil {
		t.Fatalf("client.Bucket failed: %v", err)
	}

	testData := []byte("bridged payload")
	_, err = bucket.PutBytes(ctx, "bridge.txt", testData, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	got, _, err := bucket.GetBytes(ctx, "bridge.txt", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if !bytes.Equal(got, testData) {
		t.Fatalf("expected %q, got %q", testData, got)
	}
}

func TestBucketWriter_ConcurrentClose(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test-bucket"})
	bucket, err := blobkit.NewBucket(driver)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	w, err := bucket.NewWriter(ctx, "concurrent-writer.txt", blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	_, err = w.Write([]byte("streaming content"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	const workers = 10
	var wg sync.WaitGroup
	errs := make([]error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.Close()
		}(i)
	}

	wg.Wait()

	for i, closeErr := range errs {
		if closeErr != nil {
			t.Errorf("worker %d got non-nil close error: %v", i, closeErr)
		}
	}

	// Further writes must fail with closed error
	_, writeAfterCloseErr := w.Write([]byte("more"))
	if writeAfterCloseErr == nil {
		t.Fatal("expected error writing to closed BucketWriter, got nil")
	}
}

