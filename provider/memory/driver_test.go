package memory_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
)

func TestMemoryDriver_CRUD(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{
		Name:          "test-mem",
		Bucket:        "test-bucket",
		PublicBaseURL: "https://cdn.example.com",
	})
	defer driver.Close()

	payload := []byte("Hello BlobKit Memory Storage Engine!")
	obj := &blobkit.Object{
		ID:          "obj-001",
		Key:         "avatars/obj-001.txt",
		ContentType: "text/plain",
	}

	// 1. Put
	saved, err := driver.Put(ctx, obj, bytes.NewReader(payload), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if saved.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), saved.Size)
	}
	if saved.Provider != "test-mem" {
		t.Fatalf("expected provider 'test-mem', got %s", saved.Provider)
	}

	// 2. Head
	head, err := driver.Head(ctx, "avatars/obj-001.txt")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if head.ETag == "" {
		t.Fatal("expected non-empty ETag")
	}

	// 3. Get
	reader, err := driver.Get(ctx, "avatars/obj-001.txt", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("payload mismatch")
	}

	// 4. Get with Range: bytes=6-12 -> "BlobKit"
	rangeReader, err := driver.Get(ctx, "avatars/obj-001.txt", blobkit.GetOptions{Range: "bytes=6-12"})
	if err != nil {
		t.Fatalf("Get with Range failed: %v", err)
	}
	rangeData, _ := io.ReadAll(rangeReader)
	rangeReader.Close()
	if string(rangeData) != "BlobKit" {
		t.Fatalf("expected 'BlobKit', got %q", string(rangeData))
	}

	// 5. ResolveURL
	url, err := driver.ResolveURL("avatars/obj-001.txt")
	if err != nil {
		t.Fatalf("ResolveURL failed: %v", err)
	}
	if url != "https://cdn.example.com/avatars/obj-001.txt" {
		t.Fatalf("unexpected URL: %s", url)
	}

	// 6. Delete
	err = driver.Delete(ctx, "avatars/obj-001.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 7. Verify Not Found
	_, err = driver.Head(ctx, "avatars/obj-001.txt")
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestMemoryDriver_ListAndBatchDelete(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "my-bucket"})
	defer driver.Close()

	// Insert 5 items
	for i := 1; i <= 5; i++ {
		key := "items/item" + string(rune('0'+i)) + ".dat"
		obj := &blobkit.Object{Key: key}
		_, err := driver.Put(ctx, obj, bytes.NewReader([]byte("data")), blobkit.PutOptions{})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// List
	res, err := driver.List(ctx, blobkit.ListOptions{Prefix: "items/", Limit: 3})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(res.Objects) != 3 {
		t.Fatalf("expected 3 items, got %d", len(res.Objects))
	}
	if !res.IsTruncated {
		t.Fatal("expected IsTruncated to be true")
	}

	// Batch delete
	deleted, err := driver.DeleteBatch(ctx, []string{"items/item1.dat", "items/item2.dat"})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted items, got %d", len(deleted))
	}

	resAfter, err := driver.List(ctx, blobkit.ListOptions{Prefix: "items/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(resAfter.Objects) != 3 {
		t.Fatalf("expected 3 items remaining, got %d", len(resAfter.Objects))
	}
}
