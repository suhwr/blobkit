package testutil

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/provider/memory"
)

// TestDifferential_MemoryVsFS executes identical sequences of operations against
// two distinct driver implementations (Memory vs POSIX FS) and asserts identical
// behaviors, return values, errors, and byte contents.
func TestDifferential_MemoryVsFS(t *testing.T) {
	ctx := context.Background()

	memDriver := memory.NewDriver(memory.Config{
		Bucket: "diff-bucket",
	})
	fsDriver, err := fs.NewDriver(fs.Config{
		RootDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("failed to init fs driver: %v", err)
	}
	defer fsDriver.Close()
	defer memDriver.Close()

	drivers := []struct {
		name string
		d    blobkit.Driver
	}{
		{"memory", memDriver},
		{"posix_fs", fsDriver},
	}

	t.Run("Differential_Put_Head_Get_Equivalence", func(t *testing.T) {
		key := "diff/data.bin"
		data := []byte("differential equivalence payload 1234567890")

		for _, item := range drivers {
			obj := &blobkit.Object{
				Key:         key,
				ContentType: "application/octet-stream",
			}
			putObj, pErr := item.d.Put(ctx, obj, bytes.NewReader(data), blobkit.PutOptions{
				Size:        int64(len(data)),
				ContentType: "application/octet-stream",
			})
			if pErr != nil {
				t.Fatalf("[%s] Put failed: %v", item.name, pErr)
			}
			if putObj.Size != int64(len(data)) {
				t.Fatalf("[%s] Put size mismatch: expected %d, got %d", item.name, len(data), putObj.Size)
			}
			if putObj.ETag == "" {
				t.Fatalf("[%s] Put ETag empty", item.name)
			}

			// Head equivalence
			headObj, hErr := item.d.Head(ctx, key)
			if hErr != nil {
				t.Fatalf("[%s] Head failed: %v", item.name, hErr)
			}
			if headObj.Size != int64(len(data)) {
				t.Fatalf("[%s] Head size mismatch: expected %d, got %d", item.name, len(data), headObj.Size)
			}
			if headObj.ContentType != "application/octet-stream" {
				t.Fatalf("[%s] Head content-type mismatch: %q", item.name, headObj.ContentType)
			}

			// Get equivalence
			reader, gErr := item.d.Get(ctx, key, blobkit.GetOptions{})
			if gErr != nil {
				t.Fatalf("[%s] Get failed: %v", item.name, gErr)
			}
			body, rErr := io.ReadAll(reader)
			reader.Close()
			if rErr != nil {
				t.Fatalf("[%s] ReadAll failed: %v", item.name, rErr)
			}
			if !bytes.Equal(body, data) {
				t.Fatalf("[%s] payload mismatch", item.name)
			}
		}
	})

	t.Run("Differential_ByteRange_Equivalence", func(t *testing.T) {
		key := "diff/range.txt"
		data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

		for _, item := range drivers {
			obj := &blobkit.Object{Key: key}
			_, _ = item.d.Put(ctx, obj, bytes.NewReader(data), blobkit.PutOptions{Size: int64(len(data))})

			reader, err := item.d.Get(ctx, key, blobkit.GetOptions{
				Range: "bytes=5-9",
			})
			if err != nil {
				t.Fatalf("[%s] Get range failed: %v", item.name, err)
			}
			part, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatalf("[%s] ReadAll range failed: %v", item.name, err)
			}

			expected := string(data[5:10]) // "56789"
			if string(part) != expected {
				t.Fatalf("[%s] range content mismatch: expected %q, got %q", item.name, expected, string(part))
			}
		}
	})

	t.Run("Differential_EmptyObject_Equivalence", func(t *testing.T) {
		key := "diff/empty.txt"
		data := []byte("")

		for _, item := range drivers {
			obj := &blobkit.Object{Key: key}
			putObj, err := item.d.Put(ctx, obj, bytes.NewReader(data), blobkit.PutOptions{Size: 0})
			if err != nil {
				t.Fatalf("[%s] Put 0-byte object failed: %v", item.name, err)
			}
			if putObj.Size != 0 {
				t.Fatalf("[%s] expected size 0, got %d", item.name, putObj.Size)
			}

			headObj, err := item.d.Head(ctx, key)
			if err != nil {
				t.Fatalf("[%s] Head 0-byte object failed: %v", item.name, err)
			}
			if headObj.Size != 0 {
				t.Fatalf("[%s] expected size 0 on Head, got %d", item.name, headObj.Size)
			}

			reader, err := item.d.Get(ctx, key, blobkit.GetOptions{})
			if err != nil {
				t.Fatalf("[%s] Get 0-byte object failed: %v", item.name, err)
			}
			body, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatalf("[%s] ReadAll 0-byte object failed: %v", item.name, err)
			}
			if len(body) != 0 {
				t.Fatalf("[%s] expected 0-byte body, got %d bytes", item.name, len(body))
			}
		}
	})

	t.Run("Differential_SizeMismatch_Rollback_Equivalence", func(t *testing.T) {
		key := "diff/mismatch.txt"
		data := []byte("short")

		for _, item := range drivers {
			obj := &blobkit.Object{Key: key}
			// Claim size 100, but only provide 5 bytes
			_, err := item.d.Put(ctx, obj, bytes.NewReader(data), blobkit.PutOptions{
				ExplicitSize: true,
				Size:         100,
			})
			if !blobkit.IsPermanent(err) || !blobkit.PreserveSentinel(err) {
				t.Fatalf("[%s] expected permanent size mismatch error, got: %v", item.name, err)
			}

			// Verify object was NOT created
			_, err = item.d.Head(ctx, key)
			if !blobkit.IsNotFound(err) {
				t.Fatalf("[%s] expected ErrObjectNotFound after failed short read Put, got: %v", item.name, err)
			}
		}
	})

	t.Run("Differential_DeleteIdempotency_Equivalence", func(t *testing.T) {
		key := "diff/nonexistent-key-12345.txt"

		for _, item := range drivers {
			err := item.d.Delete(ctx, key)
			if err != nil {
				t.Fatalf("[%s] Delete of nonexistent key must be idempotent, got: %v", item.name, err)
			}
		}
	})
}
