package testutil

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/provider/memory"
)

// TestDifferential_MemoryVsFS executes identical sequences of operations pairwise against
// two distinct driver implementations (In-Memory vs POSIX FS) and asserts behavioral
// and semantic equivalence between them.
func TestDifferential_MemoryVsFS(t *testing.T) {
	ctx := context.Background()

	memDriver := memory.NewDriver(memory.Config{
		Bucket: "diff-bucket",
	})
	fsDriver, err := fs.NewDriver(fs.Config{
		RootDir:           t.TempDir(),
		EnableSidecarMeta: true,
	})
	if err != nil {
		t.Fatalf("failed to init fs driver: %v", err)
	}
	defer fsDriver.Close()
	defer memDriver.Close()

	t.Run("Differential_Put_Head_Get_Equivalence", func(t *testing.T) {
		key := "diff/data.bin"
		data := []byte("differential equivalence payload 1234567890")
		customMeta := map[string]string{"env": "test", "tenant": "suhwr"}

		// Pairwise Put
		objMem := &blobkit.Object{Key: key, ContentType: "application/octet-stream", Metadata: customMeta}
		putMem, errMem := memDriver.Put(ctx, objMem, bytes.NewReader(data), blobkit.PutOptions{
			Size:        int64(len(data)),
			ContentType: "application/octet-stream",
			Metadata:    customMeta,
		})

		objFS := &blobkit.Object{Key: key, ContentType: "application/octet-stream", Metadata: customMeta}
		putFS, errFS := fsDriver.Put(ctx, objFS, bytes.NewReader(data), blobkit.PutOptions{
			Size:        int64(len(data)),
			ContentType: "application/octet-stream",
			Metadata:    customMeta,
		})

		// 1. Assert Put equivalence
		if (errMem == nil) != (errFS == nil) {
			t.Fatalf("pairwise Put error divergence: mem=%v, fs=%v", errMem, errFS)
		}
		if putMem.Size != putFS.Size {
			t.Fatalf("pairwise Put size divergence: mem=%d, fs=%d", putMem.Size, putFS.Size)
		}
		if putMem.ContentType != putFS.ContentType {
			t.Fatalf("pairwise Put ContentType divergence: mem=%q, fs=%q", putMem.ContentType, putFS.ContentType)
		}

		// 2. Assert Head equivalence
		headMem, errHeadMem := memDriver.Head(ctx, key)
		headFS, errHeadFS := fsDriver.Head(ctx, key)
		if (errHeadMem == nil) != (errHeadFS == nil) {
			t.Fatalf("pairwise Head error divergence: mem=%v, fs=%v", errHeadMem, errHeadFS)
		}
		if headMem.Size != headFS.Size {
			t.Fatalf("pairwise Head size divergence: mem=%d, fs=%d", headMem.Size, headFS.Size)
		}
		if headMem.ContentType != headFS.ContentType {
			t.Fatalf("pairwise Head ContentType divergence: mem=%q, fs=%q", headMem.ContentType, headFS.ContentType)
		}
		if !reflect.DeepEqual(headMem.Metadata, headFS.Metadata) {
			t.Fatalf("pairwise Head Metadata divergence: mem=%v, fs=%v", headMem.Metadata, headFS.Metadata)
		}

		// 3. Assert Get byte equivalence
		rMem, errGetMem := memDriver.Get(ctx, key, blobkit.GetOptions{})
		rFS, errGetFS := fsDriver.Get(ctx, key, blobkit.GetOptions{})
		if (errGetMem == nil) != (errGetFS == nil) {
			t.Fatalf("pairwise Get error divergence: mem=%v, fs=%v", errGetMem, errGetFS)
		}
		defer rMem.Close()
		defer rFS.Close()

		bodyMem, rErrMem := io.ReadAll(rMem)
		bodyFS, rErrFS := io.ReadAll(rFS)
		if rErrMem != nil || rErrFS != nil {
			t.Fatalf("pairwise ReadAll error divergence: mem=%v, fs=%v", rErrMem, rErrFS)
		}
		if !bytes.Equal(bodyMem, bodyFS) {
			t.Fatalf("pairwise payload byte divergence between memory and fs implementations")
		}
	})

	t.Run("Differential_ByteRange_Equivalence", func(t *testing.T) {
		key := "diff/range.txt"
		data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

		_, _ = memDriver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(data), blobkit.PutOptions{Size: int64(len(data))})
		_, _ = fsDriver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(data), blobkit.PutOptions{Size: int64(len(data))})

		rMem, errMem := memDriver.Get(ctx, key, blobkit.GetOptions{Range: "bytes=5-9"})
		rFS, errFS := fsDriver.Get(ctx, key, blobkit.GetOptions{Range: "bytes=5-9"})
		if (errMem == nil) != (errFS == nil) {
			t.Fatalf("pairwise range Get error divergence: mem=%v, fs=%v", errMem, errFS)
		}
		defer rMem.Close()
		defer rFS.Close()

		partMem, _ := io.ReadAll(rMem)
		partFS, _ := io.ReadAll(rFS)
		if !bytes.Equal(partMem, partFS) {
			t.Fatalf("pairwise range payload divergence: mem=%q, fs=%q", string(partMem), string(partFS))
		}
	})

	t.Run("Differential_EmptyObject_Equivalence", func(t *testing.T) {
		key := "diff/empty.txt"
		emptyData := []byte("")

		putMem, errPutMem := memDriver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(emptyData), blobkit.PutOptions{Size: 0, ExplicitSize: true})
		putFS, errPutFS := fsDriver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(emptyData), blobkit.PutOptions{Size: 0, ExplicitSize: true})
		if (errPutMem == nil) != (errPutFS == nil) {
			t.Fatalf("pairwise 0-byte Put error divergence: mem=%v, fs=%v", errPutMem, errPutFS)
		}
		if putMem.Size != putFS.Size || putMem.Size != 0 {
			t.Fatalf("pairwise 0-byte Put size divergence: mem=%d, fs=%d", putMem.Size, putFS.Size)
		}

		headMem, errHeadMem := memDriver.Head(ctx, key)
		headFS, errHeadFS := fsDriver.Head(ctx, key)
		if (errHeadMem == nil) != (errHeadFS == nil) {
			t.Fatalf("pairwise 0-byte Head error divergence: mem=%v, fs=%v", errHeadMem, errHeadFS)
		}
		if headMem.Size != headFS.Size || headMem.Size != 0 {
			t.Fatalf("pairwise 0-byte Head size divergence: mem=%d, fs=%d", headMem.Size, headFS.Size)
		}
	})

	t.Run("Differential_SizeMismatch_Rollback_Equivalence", func(t *testing.T) {
		key := "diff/mismatch.txt"
		shortData := []byte("short")

		// Claim size 100 but only provide 5 bytes
		_, errPutMem := memDriver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(shortData), blobkit.PutOptions{
			ExplicitSize: true,
			Size:         100,
		})
		_, errPutFS := fsDriver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(shortData), blobkit.PutOptions{
			ExplicitSize: true,
			Size:         100,
		})

		// Both must fail with size mismatch error
		if blobkit.IsPermanent(errPutMem) != blobkit.IsPermanent(errPutFS) {
			t.Fatalf("pairwise permanent error divergence on short read: mem=%v, fs=%v", errPutMem, errPutFS)
		}

		// Both must have rolled back: Head must return not found for both
		_, errHeadMem := memDriver.Head(ctx, key)
		_, errHeadFS := fsDriver.Head(ctx, key)
		if !blobkit.IsNotFound(errHeadMem) || !blobkit.IsNotFound(errHeadFS) {
			t.Fatalf("pairwise rollback divergence: memNotFound=%v, fsNotFound=%v", blobkit.IsNotFound(errHeadMem), blobkit.IsNotFound(errHeadFS))
		}
	})

	t.Run("Differential_DeleteIdempotency_Equivalence", func(t *testing.T) {
		key := "diff/nonexistent-key-12345.txt"

		errMem := memDriver.Delete(ctx, key)
		errFS := fsDriver.Delete(ctx, key)

		// Both must succeed idempotently without error
		if (errMem == nil) != (errFS == nil) {
			t.Fatalf("pairwise Delete idempotency divergence: mem=%v, fs=%v", errMem, errFS)
		}

		// Subsequent Head must return not found on both
		_, errHeadMem := memDriver.Head(ctx, key)
		_, errHeadFS := fsDriver.Head(ctx, key)
		if !blobkit.IsNotFound(errHeadMem) || !blobkit.IsNotFound(errHeadFS) {
			t.Fatalf("pairwise Head not found divergence: mem=%v, fs=%v", errHeadMem, errHeadFS)
		}
	})

	t.Run("Differential_Copy_Equivalence", func(t *testing.T) {
		srcKey := "diff/src.bin"
		dstKey := "diff/dst.bin"
		data := []byte("copy payload to verify pairwise server-side copy behavior")

		_, _ = memDriver.Put(ctx, &blobkit.Object{Key: srcKey}, bytes.NewReader(data), blobkit.PutOptions{Size: int64(len(data))})
		_, _ = fsDriver.Put(ctx, &blobkit.Object{Key: srcKey}, bytes.NewReader(data), blobkit.PutOptions{Size: int64(len(data))})

		errCopyMem := memDriver.Copy(ctx, srcKey, dstKey)
		errCopyFS := fsDriver.Copy(ctx, srcKey, dstKey)
		if (errCopyMem == nil) != (errCopyFS == nil) {
			t.Fatalf("pairwise Copy error divergence: mem=%v, fs=%v", errCopyMem, errCopyFS)
		}

		// Assert destination objects are identical
		dstHeadMem, _ := memDriver.Head(ctx, dstKey)
		dstHeadFS, _ := fsDriver.Head(ctx, dstKey)
		if dstHeadMem.Size != dstHeadFS.Size {
			t.Fatalf("pairwise Copy destination size divergence: mem=%d, fs=%d", dstHeadMem.Size, dstHeadFS.Size)
		}

		// Assert source objects still exist in both
		srcHeadMem, errSrcMem := memDriver.Head(ctx, srcKey)
		srcHeadFS, errSrcFS := fsDriver.Head(ctx, srcKey)
		if errSrcMem != nil || errSrcFS != nil || srcHeadMem.Size != srcHeadFS.Size {
			t.Fatalf("pairwise source preservation divergence after copy: mem=%v, fs=%v", errSrcMem, errSrcFS)
		}
	})
}
