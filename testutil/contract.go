package testutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
)

// DriverContractOptions configures the execution of RunDriverContractTests.
type DriverContractOptions struct {
	SkipMultipart   bool
	SkipPresign     bool
	SkipCopy        bool
	SkipBatch       bool
	SkipRange       bool
	SkipConditional bool
}

// RunDriverContractTests runs a standardized conformance and compliance suite
// across any blobkit.Driver implementation to ensure semantic parity.
func RunDriverContractTests(t *testing.T, factory func(t *testing.T) (blobkit.Driver, func()), opts ...DriverContractOptions) {
	var opt DriverContractOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	t.Run("Contract_Put_Get_Head_Delete", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		key := "contract-tests/crud_object.txt"
		payload := []byte("conformance test payload: " + time.Now().String())
		inputObj := &blobkit.Object{
			Key:         key,
			ContentType: "text/plain; charset=utf-8",
			Metadata: map[string]string{
				"environment": "test",
				"suite":       "contract",
			},
		}

		// 1. Put
		putObj, err := driver.Put(ctx, inputObj, bytes.NewReader(payload), blobkit.PutOptions{
			Size:        int64(len(payload)),
			ContentType: "text/plain; charset=utf-8",
			Metadata: map[string]string{
				"environment": "test",
				"suite":       "contract",
			},
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if putObj.Key != key {
			t.Errorf("expected Key=%q, got %q", key, putObj.Key)
		}
		if putObj.Size != int64(len(payload)) {
			t.Errorf("expected Size=%d, got %d", len(payload), putObj.Size)
		}

		// 2. Head
		headObj, err := driver.Head(ctx, key)
		if err != nil {
			t.Fatalf("Head failed: %v", err)
		}
		if headObj.Key != key {
			t.Errorf("Head Key mismatch: expected %q, got %q", key, headObj.Key)
		}
		if headObj.Size != int64(len(payload)) {
			t.Errorf("Head Size mismatch: expected %d, got %d", len(payload), headObj.Size)
		}

		// 3. Get
		reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		readData, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("reading body failed: %v", err)
		}
		if !bytes.Equal(readData, payload) {
			t.Fatalf("data mismatch: expected %q, got %q", string(payload), string(readData))
		}

		// 4. Delete
		if err := driver.Delete(ctx, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// 5. Head after Delete should return ErrObjectNotFound
		_, err = driver.Head(ctx, key)
		if err == nil {
			t.Fatalf("expected ErrObjectNotFound after Delete, got nil error")
		}
		if !errors.Is(err, blobkit.ErrObjectNotFound) {
			t.Fatalf("expected ErrObjectNotFound, got: %v", err)
		}

		// 6. Get after Delete should return ErrObjectNotFound
		_, err = driver.Get(ctx, key, blobkit.GetOptions{})
		if err == nil {
			t.Fatalf("expected ErrObjectNotFound on Get after Delete, got nil")
		}
		if !errors.Is(err, blobkit.ErrObjectNotFound) {
			t.Fatalf("expected ErrObjectNotFound on Get, got: %v", err)
		}
	})

	t.Run("Contract_ByteRangeGet", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		if opt.SkipRange || (driver.Capabilities()&blobkit.CapByteRangeGet == 0) {
			t.Skip("skipping CapByteRangeGet test")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		key := "contract-tests/range_sample.bin"
		payload := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
		_, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(payload), blobkit.PutOptions{
			Size: int64(len(payload)),
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Subslice: offset 5 to 9 => 5 bytes ("fghij")
		reader, err := driver.Get(ctx, key, blobkit.GetOptions{
			Range: "bytes=5-9",
		})
		if err != nil {
			t.Fatalf("Get range failed: %v", err)
		}
		sliceData, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("reading range failed: %v", err)
		}
		if string(sliceData) != "fghij" {
			t.Fatalf("range data mismatch: expected %q, got %q", "fghij", string(sliceData))
		}
	})

	t.Run("Contract_Copy", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		if opt.SkipCopy || (driver.Capabilities()&blobkit.CapCopy == 0) {
			t.Skip("skipping CapCopy test")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		srcKey := "contract-tests/copy_src.txt"
		dstKey := "contract-tests/copy_dst.txt"
		content := []byte("copy contract test data")

		_, err := driver.Put(ctx, &blobkit.Object{Key: srcKey}, bytes.NewReader(content), blobkit.PutOptions{
			Size: int64(len(content)),
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if err := driver.Copy(ctx, srcKey, dstKey); err != nil {
			t.Fatalf("Copy failed: %v", err)
		}

		// Destination must exist with matching content
		dstReader, err := driver.Get(ctx, dstKey, blobkit.GetOptions{})
		if err != nil {
			t.Fatalf("Get dest failed: %v", err)
		}
		dstData, err := io.ReadAll(dstReader)
		_ = dstReader.Close()
		if err != nil {
			t.Fatalf("reading dest failed: %v", err)
		}
		if !bytes.Equal(dstData, content) {
			t.Fatalf("dest data mismatch: expected %q, got %q", string(content), string(dstData))
		}

		// Source must still exist
		srcHead, err := driver.Head(ctx, srcKey)
		if err != nil || srcHead == nil {
			t.Fatalf("source object should still exist after copy: %v", err)
		}

		// Non-existent source must fail with ErrObjectNotFound
		err = driver.Copy(ctx, "contract-tests/non_existent.txt", "contract-tests/dest_non.txt")
		if err == nil {
			t.Fatal("expected error when copying non-existent source, got nil")
		}
		if !errors.Is(err, blobkit.ErrObjectNotFound) {
			t.Fatalf("expected ErrObjectNotFound, got: %v", err)
		}
	})

	t.Run("Contract_BatchDelete", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		if opt.SkipBatch || (driver.Capabilities()&blobkit.CapBatchDelete == 0) {
			t.Skip("skipping CapBatchDelete test")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		keys := []string{
			"contract-tests/batch_1.txt",
			"contract-tests/batch_2.txt",
			"contract-tests/batch_3.txt",
		}
		for _, k := range keys {
			_, err := driver.Put(ctx, &blobkit.Object{Key: k}, strings.NewReader("sample"), blobkit.PutOptions{})
			if err != nil {
				t.Fatalf("Put %s failed: %v", k, err)
			}
		}

		deleted, err := driver.DeleteBatch(ctx, keys)
		if err != nil {
			t.Fatalf("DeleteBatch failed: %v", err)
		}
		if len(deleted) == 0 {
			t.Errorf("expected deleted keys to be reported, got empty list")
		}

		for _, k := range keys {
			_, err := driver.Head(ctx, k)
			if err == nil || !errors.Is(err, blobkit.ErrObjectNotFound) {
				t.Fatalf("key %s should be deleted, got err=%v", k, err)
			}
		}
	})

	t.Run("Contract_List", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		prefix := fmt.Sprintf("contract-list-%d/", time.Now().UnixNano())
		keys := []string{
			prefix + "item1.txt",
			prefix + "item2.txt",
			prefix + "nested/item3.txt",
		}
		for _, k := range keys {
			_, err := driver.Put(ctx, &blobkit.Object{Key: k}, strings.NewReader("list content"), blobkit.PutOptions{})
			if err != nil {
				t.Fatalf("Put %s failed: %v", k, err)
			}
		}

		res, err := driver.List(ctx, blobkit.ListOptions{
			Prefix: prefix,
		})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(res.Objects) < 3 {
			t.Fatalf("expected at least 3 objects under prefix %s, got %d", prefix, len(res.Objects))
		}
	})

	t.Run("Contract_PathTraversalAndSecurity", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		invalidKeys := []string{
			"../secret.txt",
			"../../etc/passwd",
			"/root/file.txt",
			"contract/../../escape.txt",
		}

		for _, badKey := range invalidKeys {
			// Put with bad key must be rejected
			_, err := driver.Put(ctx, &blobkit.Object{Key: badKey}, strings.NewReader("payload"), blobkit.PutOptions{})
			if err == nil {
				t.Fatalf("expected error on Put with malicious key %q, got nil", badKey)
			}
			if !errors.Is(err, blobkit.ErrInvalidKey) && !errors.Is(err, blobkit.ErrSecurityViolation) {
				t.Fatalf("expected ErrInvalidKey or ErrSecurityViolation for key %q, got: %v", badKey, err)
			}

			// Head with bad key must be rejected
			_, err = driver.Head(ctx, badKey)
			if err == nil {
				t.Fatalf("expected error on Head with malicious key %q, got nil", badKey)
			}
			if !errors.Is(err, blobkit.ErrInvalidKey) && !errors.Is(err, blobkit.ErrSecurityViolation) {
				t.Fatalf("expected ErrInvalidKey or ErrSecurityViolation for Head %q, got: %v", badKey, err)
			}

			// Get with bad key must be rejected
			_, err = driver.Get(ctx, badKey, blobkit.GetOptions{})
			if err == nil {
				t.Fatalf("expected error on Get with malicious key %q, got nil", badKey)
			}
			if !errors.Is(err, blobkit.ErrInvalidKey) && !errors.Is(err, blobkit.ErrSecurityViolation) {
				t.Fatalf("expected ErrInvalidKey or ErrSecurityViolation for Get %q, got: %v", badKey, err)
			}

			// Delete with bad key must be rejected
			err = driver.Delete(ctx, badKey)
			if err == nil {
				t.Fatalf("expected error on Delete with malicious key %q, got nil", badKey)
			}
			if !errors.Is(err, blobkit.ErrInvalidKey) && !errors.Is(err, blobkit.ErrSecurityViolation) {
				t.Fatalf("expected ErrInvalidKey or ErrSecurityViolation for Delete %q, got: %v", badKey, err)
			}
		}
	})

	t.Run("Contract_ContextCancellation", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // immediately cancelled

		_, err := driver.Put(ctx, &blobkit.Object{Key: "contract/cancel.txt"}, strings.NewReader("data"), blobkit.PutOptions{})
		if err == nil {
			t.Fatal("expected error on cancelled context Put, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected errors.Is(err, context.Canceled), got: %v", err)
		}

		_, err = driver.Get(ctx, "contract/cancel.txt", blobkit.GetOptions{})
		if err == nil {
			t.Fatal("expected error on cancelled context Get, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected errors.Is(err, context.Canceled), got: %v", err)
		}
	})

	t.Run("Contract_MultipartSession", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		if opt.SkipMultipart || (driver.Capabilities()&blobkit.CapMultipartSession == 0) {
			t.Skip("skipping CapMultipartSession test")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		key := "contract-tests/multipart_lifecycle.bin"
		targetObj := &blobkit.Object{Key: key, ContentType: "application/octet-stream"}

		// 1. CreateMultipart
		uploadID, err := driver.CreateMultipart(ctx, targetObj, blobkit.PutOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			t.Fatalf("CreateMultipart failed: %v", err)
		}
		if uploadID == "" {
			t.Fatal("expected non-empty uploadID")
		}

		// 2. UploadPart 1 & 2
		part1Data := []byte("part-one-payload---")
		part2Data := []byte("part-two-payload")
		etag1, err := driver.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(part1Data), int64(len(part1Data)))
		if err != nil {
			t.Fatalf("UploadPart 1 failed: %v", err)
		}
		if etag1 == "" {
			t.Fatal("expected non-empty etag for part 1")
		}

		etag2, err := driver.UploadPart(ctx, key, uploadID, 2, bytes.NewReader(part2Data), int64(len(part2Data)))
		if err != nil {
			t.Fatalf("UploadPart 2 failed: %v", err)
		}
		if etag2 == "" {
			t.Fatal("expected non-empty etag for part 2")
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
		completedObj, err := driver.CompleteMultipart(ctx, targetObj, uploadID, parts)
		if err != nil {
			t.Fatalf("CompleteMultipart failed: %v", err)
		}
		expectedTotalSize := int64(len(part1Data) + len(part2Data))
		if completedObj.Size != expectedTotalSize {
			t.Fatalf("expected completed size %d, got %d", expectedTotalSize, completedObj.Size)
		}

		// 5. Verify whole body
		reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
		if err != nil {
			t.Fatalf("Get completed multipart failed: %v", err)
		}
		fullData, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("ReadAll completed multipart failed: %v", err)
		}
		expectedBody := append(part1Data, part2Data...)
		if !bytes.Equal(fullData, expectedBody) {
			t.Fatalf("multipart body mismatch: got %q, want %q", string(fullData), string(expectedBody))
		}

		// 6. AbortMultipart test
		abortID, err := driver.CreateMultipart(ctx, &blobkit.Object{Key: "contract-tests/to_abort.bin"}, blobkit.PutOptions{})
		if err != nil {
			t.Fatalf("CreateMultipart for abort failed: %v", err)
		}
		if err := driver.AbortMultipart(ctx, "contract-tests/to_abort.bin", abortID); err != nil {
			t.Fatalf("AbortMultipart failed: %v", err)
		}
	})

	t.Run("Contract_EmptyObject", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		key := "contract-tests/empty_object.txt"
		putObj, err := driver.Put(ctx, &blobkit.Object{Key: key}, strings.NewReader(""), blobkit.PutOptions{
			Size:         0,
			ExplicitSize: true,
		})
		if err != nil {
			t.Fatalf("Put empty object failed: %v", err)
		}
		if putObj.Size != 0 {
			t.Errorf("expected Size=0, got %d", putObj.Size)
		}

		headObj, err := driver.Head(ctx, key)
		if err != nil {
			t.Fatalf("Head empty object failed: %v", err)
		}
		if headObj.Size != 0 {
			t.Errorf("Head expected Size=0, got %d", headObj.Size)
		}

		reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
		if err != nil {
			t.Fatalf("Get empty object failed: %v", err)
		}
		defer reader.Close()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll empty object failed: %v", err)
		}
		if len(data) != 0 {
			t.Fatalf("expected 0 bytes, got %d", len(data))
		}

		if err := driver.Delete(ctx, key); err != nil {
			t.Fatalf("Delete empty object failed: %v", err)
		}
	})

	t.Run("Contract_SizeMismatch", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 1. Shorter: declared 10, actual 5
		keyShorter := "contract-tests/mismatch_shorter.txt"
		_, err := driver.Put(ctx, &blobkit.Object{Key: keyShorter}, strings.NewReader("12345"), blobkit.PutOptions{
			Size:         10,
			ExplicitSize: true,
		})
		if err == nil {
			t.Fatal("expected ErrSizeMismatch when stream is shorter than declared size, got nil")
		}
		if !errors.Is(err, blobkit.ErrSizeMismatch) {
			t.Fatalf("expected ErrSizeMismatch, got %v", err)
		}

		// 2. Longer: declared 5, actual 10
		keyLonger := "contract-tests/mismatch_longer.txt"
		_, err = driver.Put(ctx, &blobkit.Object{Key: keyLonger}, strings.NewReader("0123456789"), blobkit.PutOptions{
			Size:         5,
			ExplicitSize: true,
		})
		if err == nil {
			t.Fatal("expected ErrSizeMismatch when stream is longer than declared size, got nil")
		}
		if !errors.Is(err, blobkit.ErrSizeMismatch) {
			t.Fatalf("expected ErrSizeMismatch, got %v", err)
		}
	})

	t.Run("Contract_DeleteIdempotent", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 1. Delete non-existent key must return nil
		nonExistentKey := "contract-tests/non_existent_idempotent_delete.txt"
		if err := driver.Delete(ctx, nonExistentKey); err != nil {
			t.Fatalf("Delete non-existent key must return nil, got: %v", err)
		}

		// 2. Put then double-delete must succeed
		putKey := "contract-tests/idempotent_double_delete.txt"
		_, err := driver.Put(ctx, &blobkit.Object{Key: putKey}, strings.NewReader("temp"), blobkit.PutOptions{})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := driver.Delete(ctx, putKey); err != nil {
			t.Fatalf("First delete failed: %v", err)
		}
		if err := driver.Delete(ctx, putKey); err != nil {
			t.Fatalf("Second delete of already-deleted key must return nil, got: %v", err)
		}
	})

	t.Run("Contract_ConditionalRequests", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		if opt.SkipConditional {
			t.Skip("skipping conditional requests test")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		key := "contract-tests/conditional_object.txt"
		payload := []byte("conditional payload test")
		putObj, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(payload), blobkit.PutOptions{
			Size: int64(len(payload)),
		})
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		headObj, err := driver.Head(ctx, key)
		if err != nil {
			t.Fatalf("Head failed: %v", err)
		}
		etag := headObj.ETag
		if etag == "" {
			etag = putObj.ETag
		}

		if etag != "" {
			// IfMatch with matching ETag -> success
			r, err := driver.Get(ctx, key, blobkit.GetOptions{IfMatch: etag})
			if err != nil {
				t.Fatalf("Get with matching IfMatch failed: %v", err)
			}
			_ = r.Close()

			// IfMatch with non-matching ETag -> ErrPreconditionFailed
			_, err = driver.Get(ctx, key, blobkit.GetOptions{IfMatch: "\"non-matching-etag\""})
			if err == nil {
				t.Fatal("expected ErrPreconditionFailed for mismatched IfMatch, got nil")
			}
			if !errors.Is(err, blobkit.ErrPreconditionFailed) {
				t.Fatalf("expected ErrPreconditionFailed, got %v", err)
			}

			// IfNoneMatch with matching ETag -> ErrPreconditionFailed
			_, err = driver.Get(ctx, key, blobkit.GetOptions{IfNoneMatch: etag})
			if err == nil {
				t.Fatal("expected ErrPreconditionFailed for matching IfNoneMatch, got nil")
			}
			if !errors.Is(err, blobkit.ErrPreconditionFailed) {
				t.Fatalf("expected ErrPreconditionFailed, got %v", err)
			}

			// IfNoneMatch with different ETag -> success
			r, err = driver.Get(ctx, key, blobkit.GetOptions{IfNoneMatch: "\"different-etag\""})
			if err != nil {
				t.Fatalf("Get with different IfNoneMatch failed: %v", err)
			}
			_ = r.Close()
		}

		if !headObj.UpdatedAt.IsZero() {
			future := headObj.UpdatedAt.Add(1 * time.Hour)
			// IfModifiedSince in future -> ErrPreconditionFailed (not modified since future)
			_, err = driver.Get(ctx, key, blobkit.GetOptions{
				IfModifiedSince: &future,
			})
			if err == nil {
				t.Fatal("expected ErrPreconditionFailed for IfModifiedSince in future, got nil")
			}
			if !errors.Is(err, blobkit.ErrPreconditionFailed) {
				t.Fatalf("expected ErrPreconditionFailed for IfModifiedSince, got %v", err)
			}

			past := headObj.UpdatedAt.Add(-1 * time.Hour)
			// IfUnmodifiedSince in past -> ErrPreconditionFailed (modified since past)
			_, err = driver.Get(ctx, key, blobkit.GetOptions{
				IfUnmodifiedSince: &past,
			})
			if err == nil {
				t.Fatal("expected ErrPreconditionFailed for IfUnmodifiedSince in past, got nil")
			}
			if !errors.Is(err, blobkit.ErrPreconditionFailed) {
				t.Fatalf("expected ErrPreconditionFailed for IfUnmodifiedSince, got %v", err)
			}
		}

		_ = driver.Delete(ctx, key)
	})

	t.Run("Contract_ListDelimiter", func(t *testing.T) {
		driver, cleanup := factory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		prefix := fmt.Sprintf("contract-delim-%d/", time.Now().UnixNano())
		keys := []string{
			prefix + "file1.txt",
			prefix + "file2.txt",
			prefix + "dirA/file3.txt",
			prefix + "dirA/file4.txt",
			prefix + "dirB/file5.txt",
		}
		for _, k := range keys {
			_, err := driver.Put(ctx, &blobkit.Object{Key: k}, strings.NewReader("delim data"), blobkit.PutOptions{})
			if err != nil {
				t.Fatalf("Put %s failed: %v", k, err)
			}
		}

		res, err := driver.List(ctx, blobkit.ListOptions{
			Prefix:    prefix,
			Delimiter: "/",
		})
		if err != nil {
			t.Fatalf("List with delimiter failed: %v", err)
		}

		if len(res.Objects) != 2 {
			t.Errorf("expected 2 direct objects, got %d", len(res.Objects))
		}
		if len(res.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d (%v)", len(res.CommonPrefixes), res.CommonPrefixes)
		}

		expectedPrefixes := map[string]bool{
			prefix + "dirA/": true,
			prefix + "dirB/": true,
		}
		for _, cp := range res.CommonPrefixes {
			if !expectedPrefixes[cp] {
				t.Errorf("unexpected common prefix: %s", cp)
			}
		}

		for _, k := range keys {
			_ = driver.Delete(ctx, k)
		}
	})
}
