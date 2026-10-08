package blobkit_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
)

func TestSeeker_RandomAccessAndSeeking(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test"})
	bucket, err := blobkit.NewBucket(driver)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	// 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ
	payload := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	key := "data/alphabet.txt"
	_, err = bucket.PutBytes(ctx, key, payload, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	seeker, err := bucket.OpenSeeker(ctx, key)
	if err != nil {
		t.Fatalf("OpenSeeker failed: %v", err)
	}
	defer seeker.Close()

	if seeker.Size() != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), seeker.Size())
	}

	// 1. Read first 5 bytes from start
	buf := make([]byte, 5)
	n, err := seeker.Read(buf)
	if err != nil || n != 5 || string(buf) != "01234" {
		t.Fatalf("expected '01234', got %q (n=%d err=%v)", string(buf), n, err)
	}

	// 2. Seek to offset 10 from start ('A')
	pos, err := seeker.Seek(10, io.SeekStart)
	if err != nil || pos != 10 {
		t.Fatalf("Seek to 10 failed: pos=%d err=%v", pos, err)
	}
	n, err = seeker.Read(buf)
	if err != nil || n != 5 || string(buf) != "ABCDE" {
		t.Fatalf("expected 'ABCDE', got %q", string(buf))
	}

	// 3. Seek current (+2, skip 'FG', land on 'H')
	pos, err = seeker.Seek(2, io.SeekCurrent)
	if err != nil || pos != 17 {
		t.Fatalf("Seek current +2 failed: pos=%d err=%v", pos, err)
	}
	n, err = seeker.Read(buf)
	if err != nil || n != 5 || string(buf) != "HIJKL" {
		t.Fatalf("expected 'HIJKL', got %q", string(buf))
	}

	// 4. Seek from end (-3, 'XYZ')
	buf3 := make([]byte, 3)
	pos, err = seeker.Seek(-3, io.SeekEnd)
	if err != nil || pos != int64(len(payload)-3) {
		t.Fatalf("Seek end -3 failed: pos=%d err=%v", pos, err)
	}
	n, err = seeker.Read(buf3)
	if err != nil || n != 3 || string(buf3) != "XYZ" {
		t.Fatalf("expected 'XYZ', got %q", string(buf3))
	}

	// 5. Read past end returns EOF
	n, err = seeker.Read(buf3)
	if err != io.EOF || n != 0 {
		t.Fatalf("expected (0, io.EOF), got (%d, %v)", n, err)
	}

	// 6. Negative seek error
	if _, err := seeker.Seek(-1, io.SeekStart); err == nil {
		t.Fatalf("expected error on negative seek offset")
	}
}

func TestSeeker_ConcurrentReadAt(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test"})
	bucket, _ := blobkit.NewBucket(driver)

	payload := []byte("The quick brown fox jumps over the lazy dog.")
	key := "text/fox.txt"
	_, _ = bucket.PutBytes(ctx, key, payload, blobkit.PutOptions{})

	seeker, err := bucket.OpenSeeker(ctx, key)
	if err != nil {
		t.Fatalf("OpenSeeker failed: %v", err)
	}
	defer seeker.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 5)
			// Read "brown" at offset 10
			n, err := seeker.ReadAt(buf, 10)
			if err != nil || n != 5 || string(buf) != "brown" {
				t.Errorf("ReadAt failed: got %q (n=%d err=%v)", string(buf), n, err)
			}

			// Read "lazy" at offset 35
			buf4 := make([]byte, 4)
			n, err = seeker.ReadAt(buf4, 35)
			if err != nil || n != 4 || string(buf4) != "lazy" {
				t.Errorf("ReadAt failed: got %q (n=%d err=%v)", string(buf4), n, err)
			}
		}()
	}
	wg.Wait()
}

func TestSeeker_ZipReaderIntegration(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "test"})
	bucket, _ := blobkit.NewBucket(driver)

	// Create a real ZIP archive in memory
	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)

	f1, _ := zw.Create("hello.txt")
	_, _ = f1.Write([]byte("Hello from ZIP inside remote BlobKit!"))
	f2, _ := zw.Create("folder/nested.json")
	_, _ = f2.Write([]byte(`{"status":"ok"}`))
	_ = zw.Close()

	key := "archives/bundle.zip"
	_, err := bucket.PutBytes(ctx, key, zipBuf.Bytes(), blobkit.PutOptions{
		ContentType: "application/zip",
	})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	// Open Seeker over the zip file
	seeker, err := bucket.OpenSeeker(ctx, key)
	if err != nil {
		t.Fatalf("OpenSeeker failed: %v", err)
	}
	defer seeker.Close()

	// Pass seeker directly to standard library zip.NewReader!
	// (zip.NewReader only reads the central directory at the end of the file via ReadAt)
	zr, err := zip.NewReader(seeker, seeker.Size())
	if err != nil {
		t.Fatalf("zip.NewReader failed: %v", err)
	}

	if len(zr.File) != 2 {
		t.Fatalf("expected 2 files in zip, got %d", len(zr.File))
	}

	// Read hello.txt directly from zip
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatalf("open zipped file failed: %v", err)
	}
	content, _ := io.ReadAll(rc)
	_ = rc.Close()

	if string(content) != "Hello from ZIP inside remote BlobKit!" {
		t.Fatalf("unexpected content: %q", string(content))
	}
}

func TestSeeker_ClientBridge(t *testing.T) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{Bucket: "bridge"})
	client, _ := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithRegistry(registry.NewMemoryStore()),
	)
	defer client.Close()

	payload := []byte("0123456789")
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "test",
		Filename:  "sample.txt",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 1. Open seeker by ObjectID
	seeker, err := client.OpenSeeker(ctx, obj.ID)
	if err != nil {
		t.Fatalf("client.OpenSeeker by ID failed: %v", err)
	}
	defer seeker.Close()

	buf := make([]byte, 4)
	_, _ = seeker.Seek(4, io.SeekStart)
	_, _ = seeker.Read(buf)
	if string(buf) != "4567" {
		t.Fatalf("expected '4567', got %q", string(buf))
	}

	// 2. Open seeker directly by physical storage Key
	seekerByKey, err := client.OpenSeeker(ctx, obj.Key)
	if err != nil {
		t.Fatalf("client.OpenSeeker by Key failed: %v", err)
	}
	defer seekerByKey.Close()

	bufKey := make([]byte, 4)
	_, _ = seekerByKey.Seek(6, io.SeekStart)
	_, _ = seekerByKey.Read(bufKey)
	if string(bufKey) != "6789" {
		t.Fatalf("expected '6789', got %q", string(bufKey))
	}
}

func TestSeeker_UnsupportedCapability(t *testing.T) {
	ctx := context.Background()
	// Create a dummy base driver that lacks CapByteRangeGet
	base := blobkit.NewBaseDriver("no-range", blobkit.CapDirectPut)
	bucket, _ := blobkit.NewBucket(&base)

	_, err := bucket.OpenSeeker(ctx, "file.txt")
	if !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on driver without CapByteRangeGet, got: %v", err)
	}
}
