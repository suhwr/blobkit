package blobkit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/suhwr/blobkit"
)

// customStubDriver demonstrates how a third-party or user-defined storage engine
// easily embeds BaseDriver to provide only Put and Get, automatically fulfilling
// all contract obligations for unsupported capabilities.
type customStubDriver struct {
	blobkit.BaseDriver
	storage map[string][]byte
}

func newCustomStubDriver() *customStubDriver {
	return &customStubDriver{
		BaseDriver: blobkit.NewBaseDriver("custom-kv", blobkit.CapDirectPut),
		storage:    make(map[string][]byte),
	}
}

func (d *customStubDriver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if r == nil {
		return nil, blobkit.ErrNilReader
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	d.storage[obj.Key] = data
	stored := *obj
	stored.Size = int64(len(data))
	stored.Provider = d.Name()
	return &stored, nil
}

func (d *customStubDriver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	data, ok := d.storage[key]
	if !ok {
		return nil, blobkit.ErrObjectNotFound
	}
	return &blobkit.ObjectReader{
		Object: blobkit.Object{
			Key:  key,
			Size: int64(len(data)),
		},
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func TestBaseDriver_DefaultContractBehavior(t *testing.T) {
	ctx := context.Background()
	base := blobkit.NewBaseDriver("test-base", 0)

	if base.Name() != "test-base" {
		t.Fatalf("expected name test-base, got %s", base.Name())
	}
	if base.Capabilities() != 0 {
		t.Fatalf("expected caps 0, got %d", base.Capabilities())
	}

	// Verify all default stubs return ErrUnsupportedOperation (ErrNotSupported)
	if _, err := base.Put(ctx, &blobkit.Object{Key: "k"}, bytes.NewReader(nil), blobkit.PutOptions{}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on Put, got: %v", err)
	}
	if _, err := base.Get(ctx, "k", blobkit.GetOptions{}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on Get, got: %v", err)
	}
	if _, err := base.Head(ctx, "k"); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on Head, got: %v", err)
	}
	if err := base.Delete(ctx, "k"); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on Delete, got: %v", err)
	}
	if _, err := base.DeleteBatch(ctx, []string{"k"}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on DeleteBatch, got: %v", err)
	}
	if err := base.Copy(ctx, "src", "dst"); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on Copy, got: %v", err)
	}
	if _, err := base.List(ctx, blobkit.ListOptions{}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on List, got: %v", err)
	}
	if _, err := base.PresignGet(ctx, "k", blobkit.PresignOptions{}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on PresignGet, got: %v", err)
	}
	if _, err := base.PresignPut(ctx, "k", blobkit.PresignOptions{}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on PresignPut, got: %v", err)
	}
	if _, err := base.ResolveURL("k"); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on ResolveURL, got: %v", err)
	}
	if _, err := base.CreateMultipart(ctx, &blobkit.Object{Key: "k"}, blobkit.PutOptions{}); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on CreateMultipart, got: %v", err)
	}
	if _, err := base.UploadPart(ctx, "k", "up1", 1, bytes.NewReader(nil), 0); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on UploadPart, got: %v", err)
	}
	if _, err := base.CompleteMultipart(ctx, &blobkit.Object{Key: "k"}, "up1", nil); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on CompleteMultipart, got: %v", err)
	}
	if err := base.AbortMultipart(ctx, "k", "up1"); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on AbortMultipart, got: %v", err)
	}
	if _, err := base.ListParts(ctx, "k", "up1"); !errors.Is(err, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation on ListParts, got: %v", err)
	}
	if err := base.Close(); err != nil {
		t.Fatalf("expected nil error on Close, got: %v", err)
	}
}

func TestCustomStubDriver_WithBucket(t *testing.T) {
	ctx := context.Background()
	driver := newCustomStubDriver()

	bucket, err := blobkit.NewBucket(driver)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	payload := []byte("custom storage payload")
	_, err = bucket.PutBytes(ctx, "custom/file.txt", payload, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	got, _, err := bucket.GetBytes(ctx, "custom/file.txt", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("GetBytes failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("expected %q, got %q", payload, got)
	}

	// Calling an unadvertised operation on the custom driver returns ErrNotSupported
	if err := bucket.Copy(ctx, "custom/file.txt", "copy.txt"); !errors.Is(err, blobkit.ErrNotSupported) {
		t.Fatalf("expected ErrNotSupported on Copy, got: %v", err)
	}
}

func TestObjectReader_WriteTo(t *testing.T) {
	data := []byte("Testing WriterTo zero-copy streaming")
	reader := &blobkit.ObjectReader{
		Object: blobkit.Object{Size: int64(len(data))},
		Body:   io.NopCloser(bytes.NewReader(data)),
	}

	var buf bytes.Buffer
	n, err := reader.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("expected %q, got %q", data, buf.Bytes())
	}

	// Nil body check
	nilReader := &blobkit.ObjectReader{}
	nZero, errZero := nilReader.WriteTo(&buf)
	if errZero != nil || nZero != 0 {
		t.Fatalf("expected (0, nil) for nil body WriteTo")
	}
}
