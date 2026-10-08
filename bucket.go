package blobkit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// Bucket provides a lightweight, stateless, key-value object storage abstraction.
// It bypasses the database metadata registry, canonical ObjectID translations,
// and background lifecycle management of Client, communicating directly with a Driver
// using raw storage keys (e.g. "avatars/user-123.png").
//
// Use Bucket when your application needs high-throughput, direct wire-level object storage
// without database dependencies or opinionated key generation.
type Bucket struct {
	driver Driver
}

// NewBucket constructs a low-level Bucket backed by the provided Driver.
func NewBucket(driver Driver) (*Bucket, error) {
	if driver == nil {
		return nil, errors.New("blobkit: driver cannot be nil")
	}
	return &Bucket{driver: driver}, nil
}

// Driver returns the underlying Driver instance.
func (b *Bucket) Driver() Driver {
	return b.driver
}

// Name returns the driver identifier (e.g. "s3-primary", "r2-edge", "memory").
func (b *Bucket) Name() string {
	return b.driver.Name()
}

// Capabilities returns the supported capability bitmask of the driver.
func (b *Bucket) Capabilities() Capability {
	return b.driver.Capabilities()
}

// Put uploads an object stream directly to the specified key.
func (b *Bucket) Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*Object, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("put", key, b.driver.Name(), err)
	}
	if r == nil {
		return nil, WrapError("put", key, b.driver.Name(), ErrNilReader)
	}

	obj := &Object{
		Key:         key,
		ContentType: opts.ContentType,
		Metadata:    opts.Metadata,
		Status:      StateCommitted,
		Provider:    b.driver.Name(),
	}

	saved, err := b.driver.Put(ctx, obj, r, opts)
	if err != nil {
		return nil, err
	}
	return saved, nil
}

// PutBytes is a convenience method to store an in-memory byte slice directly.
func (b *Bucket) PutBytes(ctx context.Context, key string, data []byte, opts PutOptions) (*Object, error) {
	opts.Size = int64(len(data))
	opts.ExplicitSize = true
	return b.Put(ctx, key, bytes.NewReader(data), opts)
}

// Get retrieves an object stream and its metadata.
func (b *Bucket) Get(ctx context.Context, key string, opts GetOptions) (*ObjectReader, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("get", key, b.driver.Name(), err)
	}
	return b.driver.Get(ctx, key, opts)
}

// GetBytes reads the entire object payload into an in-memory byte slice.
func (b *Bucket) GetBytes(ctx context.Context, key string, opts GetOptions) ([]byte, *Object, error) {
	r, err := b.Get(ctx, key, opts)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, WrapError("read_payload", key, b.driver.Name(), err)
	}
	return data, &r.Object, nil
}

// Head retrieves metadata for an object without reading its payload body.
func (b *Bucket) Head(ctx context.Context, key string) (*Object, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("head", key, b.driver.Name(), err)
	}
	return b.driver.Head(ctx, key)
}

// Exists reports whether an object currently exists at the specified key.
func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	_, err := b.Head(ctx, key)
	if err == nil {
		return true, nil
	}
	if IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// Delete permanently removes an object by key.
func (b *Bucket) Delete(ctx context.Context, key string) error {
	if err := ValidateKey(key); err != nil {
		return WrapError("delete", key, b.driver.Name(), err)
	}
	return b.driver.Delete(ctx, key)
}

// DeleteBatch removes multiple keys in a single operation.
func (b *Bucket) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	for _, k := range keys {
		if err := ValidateKey(k); err != nil {
			return nil, WrapError("delete_batch", k, b.driver.Name(), err)
		}
	}
	return b.driver.DeleteBatch(ctx, keys)
}

// Copy duplicates an object within this bucket.
func (b *Bucket) Copy(ctx context.Context, srcKey, dstKey string) error {
	if err := ValidateKey(srcKey); err != nil {
		return WrapError("copy", srcKey, b.driver.Name(), err)
	}
	if err := ValidateKey(dstKey); err != nil {
		return WrapError("copy", dstKey, b.driver.Name(), err)
	}
	return b.driver.Copy(ctx, srcKey, dstKey)
}

// List queries objects matching prefix and filter criteria.
func (b *Bucket) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	return b.driver.List(ctx, opts)
}

// PresignGet generates a temporary signed download URL for direct client access.
func (b *Bucket) PresignGet(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("presign_get", key, b.driver.Name(), err)
	}
	return b.driver.PresignGet(ctx, key, opts)
}

// PresignPut generates a temporary signed upload URL for direct browser-to-storage uploads.
func (b *Bucket) PresignPut(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("presign_put", key, b.driver.Name(), err)
	}
	return b.driver.PresignPut(ctx, key, opts)
}

// ResolveURL builds a public access or CDN delivery URL for the given key.
func (b *Bucket) ResolveURL(key string) (string, error) {
	if err := ValidateKey(key); err != nil {
		return "", WrapError("resolve_url", key, b.driver.Name(), err)
	}
	return b.driver.ResolveURL(key)
}

// CreateMultipart initializes a multi-part upload session directly on the storage provider.
func (b *Bucket) CreateMultipart(ctx context.Context, key string, opts PutOptions) (uploadID string, err error) {
	if err := ValidateKey(key); err != nil {
		return "", WrapError("create_multipart", key, b.driver.Name(), err)
	}
	obj := &Object{
		Key:         key,
		ContentType: opts.ContentType,
		Metadata:    opts.Metadata,
		Status:      StatePending,
		Provider:    b.driver.Name(),
	}
	return b.driver.CreateMultipart(ctx, obj, opts)
}

// UploadPart uploads a single part of an ongoing multi-part upload session.
func (b *Bucket) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (etag string, err error) {
	if err := ValidateKey(key); err != nil {
		return "", WrapError("upload_part", key, b.driver.Name(), err)
	}
	return b.driver.UploadPart(ctx, key, uploadID, partNumber, r, size)
}

// CompleteMultipart finalizes an ongoing multi-part upload session.
func (b *Bucket) CompleteMultipart(ctx context.Context, key string, uploadID string, parts []CompletedPart) (*Object, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("complete_multipart", key, b.driver.Name(), err)
	}
	obj := &Object{
		Key:      key,
		Status:   StateCommitted,
		Provider: b.driver.Name(),
	}
	return b.driver.CompleteMultipart(ctx, obj, uploadID, parts)
}

// AbortMultipart cancels a multi-part upload session and cleans up staged parts.
func (b *Bucket) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	if err := ValidateKey(key); err != nil {
		return WrapError("abort_multipart", key, b.driver.Name(), err)
	}
	return b.driver.AbortMultipart(ctx, key, uploadID)
}

// ListParts returns the list of parts already committed to an active multipart session.
func (b *Bucket) ListParts(ctx context.Context, key string, uploadID string) ([]CompletedPart, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("list_parts", key, b.driver.Name(), err)
	}
	return b.driver.ListParts(ctx, key, uploadID)
}

// BucketWriter implements io.WriteCloser for streaming writes directly into storage.
type BucketWriter struct {
	once   sync.Once
	pw     *io.PipeWriter
	doneCh chan error
	err    error
	closed atomic.Bool
}

// Write writes a slice of bytes to the underlying streaming pipeline.
func (w *BucketWriter) Write(p []byte) (n int, err error) {
	if w.closed.Load() {
		return 0, errors.New("blobkit: writer is closed")
	}
	return w.pw.Write(p)
}

// Close finalizes the upload stream and waits for the storage backend to confirm persistence.
func (w *BucketWriter) Close() error {
	w.once.Do(func() {
		w.closed.Store(true)
		_ = w.pw.Close()
		w.err = <-w.doneCh
	})
	return w.err
}

// CloseWithError aborts the streaming upload with the specified error.
func (w *BucketWriter) CloseWithError(err error) error {
	w.once.Do(func() {
		w.closed.Store(true)
		_ = w.pw.CloseWithError(err)
		w.err = <-w.doneCh
	})
	return w.err
}

// NewWriter creates an io.WriteCloser that streams data directly to the given key.
// The upload executes concurrently as bytes are written. Calling Close() finalizes the upload.
func (b *Bucket) NewWriter(ctx context.Context, key string, opts PutOptions) (*BucketWriter, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("new_writer", key, b.driver.Name(), err)
	}

	opts.Size = -1 // streaming with unknown size
	pr, pw := io.Pipe()
	doneCh := make(chan error, 1)

	go func() {
		defer pr.Close()
		_, err := b.Put(ctx, key, pr, opts)
		doneCh <- err
	}()

	return &BucketWriter{
		pw:     pw,
		doneCh: doneCh,
	}, nil
}

// Close gracefully closes the underlying Driver.
func (b *Bucket) Close() error {
	return b.driver.Close()
}
