package blobkit

import (
	"context"
	"io"
)

// BaseDriver provides a normative, contract-compliant base implementation of Driver.
// Embed BaseDriver in your custom storage drivers to automatically fulfill all optional
// capabilities with standardized ErrUnsupportedOperation errors according to the Driver contract.
//
// By embedding BaseDriver, authors of custom storage drivers only need to implement
// the operations their backend actually supports (e.g. Put, Get, Delete), while guaranteeing
// that unadvertised capabilities strictly conform to contract requirements.
type BaseDriver struct {
	name string
	caps Capability
}

// NewBaseDriver initializes a BaseDriver with a deterministic driver identifier and advertised capabilities.
func NewBaseDriver(name string, caps Capability) BaseDriver {
	return BaseDriver{
		name: name,
		caps: caps,
	}
}

// Name returns the driver identifier.
func (b *BaseDriver) Name() string {
	return b.name
}

// Capabilities returns the bitmask of features supported by the driver.
func (b *BaseDriver) Capabilities() Capability {
	return b.caps
}

// Put is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) Put(ctx context.Context, obj *Object, r io.Reader, opts PutOptions) (*Object, error) {
	key := ""
	if obj != nil {
		key = obj.Key
	}
	return nil, WrapError("put", key, b.name, ErrUnsupportedOperation)
}

// Get is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) Get(ctx context.Context, key string, opts GetOptions) (*ObjectReader, error) {
	return nil, WrapError("get", key, b.name, ErrUnsupportedOperation)
}

// Head is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) Head(ctx context.Context, key string) (*Object, error) {
	return nil, WrapError("head", key, b.name, ErrUnsupportedOperation)
}

// Delete is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) Delete(ctx context.Context, key string) error {
	return WrapError("delete", key, b.name, ErrUnsupportedOperation)
}

// DeleteBatch is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	return nil, WrapError("delete_batch", "", b.name, ErrUnsupportedOperation)
}

// Copy is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) Copy(ctx context.Context, srcKey, dstKey string) error {
	return WrapError("copy", srcKey, b.name, ErrUnsupportedOperation)
}

// List is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	return nil, WrapError("list", opts.Prefix, b.name, ErrUnsupportedOperation)
}

// PresignGet is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) PresignGet(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error) {
	return nil, WrapError("presign_get", key, b.name, ErrUnsupportedOperation)
}

// PresignPut is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) PresignPut(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error) {
	return nil, WrapError("presign_put", key, b.name, ErrUnsupportedOperation)
}

// ResolveURL is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) ResolveURL(key string) (string, error) {
	return "", WrapError("resolve_url", key, b.name, ErrUnsupportedOperation)
}

// CreateMultipart is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) CreateMultipart(ctx context.Context, obj *Object, opts PutOptions) (string, error) {
	key := ""
	if obj != nil {
		key = obj.Key
	}
	return "", WrapError("create_multipart", key, b.name, ErrUnsupportedOperation)
}

// UploadPart is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	return "", WrapError("upload_part", key, b.name, ErrUnsupportedOperation)
}

// CompleteMultipart is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) CompleteMultipart(ctx context.Context, obj *Object, uploadID string, parts []CompletedPart) (*Object, error) {
	key := ""
	if obj != nil {
		key = obj.Key
	}
	return nil, WrapError("complete_multipart", key, b.name, ErrUnsupportedOperation)
}

// AbortMultipart is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	return WrapError("abort_multipart", key, b.name, ErrUnsupportedOperation)
}

// ListParts is a default stub that returns ErrUnsupportedOperation.
func (b *BaseDriver) ListParts(ctx context.Context, key string, uploadID string) ([]CompletedPart, error) {
	return nil, WrapError("list_parts", key, b.name, ErrUnsupportedOperation)
}

// Close gracefully closes the driver. Default is a no-op returning nil.
func (b *BaseDriver) Close() error {
	return nil
}
