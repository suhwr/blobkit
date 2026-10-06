package blobkit

import (
	"context"
	"io"
)

// Capability is a bitmask representing features supported by a storage driver.
type Capability uint32

const (
	// CapDirectPut indicates support for single-shot uploads.
	CapDirectPut Capability = 1 << iota

	// CapMultipartPut indicates support for internal chunked multipart uploads.
	CapMultipartPut

	// CapPresignGet indicates support for generating presigned GET URLs.
	CapPresignGet

	// CapPresignPut indicates support for generating presigned PUT URLs.
	CapPresignPut

	// CapBatchDelete indicates support for deleting multiple keys in a single operation.
	CapBatchDelete

	// CapByteRangeGet indicates support for HTTP Range requests.
	CapByteRangeGet

	// CapCopy indicates support for server-side object duplication.
	CapCopy

	// CapMultipartSession indicates support for resumable multi-part upload primitives.
	CapMultipartSession
)

// CompletedPart records information about an uploaded multipart part.
type CompletedPart struct {
	// PartNumber is the 1-based index of the part.
	PartNumber int32 `json:"part_number"`

	// ETag is the entity tag assigned by the storage provider for this part.
	ETag string `json:"etag"`

	// Size is the part size in bytes.
	Size int64 `json:"size"`
}

// Driver is the storage abstraction interface implemented by backend drivers
// (e.g. AWS S3, Cloudflare R2, MinIO, Wasabi, In-Memory).
type Driver interface {
	// Name returns the driver identifier (e.g. "r2-primary", "s3-backup", "memory").
	Name() string

	// Capabilities returns the bitmask of operations supported by this driver.
	Capabilities() Capability

	// Put uploads an object stream to the driver backend.
	Put(ctx context.Context, obj *Object, r io.Reader, opts PutOptions) (*Object, error)

	// Get retrieves an object stream and its metadata.
	Get(ctx context.Context, key string, opts GetOptions) (*ObjectReader, error)

	// Head inspects an object and returns its metadata without fetching the body.
	Head(ctx context.Context, key string) (*Object, error)

	// Delete removes an object from storage.
	Delete(ctx context.Context, key string) error

	// DeleteBatch removes multiple keys in a batch, returning deleted keys.
	DeleteBatch(ctx context.Context, keys []string) ([]string, error)

	// Copy duplicates an object within this driver backend.
	Copy(ctx context.Context, srcKey, dstKey string) error

	// List queries objects matching criteria.
	List(ctx context.Context, opts ListOptions) (*ListResult, error)

	// PresignGet creates a temporary signed URL for object download.
	PresignGet(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error)

	// PresignPut creates a temporary signed URL for direct object upload.
	PresignPut(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error)

	// ResolveURL builds a public access or CDN URL for the given key.
	ResolveURL(key string) (string, error)

	// CreateMultipart initiates a resumable multi-part upload session on the backend.
	CreateMultipart(ctx context.Context, obj *Object, opts PutOptions) (uploadID string, err error)

	// UploadPart uploads a single chunk of a multipart upload.
	UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (etag string, err error)

	// CompleteMultipart finalizes a multipart upload session.
	CompleteMultipart(ctx context.Context, obj *Object, uploadID string, parts []CompletedPart) (*Object, error)

	// AbortMultipart cancels a multipart upload and cleans up stored parts.
	AbortMultipart(ctx context.Context, key string, uploadID string) error

	// ListParts returns the list of parts already uploaded for an active session.
	ListParts(ctx context.Context, key string, uploadID string) ([]CompletedPart, error)

	// Close gracefully terminates open connections and worker pools.
	Close() error
}
