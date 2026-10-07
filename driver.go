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
// (e.g. AWS S3, Cloudflare R2, MinIO, Wasabi, In-Memory, POSIX FS, SFTP, WebDAV, GCS, Azure, Google Drive).
//
// Driver Contract (Normative Specification):
// All Driver implementations MUST conform strictly to the semantic guarantees defined here.
// Implementations MUST NOT redefine or weaken these guarantees based on backend quirks.
type Driver interface {
	// Name returns the driver identifier (e.g. "r2-primary", "s3-backup", "memory").
	// MUST return a non-empty, deterministic string identifying this driver instance.
	Name() string

	// Capabilities returns the bitmask of operations supported by this driver.
	// MUST accurately declare supported features. A driver MUST NOT declare a capability
	// it cannot fulfill according to the normative contract.
	Capabilities() Capability

	// Put uploads an object stream to the driver backend.
	//
	// Semantic Guarantees:
	// - Inputs: obj MUST be non-nil with a valid Key. r MUST be non-nil.
	// - Missing/Invalid inputs: If r is nil, MUST return ErrNilReader. If obj is nil or Key is invalid, MUST return ErrInvalidKey or ErrSecurityViolation.
	// - Size semantics: If opts.ExplicitSize is true or opts.Size >= 0, the stream MUST yield exactly opts.Size bytes.
	//   If fewer bytes are read, MUST fail with ErrSizeMismatch.
	//   If surplus bytes remain after reading opts.Size bytes, MUST fail with ErrSizeMismatch.
	//   On ErrSizeMismatch or failure, the driver MUST NOT leave partially-created or corrupted objects in storage.
	// - Empty objects: MUST support storing, overwriting, and retrieving valid 0-byte objects.
	// - Overwrites: If an object already exists at obj.Key, Put MUST overwrite it.
	//   Filesystem implementations MUST perform atomic replacement (e.g. via temporary file rename).
	// - Context & Cancellation: If ctx is canceled or times out before or during upload, MUST abort and return ctx.Err().
	//   MUST clean up partial objects on cancellation.
	// - Concurrency: Put MUST be safe for concurrent execution across identical or distinct keys.
	Put(ctx context.Context, obj *Object, r io.Reader, opts PutOptions) (*Object, error)

	// Get retrieves an object stream and its metadata.
	//
	// Semantic Guarantees:
	// - Inputs: key MUST be valid. If key is invalid, MUST return ErrInvalidKey or ErrSecurityViolation.
	// - Missing object: If the object does not exist, MUST return ErrObjectNotFound.
	// - Empty object: If the object is 0 bytes, MUST return an ObjectReader whose Body yields 0 bytes and io.EOF.
	// - Preconditions: If opts contains IfMatch, IfNoneMatch, IfModifiedSince, or IfUnmodifiedSince,
	//   the driver MUST evaluate them. If preconditions fail, MUST return ErrPreconditionFailed.
	// - Byte-Range: If opts.Range is set and CapByteRangeGet is supported, MUST return the requested subslice.
	// - Context & Cancellation: If ctx is canceled, reading or requesting MUST return ctx.Err().
	Get(ctx context.Context, key string, opts GetOptions) (*ObjectReader, error)

	// Head inspects an object and returns its metadata without fetching the body.
	//
	// Semantic Guarantees:
	// - Inputs: key MUST be valid. If invalid, MUST return ErrInvalidKey or ErrSecurityViolation.
	// - Missing object: If the object does not exist, MUST return ErrObjectNotFound.
	// - Empty object: If the object is 0 bytes, MUST return Object with Size == 0 and nil error.
	// - Content: MUST NOT read or consume the body payload.
	Head(ctx context.Context, key string) (*Object, error)

	// Delete removes an object from storage.
	//
	// Semantic Guarantees:
	// - Inputs: key MUST be valid. If invalid, MUST return ErrInvalidKey or ErrSecurityViolation.
	// - Idempotency: Deleting an already-deleted or non-existent key MUST succeed and return nil error.
	// - Overwrite & Concurrency: MUST be safe for concurrent execution.
	// - Context: If ctx is canceled before execution, MUST return ctx.Err().
	Delete(ctx context.Context, key string) error

	// DeleteBatch removes multiple keys in a batch, returning deleted keys.
	//
	// Semantic Guarantees:
	// - CapBatchDelete: Only invoked if driver declares CapBatchDelete.
	// - Empty input: If keys is empty, MUST return empty list and nil error.
	// - Partial failure: MUST return the list of successfully deleted keys alongside any error encountered.
	DeleteBatch(ctx context.Context, keys []string) ([]string, error)

	// Copy duplicates an object within this driver backend.
	//
	// Semantic Guarantees:
	// - CapCopy: Only invoked if driver declares CapCopy.
	// - Missing source: If srcKey does not exist, MUST return ErrObjectNotFound.
	// - Destination: Overwrites dstKey if it exists. Source object MUST remain unchanged.
	Copy(ctx context.Context, srcKey, dstKey string) error

	// List queries objects matching criteria.
	//
	// Semantic Guarantees:
	// - Prefix & Delimiter: MUST accurately filter by prefix and group common prefixes when delimiter is provided.
	List(ctx context.Context, opts ListOptions) (*ListResult, error)

	// PresignGet creates a temporary signed URL for object download.
	// CapPresignGet: Only invoked if driver declares CapPresignGet.
	PresignGet(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error)

	// PresignPut creates a temporary signed URL for direct object upload.
	// CapPresignPut: Only invoked if driver declares CapPresignPut.
	PresignPut(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error)

	// ResolveURL builds a public access or CDN URL for the given key.
	ResolveURL(key string) (string, error)

	// CreateMultipart initiates a resumable multi-part upload session on the backend.
	//
	// Semantic Guarantees:
	// - CapMultipartSession: Only invoked if driver declares CapMultipartSession.
	// - Inputs: obj MUST be non-nil with valid Key.
	// - State: Initializes session in StateCreated. Returns unique uploadID.
	CreateMultipart(ctx context.Context, obj *Object, opts PutOptions) (uploadID string, err error)

	// UploadPart uploads a single chunk of a multipart upload.
	//
	// Semantic Guarantees:
	// - Inputs: partNumber MUST be >= 1. size MUST match the exact payload read from r.
	// - Size verification: If r yields fewer or more bytes than size, MUST fail with ErrSizeMismatch.
	// - Session existence: If uploadID does not exist or has been completed/aborted, MUST return ErrSessionNotFound.
	UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (etag string, err error)

	// CompleteMultipart finalizes a multipart upload session.
	//
	// Semantic Guarantees:
	// - Inputs: parts MUST contain at least one part (unless backend allows 0-part empty).
	// - Parts validation: parts MUST be ordered or sortable by partNumber without missing or duplicate parts.
	// - State: Transitions session to StateCompleted. Subsequent operations on uploadID MUST fail with ErrSessionNotFound.
	// - Cleanup: MUST clean up temporary staged chunks.
	CompleteMultipart(ctx context.Context, obj *Object, uploadID string, parts []CompletedPart) (*Object, error)

	// AbortMultipart cancels a multipart upload and cleans up stored parts.
	//
	// Semantic Guarantees:
	// - State: Transitions session to StateAborted. Cleans up all staged chunks.
	// - Idempotency: Calling AbortMultipart on an already-aborted session SHOULD return nil error or ErrSessionNotFound.
	AbortMultipart(ctx context.Context, key string, uploadID string) error

	// ListParts returns the list of parts already uploaded for an active session.
	//
	// Semantic Guarantees:
	// - If uploadID does not exist or is aborted/completed, MUST return ErrSessionNotFound.
	ListParts(ctx context.Context, key string, uploadID string) ([]CompletedPart, error)

	// Close gracefully terminates open connections and worker pools.
	Close() error
}
