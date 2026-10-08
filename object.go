package blobkit

import (
	"io"
	"time"
)

// Visibility defines the access control scope of an object.
type Visibility string

const (
	// VisibilityPublic marks the object as publicly deliverable via CDN or bucket public endpoint.
	VisibilityPublic Visibility = "public"

	// VisibilityPrivate marks the object as private, requiring presigned URLs or authenticated streams.
	VisibilityPrivate Visibility = "private"
)

// LifecycleState defines the storage lifecycle status of an object.
type LifecycleState string

const (
	// StatePending indicates that the upload is initiated or pending validation.
	StatePending LifecycleState = "pending"

	// StateCommitted indicates that the upload is finalized and verified in storage.
	StateCommitted LifecycleState = "committed"

	// StateAborted indicates that the upload failed or was aborted before commit.
	StateAborted LifecycleState = "aborted"

	// StateDeleted indicates that the object has been soft-deleted or scheduled for purge.
	StateDeleted LifecycleState = "deleted"
)

// Object represents a stored blob and its canonical system and custom metadata.
type Object struct {
	// ID is the logical identifier (typically UUIDv7) used for database indexing and reference.
	ID string `json:"id"`

	// Namespace partitions objects logically (e.g. "avatars", "bots/autorespon", "media").
	Namespace string `json:"namespace"`

	// OwnerID identifies the tenant, user, or entity owning this object.
	OwnerID string `json:"owner_id,omitempty"`

	// Key is the physical storage path within the provider's bucket.
	Key string `json:"key"`

	// Bucket is the name of the bucket where the object resides.
	Bucket string `json:"bucket"`

	// Size is the object content length in bytes.
	Size int64 `json:"size"`

	// ContentType is the detected or explicitly specified MIME type.
	ContentType string `json:"content_type"`

	// ETag is the entity tag assigned by the storage provider.
	ETag string `json:"etag,omitempty"`

	// ChecksumSHA256 is the hex-encoded SHA-256 hash of the object payload (if computed).
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`

	// OriginalFilename is the client-facing filename (e.g. "photo.jpg").
	OriginalFilename string `json:"original_filename,omitempty"`

	// Visibility determines whether the object is public or private.
	Visibility Visibility `json:"visibility"`

	// Status indicates the current lifecycle state (pending, committed, deleted).
	Status LifecycleState `json:"status"`

	// Metadata contains user-defined metadata key-value pairs.
	Metadata map[string]string `json:"metadata,omitempty"`

	// RetentionUntil specifies when retention expires. The object cannot be deleted before this date.
	RetentionUntil *time.Time `json:"retention_until,omitempty"`

	// ExpiresAt specifies when the object expires and is eligible for garbage collection.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// LegalHold prevents deletion or modification when true, regardless of retention expiration.
	LegalHold bool `json:"legal_hold,omitempty"`

	// DeletedAt records when the object was soft-deleted.
	DeletedAt *time.Time `json:"deleted_at,omitempty"`

	// ClientChecksum is the optional client-declared payload checksum.
	ClientChecksum string `json:"client_checksum,omitempty"`

	// CreatedAt is the logical creation timestamp.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is the last modification timestamp.
	UpdatedAt time.Time `json:"updated_at"`

	// Provider is the identifier of the driver that stored this object (e.g. "r2-primary", "s3-backup").
	Provider string `json:"provider"`
}

// ObjectReader pairs object metadata with an active streaming body.
// Callers must call Close() when finished reading.
type ObjectReader struct {
	Object
	Body io.ReadCloser
}

// Close closes the underlying reader stream.
func (r *ObjectReader) Close() error {
	if r.Body != nil {
		return r.Body.Close()
	}
	return nil
}

// Read implements io.Reader by delegating to the underlying body stream.
func (r *ObjectReader) Read(p []byte) (n int, err error) {
	if r.Body == nil {
		return 0, io.EOF
	}
	return r.Body.Read(p)
}

// WriteTo implements io.WriterTo by forwarding the stream directly to w.
// If the underlying Body implements io.WriterTo (such as *os.File), it enables
// zero-copy kernel transfer mechanisms (e.g. Linux sendfile(2) / splice(2)) in standard HTTP handlers.
func (r *ObjectReader) WriteTo(w io.Writer) (int64, error) {
	if r.Body == nil {
		return 0, nil
	}
	if wt, ok := r.Body.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, r.Body)
}

// PutOptions configures an upload operation, conveying semantic intent.
type PutOptions struct {
	// ID is an optional predetermined logical ID. If empty, a UUIDv7 is generated automatically.
	ID string

	// Key is an optional explicit physical storage key. If empty, KeyGenerator determines it.
	Key string

	// Namespace partitions objects logically (e.g. "avatars", "bots/autorespon", "media").
	Namespace string

	// OwnerID identifies the owner/tenant of the object.
	OwnerID string

	// Filename is the original client filename (e.g. "report.pdf"), used for Content-Disposition & MIME hints.
	Filename string

	// ContentType is the MIME type. If empty, BlobKit sniffs up to the first 512 bytes.
	ContentType string

	// Size is the known payload size in bytes. Set to SizeUnknown (-1) if unknown or streaming.
	Size int64

	// ExplicitSize indicates that Size was intentionally specified (even if 0),
	// allowing distinction between an unassigned size (streaming) and an explicit 0-byte upload.
	ExplicitSize bool

	// Visibility controls whether the object is public or private. Defaults to VisibilityPrivate.
	Visibility Visibility

	// Metadata contains custom user key-value metadata to store alongside the object.
	Metadata map[string]string

	// ContentDisposition customizes the Content-Disposition HTTP header.
	ContentDisposition string

	// CacheControl customizes the Cache-Control HTTP header (e.g. "public, max-age=31536000, immutable").
	CacheControl string

	// RetentionUntil locks the object against deletion or modification until this timestamp.
	RetentionUntil *time.Time

	// ExpiresAt marks the object to expire automatically after this timestamp.
	ExpiresAt *time.Time

	// LegalHold marks the object with an active legal hold.
	LegalHold bool

	// ClientChecksum specifies an expected payload SHA-256 hash to enforce on upload.
	ClientChecksum string

	// VerifyIntegrity forces BlobKit to stream-hash and verify the payload SHA-256 during upload.
	VerifyIntegrity bool

	// Policy overrides client-wide upload validation rules for this specific upload.
	Policy *Policy

	// Provider forces the upload to use a specific registered driver instead of the router policy.
	Provider string
}

// GetOptions configures an object download or stream retrieval.
type GetOptions struct {
	// Range specifies an HTTP byte range (e.g. "bytes=0-1024").
	Range string

	// IfMatch returns the object only if its ETag matches this value.
	IfMatch string

	// IfNoneMatch returns the object only if its ETag does not match this value.
	IfNoneMatch string

	// IfModifiedSince returns the object only if it has been modified since this timestamp.
	IfModifiedSince *time.Time

	// IfUnmodifiedSince returns the object only if it has not been modified since this timestamp.
	IfUnmodifiedSince *time.Time
}

// ListOptions specifies parameters for listing objects in a bucket or prefix.
type ListOptions struct {
	// Prefix filters objects starting with this prefix string.
	Prefix string

	// Delimiter causes keys containing this delimiter to be grouped in CommonPrefixes.
	Delimiter string

	// Cursor is the continuation token for pagination.
	Cursor string

	// Limit specifies the maximum number of items to return.
	Limit int
}

// ListResult holds the outcome of a List operation.
type ListResult struct {
	// Objects is the list of objects returned.
	Objects []Object `json:"objects"`

	// NextCursor is the continuation token to pass to subsequent List calls, or empty if done.
	NextCursor string `json:"next_cursor,omitempty"`

	// CommonPrefixes contains prefixes grouped by the delimiter (e.g. directories).
	CommonPrefixes []string `json:"common_prefixes,omitempty"`

	// IsTruncated indicates whether more items remain in the bucket.
	IsTruncated bool `json:"is_truncated"`
}
