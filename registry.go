package blobkit

import (
	"context"
	"time"
)

// Record represents the canonical metadata and indexing record stored in the database registry.
// NOTE: Full URL is explicitly omitted from canonical record storage, as URLs are delivery/access
// details dynamically constructed by BlobKit when requested (CDN or presigned).
type Record struct {
	// ObjectID is the immutable logical identifier (UUIDv7) and primary database key.
	ObjectID string `json:"object_id"`

	// Namespace partitions objects logically (e.g. "avatars", "bots/autorespon", "media").
	Namespace string `json:"namespace"`

	// OwnerID identifies the tenant, user, or bot owning this object.
	OwnerID string `json:"owner_id,omitempty"`

	// Key is the physical storage key inside the provider's bucket.
	Key string `json:"key"`

	// Bucket is the storage bucket name.
	Bucket string `json:"bucket"`

	// Provider is the identifier of the driver holding the bytes (e.g. "r2-primary").
	Provider string `json:"provider"`

	// MIMEType is the verified Content-Type of the blob.
	MIMEType string `json:"mime_type"`

	// Size is the content length in bytes.
	Size int64 `json:"size"`

	// ChecksumSHA256 is the hex-encoded SHA-256 payload digest.
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`

	// OriginalFilename is the client-facing filename (e.g. "photo.png").
	OriginalFilename string `json:"original_filename,omitempty"`

	// Visibility specifies access level (public or private).
	Visibility Visibility `json:"visibility"`

	// Status indicates lifecycle state (pending, committed, deleted).
	Status LifecycleState `json:"status"`

	// Metadata holds arbitrary user-defined key-value attributes.
	Metadata map[string]string `json:"metadata,omitempty"`

	// CreatedAt is the logical creation timestamp.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is the last modification timestamp.
	UpdatedAt time.Time `json:"updated_at"`

	// DeletedAt records when the object was marked deleted (if soft-deleted).
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

// ToObject converts a registry Record into a BlobKit Object reference.
func (r *Record) ToObject() Object {
	return Object{
		ID:               r.ObjectID,
		Namespace:        r.Namespace,
		OwnerID:          r.OwnerID,
		Key:              r.Key,
		Bucket:           r.Bucket,
		Size:             r.Size,
		ContentType:      r.MIMEType,
		ChecksumSHA256:   r.ChecksumSHA256,
		OriginalFilename: r.OriginalFilename,
		Visibility:       r.Visibility,
		Status:           r.Status,
		Metadata:         r.Metadata,
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
		Provider:         r.Provider,
	}
}

// Filter specifies query criteria for discovering objects by metadata without needing URLs.
type Filter struct {
	// ObjectID filters by canonical identifier.
	ObjectID string

	// Namespace filters by category or sub-category (e.g. "bots/autorespon").
	Namespace string

	// OwnerID filters by owner, user, or tenant.
	OwnerID string

	// MIMEType filters by MIME type (e.g. "image/png").
	MIMEType string

	// OriginalFilename filters by user-facing filename.
	OriginalFilename string

	// Status filters by lifecycle state (pending, committed, deleted).
	Status LifecycleState

	// Provider filters by storage driver name.
	Provider string

	// CreatedAfter filters records created on or after this timestamp.
	CreatedAfter *time.Time

	// CreatedBefore filters records created on or before this timestamp.
	CreatedBefore *time.Time

	// Metadata requires matching key-value pairs.
	Metadata map[string]string

	// Limit bounds the result count (default 100, max 1000).
	Limit int

	// Offset enables pagination skipping.
	Offset int
}

// MetadataStore defines the database metadata repository interface.
// When enabled, it acts as the source of truth for metadata and indexing.
type MetadataStore interface {
	// Save creates or updates an object record in the registry.
	Save(ctx context.Context, record *Record) error

	// GetByID retrieves a record by its logical ObjectID.
	GetByID(ctx context.Context, objectID string) (*Record, error)

	// GetByKey retrieves a record by its physical storage key.
	GetByKey(ctx context.Context, key string) (*Record, error)

	// Find queries records matching filter criteria.
	Find(ctx context.Context, filter Filter) ([]Record, error)

	// UpdateStatus transitions an object lifecycle state (e.g. pending -> committed, or soft deleted).
	UpdateStatus(ctx context.Context, objectID string, status LifecycleState) error

	// Delete permanently removes or soft-deletes a record from the registry.
	Delete(ctx context.Context, objectID string) error

	// Close gracefully closes any open database pool connections.
	Close() error
}
