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

	// ETag is the HTTP entity tag from the storage provider.
	ETag string `json:"etag,omitempty"`

	// OriginalFilename is the client-facing filename (e.g. "photo.png").
	OriginalFilename string `json:"original_filename,omitempty"`

	// Visibility specifies access level (public or private).
	Visibility Visibility `json:"visibility"`

	// Status indicates lifecycle state (pending, committed, deleted).
	Status LifecycleState `json:"status"`

	// Metadata holds arbitrary user-defined key-value attributes.
	Metadata map[string]string `json:"metadata,omitempty"`

	// RetentionUntil specifies when retention lock expires.
	RetentionUntil *time.Time `json:"retention_until,omitempty"`

	// ExpiresAt specifies when the object automatically expires.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// LegalHold prevents deletion or modification when true.
	LegalHold bool `json:"legal_hold,omitempty"`

	// ClientChecksum is the client-provided checksum (if any).
	ClientChecksum string `json:"client_checksum,omitempty"`

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
		ETag:             r.ETag,
		ChecksumSHA256:   r.ChecksumSHA256,
		OriginalFilename: r.OriginalFilename,
		Visibility:       r.Visibility,
		Status:           r.Status,
		Metadata:         r.Metadata,
		RetentionUntil:   r.RetentionUntil,
		ExpiresAt:        r.ExpiresAt,
		LegalHold:        r.LegalHold,
		DeletedAt:        r.DeletedAt,
		ClientChecksum:   r.ClientChecksum,
		CreatedAt:        r.CreatedAt,
		UpdatedAt:        r.UpdatedAt,
		Provider:         r.Provider,
	}
}

// Filter specifies query criteria for discovering objects by metadata without needing URLs.
type Filter struct {
	// ObjectID filters by canonical identifier.
	ObjectID string

	// Key filters by physical storage key.
	Key string

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

	// DeletedBefore filters records deleted on or before this timestamp.
	DeletedBefore *time.Time

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

	// UpdateMetadata replaces or updates the user-defined metadata map for an object.
	UpdateMetadata(ctx context.Context, objectID string, metadata map[string]string) error

	// UpdateFilename updates the client-facing filename of an object without moving physical bytes.
	UpdateFilename(ctx context.Context, objectID string, newFilename string) error

	// Delete marks an object as soft-deleted or removes it.
	Delete(ctx context.Context, objectID string) error

	// HardDelete permanently purges the record from the database registry.
	HardDelete(ctx context.Context, objectID string) error

	// FindExpired returns committed objects whose ExpiresAt is on or before the given timestamp.
	FindExpired(ctx context.Context, before time.Time, limit int) ([]Record, error)

	// SaveSession persists or updates a resumable multipart upload session.
	SaveSession(ctx context.Context, session *UploadSession) error

	// GetSession retrieves an active upload session by ID.
	GetSession(ctx context.Context, sessionID string) (*UploadSession, error)

	// DeleteSession removes a session upon completion or abort.
	DeleteSession(ctx context.Context, sessionID string) error

	// FindStaleSessions returns active sessions whose ExpiresAt is on or before the given timestamp.
	FindStaleSessions(ctx context.Context, before time.Time, limit int) ([]UploadSession, error)

	// Close gracefully closes any open database pool connections.
	Close() error
}
