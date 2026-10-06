package blobkit

import "time"

// SessionState represents the lifecycle status of a resumable multipart upload session.
type SessionState string

const (
	// SessionActive indicates the upload session is open and receiving parts.
	SessionActive SessionState = "active"

	// SessionCommitted indicates the upload session has been finalized into an object.
	SessionCommitted SessionState = "committed"

	// SessionAborted indicates the upload session was cancelled or cleaned up.
	SessionAborted SessionState = "aborted"
)

// UploadSession encapsulates the state required to track, resume, or abort a multipart upload.
type UploadSession struct {
	// ID is the unique identifier of the session (UUIDv7).
	ID string `json:"id"`

	// ObjectID is the logical object ID being constructed.
	ObjectID string `json:"object_id"`

	// Key is the physical storage key in the bucket.
	Key string `json:"key"`

	// Bucket is the target bucket name.
	Bucket string `json:"bucket"`

	// Provider is the storage driver managing the multipart upload.
	Provider string `json:"provider"`

	// UploadID is the underlying driver's multipart upload identifier.
	UploadID string `json:"upload_id"`

	// PartSize is the planned byte size per chunk (typically >= 5MB).
	PartSize int64 `json:"part_size"`

	// TotalSize is the optional declared total object size, or 0 if unknown.
	TotalSize int64 `json:"total_size"`

	// Parts tracks all successfully uploaded parts for this session.
	Parts []CompletedPart `json:"parts"`

	// ExpiresAt specifies when this session becomes stale and eligible for auto-cleanup.
	ExpiresAt time.Time `json:"expires_at"`

	// CreatedAt is the timestamp when the session was initiated.
	CreatedAt time.Time `json:"created_at"`

	// Status indicates whether the session is active, committed, or aborted.
	Status SessionState `json:"status"`
}
