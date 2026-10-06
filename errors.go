package blobkit

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	// ErrObjectNotFound indicates that the requested object does not exist in storage.
	ErrObjectNotFound = errors.New("blobkit: object not found")

	// ErrBucketNotFound indicates that the target bucket does not exist.
	ErrBucketNotFound = errors.New("blobkit: bucket not found")

	// ErrInvalidKey indicates that the provided object key is empty or malformed.
	ErrInvalidKey = errors.New("blobkit: invalid object key")

	// ErrInvalidID indicates that the provided object identifier is malformed.
	ErrInvalidID = errors.New("blobkit: invalid object id")

	// ErrPreconditionFailed indicates that an ETag or If-Match condition was not met.
	ErrPreconditionFailed = errors.New("blobkit: precondition failed")

	// ErrProviderUnavailable indicates that the storage backend is unreachable or returning 5xx.
	ErrProviderUnavailable = errors.New("blobkit: provider unavailable")

	// ErrQuotaExceeded indicates that the storage account quota or size limit was reached.
	ErrQuotaExceeded = errors.New("blobkit: storage quota exceeded")

	// ErrUploadTooLarge indicates that the payload exceeds the provider's maximum single or multipart upload limit.
	ErrUploadTooLarge = errors.New("blobkit: upload size exceeds maximum allowed")

	// ErrUnsupportedOperation indicates that the provider does not support the requested operation.
	ErrUnsupportedOperation = errors.New("blobkit: unsupported operation")

	// ErrMemoryBudgetExceeded indicates that the global or stream multipart memory budget has been exhausted.
	ErrMemoryBudgetExceeded = errors.New("blobkit: memory budget exceeded")

	// ErrNilReader indicates that a nil io.Reader was passed to an upload operation.
	ErrNilReader = errors.New("blobkit: nil reader provided")
)

// Regex patterns to scrub sensitive data from error messages (tokens, credentials, internal IP/proxy addresses).
var (
	reSecretToken = regexp.MustCompile(`(?i)(token|key|secret|password|sig|signature)=([a-zA-Z0-9_\-\.%]+)`)
	reProxyDial   = regexp.MustCompile(`proxyconnect tcp: dial tcp [0-9\.:]+: `)
	reInternalIP  = regexp.MustCompile(`(dial tcp (?:127\.0\.0\.1|10\.\d+\.\d+\.\d+|172\.(?:1[6-9]|2\d|3[01])\.\d+\.\d+|192\.168\.\d+\.\d+):\d+)`)
)

// StorageError represents a standardized, sanitized error returned by BlobKit operations.
// It wraps the underlying driver error without leaking sensitive endpoint credentials or internal network topologies.
type StorageError struct {
	Op       string
	Key      string
	Provider string
	Err      error
}

// Error formats the sanitized error message.
func (e *StorageError) Error() string {
	msg := ""
	if e.Err != nil {
		msg = SanitizeErrorMessage(e.Err.Error())
	}
	var b strings.Builder
	b.WriteString("blobkit: ")
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(" ")
	}
	if e.Key != "" {
		b.WriteString("key=")
		b.WriteString(e.Key)
		b.WriteString(": ")
	}
	if e.Provider != "" {
		b.WriteString("[provider=")
		b.WriteString(e.Provider)
		b.WriteString("] ")
	}
	b.WriteString(msg)
	return b.String()
}

// Unwrap returns the underlying error.
func (e *StorageError) Unwrap() error {
	return e.Err
}

// Is reports whether the target matches this error or its wrapped error.
func (e *StorageError) Is(target error) bool {
	if errors.Is(e.Err, target) {
		return true
	}
	return false
}

// WrapError encapsulates an error with operation context and sanitization.
func WrapError(op, key, provider string, err error) error {
	if err == nil {
		return nil
	}
	return &StorageError{
		Op:       op,
		Key:      key,
		Provider: provider,
		Err:      err,
	}
}

// SanitizeErrorMessage strips internal proxy dials, local IPs, and signed URL tokens
// from error strings so they are safe for client logging and user-facing error reporting.
func SanitizeErrorMessage(raw string) string {
	if raw == "" {
		return ""
	}
	sanitized := reProxyDial.ReplaceAllString(raw, "")
	sanitized = reInternalIP.ReplaceAllString(sanitized, "dial tcp [scrubbed]")
	sanitized = reSecretToken.ReplaceAllLiteralString(sanitized, "$1=[redacted]")
	return sanitized
}

// FormatError returns a sanitized error string.
func FormatError(err error) string {
	if err == nil {
		return ""
	}
	var se *StorageError
	if errors.As(err, &se) {
		return se.Error()
	}
	return SanitizeErrorMessage(err.Error())
}

// NewErrorf constructs a formatted StorageError.
func NewErrorf(op, key, provider, format string, a ...any) error {
	return &StorageError{
		Op:       op,
		Key:      key,
		Provider: provider,
		Err:      fmt.Errorf(format, a...),
	}
}
