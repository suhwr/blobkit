package blobkit

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	// ErrObjectNotFound indicates that the requested object does not exist in storage.
	ErrObjectNotFound = errors.New("blobkit: object not found")

	// ErrNotFound is an alias for ErrObjectNotFound for ergonomic parity.
	ErrNotFound = ErrObjectNotFound

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

	// ErrNotSupported is an alias for ErrUnsupportedOperation.
	ErrNotSupported = ErrUnsupportedOperation

	// ErrMemoryBudgetExceeded indicates that the global or stream multipart memory budget has been exhausted.
	ErrMemoryBudgetExceeded = errors.New("blobkit: memory budget exceeded")

	// ErrNilReader indicates that a nil io.Reader was passed to an upload operation.
	ErrNilReader = errors.New("blobkit: nil reader provided")

	// ErrSecurityViolation indicates that an operation violated security policies (e.g. traversal, forbidden extensions).
	ErrSecurityViolation = errors.New("blobkit: security violation")

	// ErrMIMEMismatch indicates that the content stream magic bytes contradict the claimed file extension.
	ErrMIMEMismatch = errors.New("blobkit: MIME type mismatch with content")

	// ErrInvalidFilename indicates that the provided filename is empty, malformed, or too long.
	ErrInvalidFilename = errors.New("blobkit: invalid filename")

	// ErrChecksumMismatch indicates that the computed payload hash does not match the expected checksum.
	ErrChecksumMismatch = errors.New("blobkit: checksum mismatch")

	// ErrSizeMismatch indicates that the uploaded byte count differs from the declared size.
	ErrSizeMismatch = errors.New("blobkit: size mismatch")

	// ErrObjectLocked indicates that retention policy or active legal hold prohibits modification or deletion.
	ErrObjectLocked = errors.New("blobkit: object retention locked or legal hold active")

	// ErrSessionNotFound indicates that the referenced resumable multipart upload session does not exist.
	ErrSessionNotFound = errors.New("blobkit: upload session not found")

	// ErrSessionExpired indicates that the resumable multipart upload session has expired.
	ErrSessionExpired = errors.New("blobkit: upload session expired")
)

var (
	reSecretToken = regexp.MustCompile(`(?i)(token|access_token|refresh_token|api_key|apikey|secret_key|secret|client_secret|password|passwd|sig|signature|x-amz-signature|x-amz-credential|x-goog-signature)=([a-zA-Z0-9_\-\.%]+)`)
	reJSONSecret  = regexp.MustCompile(`(?i)"(token|access_token|refresh_token|api_key|apikey|secret_key|secret|client_secret|password|passwd|sig|signature)"\s*:\s*"[^"]*"`)
	reBearerAuth  = regexp.MustCompile(`(?i)(Bearer\s+)[A-Za-z0-9\-\._~+/]+=*`)
	reBasicAuth   = regexp.MustCompile(`(?i)(Basic\s+)[A-Za-z0-9+/=]+`)
	rePrivateKey  = regexp.MustCompile(`(?s)-----BEGIN[^\-]+PRIVATE KEY-----.*?-----END[^\-]+PRIVATE KEY-----`)
	reProxyDial   = regexp.MustCompile(`proxyconnect tcp: dial tcp [0-9\.:]+: `)
	reInternalIP  = regexp.MustCompile(`(dial tcp (?:127\.0\.0\.1|10\.\d+\.\d+\.\d+|172\.(?:1[6-9]|2\d|3[01])\.\d+\.\d+|192\.168\.\d+\.\d+|\[::1\]|\[fe80:[0-9a-f:]+\]|\[(?:fc|fd)[0-9a-f:]+\]):\d+)`)
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

// PreserveSentinel reports whether err matches a domain sentinel or standard context cancellation error
// that should be preserved as-is without being wrapped into a generic errors.New().
func PreserveSentinel(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrObjectNotFound) ||
		errors.Is(err, ErrBucketNotFound) ||
		errors.Is(err, ErrInvalidKey) ||
		errors.Is(err, ErrInvalidID) ||
		errors.Is(err, ErrPreconditionFailed) ||
		errors.Is(err, ErrProviderUnavailable) ||
		errors.Is(err, ErrQuotaExceeded) ||
		errors.Is(err, ErrUploadTooLarge) ||
		errors.Is(err, ErrUnsupportedOperation) ||
		errors.Is(err, ErrMemoryBudgetExceeded) ||
		errors.Is(err, ErrNilReader) ||
		errors.Is(err, ErrSecurityViolation) ||
		errors.Is(err, ErrMIMEMismatch) ||
		errors.Is(err, ErrInvalidFilename) ||
		errors.Is(err, ErrChecksumMismatch) ||
		errors.Is(err, ErrSizeMismatch) ||
		errors.Is(err, ErrObjectLocked) ||
		errors.Is(err, ErrSessionNotFound) ||
		errors.Is(err, ErrSessionExpired)
}

// SanitizeErrorMessage strips internal proxy dials, local IPs, authorization headers, private keys,
// and secret tokens from error strings so they are safe for client logging and user-facing error reporting.
func SanitizeErrorMessage(raw string) string {
	if raw == "" {
		return ""
	}
	sanitized := reProxyDial.ReplaceAllString(raw, "")
	sanitized = reInternalIP.ReplaceAllString(sanitized, "dial tcp [scrubbed]")
	sanitized = reBearerAuth.ReplaceAllString(sanitized, "$1[redacted]")
	sanitized = reBasicAuth.ReplaceAllString(sanitized, "$1[redacted]")
	sanitized = rePrivateKey.ReplaceAllString(sanitized, "[redacted private key]")
	sanitized = reSecretToken.ReplaceAllString(sanitized, "$1=[redacted]")
	sanitized = reJSONSecret.ReplaceAllString(sanitized, `"$1":"[redacted]"`)
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

// Error predicate helpers for idiomatic developer experience:

// IsNotFound reports whether err represents an object or bucket not found error.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrObjectNotFound) || errors.Is(err, ErrBucketNotFound)
}

// IsPreconditionFailed reports whether err represents a precondition failure.
func IsPreconditionFailed(err error) bool {
	return errors.Is(err, ErrPreconditionFailed)
}

// IsProviderUnavailable reports whether err represents a backend outage or network failure.
func IsProviderUnavailable(err error) bool {
	return errors.Is(err, ErrProviderUnavailable)
}

// IsQuotaExceeded reports whether err represents a storage quota limit error.
func IsQuotaExceeded(err error) bool {
	return errors.Is(err, ErrQuotaExceeded)
}

// IsSecurityViolation reports whether err represents a security or validation policy rejection.
func IsSecurityViolation(err error) bool {
	return errors.Is(err, ErrSecurityViolation) || errors.Is(err, ErrMIMEMismatch)
}

// IsObjectLocked reports whether err represents a retention lock or legal hold block.
func IsObjectLocked(err error) bool {
	return errors.Is(err, ErrObjectLocked)
}
