package blobkit

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// URLResolver maps a storage object key to an accessible public or CDN URL.
type URLResolver interface {
	ResolveURL(key string) string
}

// EscapeURLPath escapes each segment of a key path with url.PathEscape,
// preserving '/' path separators.
func EscapeURLPath(key string) string {
	cleanKey := strings.TrimLeft(key, "/")
	if cleanKey == "" {
		return ""
	}
	parts := strings.Split(cleanKey, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// CDNResolver resolves object keys against a base CDN or public domain URL (e.g. "https://cdn.example.com").
type CDNResolver struct {
	baseURL string
}

// NewCDNResolver initializes a CDNResolver. Trailing slashes are stripped from baseURL.
func NewCDNResolver(baseURL string) *CDNResolver {
	return &CDNResolver{
		baseURL: strings.TrimRight(baseURL, "/"),
	}
}

// ResolveURL appends the sanitized key to the configured base URL.
func (c *CDNResolver) ResolveURL(key string) string {
	if c.baseURL == "" || key == "" {
		return ""
	}
	escapedKey := EscapeURLPath(key)
	return fmt.Sprintf("%s/%s", c.baseURL, escapedKey)
}

// S3PathResolver formats standard path-style or virtual-hosted URLs for S3 endpoints.
type S3PathResolver struct {
	endpoint string
	bucket   string
}

// NewS3PathResolver returns an S3PathResolver.
func NewS3PathResolver(endpoint, bucket string) *S3PathResolver {
	return &S3PathResolver{
		endpoint: strings.TrimRight(endpoint, "/"),
		bucket:   bucket,
	}
}

// ResolveURL builds a path-style URL: endpoint/bucket/key.
func (s *S3PathResolver) ResolveURL(key string) string {
	if s.endpoint == "" || s.bucket == "" || key == "" {
		return ""
	}
	escapedKey := EscapeURLPath(key)
	return fmt.Sprintf("%s/%s/%s", s.endpoint, s.bucket, escapedKey)
}

// CustomResolver adapts an arbitrary formatting function into a URLResolver.
type CustomResolver func(key string) string

// ResolveURL invokes the underlying custom function.
func (f CustomResolver) ResolveURL(key string) string {
	if f == nil {
		return ""
	}
	return f(key)
}

// PresignedURL contains a time-limited signed URL for direct client access or uploads.
type PresignedURL struct {
	// URL is the signed HTTP address.
	URL string `json:"url"`

	// Method is the expected HTTP verb (e.g. "GET" or "PUT").
	Method string `json:"method"`

	// ExpiresAt is the timestamp when the signature becomes invalid.
	ExpiresAt time.Time `json:"expires_at"`

	// SignedHeaders contains any required HTTP headers that were included in the signature calculation.
	SignedHeaders map[string]string `json:"signed_headers,omitempty"`
}

// PresignOptions configures parameters for URL presigning.
type PresignOptions struct {
	// Expiry is the validity duration of the presigned URL. Default is 15 minutes.
	Expiry time.Duration

	// ContentType specifies the expected Content-Type header to constrain PUT presigned URLs.
	ContentType string

	// ContentDisposition specifies the expected Content-Disposition header.
	ContentDisposition string

	// QueryParams allows passing additional query parameters if supported.
	QueryParams url.Values
}

// DefaultPresignExpiry is the default duration for generated presigned URLs.
const DefaultPresignExpiry = 15 * time.Minute
