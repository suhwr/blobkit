package gcs

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	// MinChunkSize is the mandatory minimum chunk alignment required by the Google Cloud Storage
	// Resumable Upload protocol (256 KiB = 262,144 bytes).
	MinChunkSize = 256 * 1024

	// DefaultChunkSize is the default chunk size used for resumable uploads (8 MiB = 32 * 256 KiB).
	DefaultChunkSize = 8 * 1024 * 1024

	// DefaultStorageAPIBaseURL is the official Google Cloud Storage JSON API v1 endpoint.
	DefaultStorageAPIBaseURL = "https://storage.googleapis.com/storage/v1"

	// DefaultUploadAPIBaseURL is the official Google Cloud Storage Resumable Upload API endpoint.
	DefaultUploadAPIBaseURL = "https://storage.googleapis.com/upload/storage/v1"
)

// Config specifies configuration parameters for the Google Cloud Storage driver.
type Config struct {
	// Name is the driver identifier (e.g. "gcs-primary", "gcs-backup"). Defaults to "gcs".
	Name string

	// Bucket is the Google Cloud Storage bucket name. Required.
	Bucket string

	// HTTPClient is an optional preconfigured HTTP client. If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// TokenFunc is an optional function that supplies fresh OAuth2 bearer tokens.
	TokenFunc func(ctx context.Context) (string, error)

	// BearerToken is a static OAuth2 token. Used primarily for testing or short-lived executions.
	BearerToken string

	// ServiceAccountEmail is the Google Cloud service account email address used for V4 Signed URLs.
	// Optional if URL presigning is not needed.
	ServiceAccountEmail string

	// PrivateKeyPEM is the RSA private key in PEM format (PKCS#1 or PKCS#8) used for V4 Signed URLs.
	// Optional if URL presigning is not needed.
	PrivateKeyPEM []byte

	// SignBytesFunc is an optional custom RSA-SHA256 signing callback (alternative to PrivateKeyPEM).
	SignBytesFunc func(ctx context.Context, payload []byte) ([]byte, error)

	// ChunkSize is the chunk size in bytes for resumable multipart uploads.
	// Must be an exact multiple of 256 KiB (MinChunkSize). Defaults to DefaultChunkSize (8 MiB).
	ChunkSize int

	// PublicBaseURL is an optional base URL for public CDN/proxy access (e.g. "https://cdn.example.com").
	PublicBaseURL string

	// StorageAPIBaseURL is the base URL for the GCS JSON API. Defaults to DefaultStorageAPIBaseURL.
	StorageAPIBaseURL string

	// UploadAPIBaseURL is the base URL for the GCS Upload API. Defaults to DefaultUploadAPIBaseURL.
	UploadAPIBaseURL string

	// Parsed RSA private key (computed during validation)
	parsedPrivateKey *rsa.PrivateKey
}

// Validate checks and sanitizes the configuration, setting default values where applicable.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		c.Name = "gcs"
	}

	c.Bucket = strings.TrimSpace(c.Bucket)
	if c.Bucket == "" {
		return errors.New("blobkit/gcs: bucket is required")
	}

	if c.TokenFunc == nil && strings.TrimSpace(c.BearerToken) == "" {
		return errors.New("blobkit/gcs: either TokenFunc or BearerToken must be provided for authentication")
	}

	if c.ChunkSize <= 0 {
		c.ChunkSize = DefaultChunkSize
	} else if c.ChunkSize%MinChunkSize != 0 {
		return fmt.Errorf("blobkit/gcs: ChunkSize must be an exact multiple of 256 KiB (%d bytes)", MinChunkSize)
	}

	if strings.TrimSpace(c.StorageAPIBaseURL) == "" {
		c.StorageAPIBaseURL = DefaultStorageAPIBaseURL
	}
	c.StorageAPIBaseURL = strings.TrimRight(c.StorageAPIBaseURL, "/")

	if strings.TrimSpace(c.UploadAPIBaseURL) == "" {
		c.UploadAPIBaseURL = DefaultUploadAPIBaseURL
	}
	c.UploadAPIBaseURL = strings.TrimRight(c.UploadAPIBaseURL, "/")

	if strings.TrimSpace(c.PublicBaseURL) != "" {
		c.PublicBaseURL = strings.TrimRight(c.PublicBaseURL, "/")
	}

	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{
			Timeout: 60 * time.Second,
		}
	}

	// Validate and parse RSA Private Key if provided for V4 Signed URLs
	if len(c.PrivateKeyPEM) > 0 {
		key, err := parseRSAPrivateKey(c.PrivateKeyPEM)
		if err != nil {
			return fmt.Errorf("blobkit/gcs: invalid PrivateKeyPEM: %v", err)
		}
		c.parsedPrivateKey = key
	}

	if (c.parsedPrivateKey != nil || c.SignBytesFunc != nil) && strings.TrimSpace(c.ServiceAccountEmail) == "" {
		return errors.New("blobkit/gcs: ServiceAccountEmail is required when PrivateKeyPEM or SignBytesFunc is provided")
	}

	return nil
}

// parseRSAPrivateKey parses an RSA private key from PEM bytes (supporting PKCS#1 and PKCS#8).
func parseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("failed to decode PEM block containing private key")
	}

	// Try PKCS#1 first
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}

	// Try PKCS#8
	keyInterface, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		if rsaKey, ok := keyInterface.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
		return nil, errors.New("parsed PKCS#8 key is not an RSA private key")
	}

	return nil, fmt.Errorf("failed to parse RSA private key (tried PKCS#1 and PKCS#8): %v", err)
}
