package s3

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/suhwr/blobkit"
)

// Default settings for S3-compatible providers.
const (
	DefaultMultipartThreshold   int64 = 8 * 1024 * 1024  // 8 MB
	DefaultMultipartPartSize    int64 = 5 * 1024 * 1024  // 5 MB (S3 min part size)
	DefaultMultipartConcurrency       = 2                // Concurrency per upload stream
	DefaultGlobalMemoryLimit    int64 = 64 * 1024 * 1024 // 64 MB global ceiling
	DefaultMaxRetries                 = 3
	DefaultRegion                     = "auto"
)

// Config holds connection parameters and performance tuning options for an S3-compatible provider.
type Config struct {
	// Name is the driver identifier (e.g. "r2-primary", "s3-backup", "minio").
	Name string

	// Endpoint is the S3 API endpoint URL (e.g. "https://<account>.r2.cloudflarestorage.com").
	Endpoint string

	// Region is the storage region (default "auto" for Cloudflare R2, "us-east-1" for AWS S3).
	Region string

	// Bucket is the target bucket name.
	Bucket string

	// AccessKeyID is the API access key.
	AccessKeyID string

	// SecretAccessKey is the API secret key.
	SecretAccessKey string

	// SessionToken is an optional temporary security token.
	SessionToken string

	// UsePathStyle enables path-style bucket addressing (http://endpoint/bucket/key), required for MinIO and LocalStack.
	UsePathStyle bool

	// PublicBaseURL is the public delivery URL (e.g. "https://cdn.example.com").
	PublicBaseURL string

	// HTTPClient provides an optional custom HTTP client. If nil, an optimized pooled transport is created.
	HTTPClient *http.Client

	// MaxRetries specifies the maximum retry count for transient network failures.
	MaxRetries int

	// MultipartThreshold is the byte size above which chunked multipart upload is used.
	MultipartThreshold int64

	// MultipartPartSize is the chunk size for multipart parts (minimum 5MB for S3/R2).
	MultipartPartSize int64

	// MultipartConcurrency limits concurrent worker goroutines per upload.
	MultipartConcurrency int

	// GlobalMemoryLimiter enforces a process-wide memory ceiling across all concurrent multipart uploads.
	GlobalMemoryLimiter *MemoryLimiter
}

// Validate checks for mandatory fields and applies defaults.
func (c *Config) Validate() error {
	if c.Bucket == "" {
		return blobkit.ErrBucketNotFound
	}
	if c.Name == "" {
		c.Name = "s3"
	}
	if c.Region == "" {
		c.Region = DefaultRegion
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = DefaultMaxRetries
	}
	if c.MultipartThreshold <= 0 {
		c.MultipartThreshold = DefaultMultipartThreshold
	}
	if c.MultipartPartSize < DefaultMultipartPartSize {
		c.MultipartPartSize = DefaultMultipartPartSize
	}
	if c.MultipartConcurrency <= 0 {
		c.MultipartConcurrency = DefaultMultipartConcurrency
	}
	if c.GlobalMemoryLimiter == nil {
		c.GlobalMemoryLimiter = NewMemoryLimiter(DefaultGlobalMemoryLimit)
	}
	c.PublicBaseURL = strings.TrimRight(c.PublicBaseURL, "/")
	return nil
}

// NewPooledHTTPTransport creates a persistent, high-throughput HTTP/2 transport
// configured specifically for high-concurrency object storage workloads.
func NewPooledHTTPTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
}
