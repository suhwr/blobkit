package gdrive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	// MinChunkSize is the mandatory minimum chunk alignment required by the Google Drive
	// Resumable Upload protocol (256 KiB = 262,144 bytes).
	MinChunkSize = 256 * 1024

	// DefaultChunkSize is the default chunk size used for resumable uploads (8 MiB = 32 * 256 KiB).
	DefaultChunkSize = 8 * 1024 * 1024

	// DefaultKeyCacheCapacity is the default maximum number of items in the in-memory key-to-fileID cache.
	DefaultKeyCacheCapacity = 10000

	// DefaultDriveAPIBaseURL is the official Google Drive REST API v3 endpoint.
	DefaultDriveAPIBaseURL = "https://www.googleapis.com/drive/v3"

	// DefaultUploadAPIBaseURL is the official Google Drive Resumable Upload API endpoint.
	DefaultUploadAPIBaseURL = "https://www.googleapis.com/upload/drive/v3"
)

// Config specifies configuration options for the Google Drive storage driver.
type Config struct {
	// Name is the driver identifier (e.g. "gdrive", "gdrive-cold"). Defaults to "gdrive".
	Name string

	// FolderID is the ID of the Google Drive folder or Shared Drive where all BlobKit objects reside.
	// Required.
	FolderID string

	// HTTPClient is an optional preconfigured HTTP client. If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// TokenFunc is an optional function that supplies fresh OAuth2 bearer tokens.
	TokenFunc func(ctx context.Context) (string, error)

	// BearerToken is a static OAuth2 token. Used primarily for testing or short-lived executions.
	BearerToken string

	// User OAuth2 credentials (automatically initializes TokenFunc if TokenFunc is nil).
	ClientID     string
	ClientSecret string
	RefreshToken string
	OAuthFile    string
	OAuthJSON    []byte

	// Service Account credentials (automatically initializes TokenFunc if TokenFunc is nil).
	ServiceAccountFile string
	ServiceAccountJSON []byte

	// TokenURI allows overriding the OAuth2 token endpoint (defaults to https://oauth2.googleapis.com/token).
	TokenURI string

	// ChunkSize is the chunk size in bytes for resumable multipart uploads.
	// Must be an exact multiple of 256 KiB (MinChunkSize). Defaults to DefaultChunkSize (8 MiB).
	ChunkSize int

	// SupportsAllDrives indicates whether the driver should interact with Google Workspace Shared Drives.
	// Defaults to true.
	SupportsAllDrives bool

	// PublicBaseURL is an optional base URL for public CDN/proxy access (e.g. "https://cdn.example.com").
	PublicBaseURL string

	// KeyCacheCapacity is the capacity of the in-memory key-to-fileID cache.
	// Defaults to DefaultKeyCacheCapacity (10,000). Set to -1 to disable caching.
	KeyCacheCapacity int

	// RequestTimeout is the timeout applied to individual HTTP metadata requests.
	// Defaults to 30 seconds if not specified.
	RequestTimeout time.Duration

	// DriveAPIBaseURL allows overriding the Google Drive API endpoint (useful for mock testing).
	DriveAPIBaseURL string

	// UploadAPIBaseURL allows overriding the Google Drive Resumable Upload endpoint (useful for mock testing).
	UploadAPIBaseURL string
}

// Validate checks configuration parameters for validity and sets reasonable defaults.
func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = "gdrive"
	}

	c.FolderID = strings.TrimSpace(c.FolderID)
	if c.FolderID == "" {
		return errors.New("blobkit/gdrive: FolderID is required")
	}

	if c.ChunkSize <= 0 {
		c.ChunkSize = DefaultChunkSize
	} else if c.ChunkSize%MinChunkSize != 0 {
		return fmt.Errorf("blobkit/gdrive: ChunkSize (%d bytes) must be an exact multiple of 256 KiB (%d bytes)", c.ChunkSize, MinChunkSize)
	}

	if c.KeyCacheCapacity == 0 {
		c.KeyCacheCapacity = DefaultKeyCacheCapacity
	}

	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}

	if c.DriveAPIBaseURL == "" {
		c.DriveAPIBaseURL = DefaultDriveAPIBaseURL
	}
	c.DriveAPIBaseURL = strings.TrimRight(c.DriveAPIBaseURL, "/")

	if c.UploadAPIBaseURL == "" {
		c.UploadAPIBaseURL = DefaultUploadAPIBaseURL
	}
	c.UploadAPIBaseURL = strings.TrimRight(c.UploadAPIBaseURL, "/")

	return nil
}
