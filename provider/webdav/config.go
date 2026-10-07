package webdav

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config specifies options for the WebDAV storage driver.
type Config struct {
	// Name is the driver identifier (e.g. "webdav", "nextcloud", "truenas"). Defaults to "webdav".
	Name string

	// Endpoint is the base WebDAV URL (e.g. "https://cloud.example.com/remote.php/dav/files/user").
	// Required.
	Endpoint string

	// Username is the HTTP Basic authentication username.
	Username string

	// Password is the HTTP Basic authentication password or application password.
	Password string

	// BearerToken is an optional HTTP Bearer authentication token.
	BearerToken string

	// HTTPClient is an optional preconfigured HTTP client. If nil, a client with reasonable timeouts is used.
	HTTPClient *http.Client

	// PublicBaseURL is an optional base URL for public CDN/proxy access (e.g. "https://cdn.example.com").
	PublicBaseURL string

	// RequestTimeout is the timeout applied to individual metadata and collection requests.
	// Defaults to 30 seconds.
	RequestTimeout time.Duration
}

// Validate checks configuration parameters and sets defaults.
func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = "webdav"
	}

	c.Endpoint = strings.TrimSpace(c.Endpoint)
	if c.Endpoint == "" {
		return errors.New("blobkit/webdav: Endpoint is required")
	}

	u, err := url.Parse(c.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("blobkit/webdav: invalid Endpoint URL %q (must be http or https)", c.Endpoint)
	}

	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	c.PublicBaseURL = strings.TrimRight(c.PublicBaseURL, "/")

	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}

	return nil
}
