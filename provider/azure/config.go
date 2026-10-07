package azure

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultAPIVersion is the Azure Storage REST API version used by the driver.
	DefaultAPIVersion = "2020-10-02"

	// DefaultEndpointSuffix is the standard Azure public cloud storage endpoint suffix.
	DefaultEndpointSuffix = "blob.core.windows.net"
)

// Config specifies options for the Azure Blob Storage driver.
type Config struct {
	// Name is the driver identifier (e.g. "azure", "azure-primary"). Defaults to "azure".
	Name string

	// AccountName is the Azure Storage account name. Required.
	AccountName string

	// AccountKey is the primary or secondary access key for SharedKey authentication (base64-encoded).
	AccountKey string

	// Container is the target Azure Blob container name. Required.
	Container string

	// SASToken is an optional pre-existing Shared Access Signature token.
	SASToken string

	// CustomEndpoint allows overriding the service endpoint (e.g. for Azurite emulator or private endpoints).
	CustomEndpoint string

	// PublicBaseURL is an optional base URL for public CDN/proxy delivery (e.g. "https://cdn.example.com").
	PublicBaseURL string

	// APIVersion is the Azure REST API version. Defaults to DefaultAPIVersion ("2020-10-02").
	APIVersion string

	// RequestTimeout is the timeout applied to individual metadata requests. Defaults to 30s.
	RequestTimeout time.Duration

	// HTTPClient is an optional preconfigured HTTP client.
	HTTPClient *http.Client
}

// Validate checks configuration parameters and sets defaults.
func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = "azure"
	}

	c.AccountName = strings.TrimSpace(c.AccountName)
	if c.AccountName == "" {
		return errors.New("blobkit/azure: AccountName is required")
	}

	c.Container = strings.TrimSpace(c.Container)
	if c.Container == "" {
		return errors.New("blobkit/azure: Container is required")
	}

	c.AccountKey = strings.TrimSpace(c.AccountKey)
	c.SASToken = strings.TrimSpace(c.SASToken)
	c.SASToken = strings.TrimPrefix(c.SASToken, "?")

	if c.APIVersion == "" {
		c.APIVersion = DefaultAPIVersion
	}

	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}

	if c.CustomEndpoint != "" {
		u, err := url.Parse(c.CustomEndpoint)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("blobkit/azure: invalid CustomEndpoint URL: %w", err)
		}
		c.CustomEndpoint = strings.TrimRight(c.CustomEndpoint, "/")
	}

	c.PublicBaseURL = strings.TrimRight(c.PublicBaseURL, "/")

	return nil
}

// EndpointURL returns the root service URL for the configured account.
func (c *Config) EndpointURL() string {
	if c.CustomEndpoint != "" {
		return c.CustomEndpoint
	}
	return fmt.Sprintf("https://%s.%s", c.AccountName, DefaultEndpointSuffix)
}
