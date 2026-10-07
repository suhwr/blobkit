package sftp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

const (
	// DefaultPort is the standard SSH/SFTP port.
	DefaultPort = 22

	// DefaultTimeout is the standard timeout for SSH dial and handshake.
	DefaultTimeout = 30 * time.Second

	// DefaultDirMode is the default POSIX permission for directory creation.
	DefaultDirMode os.FileMode = 0755

	// DefaultFileMode is the default POSIX permission for file creation.
	DefaultFileMode os.FileMode = 0644
)

// Config specifies configuration parameters for the SFTP storage driver.
type Config struct {
	// Name is the driver identifier (e.g. "sftp-primary", "sftp-backup"). Defaults to "sftp".
	Name string

	// Host is the remote SSH host or IP address. Required.
	Host string

	// Port is the remote SSH port. Defaults to 22.
	Port int

	// User is the SSH login username. Required.
	User string

	// Password is the SSH login password. Optional if PrivateKeyPEM is provided.
	Password string

	// PrivateKeyPEM contains raw RSA, ECDSA, or Ed25519 private key PEM bytes. Optional if Password is provided.
	PrivateKeyPEM []byte

	// PrivateKeyPassphrase is the decryption passphrase for encrypted private keys.
	PrivateKeyPassphrase string

	// HostKey is the expected remote server public key for strict host key verification.
	HostKey ssh.PublicKey

	// InsecureIgnoreHostKey disables SSH host key verification (recommended only for dev/testing).
	InsecureIgnoreHostKey bool

	// BaseDir is the root directory path on the remote SFTP server where blobs are stored.
	BaseDir string

	// EnableSidecarMeta persists <key>.meta.json sidecar files on the SFTP server for metadata fidelity.
	EnableSidecarMeta bool

	// PublicBaseURL is an optional base URL for public CDN/HTTP gateway access (e.g. "https://cdn.example.com").
	PublicBaseURL string

	// Timeout is the SSH network connection and handshake timeout. Defaults to 30 seconds.
	Timeout time.Duration

	// DirMode specifies the file mode permissions used when creating remote directories. Defaults to 0755.
	DirMode os.FileMode

	// FileMode specifies the file mode permissions used when creating remote files. Defaults to 0644.
	FileMode os.FileMode
}

// Validate sanitizes the configuration, enforces required fields, and applies default values.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		c.Name = "sftp"
	}

	c.Host = strings.TrimSpace(c.Host)
	if c.Host == "" {
		return errors.New("blobkit/sftp: Host is required")
	}

	if c.Port <= 0 {
		c.Port = DefaultPort
	}

	c.User = strings.TrimSpace(c.User)
	if c.User == "" {
		return errors.New("blobkit/sftp: User is required")
	}

	if c.Password == "" && len(c.PrivateKeyPEM) == 0 {
		return errors.New("blobkit/sftp: either Password or PrivateKeyPEM must be provided for authentication")
	}

	if !c.InsecureIgnoreHostKey && c.HostKey == nil {
		return errors.New("blobkit/sftp: HostKey is required unless InsecureIgnoreHostKey is explicitly set to true")
	}

	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}

	if c.DirMode == 0 {
		c.DirMode = DefaultDirMode
	}

	if c.FileMode == 0 {
		c.FileMode = DefaultFileMode
	}

	if strings.TrimSpace(c.PublicBaseURL) != "" {
		c.PublicBaseURL = strings.TrimRight(c.PublicBaseURL, "/")
	}

	// Clean base directory path
	c.BaseDir = strings.TrimRight(c.BaseDir, "/")

	return nil
}

// buildSSHClientConfig constructs an *ssh.ClientConfig based on the driver configuration.
func (c *Config) buildSSHClientConfig() (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	if len(c.PrivateKeyPEM) > 0 {
		var signer ssh.Signer
		var err error
		if c.PrivateKeyPassphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(c.PrivateKeyPEM, []byte(c.PrivateKeyPassphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(c.PrivateKeyPEM)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse SSH private key: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if c.Password != "" {
		authMethods = append(authMethods, ssh.Password(c.Password))
	}

	var hostKeyCallback ssh.HostKeyCallback
	if c.InsecureIgnoreHostKey {
		hostKeyCallback = ssh.InsecureIgnoreHostKey() //nolint:gosec
	} else if c.HostKey != nil {
		hostKeyCallback = ssh.FixedHostKey(c.HostKey)
	}

	return &ssh.ClientConfig{
		User:            c.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         c.Timeout,
	}, nil
}

// address returns the host:port string for network dialing.
func (c *Config) address() string {
	return net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port))
}
