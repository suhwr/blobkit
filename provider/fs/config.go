package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config specifies options for the Local POSIX Filesystem storage driver.
type Config struct {
	// Name is the driver identifier (e.g. "fs", "local-nvme"). Defaults to "fs".
	Name string

	// RootDir is the root directory path on the local filesystem where blobs are stored.
	// Required.
	RootDir string

	// PublicBaseURL is an optional base URL for public access (e.g. "http://localhost:8080/blobs").
	PublicBaseURL string

	// DirMode specifies the permission bits for newly created directories. Defaults to 0755.
	DirMode os.FileMode

	// FileMode specifies the permission bits for newly created files. Defaults to 0644.
	FileMode os.FileMode

	// EnableSidecarMeta specifies whether to persist .meta.json sidecar files for metadata parity.
	// Defaults to true.
	EnableSidecarMeta bool

	// StagingDir is the directory name inside RootDir used for staging multipart chunks.
	// Defaults to ".staging".
	StagingDir string
}

// Validate checks configuration parameters, applies defaults, and ensures RootDir is accessible.
func (c *Config) Validate() error {
	if c.Name == "" {
		c.Name = "fs"
	}

	c.RootDir = strings.TrimSpace(c.RootDir)
	if c.RootDir == "" {
		return errors.New("blobkit/fs: RootDir is required")
	}

	absRoot, err := filepath.Abs(c.RootDir)
	if err != nil {
		return fmt.Errorf("blobkit/fs: failed to resolve absolute RootDir: %w", err)
	}
	c.RootDir = filepath.Clean(absRoot)

	if c.DirMode == 0 {
		c.DirMode = 0755
	}
	if c.FileMode == 0 {
		c.FileMode = 0644
	}

	if c.StagingDir == "" {
		c.StagingDir = ".staging"
	}

	// Ensure RootDir exists and is writable
	if err := os.MkdirAll(c.RootDir, c.DirMode); err != nil {
		return fmt.Errorf("blobkit/fs: failed to create RootDir %q: %w", c.RootDir, err)
	}

	c.PublicBaseURL = strings.TrimRight(c.PublicBaseURL, "/")

	return nil
}
