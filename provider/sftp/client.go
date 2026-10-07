package sftp

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/suhwr/blobkit"
	"golang.org/x/crypto/ssh"
)

// sidecarMetadata records user metadata, MIME content types, and hashes alongside the payload.
type sidecarMetadata struct {
	ID          string            `json:"id"`
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	ETag        string            `json:"etag,omitempty"`
	SHA256      string            `json:"sha256,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// getClient retrieves or lazily establishes a multiplexed SFTP client connection.
func (d *Driver) getClient() (*sftp.Client, error) {
	d.connMu.Lock()
	defer d.connMu.Unlock()

	if d.sftpClient != nil {
		// Test liveness with a lightweight Stat
		if _, err := d.sftpClient.Getwd(); err == nil {
			return d.sftpClient, nil
		}
		// Connection broken, clean up old client
		_ = d.sftpClient.Close()
		if d.sshClient != nil {
			_ = d.sshClient.Close()
		}
		d.sftpClient = nil
		d.sshClient = nil
	}

	sshCfg, err := d.cfg.buildSSHClientConfig()
	if err != nil {
		return nil, blobkit.WrapError("dial", "", d.cfg.Name, err)
	}

	sshClient, err := ssh.Dial("tcp", d.cfg.address(), sshCfg)
	if err != nil {
		return nil, mapSFTPError("dial", "", d.cfg.Name, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, mapSFTPError("sftp_new", "", d.cfg.Name, err)
	}

	d.sshClient = sshClient
	d.sftpClient = sftpClient
	return d.sftpClient, nil
}

// resolvePath sanitizes the object key and guarantees that the resulting absolute path
// is strictly contained within BaseDir, rejecting any path traversal attempts.
func (d *Driver) resolvePath(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", blobkit.ErrInvalidKey
	}

	// Reject null bytes, carriage returns, and backslashes
	if strings.ContainsRune(key, '\x00') || strings.ContainsRune(key, '\r') || strings.Contains(key, `\`) {
		return "", blobkit.WrapError("path", key, d.cfg.Name, blobkit.ErrSecurityViolation)
	}

	// Check for any parent traversal segments in original key
	for _, part := range strings.Split(key, "/") {
		if part == ".." {
			return "", blobkit.WrapError("path", key, d.cfg.Name, blobkit.ErrSecurityViolation)
		}
	}

	cleanKey := path.Clean(key)

	// Prevent directory traversal or root path escapes
	if cleanKey == "." || cleanKey == "/" || path.IsAbs(cleanKey) || strings.HasPrefix(cleanKey, "..") {
		return "", blobkit.WrapError("path", key, d.cfg.Name, blobkit.ErrSecurityViolation)
	}

	base := d.cfg.BaseDir
	if base == "" {
		base = "/"
	}
	cleanBase := path.Clean(base)

	target := path.Join(cleanBase, cleanKey)

	// Strict chroot containment check: target MUST have cleanBase + "/" as prefix
	expectedPrefix := cleanBase
	if !strings.HasSuffix(expectedPrefix, "/") {
		expectedPrefix += "/"
	}

	if !strings.HasPrefix(target, expectedPrefix) && target != cleanBase {
		return "", blobkit.WrapError("path", key, d.cfg.Name, blobkit.ErrSecurityViolation)
	}

	return target, nil
}

// sidecarPath returns the corresponding .meta.json sidecar filepath for an object.
func sidecarPath(targetPath string) string {
	return targetPath + ".meta.json"
}

// writeSidecar stores metadata in a JSON sidecar file atomically.
func (d *Driver) writeSidecar(client *sftp.Client, targetPath string, meta *sidecarMetadata) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	metaTarget := sidecarPath(targetPath)
	tmpPath := fmt.Sprintf("%s.tmp.%s", metaTarget, randomUUID())

	f, err := client.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = client.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = client.Remove(tmpPath)
		return err
	}

	// Rename atomically
	if err := client.Rename(tmpPath, metaTarget); err != nil {
		_ = client.Remove(tmpPath)
		return err
	}

	return nil
}

// readSidecar loads object metadata from the .meta.json sidecar file.
func (d *Driver) readSidecar(client *sftp.Client, targetPath string) (*sidecarMetadata, error) {
	metaFile := sidecarPath(targetPath)
	f, err := client.Open(metaFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	var meta sidecarMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

// randomUUID generates a quick random hex string for atomic temporary file naming.
func randomUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
