package fs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type fsMetadata struct {
	ID             string            `json:"id,omitempty"`
	Key            string            `json:"key"`
	ContentType    string            `json:"content_type"`
	ETag           string            `json:"etag"`
	ChecksumSHA256 string            `json:"checksum_sha256,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

// writeMeta atomically serializes and writes sidecar metadata to a temporary file and renames it.
func writeMeta(targetPath string, meta *fsMetadata, fileMode os.FileMode) error {
	mp := metaPath(targetPath)
	dir := filepath.Dir(mp)

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("blobkit/fs: failed to encode metadata: %w", err)
	}

	tmpFile, err := os.CreateTemp(dir, ".tmp_meta_*")
	if err != nil {
		return fmt.Errorf("blobkit/fs: failed to create temporary meta file: %w", err)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("blobkit/fs: failed to write meta content: %w", err)
	}

	if err := tmpFile.Chmod(fileMode); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("blobkit/fs: failed to chmod meta file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("blobkit/fs: failed to close meta file: %w", err)
	}

	if err := os.Rename(tmpName, mp); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("blobkit/fs: failed to commit meta file: %w", err)
	}

	return nil
}

// readMeta reads and deserializes the sidecar metadata file if it exists.
func readMeta(targetPath string) (*fsMetadata, error) {
	mp := metaPath(targetPath)
	data, err := os.ReadFile(mp)
	if err != nil {
		return nil, err
	}

	var meta fsMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("blobkit/fs: corrupted sidecar metadata: %w", err)
	}

	return &meta, nil
}

// deleteMeta removes the associated sidecar metadata file if it exists.
func deleteMeta(targetPath string) error {
	mp := metaPath(targetPath)
	err := os.Remove(mp)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
