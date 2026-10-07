package fs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/suhwr/blobkit"
)

type fsMultipartSession struct {
	mu         sync.Mutex
	uploadID   string
	key        string
	obj        blobkit.Object
	stagingDir string
	createdAt  time.Time
}

// CreateMultipart initiates a chunked multipart upload session on local disk.
func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	if obj == nil || strings.TrimSpace(obj.Key) == "" {
		return "", blobkit.ErrInvalidKey
	}

	// Verify key resolves cleanly
	if _, err := resolvePath(d.cfg.RootDir, obj.Key, d.cfg.StagingDir); err != nil {
		return "", err
	}

	uploadID := uuid.New().String()
	stagingPath := filepath.Join(d.cfg.RootDir, d.cfg.StagingDir, uploadID)

	if err := os.MkdirAll(stagingPath, d.cfg.DirMode); err != nil {
		return "", blobkit.WrapError("create_multipart", obj.Key, d.cfg.Name, err)
	}

	session := &fsMultipartSession{
		uploadID:   uploadID,
		key:        obj.Key,
		obj:        *obj,
		stagingDir: stagingPath,
		createdAt:  time.Now().UTC(),
	}

	// Write session manifest
	manifestPath := filepath.Join(stagingPath, ".session.json")
	if manifestData, err := json.Marshal(session); err == nil {
		_ = os.WriteFile(manifestPath, manifestData, d.cfg.FileMode)
	}

	d.sessionsMu.Lock()
	d.sessions[uploadID] = session
	d.sessionsMu.Unlock()

	return uploadID, nil
}

// UploadPart writes an individual chunk to the staging directory and calculates its ETag.
func (d *Driver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	if r == nil {
		return "", blobkit.ErrNilReader
	}
	if partNumber <= 0 {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, fmt.Errorf("part number must be >= 1"))
	}

	d.sessionsMu.RLock()
	session, found := d.sessions[uploadID]
	d.sessionsMu.RUnlock()

	if !found {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrInvalidID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	partFile := filepath.Join(session.stagingDir, fmt.Sprintf("part_%05d", partNumber))
	f, err := os.OpenFile(partFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, d.cfg.FileMode)
	if err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}
	defer f.Close()

	hasher := md5.New()
	mw := io.MultiWriter(f, hasher)

	var reader io.Reader = r
	if size > 0 {
		reader = io.LimitReader(r, size)
	}

	if _, err := io.Copy(mw, reader); err != nil {
		_ = os.Remove(partFile)
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))
	return etag, nil
}

// CompleteMultipart concatenates staged chunks sequentially into the final file atomically.
func (d *Driver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	d.sessionsMu.Lock()
	session, found := d.sessions[uploadID]
	if found {
		delete(d.sessions, uploadID)
	}
	d.sessionsMu.Unlock()

	if !found {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrInvalidID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	targetPath, err := resolvePath(d.cfg.RootDir, obj.Key, d.cfg.StagingDir)
	if err != nil {
		return nil, err
	}

	if err := ensureParentDir(targetPath, d.cfg.DirMode); err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	// Sort parts by PartNumber
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	dir := filepath.Dir(targetPath)
	tmpFile, err := os.CreateTemp(dir, ".tmp_complete_*")
	if err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}
	tmpName := tmpFile.Name()

	md5Hash := md5.New()
	shaHash := sha256.New()
	mw := io.MultiWriter(tmpFile, md5Hash, shaHash)

	var totalSize int64
	for _, p := range parts {
		partPath := filepath.Join(session.stagingDir, fmt.Sprintf("part_%05d", p.PartNumber))
		pf, err := os.Open(partPath)
		if err != nil {
			tmpFile.Close()
			_ = os.Remove(tmpName)
			return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, fmt.Errorf("missing part %d: %w", p.PartNumber, err))
		}

		n, err := io.Copy(mw, pf)
		pf.Close()
		if err != nil {
			tmpFile.Close()
			_ = os.Remove(tmpName)
			return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
		}
		totalSize += n
	}

	if err := tmpFile.Chmod(d.cfg.FileMode); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	// Atomic commit to target path
	if err := os.Rename(tmpName, targetPath); err != nil {
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	now := time.Now().UTC()
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(md5Hash.Sum(nil)))
	shaHex := hex.EncodeToString(shaHash.Sum(nil))

	resObj := *obj
	resObj.Bucket = d.cfg.RootDir
	resObj.Size = totalSize
	resObj.ETag = etag
	resObj.ChecksumSHA256 = shaHex
	resObj.UpdatedAt = now
	if resObj.CreatedAt.IsZero() {
		resObj.CreatedAt = now
	}
	resObj.Provider = d.cfg.Name
	resObj.Status = blobkit.StateCommitted

	// Write sidecar metadata
	if d.cfg.EnableSidecarMeta {
		meta := &fsMetadata{
			ID:             resObj.ID,
			Key:            resObj.Key,
			ContentType:    resObj.ContentType,
			ETag:           etag,
			ChecksumSHA256: shaHex,
			Metadata:       resObj.Metadata,
			CreatedAt:      resObj.CreatedAt,
			UpdatedAt:      resObj.UpdatedAt,
		}
		_ = writeMeta(targetPath, meta, d.cfg.FileMode)
	}

	// Clean up staging directory
	_ = os.RemoveAll(session.stagingDir)

	return &resObj, nil
}

// AbortMultipart removes all staged chunks and cancels the session.
func (d *Driver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	d.sessionsMu.Lock()
	session, found := d.sessions[uploadID]
	if found {
		delete(d.sessions, uploadID)
	}
	d.sessionsMu.Unlock()

	if !found {
		// Attempt direct cleanup if staging directory exists
		stagingPath := filepath.Join(d.cfg.RootDir, d.cfg.StagingDir, uploadID)
		_ = os.RemoveAll(stagingPath)
		return nil
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	_ = os.RemoveAll(session.stagingDir)
	return nil
}

// ListParts returns the list of parts currently present in the staging directory.
func (d *Driver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	d.sessionsMu.RLock()
	session, found := d.sessions[uploadID]
	d.sessionsMu.RUnlock()

	var stagingPath string
	if found {
		stagingPath = session.stagingDir
	} else {
		stagingPath = filepath.Join(d.cfg.RootDir, d.cfg.StagingDir, uploadID)
	}

	entries, err := os.ReadDir(stagingPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, blobkit.ErrInvalidID)
		}
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, err)
	}

	var parts []blobkit.CompletedPart
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "part_") {
			continue
		}

		numStr := strings.TrimPrefix(name, "part_")
		partNum, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}

		info, err := e.Info()
		if err != nil {
			continue
		}

		parts = append(parts, blobkit.CompletedPart{
			PartNumber: int32(partNum),
			Size:       info.Size(),
		})
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}
