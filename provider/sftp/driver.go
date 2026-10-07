package sftp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/suhwr/blobkit"
	"golang.org/x/crypto/ssh"
)

// Ensure Driver implements blobkit.Driver at compile time.
var _ blobkit.Driver = (*Driver)(nil)

// Driver implements the blobkit.Driver interface for remote SFTP servers.
type Driver struct {
	cfg        Config
	connMu     sync.Mutex
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

// NewDriver constructs and validates an SFTP storage driver.
func NewDriver(cfg Config) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Driver{
		cfg: cfg,
	}, nil
}

// Name returns the driver identifier.
func (d *Driver) Name() string {
	return d.cfg.Name
}

// Capabilities returns the feature bitmask supported by the SFTP driver.
func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
		blobkit.CapByteRangeGet |
		blobkit.CapBatchDelete |
		blobkit.CapCopy
}

func resolveMetadata(obj *blobkit.Object, opts blobkit.PutOptions) map[string]string {
	if len(opts.Metadata) > 0 {
		return opts.Metadata
	}
	if obj != nil && len(obj.Metadata) > 0 {
		return obj.Metadata
	}
	return nil
}

// Put streams an object to the remote SFTP server, writing to a temporary file
// before atomically renaming to prevent partial reads.
func (d *Driver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if r == nil {
		return nil, blobkit.ErrNilReader
	}
	if obj == nil || obj.Key == "" {
		return nil, blobkit.ErrInvalidKey
	}

	client, err := d.getClient()
	if err != nil {
		return nil, err
	}

	targetPath, err := d.resolvePath(obj.Key)
	if err != nil {
		return nil, err
	}

	// Ensure remote directory exists
	targetDir := path.Dir(targetPath)
	if err := client.MkdirAll(targetDir); err != nil {
		return nil, mapSFTPError("mkdir_all", obj.Key, d.cfg.Name, err)
	}

	// Staging to temporary file for atomic commit
	tmpPath := fmt.Sprintf("%s.tmp.%s", targetPath, randomUUID())
	f, err := client.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return nil, mapSFTPError("open_tmp", obj.Key, d.cfg.Name, err)
	}

	// Compute SHA-256 while streaming
	hasher := sha256.New()
	tee := io.TeeReader(r, hasher)
	written, copyErr := io.Copy(f, tee)
	closeErr := f.Close()

	if copyErr != nil {
		_ = client.Remove(tmpPath)
		return nil, mapSFTPError("put_copy", obj.Key, d.cfg.Name, copyErr)
	}
	if closeErr != nil {
		_ = client.Remove(tmpPath)
		return nil, mapSFTPError("put_close", obj.Key, d.cfg.Name, closeErr)
	}

	// Atomically swap temp file to final destination
	// Remove destination first if needed (some SFTP servers do not overwrite on Rename)
	_ = client.Remove(targetPath)
	if err := client.Rename(tmpPath, targetPath); err != nil {
		_ = client.Remove(tmpPath)
		return nil, mapSFTPError("put_rename", obj.Key, d.cfg.Name, err)
	}

	shaHex := hex.EncodeToString(hasher.Sum(nil))
	now := time.Now().UTC()

	contentType := obj.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	resolvedMeta := resolveMetadata(obj, opts)
	if d.cfg.EnableSidecarMeta {
		meta := &sidecarMetadata{
			ID:          obj.ID,
			Key:         obj.Key,
			Size:        written,
			ContentType: contentType,
			ETag:        fmt.Sprintf(`"%s"`, shaHex),
			SHA256:      shaHex,
			Metadata:    resolvedMeta,
			UpdatedAt:   now,
		}
		_ = d.writeSidecar(client, targetPath, meta)
	}

	resObj := *obj
	resObj.Size = written
	resObj.ContentType = contentType
	resObj.ChecksumSHA256 = shaHex
	resObj.ETag = fmt.Sprintf(`"%s"`, shaHex)
	resObj.Provider = d.cfg.Name
	resObj.UpdatedAt = now
	resObj.Status = blobkit.StateCommitted
	resObj.Metadata = resolvedMeta

	return &resObj, nil
}

// readCloser wraps an io.Reader and an io.Closer into a combined io.ReadCloser.
type readCloser struct {
	io.Reader
	io.Closer
}

// Get opens a remote SFTP file and streams its content, supporting HTTP byte-range seeks.
func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	client, err := d.getClient()
	if err != nil {
		return nil, err
	}

	targetPath, err := d.resolvePath(key)
	if err != nil {
		return nil, err
	}

	f, err := client.Open(targetPath)
	if err != nil {
		return nil, mapSFTPError("get", key, d.cfg.Name, err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, mapSFTPError("get_stat", key, d.cfg.Name, err)
	}

	totalSize := fi.Size()
	var stream io.Reader = f
	streamSize := totalSize

	// Handle Byte-Range requests
	if opts.Range != "" && strings.HasPrefix(opts.Range, "bytes=") {
		spec := strings.TrimPrefix(opts.Range, "bytes=")
		parts := strings.Split(spec, "-")
		if len(parts) == 2 {
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			end := totalSize - 1
			if parts[1] != "" {
				if parsedEnd, err := strconv.ParseInt(parts[1], 10, 64); err == nil && parsedEnd < totalSize {
					end = parsedEnd
				}
			}

			if start <= end && start < totalSize {
				if _, err := f.Seek(start, io.SeekStart); err != nil {
					_ = f.Close()
					return nil, mapSFTPError("get_seek", key, d.cfg.Name, err)
				}
				streamSize = (end - start) + 1
				stream = io.LimitReader(f, streamSize)
			}
		}
	}

	contentType := "application/octet-stream"
	var customMeta map[string]string
	var etag string
	objID := ""

	if d.cfg.EnableSidecarMeta {
		if meta, err := d.readSidecar(client, targetPath); err == nil && meta != nil {
			if meta.ContentType != "" {
				contentType = meta.ContentType
			}
			customMeta = meta.Metadata
			etag = meta.ETag
			objID = meta.ID
		}
	}

	obj := blobkit.Object{
		ID:          objID,
		Key:         key,
		Size:        streamSize,
		ContentType: contentType,
		ETag:        etag,
		Metadata:    customMeta,
		UpdatedAt:   fi.ModTime().UTC(),
		Provider:    d.cfg.Name,
		Status:      blobkit.StateCommitted,
	}

	return &blobkit.ObjectReader{
		Object: obj,
		Body:   readCloser{Reader: stream, Closer: f},
	}, nil
}

// Head inspects a remote SFTP file and loads metadata without reading the file body.
func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	client, err := d.getClient()
	if err != nil {
		return nil, err
	}

	targetPath, err := d.resolvePath(key)
	if err != nil {
		return nil, err
	}

	fi, err := client.Stat(targetPath)
	if err != nil {
		return nil, mapSFTPError("head", key, d.cfg.Name, err)
	}

	contentType := "application/octet-stream"
	var customMeta map[string]string
	var etag string
	var shaHex string
	objID := ""

	if d.cfg.EnableSidecarMeta {
		if meta, err := d.readSidecar(client, targetPath); err == nil && meta != nil {
			if meta.ContentType != "" {
				contentType = meta.ContentType
			}
			customMeta = meta.Metadata
			etag = meta.ETag
			shaHex = meta.SHA256
			objID = meta.ID
		}
	}

	return &blobkit.Object{
		ID:             objID,
		Key:            key,
		Size:           fi.Size(),
		ContentType:    contentType,
		ETag:           etag,
		ChecksumSHA256: shaHex,
		Metadata:       customMeta,
		UpdatedAt:      fi.ModTime().UTC(),
		Provider:       d.cfg.Name,
		Status:         blobkit.StateCommitted,
	}, nil
}

// Delete permanently removes a file and its associated .meta.json sidecar from the SFTP server.
func (d *Driver) Delete(ctx context.Context, key string) error {
	client, err := d.getClient()
	if err != nil {
		return err
	}

	targetPath, err := d.resolvePath(key)
	if err != nil {
		return err
	}

	err = client.Remove(targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, sftp.ErrSSHFxNoSuchFile) {
			return blobkit.WrapError("delete", key, d.cfg.Name, blobkit.ErrObjectNotFound)
		}
		return mapSFTPError("delete", key, d.cfg.Name, err)
	}

	// Clean up sidecar file if present
	_ = client.Remove(sidecarPath(targetPath))

	return nil
}

// DeleteBatch removes multiple keys concurrently using a bounded worker pool.
func (d *Driver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	type delResult struct {
		key string
		err error
	}

	concurrency := 16
	if len(keys) < concurrency {
		concurrency = len(keys)
	}

	keyChan := make(chan string, len(keys))
	for _, k := range keys {
		keyChan <- k
	}
	close(keyChan)

	resChan := make(chan delResult, len(keys))
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keyChan {
				err := d.Delete(ctx, k)
				resChan <- delResult{key: k, err: err}
			}
		}()
	}

	wg.Wait()
	close(resChan)

	var deleted []string
	var errMsgs []string

	for res := range resChan {
		if res.err == nil {
			deleted = append(deleted, res.key)
		} else {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", res.key, res.err))
		}
	}

	if len(errMsgs) > 0 {
		return deleted, blobkit.WrapError("delete_batch", "", d.cfg.Name, fmt.Errorf("errors during batch delete: %s", strings.Join(errMsgs, "; ")))
	}

	return deleted, nil
}

// Copy duplicates an object server-side using streaming across SFTP files.
func (d *Driver) Copy(ctx context.Context, srcKey, dstKey string) error {
	client, err := d.getClient()
	if err != nil {
		return err
	}

	srcPath, err := d.resolvePath(srcKey)
	if err != nil {
		return err
	}

	dstPath, err := d.resolvePath(dstKey)
	if err != nil {
		return err
	}

	srcFile, err := client.Open(srcPath)
	if err != nil {
		return mapSFTPError("copy_open_src", srcKey, d.cfg.Name, err)
	}
	defer srcFile.Close()

	if err := client.MkdirAll(path.Dir(dstPath)); err != nil {
		return mapSFTPError("copy_mkdir", dstKey, d.cfg.Name, err)
	}

	tmpDst := fmt.Sprintf("%s.tmp.%s", dstPath, randomUUID())
	dstFile, err := client.OpenFile(tmpDst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return mapSFTPError("copy_open_dst", dstKey, d.cfg.Name, err)
	}

	_, copyErr := io.Copy(dstFile, srcFile)
	closeErr := dstFile.Close()

	if copyErr != nil {
		_ = client.Remove(tmpDst)
		return mapSFTPError("copy_stream", srcKey, d.cfg.Name, copyErr)
	}
	if closeErr != nil {
		_ = client.Remove(tmpDst)
		return mapSFTPError("copy_close", dstKey, d.cfg.Name, closeErr)
	}

	_ = client.Remove(dstPath)
	if err := client.Rename(tmpDst, dstPath); err != nil {
		_ = client.Remove(tmpDst)
		return mapSFTPError("copy_rename", dstKey, d.cfg.Name, err)
	}

	// Copy sidecar metadata if available
	if d.cfg.EnableSidecarMeta {
		if meta, err := d.readSidecar(client, srcPath); err == nil && meta != nil {
			meta.Key = dstKey
			meta.UpdatedAt = time.Now().UTC()
			_ = d.writeSidecar(client, dstPath, meta)
		}
	}

	return nil
}

// List queries objects on the remote SFTP server matching prefix and delimiter filters.
func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	client, err := d.getClient()
	if err != nil {
		return nil, err
	}

	base := d.cfg.BaseDir
	if base == "" {
		base = "/"
	}

	var objects []blobkit.Object
	prefixesMap := make(map[string]bool)

	walker := client.Walk(base)
	for walker.Step() {
		if walker.Err() != nil {
			continue
		}

		itemPath := walker.Path()
		// Calculate key relative to base
		relKey := strings.TrimPrefix(itemPath, base)
		relKey = strings.TrimPrefix(relKey, "/")
		if relKey == "" {
			continue
		}

		// Skip metadata sidecars
		if strings.HasSuffix(relKey, ".meta.json") || strings.Contains(relKey, ".tmp.") {
			continue
		}

		fi := walker.Stat()
		if fi == nil || fi.IsDir() {
			continue
		}

		// Filter by Prefix
		if opts.Prefix != "" && !strings.HasPrefix(relKey, opts.Prefix) {
			continue
		}

		// Filter by Delimiter
		if opts.Delimiter != "" {
			rest := strings.TrimPrefix(relKey, opts.Prefix)
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				commonPrefix := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				prefixesMap[commonPrefix] = true
				continue
			}
		}

		objects = append(objects, blobkit.Object{
			Key:         relKey,
			Size:        fi.Size(),
			ContentType: "application/octet-stream",
			UpdatedAt:   fi.ModTime().UTC(),
			Provider:    d.cfg.Name,
			Status:      blobkit.StateCommitted,
		})
	}

	var commonPrefixes []string
	for p := range prefixesMap {
		commonPrefixes = append(commonPrefixes, p)
	}

	return &blobkit.ListResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
	}, nil
}

// PresignGet is unsupported on native SFTP.
func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// PresignPut is unsupported on native SFTP.
func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// ResolveURL builds a public access or CDN URL if configured.
func (d *Driver) ResolveURL(key string) (string, error) {
	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.PublicBaseURL, "/"), strings.TrimLeft(key, "/")), nil
	}
	return "", blobkit.WrapError("resolve_url", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// CreateMultipart is unsupported on native SFTP.
func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	return "", blobkit.WrapError("create_multipart", obj.Key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// UploadPart is unsupported on native SFTP.
func (d *Driver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// CompleteMultipart is unsupported on native SFTP.
func (d *Driver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// AbortMultipart is unsupported on native SFTP.
func (d *Driver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	return blobkit.WrapError("abort_multipart", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// ListParts is unsupported on native SFTP.
func (d *Driver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// Close gracefully terminates open SFTP and SSH network connections.
func (d *Driver) Close() error {
	d.connMu.Lock()
	defer d.connMu.Unlock()

	var errs []string
	if d.sftpClient != nil {
		if err := d.sftpClient.Close(); err != nil {
			errs = append(errs, err.Error())
		}
		d.sftpClient = nil
	}
	if d.sshClient != nil {
		if err := d.sshClient.Close(); err != nil {
			errs = append(errs, err.Error())
		}
		d.sshClient = nil
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}
