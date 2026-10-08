package fs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// Driver implements blobkit.Driver for the local POSIX filesystem.
type Driver struct {
	cfg        Config
	sessionsMu sync.RWMutex
	sessions   map[string]*fsMultipartSession
}

// NewDriver constructs and initializes a new Local POSIX Filesystem storage driver.
func NewDriver(cfg Config) (*Driver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Driver{
		cfg:      cfg,
		sessions: make(map[string]*fsMultipartSession),
	}, nil
}

// Name returns the driver identifier.
func (d *Driver) Name() string {
	return d.cfg.Name
}

// Capabilities declares features supported by the filesystem driver.
func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
		blobkit.CapMultipartPut |
		blobkit.CapMultipartSession |
		blobkit.CapByteRangeGet |
		blobkit.CapCopy |
		blobkit.CapBatchDelete
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

// Put writes an object stream atomically to the local filesystem using a temporary file.
func (d *Driver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("put", "", d.cfg.Name, err)
	}
	if r == nil {
		return nil, blobkit.ErrNilReader
	}
	if obj == nil || strings.TrimSpace(obj.Key) == "" {
		return nil, blobkit.ErrInvalidKey
	}

	targetPath, err := resolvePath(d.cfg.RootDir, obj.Key, d.cfg.StagingDir)
	if err != nil {
		return nil, err
	}

	resolvedR, effectiveSize, hasExplicitSize, err := blobkit.ResolvePayload(r, opts)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	sizeReader := blobkit.NewSizeReader(resolvedR, effectiveSize, hasExplicitSize)

	if err := ensureParentDir(targetPath, d.cfg.DirMode); err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	dir := filepath.Dir(targetPath)
	tmpFile, err := os.CreateTemp(dir, ".tmp_put_*")
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}
	tmpName := tmpFile.Name()

	md5Hash := md5.New()
	shaHash := sha256.New()
	mw := io.MultiWriter(tmpFile, md5Hash, shaHash)

	written, err := io.Copy(mw, sizeReader)
	if err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	if err := ctx.Err(); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	if err := sizeReader.Verify(); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	if err := tmpFile.Chmod(d.cfg.FileMode); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	// Atomic rename to commit file
	if err := os.Rename(tmpName, targetPath); err != nil {
		_ = os.Remove(tmpName)
		return nil, blobkit.WrapError("put", obj.Key, d.cfg.Name, err)
	}

	now := time.Now().UTC()
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(md5Hash.Sum(nil)))
	shaHex := hex.EncodeToString(shaHash.Sum(nil))

	contentType := obj.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(obj.Key))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}

	stored := *obj
	stored.Bucket = d.cfg.RootDir
	stored.Size = written
	stored.ContentType = contentType
	stored.ETag = etag
	stored.ChecksumSHA256 = shaHex
	stored.UpdatedAt = now
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.Provider = d.cfg.Name
	stored.Status = blobkit.StateCommitted
	stored.Metadata = resolveMetadata(obj, opts)

	// Persist sidecar metadata
	if d.cfg.EnableSidecarMeta {
		meta := &fsMetadata{
			ID:             stored.ID,
			Key:            stored.Key,
			ContentType:    contentType,
			ETag:           etag,
			ChecksumSHA256: shaHex,
			Metadata:       stored.Metadata,
			CreatedAt:      stored.CreatedAt,
			UpdatedAt:      stored.UpdatedAt,
		}
		if err := writeMeta(targetPath, meta, d.cfg.FileMode); err != nil {
			_ = os.Remove(targetPath)
			return nil, blobkit.WrapError("put", stored.Key, d.cfg.Name, err)
		}
	}

	return &stored, nil
}

// Get retrieves an object stream and its metadata from the filesystem.
// Supports HTTP Byte-Range requests via file seeking.
func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}
	targetPath, err := resolvePath(d.cfg.RootDir, key, d.cfg.StagingDir)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blobkit.WrapError("get", key, d.cfg.Name, blobkit.ErrObjectNotFound)
		}
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		f.Close()
		return nil, blobkit.WrapError("get", key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	metaObj, err := d.Head(ctx, key)
	if err != nil {
		f.Close()
		return nil, err
	}

	if err := blobkit.CheckPreconditions(metaObj.ETag, metaObj.UpdatedAt, opts); err != nil {
		f.Close()
		return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
	}

	var reader io.ReadCloser = f
	totalSize := fi.Size()

	// Parse HTTP Range header if requested (e.g. "bytes=100-200", "bytes=100-", or "bytes=-500")
	if opts.Range != "" && strings.HasPrefix(opts.Range, "bytes=") {
		rangeSpec := strings.TrimPrefix(opts.Range, "bytes=")
		parts := strings.Split(rangeSpec, "-")
		if len(parts) == 2 {
			var start, end int64
			if parts[0] == "" && parts[1] != "" {
				// Suffix range: bytes=-N
				suffixLen, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil || suffixLen <= 0 {
					f.Close()
					return nil, blobkit.WrapError("get", key, d.cfg.Name, blobkit.ErrPreconditionFailed)
				}
				if suffixLen >= totalSize {
					start = 0
				} else {
					start = totalSize - suffixLen
				}
				end = totalSize - 1
			} else if parts[0] != "" {
				var err1, err2 error
				start, err1 = strconv.ParseInt(parts[0], 10, 64)
				end = totalSize - 1
				if parts[1] != "" {
					end, err2 = strconv.ParseInt(parts[1], 10, 64)
				}
				if err1 != nil || err2 != nil || start < 0 || start > totalSize || end < start {
					f.Close()
					return nil, blobkit.WrapError("get", key, d.cfg.Name, blobkit.ErrPreconditionFailed)
				}
			} else {
				f.Close()
				return nil, blobkit.WrapError("get", key, d.cfg.Name, blobkit.ErrPreconditionFailed)
			}

			if end >= totalSize {
				end = totalSize - 1
			}

			length := end - start + 1
			if _, err := f.Seek(start, io.SeekStart); err != nil {
				f.Close()
				return nil, blobkit.WrapError("get", key, d.cfg.Name, err)
			}

			metaObj.Size = length
			reader = &rangeReadCloser{
				limitReader: io.LimitReader(f, length),
				closer:      f,
			}
		}
	}

	return &blobkit.ObjectReader{
		Object: *metaObj,
		Body:   reader,
	}, nil
}

// Head inspects an object and returns its metadata without reading the body.
func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}
	targetPath, err := resolvePath(d.cfg.RootDir, key, d.cfg.StagingDir)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blobkit.WrapError("head", key, d.cfg.Name, blobkit.ErrObjectNotFound)
		}
		return nil, blobkit.WrapError("head", key, d.cfg.Name, err)
	}

	if fi.IsDir() {
		return nil, blobkit.WrapError("head", key, d.cfg.Name, blobkit.ErrObjectNotFound)
	}

	// Try reading sidecar metadata
	var contentType string
	var etag string
	var shaHex string
	var userMeta map[string]string
	var createdAt time.Time
	var id string

	if d.cfg.EnableSidecarMeta {
		if meta, err := readMeta(targetPath); err == nil {
			id = meta.ID
			contentType = meta.ContentType
			etag = meta.ETag
			shaHex = meta.ChecksumSHA256
			userMeta = meta.Metadata
			createdAt = meta.CreatedAt
		}
	}

	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(key))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}

	if etag == "" {
		// Generate fast stat-based ETag if sidecar was missing
		etag = fmt.Sprintf("\"%x-%x\"", fi.ModTime().UnixNano(), fi.Size())
	}

	if createdAt.IsZero() {
		createdAt = fi.ModTime().UTC()
	}

	return &blobkit.Object{
		ID:             id,
		Key:            key,
		Bucket:         d.cfg.RootDir,
		Size:           fi.Size(),
		ContentType:    contentType,
		ETag:           etag,
		ChecksumSHA256: shaHex,
		Metadata:       userMeta,
		CreatedAt:      createdAt,
		UpdatedAt:      fi.ModTime().UTC(),
		Provider:       d.cfg.Name,
		Status:         blobkit.StateCommitted,
	}, nil
}

// Delete removes an object and its sidecar metadata from the filesystem.
func (d *Driver) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}
	targetPath, err := resolvePath(d.cfg.RootDir, key, d.cfg.StagingDir)
	if err != nil {
		return err
	}

	err = os.Remove(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return blobkit.WrapError("delete", key, d.cfg.Name, err)
	}

	_ = deleteMeta(targetPath)
	return nil
}

// DeleteBatch removes multiple keys concurrently using a bounded worker pool.
func (d *Driver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("delete_batch", "", d.cfg.Name, err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	for _, k := range keys {
		if err := blobkit.ValidateKey(k); err != nil {
			return nil, err
		}
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
		return deleted, blobkit.WrapError("delete_batch", "", d.cfg.Name, fmt.Errorf("batch delete errors: %s", strings.Join(errMsgs, "; ")))
	}

	return deleted, nil
}

// Copy duplicates an object and its sidecar metadata within the filesystem.
func (d *Driver) Copy(ctx context.Context, srcKey, dstKey string) error {
	if err := ctx.Err(); err != nil {
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}
	if srcKey == dstKey {
		_, err := d.Head(ctx, srcKey)
		return err
	}

	srcPath, err := resolvePath(d.cfg.RootDir, srcKey, d.cfg.StagingDir)
	if err != nil {
		return err
	}

	dstPath, err := resolvePath(d.cfg.RootDir, dstKey, d.cfg.StagingDir)
	if err != nil {
		return err
	}

	sf, err := os.Open(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return blobkit.WrapError("copy", srcKey, d.cfg.Name, blobkit.ErrObjectNotFound)
		}
		return blobkit.WrapError("copy", srcKey, d.cfg.Name, err)
	}
	defer sf.Close()

	if err := ensureParentDir(dstPath, d.cfg.DirMode); err != nil {
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(dstPath), ".tmp_copy_*")
	if err != nil {
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}
	tmpName := tmpFile.Name()

	if _, err := io.Copy(tmpFile, sf); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}

	if err := tmpFile.Chmod(d.cfg.FileMode); err != nil {
		tmpFile.Close()
		_ = os.Remove(tmpName)
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}

	if err := os.Rename(tmpName, dstPath); err != nil {
		_ = os.Remove(tmpName)
		return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
	}

	// Copy sidecar metadata if present
	if d.cfg.EnableSidecarMeta {
		if meta, err := readMeta(srcPath); err == nil && meta != nil {
			meta.Key = dstKey
			meta.UpdatedAt = time.Now().UTC()
			if err := writeMeta(dstPath, meta, d.cfg.FileMode); err != nil {
				_ = os.Remove(dstPath)
				return blobkit.WrapError("copy", dstKey, d.cfg.Name, err)
			}
		}
	}

	return nil
}

// List traverses the filesystem root directory, filtering by prefix and delimiter.
func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}
	var objects []blobkit.Object
	var commonPrefixes []string
	seenPrefixes := make(map[string]bool)

	stagingRel := d.cfg.StagingDir + string(filepath.Separator)

	err := filepath.WalkDir(d.cfg.RootDir, func(path string, dEntry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		// Rel path from rootDir
		rel, err := filepath.Rel(d.cfg.RootDir, path)
		if err != nil || rel == "." {
			return nil
		}

		relSlash := filepath.ToSlash(rel)

		// Skip staging directory and sidecar files
		if rel == d.cfg.StagingDir || strings.HasPrefix(rel, stagingRel) || strings.HasSuffix(rel, ".meta.json") || strings.HasPrefix(dEntry.Name(), ".tmp_") {
			if dEntry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if dEntry.IsDir() {
			return nil
		}

		// Prefix filtering
		if opts.Prefix != "" && !strings.HasPrefix(relSlash, opts.Prefix) {
			return nil
		}

		// Delimiter grouping (e.g. "/")
		if opts.Delimiter != "" {
			sub := strings.TrimPrefix(relSlash, opts.Prefix)
			idx := strings.Index(sub, opts.Delimiter)
			if idx >= 0 {
				prefix := opts.Prefix + sub[:idx+len(opts.Delimiter)]
				if !seenPrefixes[prefix] {
					seenPrefixes[prefix] = true
					commonPrefixes = append(commonPrefixes, prefix)
				}
				return nil
			}
		}

		fi, err := dEntry.Info()
		if err != nil {
			return nil
		}

		obj, err := d.Head(ctx, relSlash)
		if err == nil && obj != nil {
			objects = append(objects, *obj)
		} else {
			objects = append(objects, blobkit.Object{
				Key:       relSlash,
				Bucket:    d.cfg.RootDir,
				Size:      fi.Size(),
				UpdatedAt: fi.ModTime().UTC(),
				Provider:  d.cfg.Name,
				Status:    blobkit.StateCommitted,
			})
		}

		return nil
	})

	if err != nil {
		return nil, blobkit.WrapError("list", opts.Prefix, d.cfg.Name, err)
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})
	sort.Strings(commonPrefixes)

	// Apply Cursor pagination
	startIdx := 0
	if opts.Cursor != "" {
		for i, o := range objects {
			if o.Key > opts.Cursor {
				startIdx = i
				break
			}
		}
	}
	objects = objects[startIdx:]

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	isTruncated := false
	var nextCursor string
	if len(objects) > limit {
		isTruncated = true
		nextCursor = objects[limit-1].Key
		objects = objects[:limit]
	}

	return &blobkit.ListResult{
		Objects:        objects,
		NextCursor:     nextCursor,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}, nil
}

// PresignGet is unsupported on local disk storage without a proxy HTTP file daemon.
func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("presign_get", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// PresignPut is unsupported on local disk storage without a proxy HTTP file daemon.
func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return nil, err
	}
	return nil, blobkit.WrapError("presign_put", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
}

// ResolveURL builds a public access URL or returns a file:// URI.
func (d *Driver) ResolveURL(key string) (string, error) {
	targetPath, err := resolvePath(d.cfg.RootDir, key, d.cfg.StagingDir)
	if err != nil {
		return "", err
	}

	if d.cfg.PublicBaseURL != "" {
		return fmt.Sprintf("%s/%s", strings.TrimRight(d.cfg.PublicBaseURL, "/"), blobkit.EscapeURLPath(key)), nil
	}

	return (&url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(targetPath),
	}).String(), nil
}

// Close terminates open multipart sessions and cleans up resources.
func (d *Driver) Close() error {
	d.sessionsMu.Lock()
	d.sessions = make(map[string]*fsMultipartSession)
	d.sessionsMu.Unlock()
	return nil
}

// rangeReadCloser binds an io.LimitReader with the underlying os.File closer.
type rangeReadCloser struct {
	limitReader io.Reader
	closer      io.Closer
}

func (r *rangeReadCloser) Read(p []byte) (n int, err error) {
	return r.limitReader.Read(p)
}

func (r *rangeReadCloser) Close() error {
	return r.closer.Close()
}
