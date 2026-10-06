package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

type storedItem struct {
	obj  blobkit.Object
	data []byte
}

// Driver implements an in-memory storage driver, ideal for offline unit tests and local development.
type Driver struct {
	mu            sync.RWMutex
	name          string
	bucket        string
	publicBaseURL string
	items         map[string]storedItem
}

// Config specifies settings for the memory driver.
type Config struct {
	Name          string
	Bucket        string
	PublicBaseURL string
}

// NewDriver constructs a memory Driver.
func NewDriver(cfg Config) *Driver {
	name := cfg.Name
	if name == "" {
		name = "memory"
	}
	bucket := cfg.Bucket
	if bucket == "" {
		bucket = "test-bucket"
	}
	return &Driver{
		name:          name,
		bucket:        bucket,
		publicBaseURL: strings.TrimRight(cfg.PublicBaseURL, "/"),
		items:         make(map[string]storedItem),
	}
}

func (d *Driver) Name() string {
	return d.name
}

func (d *Driver) Capabilities() blobkit.Capability {
	return blobkit.CapDirectPut |
		blobkit.CapPresignGet |
		blobkit.CapPresignPut |
		blobkit.CapBatchDelete |
		blobkit.CapByteRangeGet
}

func (d *Driver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	if r == nil {
		return nil, blobkit.ErrNilReader
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, blobkit.WrapError("put", obj.Key, d.name, err)
	}

	h := sha256.Sum256(data)
	hashHex := hex.EncodeToString(h[:])
	etag := fmt.Sprintf("\"%s\"", hashHex[:32])

	now := time.Now().UTC()
	stored := *obj
	stored.Bucket = d.bucket
	stored.Size = int64(len(data))
	stored.ETag = etag
	stored.ChecksumSHA256 = hashHex
	stored.UpdatedAt = now
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.Provider = d.name

	d.mu.Lock()
	d.items[stored.Key] = storedItem{
		obj:  stored,
		data: data,
	}
	d.mu.Unlock()

	return &stored, nil
}

func (d *Driver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	d.mu.RLock()
	item, ok := d.items[key]
	d.mu.RUnlock()

	if !ok {
		return nil, blobkit.WrapError("get", key, d.name, blobkit.ErrObjectNotFound)
	}

	data := item.data

	// Handle byte range requests if requested
	if opts.Range != "" {
		subData, err := parseRange(opts.Range, data)
		if err != nil {
			return nil, blobkit.WrapError("get_range", key, d.name, err)
		}
		data = subData
	}

	return &blobkit.ObjectReader{
		Object: item.obj,
		Body:   io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (d *Driver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	d.mu.RLock()
	item, ok := d.items[key]
	d.mu.RUnlock()

	if !ok {
		return nil, blobkit.WrapError("head", key, d.name, blobkit.ErrObjectNotFound)
	}

	obj := item.obj
	return &obj, nil
}

func (d *Driver) Delete(ctx context.Context, key string) error {
	d.mu.Lock()
	delete(d.items, key)
	d.mu.Unlock()
	return nil
}

func (d *Driver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	deleted := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok := d.items[k]; ok {
			delete(d.items, k)
			deleted = append(deleted, k)
		}
	}
	return deleted, nil
}

func (d *Driver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	keys := make([]string, 0, len(d.items))
	for k := range d.items {
		if opts.Prefix == "" || strings.HasPrefix(k, opts.Prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	startIndex := 0
	if opts.Cursor != "" {
		for i, k := range keys {
			if k > opts.Cursor {
				startIndex = i
				break
			}
		}
	}

	limit := opts.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	res := &blobkit.ListResult{
		Objects: make([]blobkit.Object, 0),
	}

	count := 0
	for i := startIndex; i < len(keys); i++ {
		if count >= limit {
			res.IsTruncated = true
			res.NextCursor = keys[i-1]
			break
		}
		k := keys[i]
		res.Objects = append(res.Objects, d.items[k].obj)
		count++
	}

	return res, nil
}

func (d *Driver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	d.mu.RLock()
	_, ok := d.items[key]
	d.mu.RUnlock()

	if !ok {
		return nil, blobkit.WrapError("presign_get", key, d.name, blobkit.ErrObjectNotFound)
	}

	expiry := opts.Expiry
	if expiry <= 0 {
		expiry = blobkit.DefaultPresignExpiry
	}
	expTime := time.Now().Add(expiry)

	u := fmt.Sprintf("mem://%s/%s?op=get&expires=%d", d.bucket, key, expTime.Unix())
	return &blobkit.PresignedURL{
		URL:       u,
		Method:    "GET",
		ExpiresAt: expTime,
	}, nil
}

func (d *Driver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	expiry := opts.Expiry
	if expiry <= 0 {
		expiry = blobkit.DefaultPresignExpiry
	}
	expTime := time.Now().Add(expiry)

	u := fmt.Sprintf("mem://%s/%s?op=put&expires=%d", d.bucket, key, expTime.Unix())
	return &blobkit.PresignedURL{
		URL:       u,
		Method:    "PUT",
		ExpiresAt: expTime,
	}, nil
}

func (d *Driver) ResolveURL(key string) (string, error) {
	if d.publicBaseURL != "" {
		return fmt.Sprintf("%s/%s", d.publicBaseURL, strings.TrimLeft(key, "/")), nil
	}
	return fmt.Sprintf("mem://%s/%s", d.bucket, strings.TrimLeft(key, "/")), nil
}

func (d *Driver) Close() error {
	d.mu.Lock()
	d.items = make(map[string]storedItem)
	d.mu.Unlock()
	return nil
}

func parseRange(rangeHeader string, data []byte) ([]byte, error) {
	total := int64(len(data))
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return data, nil
	}
	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range header: %s", rangeHeader)
	}

	var start, end int64
	var err error

	if parts[0] == "" {
		// Suffix range: -N
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		start = total - suffix
		if start < 0 {
			start = 0
		}
		end = total - 1
	} else {
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, err
		}
		if parts[1] == "" {
			end = total - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}

	if start > end || start >= total {
		return nil, blobkit.ErrPreconditionFailed
	}
	if end >= total {
		end = total - 1
	}

	return data[start : end+1], nil
}
