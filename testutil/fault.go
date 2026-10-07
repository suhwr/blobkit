package testutil

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/suhwr/blobkit"
)

// FaultConfig defines rules for deterministic failure injection.
type FaultConfig struct {
	Latency          time.Duration
	PutErr           error
	GetErr           error
	HeadErr          error
	DeleteErr        error
	DeleteBatchErr   error
	ListErr          error
	CopyErr          error
	MultipartErr     error
	FailAfterCalls   int64 // if > 0, return error on or after this call count
	FailAfterErr     error
	PartialReadBytes int64 // truncate Get reader after N bytes
	CorruptBytesAt   int64 // flip bits in Get reader at offset N
	DropConnection   bool  // simulate connection drop (ErrUnexpectedEOF) during read
}

// FaultDriver wraps any blobkit.Driver with deterministic fault injection capabilities.
type FaultDriver struct {
	inner  blobkit.Driver
	cfg    atomic.Pointer[FaultConfig]
	calls  atomic.Int64
	faults atomic.Int64
}

// NewFaultDriver wraps a base driver with the given fault configuration.
func NewFaultDriver(inner blobkit.Driver, cfg FaultConfig) *FaultDriver {
	fd := &FaultDriver{
		inner: inner,
	}
	fd.cfg.Store(&cfg)
	return fd
}

// SetConfig dynamically updates the fault injection configuration.
func (d *FaultDriver) SetConfig(cfg FaultConfig) {
	d.cfg.Store(&cfg)
}

// TotalCalls returns the total number of operations intercepted.
func (d *FaultDriver) TotalCalls() int64 {
	return d.calls.Load()
}

// FaultsInjected returns the total number of faults triggered.
func (d *FaultDriver) FaultsInjected() int64 {
	return d.faults.Load()
}

func (d *FaultDriver) applyPreFlight(methodErr error) error {
	callNum := d.calls.Add(1)
	cfg := d.cfg.Load()
	if cfg == nil {
		return nil
	}

	if cfg.Latency > 0 {
		time.Sleep(cfg.Latency)
	}

	if cfg.FailAfterCalls > 0 && callNum >= cfg.FailAfterCalls {
		d.faults.Add(1)
		if cfg.FailAfterErr != nil {
			return cfg.FailAfterErr
		}
		return blobkit.ErrProviderUnavailable
	}

	if methodErr != nil {
		d.faults.Add(1)
		return methodErr
	}
	return nil
}

func (d *FaultDriver) Name() string {
	return d.inner.Name()
}

func (d *FaultDriver) Capabilities() blobkit.Capability {
	return d.inner.Capabilities()
}

func (d *FaultDriver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.PutErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}
	return d.inner.Put(ctx, obj, r, opts)
}

func (d *FaultDriver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.HeadErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}
	return d.inner.Head(ctx, key)
}

func (d *FaultDriver) Delete(ctx context.Context, key string) error {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.DeleteErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return err
	}
	return d.inner.Delete(ctx, key)
}

func (d *FaultDriver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.DeleteBatchErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}
	return d.inner.DeleteBatch(ctx, keys)
}

func (d *FaultDriver) List(ctx context.Context, opts blobkit.ListOptions) (*blobkit.ListResult, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.ListErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}
	return d.inner.List(ctx, opts)
}

func (d *FaultDriver) Copy(ctx context.Context, srcKey, dstKey string) error {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.CopyErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return err
	}
	return d.inner.Copy(ctx, srcKey, dstKey)
}

func (d *FaultDriver) PresignGet(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	return d.inner.PresignGet(ctx, key, opts)
}

func (d *FaultDriver) PresignPut(ctx context.Context, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	return d.inner.PresignPut(ctx, key, opts)
}

func (d *FaultDriver) ResolveURL(key string) (string, error) {
	return d.inner.ResolveURL(key)
}

func (d *FaultDriver) Close() error {
	return d.inner.Close()
}

type faultReader struct {
	inner          io.ReadCloser
	readOffset     int64
	partialBytes   int64
	corruptAt      int64
	dropConnection bool
}

func (r *faultReader) Read(p []byte) (n int, err error) {
	if r.dropConnection && r.readOffset > 0 {
		return 0, io.ErrUnexpectedEOF
	}

	if r.partialBytes > 0 && r.readOffset >= r.partialBytes {
		return 0, io.EOF
	}

	maxToRead := len(p)
	if r.partialBytes > 0 && r.readOffset+int64(maxToRead) > r.partialBytes {
		maxToRead = int(r.partialBytes - r.readOffset)
	}

	n, err = r.inner.Read(p[:maxToRead])
	if n > 0 {
		if r.corruptAt > 0 && r.readOffset <= r.corruptAt && r.readOffset+int64(n) > r.corruptAt {
			idx := r.corruptAt - r.readOffset
			p[idx] ^= 0xFF // bit flip
		}
		r.readOffset += int64(n)
	}
	return n, err
}

func (r *faultReader) Close() error {
	return r.inner.Close()
}

func (d *FaultDriver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.GetErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}

	reader, err := d.inner.Get(ctx, key, opts)
	if err != nil {
		return nil, err
	}

	if cfg != nil && (cfg.PartialReadBytes > 0 || cfg.CorruptBytesAt > 0 || cfg.DropConnection) {
		fr := &faultReader{
			inner:          reader,
			partialBytes:   cfg.PartialReadBytes,
			corruptAt:      cfg.CorruptBytesAt,
			dropConnection: cfg.DropConnection,
		}
		return &blobkit.ObjectReader{
			Object: reader.Object,
			Body:   fr,
		}, nil
	}
	return reader, nil
}

// Multipart implementation
func (d *FaultDriver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (uploadID string, err error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.MultipartErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return "", err
	}
	return d.inner.CreateMultipart(ctx, obj, opts)
}

func (d *FaultDriver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (etag string, err error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.MultipartErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return "", err
	}
	return d.inner.UploadPart(ctx, key, uploadID, partNumber, r, size)
}

func (d *FaultDriver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.MultipartErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}
	return d.inner.CompleteMultipart(ctx, obj, uploadID, parts)
}

func (d *FaultDriver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.MultipartErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return err
	}
	return d.inner.AbortMultipart(ctx, key, uploadID)
}

func (d *FaultDriver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	cfg := d.cfg.Load()
	var methodErr error
	if cfg != nil {
		methodErr = cfg.MultipartErr
	}
	if err := d.applyPreFlight(methodErr); err != nil {
		return nil, err
	}
	return d.inner.ListParts(ctx, key, uploadID)
}

// Interface assertion
var _ blobkit.Driver = (*FaultDriver)(nil)
