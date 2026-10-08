package blobkit

import (
	"context"
	"io"
	"time"
)

// DriverMiddleware wraps an existing Driver with interceptor logic.
// It allows modifying requests, injecting behaviors (such as encryption,
// compression, rate limiting, or metrics), or intercepting responses at the wire level.
type DriverMiddleware func(Driver) Driver

// WrapDriver decorates a target Driver with one or more DriverMiddlewares.
// Middlewares are executed in the order they are passed: the first middleware in the list
// is the outermost wrapper, executing first before passing control inward.
func WrapDriver(driver Driver, middlewares ...DriverMiddleware) Driver {
	wrapped := driver
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] != nil {
			wrapped = middlewares[i](wrapped)
		}
	}
	return wrapped
}

// DelegateDriver is a transparent proxy struct that implements Driver by forwarding
// all calls to its inner Driver. Custom middleware authors should embed DelegateDriver
// and selectively override only the specific methods they wish to intercept.
type DelegateDriver struct {
	Driver Driver
}

// NewDelegateDriver creates a DelegateDriver wrapping the specified target Driver.
func NewDelegateDriver(target Driver) DelegateDriver {
	return DelegateDriver{Driver: target}
}

// Name delegates to the inner driver.
func (d *DelegateDriver) Name() string {
	if d.Driver == nil {
		return ""
	}
	return d.Driver.Name()
}

// Capabilities delegates to the inner driver.
func (d *DelegateDriver) Capabilities() Capability {
	if d.Driver == nil {
		return 0
	}
	return d.Driver.Capabilities()
}

// Put delegates to the inner driver.
func (d *DelegateDriver) Put(ctx context.Context, obj *Object, r io.Reader, opts PutOptions) (*Object, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.Put(ctx, obj, r, opts)
}

// Get delegates to the inner driver.
func (d *DelegateDriver) Get(ctx context.Context, key string, opts GetOptions) (*ObjectReader, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.Get(ctx, key, opts)
}

// Head delegates to the inner driver.
func (d *DelegateDriver) Head(ctx context.Context, key string) (*Object, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.Head(ctx, key)
}

// Delete delegates to the inner driver.
func (d *DelegateDriver) Delete(ctx context.Context, key string) error {
	if d.Driver == nil {
		return ErrProviderUnavailable
	}
	return d.Driver.Delete(ctx, key)
}

// DeleteBatch delegates to the inner driver.
func (d *DelegateDriver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.DeleteBatch(ctx, keys)
}

// Copy delegates to the inner driver.
func (d *DelegateDriver) Copy(ctx context.Context, srcKey, dstKey string) error {
	if d.Driver == nil {
		return ErrProviderUnavailable
	}
	return d.Driver.Copy(ctx, srcKey, dstKey)
}

// List delegates to the inner driver.
func (d *DelegateDriver) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.List(ctx, opts)
}

// PresignGet delegates to the inner driver.
func (d *DelegateDriver) PresignGet(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.PresignGet(ctx, key, opts)
}

// PresignPut delegates to the inner driver.
func (d *DelegateDriver) PresignPut(ctx context.Context, key string, opts PresignOptions) (*PresignedURL, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.PresignPut(ctx, key, opts)
}

// ResolveURL delegates to the inner driver.
func (d *DelegateDriver) ResolveURL(key string) (string, error) {
	if d.Driver == nil {
		return "", ErrProviderUnavailable
	}
	return d.Driver.ResolveURL(key)
}

// CreateMultipart delegates to the inner driver.
func (d *DelegateDriver) CreateMultipart(ctx context.Context, obj *Object, opts PutOptions) (string, error) {
	if d.Driver == nil {
		return "", ErrProviderUnavailable
	}
	return d.Driver.CreateMultipart(ctx, obj, opts)
}

// UploadPart delegates to the inner driver.
func (d *DelegateDriver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	if d.Driver == nil {
		return "", ErrProviderUnavailable
	}
	return d.Driver.UploadPart(ctx, key, uploadID, partNumber, r, size)
}

// CompleteMultipart delegates to the inner driver.
func (d *DelegateDriver) CompleteMultipart(ctx context.Context, obj *Object, uploadID string, parts []CompletedPart) (*Object, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.CompleteMultipart(ctx, obj, uploadID, parts)
}

// AbortMultipart delegates to the inner driver.
func (d *DelegateDriver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	if d.Driver == nil {
		return ErrProviderUnavailable
	}
	return d.Driver.AbortMultipart(ctx, key, uploadID)
}

// ListParts delegates to the inner driver.
func (d *DelegateDriver) ListParts(ctx context.Context, key string, uploadID string) ([]CompletedPart, error) {
	if d.Driver == nil {
		return nil, ErrProviderUnavailable
	}
	return d.Driver.ListParts(ctx, key, uploadID)
}

// Close delegates to the inner driver.
func (d *DelegateDriver) Close() error {
	if d.Driver == nil {
		return nil
	}
	return d.Driver.Close()
}

// LatencyReporter is a callback invoked after every wire operation with exact execution duration.
type LatencyReporter func(op Operation, key string, duration time.Duration, err error)

// LatencyMiddleware creates a DriverMiddleware that measures and reports the wire-level execution duration
// of storage driver operations.
func LatencyMiddleware(reporter LatencyReporter) DriverMiddleware {
	return func(next Driver) Driver {
		if reporter == nil {
			return next
		}
		return &latencyDriver{
			DelegateDriver: NewDelegateDriver(next),
			reporter:       reporter,
		}
	}
}

type latencyDriver struct {
	DelegateDriver
	reporter LatencyReporter
}

func (l *latencyDriver) Put(ctx context.Context, obj *Object, r io.Reader, opts PutOptions) (*Object, error) {
	start := time.Now()
	res, err := l.DelegateDriver.Put(ctx, obj, r, opts)
	key := ""
	if obj != nil {
		key = obj.Key
	}
	l.reporter(OpPut, key, time.Since(start), err)
	return res, err
}

func (l *latencyDriver) Get(ctx context.Context, key string, opts GetOptions) (*ObjectReader, error) {
	start := time.Now()
	res, err := l.DelegateDriver.Get(ctx, key, opts)
	l.reporter(OpGet, key, time.Since(start), err)
	return res, err
}

func (l *latencyDriver) Head(ctx context.Context, key string) (*Object, error) {
	start := time.Now()
	res, err := l.DelegateDriver.Head(ctx, key)
	l.reporter(OpHead, key, time.Since(start), err)
	return res, err
}

func (l *latencyDriver) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := l.DelegateDriver.Delete(ctx, key)
	l.reporter(OpDelete, key, time.Since(start), err)
	return err
}
