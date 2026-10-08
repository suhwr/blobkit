package blobkit_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
)

// orderTrackerMiddleware records the entry and exit order of middlewares.
func orderTrackerMiddleware(name string, executionLog *[]string, mu *sync.Mutex) blobkit.DriverMiddleware {
	return func(next blobkit.Driver) blobkit.Driver {
		return &orderTrackingDriver{
			DelegateDriver: blobkit.NewDelegateDriver(next),
			name:           name,
			log:            executionLog,
			mu:             mu,
		}
	}
}

type orderTrackingDriver struct {
	blobkit.DelegateDriver
	name string
	log  *[]string
	mu   *sync.Mutex
}

func (o *orderTrackingDriver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	o.mu.Lock()
	*o.log = append(*o.log, o.name+":enter")
	o.mu.Unlock()

	res, err := o.DelegateDriver.Put(ctx, obj, r, opts)

	o.mu.Lock()
	*o.log = append(*o.log, o.name+":exit")
	o.mu.Unlock()

	return res, err
}

func TestMiddleware_ExecutionOrder(t *testing.T) {
	ctx := context.Background()
	rawDriver := memory.NewDriver(memory.Config{Bucket: "test"})

	var executionLog []string
	var mu sync.Mutex

	// Wrap with mw1 and mw2: mw1 should execute first (outer), then mw2 (inner)
	wrapped := blobkit.WrapDriver(rawDriver,
		orderTrackerMiddleware("mw1", &executionLog, &mu),
		orderTrackerMiddleware("mw2", &executionLog, &mu),
	)

	bucket, err := blobkit.NewBucket(wrapped)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	_, err = bucket.PutBytes(ctx, "test.txt", []byte("hello"), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	expected := []string{
		"mw1:enter",
		"mw2:enter",
		"mw2:exit",
		"mw1:exit",
	}

	if len(executionLog) != len(expected) {
		t.Fatalf("expected log length %d, got %d: %v", len(expected), len(executionLog), executionLog)
	}

	for i := range expected {
		if executionLog[i] != expected[i] {
			t.Errorf("step %d mismatch: expected %s, got %s", i, expected[i], executionLog[i])
		}
	}
}

// prefixKeyMiddleware demonstrates how middleware can transparently namespace or partition keys.
func prefixKeyMiddleware(prefix string) blobkit.DriverMiddleware {
	return func(next blobkit.Driver) blobkit.Driver {
		return &prefixDriver{
			DelegateDriver: blobkit.NewDelegateDriver(next),
			prefix:         prefix,
		}
	}
}

type prefixDriver struct {
	blobkit.DelegateDriver
	prefix string
}

func (p *prefixDriver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	cloned := *obj
	cloned.Key = p.prefix + obj.Key
	return p.DelegateDriver.Put(ctx, &cloned, r, opts)
}

func (p *prefixDriver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
	return p.DelegateDriver.Get(ctx, p.prefix+key, opts)
}

func (p *prefixDriver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	return p.DelegateDriver.Head(ctx, p.prefix+key)
}

func (p *prefixDriver) Delete(ctx context.Context, key string) error {
	return p.DelegateDriver.Delete(ctx, p.prefix+key)
}

func TestMiddleware_TransparentKeyPrefixing(t *testing.T) {
	ctx := context.Background()
	rawDriver := memory.NewDriver(memory.Config{Bucket: "test"})

	tenantDriver := blobkit.WrapDriver(rawDriver, prefixKeyMiddleware("tenants/org-42/"))
	bucket, err := blobkit.NewBucket(tenantDriver)
	if err != nil {
		t.Fatalf("NewBucket failed: %v", err)
	}

	// Application writes "avatars/logo.png"
	_, err = bucket.PutBytes(ctx, "avatars/logo.png", []byte("logo-bytes"), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	// Application checks existence via tenant bucket
	exists, err := bucket.Exists(ctx, "avatars/logo.png")
	if err != nil || !exists {
		t.Fatalf("expected object to exist under tenant bucket")
	}

	// Verify directly on raw driver that the real wire key is prefixed
	rawHead, err := rawDriver.Head(ctx, "tenants/org-42/avatars/logo.png")
	if err != nil {
		t.Fatalf("raw Head failed: expected key to be prefixed on wire: %v", err)
	}
	if rawHead.Size != int64(len("logo-bytes")) {
		t.Fatalf("size mismatch: %d", rawHead.Size)
	}
}

func TestMiddleware_LatencyReporting(t *testing.T) {
	ctx := context.Background()
	rawDriver := memory.NewDriver(memory.Config{Bucket: "test"})

	var reportedOps []blobkit.Operation
	var reportedKeys []string
	var mu sync.Mutex

	latencyMW := blobkit.LatencyMiddleware(func(op blobkit.Operation, key string, d time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		reportedOps = append(reportedOps, op)
		reportedKeys = append(reportedKeys, key)
	})

	wrapped := blobkit.WrapDriver(rawDriver, latencyMW)
	bucket, _ := blobkit.NewBucket(wrapped)

	// Execute Put, Head, Get, Delete
	_, _ = bucket.PutBytes(ctx, "metrics.json", []byte("{}"), blobkit.PutOptions{})
	_, _ = bucket.Head(ctx, "metrics.json")
	r, _ := bucket.Get(ctx, "metrics.json", blobkit.GetOptions{})
	if r != nil {
		_ = r.Close()
	}
	_ = bucket.Delete(ctx, "metrics.json")

	mu.Lock()
	defer mu.Unlock()

	if len(reportedOps) != 4 {
		t.Fatalf("expected 4 reported operations, got %d: %v", len(reportedOps), reportedOps)
	}

	expectedOps := []blobkit.Operation{
		blobkit.OpPut,
		blobkit.OpHead,
		blobkit.OpGet,
		blobkit.OpDelete,
	}

	for i, exp := range expectedOps {
		if reportedOps[i] != exp {
			t.Errorf("op %d: expected %s, got %s", i, exp, reportedOps[i])
		}
		if reportedKeys[i] != "metrics.json" {
			t.Errorf("key %d: expected metrics.json, got %s", i, reportedKeys[i])
		}
	}
}
