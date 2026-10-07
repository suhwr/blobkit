package s3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/suhwr/blobkit"
)

// MemoryLimiter enforces bounded memory allocation across streaming uploads
// to safeguard against process Out-Of-Memory (OOM) failures under heavy concurrency.
type MemoryLimiter struct {
	maxBytes  int64
	allocated atomic.Int64
	mu        sync.Mutex
	cond      *sync.Cond
	closed    bool
}

// NewMemoryLimiter creates a limiter with the specified maximum byte ceiling.
// If maxBytes <= 0, a default of 64MB is used.
func NewMemoryLimiter(maxBytes int64) *MemoryLimiter {
	if maxBytes <= 0 {
		maxBytes = 64 * 1024 * 1024 // 64 MB default
	}
	l := &MemoryLimiter{
		maxBytes: maxBytes,
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Acquire reserves the given number of bytes. It blocks until enough budget is available,
// the context is cancelled, or the limiter is closed.
func (l *MemoryLimiter) Acquire(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	if bytes > l.maxBytes {
		return fmt.Errorf("%w: requested %d bytes exceeds max ceiling %d", blobkit.ErrMemoryBudgetExceeded, bytes, l.maxBytes)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	stopWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			l.mu.Lock()
			l.cond.Broadcast()
			l.mu.Unlock()
		case <-stopWatcher:
		}
	}()
	defer close(stopWatcher)

	// Spin-wait with cancellation check
	for {
		if l.closed {
			return blobkit.ErrProviderUnavailable
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		current := l.allocated.Load()
		if current+bytes <= l.maxBytes {
			l.allocated.Add(bytes)
			return nil
		}

		// Wait for releases. In Go, cond.Wait cannot be interrupted directly by context,
		// so we wake periodically or on release.
		l.cond.Wait()
	}
}

// TryAcquire attempts to reserve bytes immediately without blocking.
func (l *MemoryLimiter) TryAcquire(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	for {
		current := l.allocated.Load()
		if current+bytes > l.maxBytes {
			return false
		}
		if l.allocated.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

// Release returns reserved bytes back to the budget and wakes waiting goroutines.
func (l *MemoryLimiter) Release(bytes int64) {
	if bytes <= 0 {
		return
	}
	l.allocated.Add(-bytes)
	l.mu.Lock()
	l.cond.Broadcast()
	l.mu.Unlock()
}

// Allocated returns the currently reserved byte count.
func (l *MemoryLimiter) Allocated() int64 {
	return l.allocated.Load()
}

// MaxBytes returns the maximum configured byte ceiling.
func (l *MemoryLimiter) MaxBytes() int64 {
	return l.maxBytes
}

// Close wakes all waiting goroutines and disallows further acquisitions.
func (l *MemoryLimiter) Close() {
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
}
