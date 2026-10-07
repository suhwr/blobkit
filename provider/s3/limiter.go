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
	closed    atomic.Bool
	mu        sync.Mutex
	cond      *sync.Cond
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
	if l.closed.Load() {
		return blobkit.ErrProviderUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Fast path: attempt immediate acquisition without lock or goroutine spawn
	if l.TryAcquire(bytes) {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed.Load() {
		return blobkit.ErrProviderUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	stopWatcher := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			l.mu.Lock()
			l.cond.Broadcast()
			l.mu.Unlock()
		case <-stopWatcher:
		}
	}()
	defer func() {
		close(stopWatcher)
		<-watcherDone
	}()

	// Spin-wait with cancellation check
	for {
		if l.closed.Load() {
			return blobkit.ErrProviderUnavailable
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		current := l.allocated.Load()
		if current <= l.maxBytes-bytes {
			if l.allocated.CompareAndSwap(current, current+bytes) {
				return nil
			}
			continue
		}

		l.cond.Wait()
	}
}

// TryAcquire attempts to reserve bytes immediately without blocking.
func (l *MemoryLimiter) TryAcquire(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	if bytes > l.maxBytes {
		return false
	}
	for {
		if l.closed.Load() {
			return false
		}
		current := l.allocated.Load()
		if current > l.maxBytes-bytes {
			return false
		}
		if l.allocated.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

// Release returns reserved bytes back to the budget and wakes waiting goroutines.
// Clamps allocation to zero to prevent negative budget from over-release.
func (l *MemoryLimiter) Release(bytes int64) {
	if bytes <= 0 {
		return
	}
	for {
		current := l.allocated.Load()
		next := current - bytes
		if next < 0 {
			next = 0
		}
		if l.allocated.CompareAndSwap(current, next) {
			break
		}
	}
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
	l.closed.Store(true)
	l.mu.Lock()
	l.cond.Broadcast()
	l.mu.Unlock()
}
