package observer

import (
	"context"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// MetricsSnapshot provides a read-only point-in-time view of collected telemetry.
type MetricsSnapshot struct {
	OpCounts         map[blobkit.Operation]int64         `json:"op_counts"`
	ErrorCounts      map[blobkit.Operation]int64         `json:"error_counts"`
	BytesTransferred map[blobkit.Operation]int64         `json:"bytes_transferred"`
	TotalDuration    map[blobkit.Operation]time.Duration `json:"total_duration"`
	LimiterWaits     map[string]time.Duration            `json:"limiter_waits"`
	LimiterCounts    map[string]int64                    `json:"limiter_counts"`
}

// MetricsCollector is a thread-safe, vendor-neutral telemetry collector implementing blobkit.Observer.
// It aggregates operation counts, error rates, latencies, streaming volumes, and limiter contention.
type MetricsCollector struct {
	mu               sync.RWMutex
	opCounts         map[blobkit.Operation]int64
	errorCounts      map[blobkit.Operation]int64
	bytesTransferred map[blobkit.Operation]int64
	totalDuration    map[blobkit.Operation]time.Duration
	limiterWaits     map[string]time.Duration
	limiterCounts    map[string]int64
}

// NewMetricsCollector instantiates an empty metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		opCounts:         make(map[blobkit.Operation]int64),
		errorCounts:      make(map[blobkit.Operation]int64),
		bytesTransferred: make(map[blobkit.Operation]int64),
		totalDuration:    make(map[blobkit.Operation]time.Duration),
		limiterWaits:     make(map[string]time.Duration),
		limiterCounts:    make(map[string]int64),
	}
}

// OnOperationStart is invoked at the start of any BlobKit storage operation.
func (m *MetricsCollector) OnOperationStart(ctx context.Context, op blobkit.Operation, key string) context.Context {
	m.mu.Lock()
	m.opCounts[op]++
	m.mu.Unlock()
	return ctx
}

// OnOperationEnd is invoked when an operation concludes.
func (m *MetricsCollector) OnOperationEnd(ctx context.Context, op blobkit.Operation, key string, duration time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalDuration[op] += duration
	if err != nil {
		m.errorCounts[op]++
	}
}

// OnBytesTransferred tracks streaming payload bytes uploaded or downloaded.
func (m *MetricsCollector) OnBytesTransferred(op blobkit.Operation, bytes int64) {
	if bytes <= 0 {
		return
	}
	m.mu.Lock()
	m.bytesTransferred[op] += bytes
	m.mu.Unlock()
}

// OnLimiterWait records backpressure delays incurred waiting on provider concurrency pools.
func (m *MetricsCollector) OnLimiterWait(driver string, waitDuration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.limiterCounts[driver]++
	m.limiterWaits[driver] += waitDuration
}

// Snapshot returns a point-in-time copy of all accumulated metric counters.
func (m *MetricsCollector) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := MetricsSnapshot{
		OpCounts:         make(map[blobkit.Operation]int64, len(m.opCounts)),
		ErrorCounts:      make(map[blobkit.Operation]int64, len(m.errorCounts)),
		BytesTransferred: make(map[blobkit.Operation]int64, len(m.bytesTransferred)),
		TotalDuration:    make(map[blobkit.Operation]time.Duration, len(m.totalDuration)),
		LimiterWaits:     make(map[string]time.Duration, len(m.limiterWaits)),
		LimiterCounts:    make(map[string]int64, len(m.limiterCounts)),
	}

	for k, v := range m.opCounts {
		snap.OpCounts[k] = v
	}
	for k, v := range m.errorCounts {
		snap.ErrorCounts[k] = v
	}
	for k, v := range m.bytesTransferred {
		snap.BytesTransferred[k] = v
	}
	for k, v := range m.totalDuration {
		snap.TotalDuration[k] = v
	}
	for k, v := range m.limiterWaits {
		snap.LimiterWaits[k] = v
	}
	for k, v := range m.limiterCounts {
		snap.LimiterCounts[k] = v
	}

	return snap
}
