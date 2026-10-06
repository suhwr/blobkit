package blobkit

import (
	"context"
	"time"
)

// Observer provides vendor-neutral instrumentation hooks for telemetry,
// metrics collection, distributed tracing, and audit logging.
type Observer interface {
	// OnOperationStart is invoked at the beginning of an object storage operation.
	// It may enrich and return a child context (e.g. for distributed tracing spans).
	OnOperationStart(ctx context.Context, op Operation, key string) context.Context

	// OnOperationEnd is invoked when an operation finishes, reporting duration and error status.
	OnOperationEnd(ctx context.Context, op Operation, key string, duration time.Duration, err error)

	// OnBytesTransferred records data transfer volume for streaming uploads and downloads.
	OnBytesTransferred(op Operation, bytes int64)

	// OnLimiterWait records backpressure delay caused by concurrency rate limiters.
	OnLimiterWait(driver string, waitDuration time.Duration)
}

// NoopObserver is a zero-cost inert observer implementation used when no telemetry is attached.
type NoopObserver struct{}

func (NoopObserver) OnOperationStart(ctx context.Context, op Operation, key string) context.Context {
	return ctx
}

func (NoopObserver) OnOperationEnd(ctx context.Context, op Operation, key string, duration time.Duration, err error) {
}

func (NoopObserver) OnBytesTransferred(op Operation, bytes int64) {
}

func (NoopObserver) OnLimiterWait(driver string, waitDuration time.Duration) {
}
