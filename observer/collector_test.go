package observer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/observer"
)

func TestMetricsCollector_ConcurrentTelemetry(t *testing.T) {
	collector := observer.NewMetricsCollector()
	ctx := context.Background()

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				c := collector.OnOperationStart(ctx, blobkit.OpPut, "key.png")
				collector.OnBytesTransferred(blobkit.OpPut, 1024)

				var err error
				if i%10 == 0 {
					err = errors.New("simulated error")
				}
				collector.OnOperationEnd(c, blobkit.OpPut, "key.png", 5*time.Millisecond, err)

				if i%5 == 0 {
					collector.OnLimiterWait("r2-primary", 2*time.Millisecond)
				}
			}
		}(w)
	}

	wg.Wait()

	snap := collector.Snapshot()
	expectedTotalOps := int64(workers * iterations)
	if snap.OpCounts[blobkit.OpPut] != expectedTotalOps {
		t.Fatalf("expected %d ops, got %d", expectedTotalOps, snap.OpCounts[blobkit.OpPut])
	}

	expectedErrors := int64(workers * 10)
	if snap.ErrorCounts[blobkit.OpPut] != expectedErrors {
		t.Fatalf("expected %d errors, got %d", expectedErrors, snap.ErrorCounts[blobkit.OpPut])
	}

	expectedBytes := expectedTotalOps * 1024
	if snap.BytesTransferred[blobkit.OpPut] != expectedBytes {
		t.Fatalf("expected %d bytes, got %d", expectedBytes, snap.BytesTransferred[blobkit.OpPut])
	}

	expectedLimiterCounts := int64(workers * 20)
	if snap.LimiterCounts["r2-primary"] != expectedLimiterCounts {
		t.Fatalf("expected %d limiter waits, got %d", expectedLimiterCounts, snap.LimiterCounts["r2-primary"])
	}
}
