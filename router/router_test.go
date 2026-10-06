package router_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/router"
)

func TestFixedRouter(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-primary"})
	r := router.NewFixedRouter(driver)

	selected, err := r.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	if selected.Name() != "mem-primary" {
		t.Fatalf("expected 'mem-primary', got %s", selected.Name())
	}

	// Incompatible forced provider
	_, err = r.Select(ctx, router.RouteContext{ForcedProvider: "unregistered"})
	if err == nil {
		t.Fatal("expected error for unregistered forced provider")
	}
}

func TestFailoverRouter(t *testing.T) {
	ctx := context.Background()
	pDriver := memory.NewDriver(memory.Config{Name: "primary"})
	sDriver := memory.NewDriver(memory.Config{Name: "secondary"})

	r := router.NewFailoverRouter(router.FailoverConfig{
		Primary:                pDriver,
		Secondary:              sDriver,
		MaxConsecutiveFailures: 2,
		Cooldown:               50 * time.Millisecond,
	})

	// 1. Initial healthy state selects primary
	sel, err := r.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "primary" {
		t.Fatalf("expected primary, got %v (err: %v)", sel, err)
	}

	// 2. 1 failure -> still primary (< 2)
	r.ReportFailure("primary", errors.New("503 error"))
	sel, err = r.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "primary" {
		t.Fatalf("expected primary after 1 failure, got %v", sel)
	}

	// 3. 2nd failure -> circuit trips to secondary
	r.ReportFailure("primary", errors.New("503 error"))
	sel, err = r.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "secondary" {
		t.Fatalf("expected secondary after circuit trip, got %v", sel)
	}

	// 4. Wait for cooldown to expire -> canary probe returns primary
	time.Sleep(60 * time.Millisecond)
	sel, err = r.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "primary" {
		t.Fatalf("expected canary probe to primary after cooldown, got %v", sel)
	}

	// 5. Success resets circuit
	r.ReportSuccess("primary")
	sel, err = r.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "primary" {
		t.Fatalf("expected primary after recovery, got %v", sel)
	}
}

func TestNamespaceRouter(t *testing.T) {
	ctx := context.Background()
	defaultDriver := memory.NewDriver(memory.Config{Name: "default-store"})
	r2Driver := memory.NewDriver(memory.Config{Name: "r2-avatars"})
	s3Driver := memory.NewDriver(memory.Config{Name: "s3-backups"})

	nr := router.NewNamespaceRouter(defaultDriver)
	nr.Register("avatars", r2Driver)
	nr.Register("bots/autorespon", r2Driver)
	nr.Register("backups", s3Driver)

	// 1. Exact match avatars
	sel, err := nr.Select(ctx, router.RouteContext{Namespace: "avatars"})
	if err != nil || sel.Name() != "r2-avatars" {
		t.Fatalf("expected 'r2-avatars', got %v", sel)
	}

	// 2. Sub-path match bots/autorespon
	sel, err = nr.Select(ctx, router.RouteContext{Namespace: "bots/autorespon/group-1"})
	if err != nil || sel.Name() != "r2-avatars" {
		t.Fatalf("expected 'r2-avatars', got %v", sel)
	}

	// 3. Backups match
	sel, err = nr.Select(ctx, router.RouteContext{Namespace: "backups"})
	if err != nil || sel.Name() != "s3-backups" {
		t.Fatalf("expected 's3-backups', got %v", sel)
	}

	// 4. Unmatched namespace -> fallback to defaultDriver
	sel, err = nr.Select(ctx, router.RouteContext{Namespace: "misc/temp"})
	if err != nil || sel.Name() != "default-store" {
		t.Fatalf("expected fallback 'default-store', got %v", sel)
	}

	// 5. Forced provider override
	sel, err = nr.Select(ctx, router.RouteContext{Namespace: "avatars", ForcedProvider: "s3-backups"})
	if err != nil || sel.Name() != "s3-backups" {
		t.Fatalf("expected forced 's3-backups', got %v", sel)
	}
}

func TestWeightedRouter(t *testing.T) {
	ctx := context.Background()
	d1 := memory.NewDriver(memory.Config{Name: "d1"})
	d2 := memory.NewDriver(memory.Config{Name: "d2"})

	wr, err := router.NewWeightedRouter([]router.WeightedTarget{
		{Driver: d1, Weight: 80},
		{Driver: d2, Weight: 20},
	})
	if err != nil {
		t.Fatalf("NewWeightedRouter failed: %v", err)
	}

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		sel, err := wr.Select(ctx, router.RouteContext{Op: router.OpPut})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		counts[sel.Name()]++
	}

	if counts["d1"] < 650 {
		t.Fatalf("expected d1 (weight 80) to receive >= 650 requests, got %d", counts["d1"])
	}

	// ForcedProvider test
	forced, err := wr.Select(ctx, router.RouteContext{ForcedProvider: "d2"})
	if err != nil || forced.Name() != "d2" {
		t.Fatalf("expected forced d2, got %v", forced)
	}
}

func TestCircuitBreakerRouter(t *testing.T) {
	ctx := context.Background()
	primary := memory.NewDriver(memory.Config{Name: "cb-primary"})
	fallback := memory.NewDriver(memory.Config{Name: "cb-fallback"})

	cb := router.NewCircuitBreakerRouter(router.CircuitBreakerConfig{
		Primary:          primary,
		Fallback:         fallback,
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Cooldown:         50 * time.Millisecond,
	})

	// 1. Initial Closed state
	if cb.State() != router.CircuitClosed {
		t.Fatalf("expected CircuitClosed, got %s", cb.State())
	}
	sel, err := cb.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "cb-primary" {
		t.Fatalf("expected primary in closed state, got %v", sel)
	}

	// 2. 1 failure -> still closed
	cb.ReportFailure("cb-primary", errors.New("timeout"))
	if cb.State() != router.CircuitClosed {
		t.Fatalf("expected CircuitClosed after 1 fail, got %s", cb.State())
	}

	// 3. 2nd failure -> trips to Open
	cb.ReportFailure("cb-primary", errors.New("timeout"))
	if cb.State() != router.CircuitOpen {
		t.Fatalf("expected CircuitOpen after 2 fails, got %s", cb.State())
	}

	// In Open state, traffic diverts to fallback
	sel, err = cb.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "cb-fallback" {
		t.Fatalf("expected fallback in open state, got %v", sel)
	}

	// 4. Wait for Cooldown -> enters HalfOpen
	time.Sleep(60 * time.Millisecond)
	if cb.State() != router.CircuitHalfOpen {
		t.Fatalf("expected CircuitHalfOpen after cooldown, got %s", cb.State())
	}

	// Canary probe to primary
	sel, err = cb.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "cb-primary" {
		t.Fatalf("expected primary canary probe in half-open, got %v", sel)
	}

	// 5. Success 1 -> still HalfOpen (threshold is 2)
	cb.ReportSuccess("cb-primary")
	if cb.State() != router.CircuitHalfOpen {
		t.Fatalf("expected still HalfOpen after 1 success, got %s", cb.State())
	}

	// 6. Success 2 -> resets to Closed
	cb.ReportSuccess("cb-primary")
	if cb.State() != router.CircuitClosed {
		t.Fatalf("expected CircuitClosed after 2 successes, got %s", cb.State())
	}

	sel, err = cb.Select(ctx, router.RouteContext{Op: router.OpPut})
	if err != nil || sel.Name() != "cb-primary" {
		t.Fatalf("expected primary after recovery, got %v", sel)
	}
}

func TestCapabilityRouter(t *testing.T) {
	ctx := context.Background()
	d1 := memory.NewDriver(memory.Config{Name: "mem-copy"})
	d2 := memory.NewDriver(memory.Config{Name: "mem-multi"})

	cr := router.NewCapabilityRouter(d1, d2)
	all := cr.AllDrivers()
	if len(all) != 2 {
		t.Fatalf("expected 2 drivers, got %d", len(all))
	}

	// Select copy operation
	sel, err := cr.Select(ctx, router.RouteContext{Op: router.OpCopy})
	if err != nil || sel == nil {
		t.Fatalf("failed to select driver with capability: %v", err)
	}
}

