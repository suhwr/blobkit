package router_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/router"
)

func TestTieredRouter_BasicAndCascade(t *testing.T) {
	ctx := context.Background()

	// Tier 1: 2 R2 accounts
	r2Acc1 := memory.NewDriver(memory.Config{Name: "r2-acc01", Bucket: "b1"})
	r2Acc2 := memory.NewDriver(memory.Config{Name: "r2-acc02", Bucket: "b2"})

	// Tier 2: 1 Wasabi account
	wasabiAcc1 := memory.NewDriver(memory.Config{Name: "wasabi-acc01", Bucket: "wb1"})

	// Tier 3: 1 AWS S3 account
	s3Acc1 := memory.NewDriver(memory.Config{Name: "s3-acc01", Bucket: "s3b1"})

	r, err := router.NewTieredRouter(router.TieredRouterConfig{
		Tiers: []router.Tier{
			{
				Name: "Tier-1-R2",
				Targets: []router.TierTarget{
					{Driver: r2Acc1, Weight: 10, MaxFailures: 2, Cooldown: 100 * time.Millisecond},
					{Driver: r2Acc2, Weight: 10, MaxFailures: 2, Cooldown: 100 * time.Millisecond},
				},
			},
			{
				Name: "Tier-2-Wasabi",
				Targets: []router.TierTarget{
					{Driver: wasabiAcc1, Weight: 5, MaxFailures: 2, Cooldown: 100 * time.Millisecond},
				},
			},
			{
				Name: "Tier-3-S3",
				Targets: []router.TierTarget{
					{Driver: s3Acc1, Weight: 1, MaxFailures: 2, Cooldown: 100 * time.Millisecond},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected NewTieredRouter error: %v", err)
	}

	// 1. Initial selections should come exclusively from Tier 1 (r2-acc01 or r2-acc02)
	for i := 0; i < 20; i++ {
		d, err := r.Select(ctx, router.RouteContext{})
		if err != nil {
			t.Fatalf("unexpected select error: %v", err)
		}
		if d.Name() != "r2-acc01" && d.Name() != "r2-acc02" {
			t.Fatalf("expected Tier 1 driver, got %s", d.Name())
		}
	}

	// 2. Direct ForcedProvider lookup works across any tier
	dWasabi, err := r.Select(ctx, router.RouteContext{ForcedProvider: "wasabi-acc01"})
	if err != nil || dWasabi.Name() != "wasabi-acc01" {
		t.Fatalf("expected wasabi-acc01, got %v (err: %v)", dWasabi, err)
	}
	dS3, err := r.Select(ctx, router.RouteContext{ForcedProvider: "s3-acc01"})
	if err != nil || dS3.Name() != "s3-acc01" {
		t.Fatalf("expected s3-acc01, got %v (err: %v)", dS3, err)
	}

	// 3. Trip r2-acc01 circuit breaker
	r.ReportFailure("r2-acc01", errors.New("rate limited 429"))
	r.ReportFailure("r2-acc01", errors.New("rate limited 429"))

	// All selections in Tier 1 should now route to r2-acc02
	for i := 0; i < 10; i++ {
		d, err := r.Select(ctx, router.RouteContext{})
		if err != nil {
			t.Fatalf("unexpected select error: %v", err)
		}
		if d.Name() != "r2-acc02" {
			t.Fatalf("expected r2-acc02 while r2-acc01 is tripped, got %s", d.Name())
		}
	}

	// 4. Trip r2-acc02 circuit breaker
	r.ReportFailure("r2-acc02", errors.New("quota exceeded"))
	r.ReportFailure("r2-acc02", errors.New("quota exceeded"))

	// Both Tier 1 accounts are down! Router should CASCADE to Tier 2 (wasabi-acc01)
	for i := 0; i < 10; i++ {
		d, err := r.Select(ctx, router.RouteContext{})
		if err != nil {
			t.Fatalf("unexpected select error: %v", err)
		}
		if d.Name() != "wasabi-acc01" {
			t.Fatalf("expected cascade to Tier 2 wasabi-acc01, got %s", d.Name())
		}
	}

	// 5. Trip wasabi-acc01 circuit breaker
	r.ReportFailure("wasabi-acc01", errors.New("500 internal server error"))
	r.ReportFailure("wasabi-acc01", errors.New("500 internal server error"))

	// Tier 1 and Tier 2 are down! Router should CASCADE to Tier 3 (s3-acc01)
	for i := 0; i < 10; i++ {
		d, err := r.Select(ctx, router.RouteContext{})
		if err != nil {
			t.Fatalf("unexpected select error: %v", err)
		}
		if d.Name() != "s3-acc01" {
			t.Fatalf("expected cascade to Tier 3 s3-acc01, got %s", d.Name())
		}
	}

	// 6. Trip s3-acc01 circuit breaker
	r.ReportFailure("s3-acc01", errors.New("network timeout"))
	r.ReportFailure("s3-acc01", errors.New("network timeout"))

	// All tiers down: should return ErrProviderUnavailable
	_, err = r.Select(ctx, router.RouteContext{})
	if !errors.Is(err, blobkit.ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable, got %v", err)
	}

	// 7. Test cooldown recovery: wait 120ms for cooldown to expire
	time.Sleep(120 * time.Millisecond)

	// Tier 1 should be re-probed
	dProbed, err := r.Select(ctx, router.RouteContext{})
	if err != nil {
		t.Fatalf("unexpected error after cooldown expiry: %v", err)
	}
	if dProbed.Name() != "r2-acc01" && dProbed.Name() != "r2-acc02" {
		t.Fatalf("expected Tier 1 recovery after cooldown, got %s", dProbed.Name())
	}

	// Success resets failures
	r.ReportSuccess(dProbed.Name())
}

func TestTieredRouter_Concurrency(t *testing.T) {
	ctx := context.Background()
	d1 := memory.NewDriver(memory.Config{Name: "p1", Bucket: "b1"})
	d2 := memory.NewDriver(memory.Config{Name: "p2", Bucket: "b2"})

	r, err := router.NewTieredRouter(router.TieredRouterConfig{
		Tiers: []router.Tier{
			{
				Name: "Tier-1",
				Targets: []router.TierTarget{
					{Driver: d1, Weight: 1},
					{Driver: d2, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				d, err := r.Select(ctx, router.RouteContext{})
				if err == nil && d != nil {
					if workerID%2 == 0 {
						r.ReportFailure(d.Name(), errors.New("transient error"))
					} else {
						r.ReportSuccess(d.Name())
					}
				}
			}
		}(i)
	}
	wg.Wait()
}
