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
