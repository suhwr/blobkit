package lifecycle_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/lifecycle"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
)

func TestSweeper_RunOnce(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-sweep"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	// 1. Create expired object
	past := time.Now().Add(-10 * time.Minute)
	expiredObj, err := client.Put(ctx, strings.NewReader("expired content"), blobkit.PutOptions{
		Namespace: "ephemeral",
		Filename:  "token.txt",
		ExpiresAt: &past,
	})
	if err != nil {
		t.Fatalf("Put expired failed: %v", err)
	}

	// 2. Create soft-deleted object with past deletion timestamp
	softObj, err := client.Put(ctx, strings.NewReader("soft-deleted content"), blobkit.PutOptions{
		Namespace: "trash",
		Filename:  "trash.txt",
	})
	if err != nil {
		t.Fatalf("Put soft failed: %v", err)
	}
	if err := client.SoftDelete(ctx, softObj.ID); err != nil {
		t.Fatalf("SoftDelete failed: %v", err)
	}
	// Manually backdate DeletedAt in store for testing TTL purge
	softRec, _ := store.GetByID(ctx, softObj.ID)
	if softRec == nil {
		// find via soft-deleted filter
		recs, _ := store.Find(ctx, registry.Filter{ObjectID: softObj.ID, Status: blobkit.StateDeleted})
		if len(recs) > 0 {
			softRec = &recs[0]
		}
	}
	if softRec != nil {
		twoHoursAgo := time.Now().Add(-2 * time.Hour)
		softRec.DeletedAt = &twoHoursAgo
		_ = store.Save(ctx, softRec)
	}

	// 3. Create stale upload session
	staleSess, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "uploads",
		Filename:  "stale.bin",
	}, 1024, time.Hour)
	if err != nil {
		t.Fatalf("InitiateResumableUpload failed: %v", err)
	}
	sRec, _ := store.GetSession(ctx, staleSess.ID)
	if sRec != nil {
		sRec.ExpiresAt = time.Now().Add(-10 * time.Minute)
		_ = store.SaveSession(ctx, sRec)
	}

	sweeper, err := lifecycle.NewSweeper(client, lifecycle.SweeperConfig{
		SoftDeleteTTL:     1 * time.Hour,
		MultipartStaleTTL: 5 * time.Minute,
		BatchSize:         50,
	})
	if err != nil {
		t.Fatalf("NewSweeper failed: %v", err)
	}

	result, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	if result.ExpiredPurged != 1 {
		t.Fatalf("expected 1 expired purged, got %d", result.ExpiredPurged)
	}
	if result.SoftDeletePurged != 1 {
		t.Fatalf("expected 1 soft-deleted purged, got %d", result.SoftDeletePurged)
	}
	if result.SessionsAborted != 1 {
		t.Fatalf("expected 1 session aborted, got %d", result.SessionsAborted)
	}

	// Verify expired object does not exist
	existsExpired, _ := client.Exists(ctx, expiredObj.ID)
	if existsExpired {
		t.Fatal("expected expired object to be permanently purged")
	}

	// Verify soft-deleted object does not exist
	existsSoft, _ := client.Exists(ctx, softObj.ID)
	if existsSoft {
		t.Fatal("expected soft-deleted object to be permanently purged")
	}

	// Verify session aborted
	_, err = client.ListSessionParts(ctx, staleSess.ID)
	if !blobkit.IsNotFound(err) && err != blobkit.ErrSessionNotFound {
		t.Fatalf("expected session not found after sweep abort, got %v", err)
	}
}

func TestSweeper_StartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := memory.NewDriver(memory.Config{Name: "mem-sweep-loop"})
	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	sweeper, err := lifecycle.NewSweeper(client, lifecycle.SweeperConfig{
		Interval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSweeper failed: %v", err)
	}

	if err := sweeper.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Start second time should return error
	if err := sweeper.Start(ctx); err == nil {
		t.Fatal("expected error on double Start")
	}

	time.Sleep(120 * time.Millisecond)
	sweeper.Stop()
}
