package blobkit_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
)

func TestClient_Reconcile(t *testing.T) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{
		Name:   "primary",
		Bucket: "reconcile-bucket",
	})
	regStore := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithRegistry(regStore),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// 1. Normal object: in both driver and registry
	normalObj, err := client.Put(ctx, bytes.NewReader([]byte("normal content")), blobkit.PutOptions{
		Key: "normal.txt",
	})
	if err != nil {
		t.Fatalf("failed to put normal object: %v", err)
	}

	// 2. Orphan object: in driver only, NOT in registry
	orphanData := []byte("orphan content")
	orphanObj := &blobkit.Object{Key: "orphan.txt", Size: int64(len(orphanData))}
	_, err = memDriver.Put(ctx, orphanObj, bytes.NewReader(orphanData), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("failed to put orphan object: %v", err)
	}

	// 3. Ghost record: in registry (StateCommitted), but NOT in driver
	ghostID := "ghost-uuid-123"
	err = regStore.Save(ctx, &registry.Record{
		ObjectID:  ghostID,
		Key:       "missing.txt",
		Bucket:    "reconcile-bucket",
		Provider:  "primary",
		Size:      100,
		Status:    blobkit.StateCommitted,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to save ghost record: %v", err)
	}

	// 4. Mismatched size object: in both, but registry has size 999 while driver has 11
	mismatchID := "mismatch-uuid-456"
	mismatchData := []byte("short-bytes")
	mismatchObj := &blobkit.Object{Key: "mismatch.txt", Size: int64(len(mismatchData))}
	_, err = memDriver.Put(ctx, mismatchObj, bytes.NewReader(mismatchData), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("failed to put mismatch object: %v", err)
	}
	err = regStore.Save(ctx, &registry.Record{
		ObjectID:  mismatchID,
		Key:       "mismatch.txt",
		Bucket:    "reconcile-bucket",
		Provider:  "primary",
		Size:      999,
		Status:    blobkit.StateCommitted,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to save mismatch record: %v", err)
	}

	// Test DryRun = true
	repDry, err := client.Reconcile(ctx, true)
	if err != nil {
		t.Fatalf("Reconcile dry-run failed: %v", err)
	}
	if len(repDry.OrphanObjects) != 1 || repDry.OrphanObjects[0] != "orphan.txt" {
		t.Fatalf("expected orphan.txt in OrphanObjects, got: %v", repDry.OrphanObjects)
	}
	if len(repDry.GhostRecords) != 1 || repDry.GhostRecords[0] != ghostID {
		t.Fatalf("expected ghostID in GhostRecords, got: %v", repDry.GhostRecords)
	}
	if len(repDry.MismatchedSize) != 1 || repDry.MismatchedSize[0] != "mismatch.txt" {
		t.Fatalf("expected mismatch.txt in MismatchedSize, got: %v", repDry.MismatchedSize)
	}
	if repDry.Repaired != 0 {
		t.Fatalf("expected 0 repairs in dry-run, got %d", repDry.Repaired)
	}

	// Verify ghost is still committed
	rec, err := regStore.GetByID(ctx, ghostID)
	if err != nil || rec.Status != blobkit.StateCommitted {
		t.Fatalf("ghost record should remain committed after dry-run, got: %v", rec)
	}

	// Test DryRun = false (live repair)
	repLive, err := client.Reconcile(ctx, false)
	if err != nil {
		t.Fatalf("Reconcile live repair failed: %v", err)
	}
	if repLive.Repaired == 0 {
		t.Fatalf("expected repairs to be > 0, got %d", repLive.Repaired)
	}

	// Verify ghost is now StateDeleted
	_, err = regStore.GetByID(ctx, ghostID)
	if !blobkit.IsNotFound(err) {
		t.Fatalf("expected ErrObjectNotFound for deleted ghost, got: %v", err)
	}
	deletedRecs, err := regStore.Find(ctx, registry.Filter{Status: blobkit.StateDeleted})
	if err != nil || len(deletedRecs) != 1 || deletedRecs[0].ObjectID != ghostID {
		t.Fatalf("expected 1 deleted record matching ghostID, got: %v, %v", deletedRecs, err)
	}

	// Normal object is still untouched
	normRec, err := regStore.GetByID(ctx, normalObj.ID)
	if err != nil || normRec.Status != blobkit.StateCommitted {
		t.Fatalf("normal record was modified unexpectedly: %v", normRec)
	}
}

func TestClient_ReconcileNoRegistry(t *testing.T) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{
		Bucket: "no-reg-bucket",
	})
	client, err := blobkit.New(blobkit.WithDriver(memDriver))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Reconcile(ctx, false)
	if err == nil {
		t.Fatal("expected error when reconciling client without registry")
	}
}
