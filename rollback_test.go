package blobkit_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
)

type faultyRegistryStore struct {
	*registry.MemoryStore
	failOnCommitted bool
	failOnUpdate    bool
	failErr         error
}

func (s *faultyRegistryStore) Save(ctx context.Context, record *registry.Record) error {
	if s.failOnCommitted && record.Status == blobkit.StateCommitted {
		return s.failErr
	}
	return s.MemoryStore.Save(ctx, record)
}

func (s *faultyRegistryStore) UpdateStatus(ctx context.Context, objectID string, status blobkit.LifecycleState) error {
	if s.failOnUpdate {
		return s.failErr
	}
	return s.MemoryStore.UpdateStatus(ctx, objectID, status)
}

func TestRollback_Put_RegistrySaveFailure(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "rollback-bucket"})
	regStore := &faultyRegistryStore{
		MemoryStore:     registry.NewMemoryStore(),
		failOnCommitted: true,
		failErr:         errors.New("database connection lost during commit"),
	}

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(regStore),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	payload := []byte("rollback test payload")
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Key: "leaked_check.txt",
	})
	if err == nil {
		t.Fatal("expected Put to fail when registry commit fails")
	}
	if obj != nil {
		t.Fatal("expected nil Object on Put failure")
	}

	// 1. Observable side effect: Physical driver MUST NOT leak the uploaded blob
	_, headErr := driver.Head(ctx, "leaked_check.txt")
	if !blobkit.IsNotFound(headErr) {
		t.Fatalf("physical blob was leaked on driver after registry commit failure: %v", headErr)
	}

	// 2. Observable side effect: Registry record MUST NOT be committed; MUST be StateAborted
	recs, err := regStore.Find(ctx, registry.Filter{Status: blobkit.StateCommitted})
	if err != nil || len(recs) != 0 {
		t.Fatalf("registry has committed records unexpectedly: %v", recs)
	}
	abortedRecs, err := regStore.Find(ctx, registry.Filter{Status: blobkit.StateAborted})
	if err != nil || len(abortedRecs) != 1 {
		t.Fatalf("expected 1 aborted record in registry, got %d", len(abortedRecs))
	}
}

func TestRollback_Put_ChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "rollback-bucket"})
	regStore := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(regStore),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	payload := []byte("valid content")
	_, err = client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Key:            "checksum_fail.txt",
		ClientChecksum: "0000000000000000000000000000000000000000000000000000000000000000", // invalid
	})
	if !errors.Is(err, blobkit.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got: %v", err)
	}

	// 1. Physical object rolled back
	_, headErr := driver.Head(ctx, "checksum_fail.txt")
	if !blobkit.IsNotFound(headErr) {
		t.Fatalf("physical blob was leaked after checksum mismatch: %v", headErr)
	}

	// 2. Registry entry marked StateAborted
	abortedRecs, err := regStore.Find(ctx, registry.Filter{Status: blobkit.StateAborted})
	if err != nil || len(abortedRecs) != 1 {
		t.Fatalf("expected 1 aborted record in registry, got %d", len(abortedRecs))
	}
}

func TestRollback_Delete_RegistryUpdateFailure(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Bucket: "rollback-bucket"})
	regStore := &faultyRegistryStore{
		MemoryStore:  registry.NewMemoryStore(),
		failOnUpdate: false,
		failErr:      errors.New("db deadlock on delete"),
	}

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(regStore),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	obj, err := client.Put(ctx, bytes.NewReader([]byte("test data")), blobkit.PutOptions{
		Key: "delete_fail.txt",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Inject error on registry update
	regStore.failOnUpdate = true

	// Delete should fail and report registry error
	delErr := client.Delete(ctx, obj.ID)
	if delErr == nil {
		t.Fatal("expected Delete to return error when registry update fails")
	}

	// Physical object was deleted
	_, headErr := driver.Head(ctx, obj.Key)
	if !blobkit.IsNotFound(headErr) {
		t.Fatalf("expected physical object to be deleted, got: %v", headErr)
	}

	// Reconcile can identify and heal this ghost record
	regStore.failOnUpdate = false
	report, err := client.Reconcile(ctx, false)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if len(report.GhostRecords) != 1 || report.GhostRecords[0] != obj.ID {
		t.Fatalf("expected ghost record %s in report, got: %v", obj.ID, report.GhostRecords)
	}
	if report.Repaired != 1 {
		t.Fatalf("expected 1 repair by reconcile, got %d", report.Repaired)
	}
}

type partialDeleteDriver struct {
	blobkit.Driver
}

func (d *partialDeleteDriver) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	// Delete only the first key, fail on the rest
	_ = d.Driver.Delete(ctx, keys[0])
	return []string{keys[0]}, errors.New("partial batch deletion failure")
}

func TestRollback_DeleteBatch_PartialFailure(t *testing.T) {
	ctx := context.Background()
	baseDriver := memory.NewDriver(memory.Config{Bucket: "partial-del-bucket"})
	driver := &partialDeleteDriver{Driver: baseDriver}
	regStore := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(regStore),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	obj1, err := client.Put(ctx, bytes.NewReader([]byte("1")), blobkit.PutOptions{Key: "k1.txt"})
	if err != nil {
		t.Fatalf("Put 1 failed: %v", err)
	}
	obj2, err := client.Put(ctx, bytes.NewReader([]byte("2")), blobkit.PutOptions{Key: "k2.txt"})
	if err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	deleted, err := client.DeleteBatch(ctx, []string{obj1.ID, obj2.ID})
	if err == nil {
		t.Fatal("expected error on partial batch deletion")
	}
	if len(deleted) != 1 || deleted[0] != "k1.txt" {
		t.Fatalf("expected k1.txt in deleted list, got: %v", deleted)
	}

	// k1 registry status MUST be StateDeleted
	rec1, _ := regStore.GetByID(ctx, obj1.ID)
	if rec1 != nil {
		t.Fatalf("k1 should not be found in active records (should be StateDeleted)")
	}
	deletedRecs, _ := regStore.Find(ctx, registry.Filter{Status: blobkit.StateDeleted})
	if len(deletedRecs) != 1 || deletedRecs[0].ObjectID != obj1.ID {
		t.Fatalf("expected k1 in StateDeleted registry records, got: %v", deletedRecs)
	}

	// k2 registry status MUST still be StateCommitted
	rec2, err := regStore.GetByID(ctx, obj2.ID)
	if err != nil || rec2.Status != blobkit.StateCommitted {
		t.Fatalf("k2 should remain StateCommitted, got: %v, %v", rec2, err)
	}
}
