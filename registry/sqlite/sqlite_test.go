package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/lifecycle"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry/sqlite"
)

func TestSQLiteStore_CRUDAndDiscovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.New(sqlite.Config{
		FilePath:    ":memory:",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("failed to init sqlite store: %v", err)
	}
	defer store.Close()

	rec := &blobkit.Record{
		ObjectID:         "01925b3a-7f28-7102-8f92-9428ad0e451b",
		Namespace:        "documents",
		OwnerID:          "tenant_1",
		Key:              "documents/2026/10/report.pdf",
		Bucket:           "vault",
		Provider:         "r2-main",
		MIMEType:         "application/pdf",
		Size:             125000,
		ChecksumSHA256:   "aabbccdd11223344",
		OriginalFilename: "report.pdf",
		Visibility:       blobkit.VisibilityPrivate,
		Status:           blobkit.StatePending,
		Metadata: map[string]string{
			"project": "apollo",
			"tier":    "premium",
		},
	}

	// 1. Save
	if err := store.Save(ctx, rec); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. GetByID
	fetched, err := store.GetByID(ctx, rec.ObjectID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if fetched.OwnerID != "tenant_1" || fetched.Metadata["project"] != "apollo" {
		t.Fatalf("unexpected fetched data: %+v", fetched)
	}

	// 3. GetByKey
	byKey, err := store.GetByKey(ctx, rec.Key)
	if err != nil {
		t.Fatalf("GetByKey failed: %v", err)
	}
	if byKey.ObjectID != rec.ObjectID {
		t.Fatalf("expected ID %s, got %s", rec.ObjectID, byKey.ObjectID)
	}

	// 4. Update status to Committed
	if err := store.UpdateStatus(ctx, rec.ObjectID, blobkit.StateCommitted); err != nil {
		t.Fatalf("UpdateStatus failed: %v", err)
	}

	// 5. Update metadata
	newMeta := map[string]string{"project": "apollo", "reviewed": "true"}
	if err := store.UpdateMetadata(ctx, rec.ObjectID, newMeta); err != nil {
		t.Fatalf("UpdateMetadata failed: %v", err)
	}

	// 6. Update filename
	if err := store.UpdateFilename(ctx, rec.ObjectID, "annual_report.pdf"); err != nil {
		t.Fatalf("UpdateFilename failed: %v", err)
	}

	updatedRec, _ := store.GetByID(ctx, rec.ObjectID)
	if updatedRec.OriginalFilename != "annual_report.pdf" || updatedRec.Metadata["reviewed"] != "true" {
		t.Fatalf("metadata/filename update mismatch: %+v", updatedRec)
	}

	// 7. Find with filters
	results, err := store.Find(ctx, blobkit.Filter{
		Namespace: "documents",
		OwnerID:   "tenant_1",
		Status:    blobkit.StateCommitted,
	})
	if err != nil || len(results) != 1 {
		t.Fatalf("Find failed: expected 1 match, got %d (err: %v)", len(results), err)
	}

	// 8. Soft Delete
	if err := store.Delete(ctx, rec.ObjectID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = store.GetByID(ctx, rec.ObjectID)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound after soft delete, got %v", err)
	}

	// 9. Hard Delete
	if err := store.HardDelete(ctx, rec.ObjectID); err != nil {
		t.Fatalf("HardDelete failed: %v", err)
	}
}

func TestSQLiteStore_ResumableSessions(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.New(sqlite.Config{
		FilePath:    ":memory:",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("failed to init sqlite store: %v", err)
	}
	defer store.Close()

	sess := &blobkit.UploadSession{
		ID:        "sess_12345",
		Key:       "videos/movie.mp4",
		UploadID:  "s3_multipart_id_999",
		Provider:  "s3-main",
		PartSize:  5 * 1024 * 1024,
		Status:    blobkit.SessionActive,
		ExpiresAt: time.Now().UTC().Add(time.Hour),
		Parts: []blobkit.CompletedPart{
			{PartNumber: 1, ETag: "etag1", Size: 5242880},
		},
	}

	// Save
	if err := store.SaveSession(ctx, sess); err != nil {
		t.Fatalf("SaveSession failed: %v", err)
	}

	// Get
	fetched, err := store.GetSession(ctx, sess.ID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if fetched.PartSize != sess.PartSize || len(fetched.Parts) != 1 {
		t.Fatalf("unexpected fetched session: %+v", fetched)
	}

	// Delete
	if err := store.DeleteSession(ctx, sess.ID); err != nil {
		t.Fatalf("DeleteSession failed: %v", err)
	}
	_, err = store.GetSession(ctx, sess.ID)
	if !errors.Is(err, blobkit.ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestSQLiteStore_WithClientAndSweeper(t *testing.T) {
	ctx := context.Background()

	// 1. Initialize SQLite store
	store, err := sqlite.New(sqlite.Config{
		FilePath:    ":memory:",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("failed to init sqlite store: %v", err)
	}
	defer store.Close()

	// 2. Initialize in-memory storage driver
	driver := memory.NewDriver(memory.Config{
		Name:          "local-mem",
		Bucket:        "test-bucket",
		PublicBaseURL: "https://media.example.com",
	})

	// 3. Initialize BlobKit client
	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	// 4. Upload object with expired timestamp
	pastTime := time.Now().UTC().Add(-10 * time.Minute)
	obj, err := client.Put(ctx, bytes.NewReader([]byte("temporary test content")), blobkit.PutOptions{
		Namespace: "temp",
		Filename:  "temp_cache.txt",
		ExpiresAt: &pastTime,
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify object exists in storage
	exists, err := client.Exists(ctx, obj.ID)
	if err != nil || !exists {
		t.Fatalf("expected object to exist initially")
	}

	// 5. Initialize Sweeper and run single reconciliation cycle
	sweeper, err := lifecycle.NewSweeper(client, lifecycle.SweeperConfig{
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to init sweeper: %v", err)
	}

	res, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatalf("sweeper RunOnce failed: %v", err)
	}
	if res.ExpiredPurged != 1 {
		t.Fatalf("expected 1 expired object purged, got %d", res.ExpiredPurged)
	}

	// Verify object was permanently purged from both physical driver and SQLite registry
	existsAfter, _ := client.Exists(ctx, obj.ID)
	if existsAfter {
		t.Fatalf("expected expired object to be purged from storage")
	}
	_, getErr := client.Get(ctx, obj.ID, blobkit.GetOptions{})
	if !errors.Is(getErr, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound after purge, got %v", getErr)
	}
}

func TestSQLiteStore_NonexistentErrors(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.New(sqlite.Config{
		FilePath:    ":memory:",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("failed to init sqlite store: %v", err)
	}
	defer store.Close()

	if err := store.UpdateStatus(ctx, "nonexistent", blobkit.StateCommitted); !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound on UpdateStatus, got %v", err)
	}

	if err := store.HardDelete(ctx, "nonexistent"); !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound on HardDelete, got %v", err)
	}

	if err := store.DeleteSession(ctx, "nonexistent"); !errors.Is(err, blobkit.ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound on DeleteSession, got %v", err)
	}
}

func TestSQLiteStore_SubNamespace_DeletedBefore_AndOrdering(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.New(sqlite.Config{
		FilePath:    ":memory:",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("failed to init sqlite store: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	_ = store.Save(ctx, &blobkit.Record{
		ObjectID:  "sq-ns-1",
		Key:       "media/file1.png",
		Namespace: "media",
		Status:    blobkit.StateCommitted,
		CreatedAt: now.Add(-10 * time.Minute),
	})
	_ = store.Save(ctx, &blobkit.Record{
		ObjectID:  "sq-ns-2",
		Key:       "media/images/file2.png",
		Namespace: "media/images",
		Status:    blobkit.StateCommitted,
		CreatedAt: now.Add(-5 * time.Minute),
	})
	_ = store.Save(ctx, &blobkit.Record{
		ObjectID:  "sq-ns-3",
		Key:       "media_other/file3.png",
		Namespace: "media_other",
		Status:    blobkit.StateCommitted,
		CreatedAt: now.Add(-1 * time.Minute),
	})

	// 1. Find by "media" should match "media" and "media/images", but NOT "media_other"
	matched, err := store.Find(ctx, blobkit.Filter{Namespace: "media"})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(matched) != 2 {
		t.Fatalf("expected 2 matches for namespace 'media', got %d", len(matched))
	}

	// 2. Ordering: verify newest first (DESC)
	if matched[0].ObjectID != "sq-ns-2" || matched[1].ObjectID != "sq-ns-1" {
		t.Fatalf("expected DESC order ('sq-ns-2' before 'sq-ns-1'), got [%s, %s]", matched[0].ObjectID, matched[1].ObjectID)
	}

	// 3. Soft-deleted records test with DeletedBefore
	delTime1 := now.Add(-2 * time.Hour)
	delTime2 := now.Add(-10 * time.Minute)
	_ = store.Save(ctx, &blobkit.Record{
		ObjectID:  "sq-del-1",
		Key:       "del1.txt",
		Status:    blobkit.StateDeleted,
		DeletedAt: &delTime1,
	})
	_ = store.Save(ctx, &blobkit.Record{
		ObjectID:  "sq-del-2",
		Key:       "del2.txt",
		Status:    blobkit.StateDeleted,
		DeletedAt: &delTime2,
	})

	cutoff := now.Add(-1 * time.Hour)
	delMatched, err := store.Find(ctx, blobkit.Filter{
		Status:        blobkit.StateDeleted,
		DeletedBefore: &cutoff,
	})
	if err != nil {
		t.Fatalf("Find deleted failed: %v", err)
	}
	if len(delMatched) != 1 || delMatched[0].ObjectID != "sq-del-1" {
		t.Fatalf("expected only sq-del-1, got %+v", delMatched)
	}
}

