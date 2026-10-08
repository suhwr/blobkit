package registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/registry"
)

func TestMemoryStore_CRUD(t *testing.T) {
	ctx := context.Background()
	store := registry.NewMemoryStore()
	defer store.Close()

	rec := &registry.Record{
		ObjectID:         "01925b3a-7f28-7102-8f92-9428ad0e451b",
		Namespace:        "media/audio",
		OwnerID:          "tenant_123",
		Key:              "media/audio/2026/10/06/audio.ogg",
		Bucket:           "app-storage",
		Provider:         "r2-primary",
		MIMEType:         "audio/ogg",
		Size:             45000,
		ChecksumSHA256:   "abcdef1234567890",
		OriginalFilename: "voice_note.ogg",
		Visibility:       blobkit.VisibilityPrivate,
		Status:           blobkit.StatePending,
		Metadata: map[string]string{
			"trigger_word": "greeting",
			"room_id":      "room_abc",
		},
	}

	// 1. Save
	err := store.Save(ctx, rec)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. GetByID
	fetched, err := store.GetByID(ctx, rec.ObjectID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if fetched.OwnerID != "tenant_123" || fetched.Status != blobkit.StatePending {
		t.Fatalf("unexpected fetched record: %+v", fetched)
	}

	// 3. GetByKey
	byKey, err := store.GetByKey(ctx, "media/audio/2026/10/06/audio.ogg")
	if err != nil {
		t.Fatalf("GetByKey failed: %v", err)
	}
	if byKey.ObjectID != rec.ObjectID {
		t.Fatalf("expected ID %s, got %s", rec.ObjectID, byKey.ObjectID)
	}

	// 4. Update status to Committed
	err = store.UpdateStatus(ctx, rec.ObjectID, blobkit.StateCommitted)
	if err != nil {
		t.Fatalf("UpdateStatus failed: %v", err)
	}
	committedRec, _ := store.GetByID(ctx, rec.ObjectID)
	if committedRec.Status != blobkit.StateCommitted {
		t.Fatalf("expected status committed, got %s", committedRec.Status)
	}

	// 5. Query / Find by Namespace and OwnerID without knowing URL
	results, err := store.Find(ctx, registry.Filter{
		Namespace: "media/audio",
		OwnerID:   "tenant_123",
		Status:    blobkit.StateCommitted,
	})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].OriginalFilename != "voice_note.ogg" {
		t.Fatalf("expected original filename 'voice_note.ogg', got %s", results[0].OriginalFilename)
	}

	// 6. Query by metadata tag
	metaResults, err := store.Find(ctx, registry.Filter{
		Metadata: map[string]string{"trigger_word": "greeting"},
	})
	if err != nil || len(metaResults) != 1 {
		t.Fatalf("expected 1 metadata match, got %d (err: %v)", len(metaResults), err)
	}

	// 7. Delete
	err = store.Delete(ctx, rec.ObjectID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = store.GetByID(ctx, rec.ObjectID)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestMemoryStore_Pagination(t *testing.T) {
	ctx := context.Background()
	store := registry.NewMemoryStore()
	defer store.Close()

	for i := 1; i <= 10; i++ {
		_ = store.Save(ctx, &registry.Record{
			ObjectID:  string(rune('a' + i)),
			Namespace: "avatars",
			Status:    blobkit.StateCommitted,
			CreatedAt: time.Now().Add(time.Duration(i) * time.Minute),
		})
	}

	// Page 1 (limit 4)
	page1, err := store.Find(ctx, registry.Filter{
		Namespace: "avatars",
		Limit:     4,
		Offset:    0,
	})
	if err != nil || len(page1) != 4 {
		t.Fatalf("expected 4 items in page 1, got %d (err: %v)", len(page1), err)
	}

	// Page 2 (limit 4, offset 4)
	page2, err := store.Find(ctx, registry.Filter{
		Namespace: "avatars",
		Limit:     4,
		Offset:    4,
	})
	if err != nil || len(page2) != 4 {
		t.Fatalf("expected 4 items in page 2, got %d (err: %v)", len(page2), err)
	}

	if page1[0].ObjectID == page2[0].ObjectID {
		t.Fatal("pagination overlap detected")
	}
}

func TestMemoryStore_NonexistentErrors(t *testing.T) {
	ctx := context.Background()
	store := registry.NewMemoryStore()
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

func TestMemoryStore_DeepCloning(t *testing.T) {
	ctx := context.Background()
	store := registry.NewMemoryStore()
	defer store.Close()

	meta := map[string]string{"foo": "bar"}
	rec := &registry.Record{
		ObjectID: "clone-id",
		Key:      "clone-key",
		Metadata: meta,
	}
	if err := store.Save(ctx, rec); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	meta["foo"] = "tampered"
	fetched, err := store.GetByID(ctx, "clone-id")
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if fetched.Metadata["foo"] != "bar" {
		t.Fatalf("expected 'bar', got %q", fetched.Metadata["foo"])
	}

	fetched.Metadata["foo"] = "retrieved-tampered"
	fetched2, _ := store.GetByID(ctx, "clone-id")
	if fetched2.Metadata["foo"] != "bar" {
		t.Fatalf("expected 'bar', got %q", fetched2.Metadata["foo"])
	}

	// Resumable session cloning
	parts := []blobkit.CompletedPart{{PartNumber: 1, ETag: "etag1", Size: 100}}
	sess := &registry.UploadSession{
		ID:    "sess-clone",
		Parts: parts,
	}
	if err := store.SaveSession(ctx, sess); err != nil {
		t.Fatalf("SaveSession failed: %v", err)
	}

	parts[0].ETag = "tampered-etag"
	fetchedSess, err := store.GetSession(ctx, "sess-clone")
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if fetchedSess.Parts[0].ETag != "etag1" {
		t.Fatalf("expected 'etag1', got %q", fetchedSess.Parts[0].ETag)
	}
}

func TestMemoryStore_ReusingKeyAfterDeletion(t *testing.T) {
	ctx := context.Background()
	store := registry.NewMemoryStore()
	defer store.Close()

	rec1 := &registry.Record{
		ObjectID: "id-1",
		Key:      "logs/app.log",
		Status:   blobkit.StateCommitted,
	}
	if err := store.Save(ctx, rec1); err != nil {
		t.Fatalf("Save rec1 failed: %v", err)
	}

	// Active key collision
	rec2 := &registry.Record{
		ObjectID: "id-2",
		Key:      "logs/app.log",
		Status:   blobkit.StateCommitted,
	}
	if err := store.Save(ctx, rec2); err == nil {
		t.Fatal("expected key collision error when saving active duplicate key")
	}

	// Delete rec1
	if err := store.UpdateStatus(ctx, "id-1", blobkit.StateDeleted); err != nil {
		t.Fatalf("UpdateStatus deleted failed: %v", err)
	}

	// Now rec2 should save successfully
	if err := store.Save(ctx, rec2); err != nil {
		t.Fatalf("Save rec2 after rec1 deletion failed: %v", err)
	}

	// GetByKey should now return rec2
	fetched, err := store.GetByKey(ctx, "logs/app.log")
	if err != nil {
		t.Fatalf("GetByKey failed: %v", err)
	}
	if fetched.ObjectID != "id-2" {
		t.Fatalf("expected 'id-2', got %q", fetched.ObjectID)
	}
}

func TestMemoryStore_SubNamespaceAndDeletedBefore(t *testing.T) {
	ctx := context.Background()
	store := registry.NewMemoryStore()
	defer store.Close()

	now := time.Now().UTC()
	_ = store.Save(ctx, &registry.Record{
		ObjectID:  "rec-ns-1",
		Key:       "media/file1.png",
		Namespace: "media",
		Status:    blobkit.StateCommitted,
		CreatedAt: now.Add(-10 * time.Minute),
	})
	_ = store.Save(ctx, &registry.Record{
		ObjectID:  "rec-ns-2",
		Key:       "media/images/file2.png",
		Namespace: "media/images",
		Status:    blobkit.StateCommitted,
		CreatedAt: now.Add(-5 * time.Minute),
	})
	_ = store.Save(ctx, &registry.Record{
		ObjectID:  "rec-ns-3",
		Key:       "media_other/file3.png",
		Namespace: "media_other",
		Status:    blobkit.StateCommitted,
		CreatedAt: now.Add(-1 * time.Minute),
	})

	// Find by "media" should match "media" and "media/images", but NOT "media_other"
	matched, err := store.Find(ctx, registry.Filter{Namespace: "media"})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(matched) != 2 {
		t.Fatalf("expected 2 matches for namespace 'media', got %d", len(matched))
	}

	// Soft-deleted records test with DeletedBefore
	delTime1 := now.Add(-2 * time.Hour)
	delTime2 := now.Add(-10 * time.Minute)
	_ = store.Save(ctx, &registry.Record{
		ObjectID:  "rec-del-1",
		Key:       "del1.txt",
		Status:    blobkit.StateDeleted,
		DeletedAt: &delTime1,
	})
	_ = store.Save(ctx, &registry.Record{
		ObjectID:  "rec-del-2",
		Key:       "del2.txt",
		Status:    blobkit.StateDeleted,
		DeletedAt: &delTime2,
	})

	cutoff := now.Add(-1 * time.Hour)
	delMatched, err := store.Find(ctx, registry.Filter{
		Status:        blobkit.StateDeleted,
		DeletedBefore: &cutoff,
	})
	if err != nil {
		t.Fatalf("Find deleted failed: %v", err)
	}
	if len(delMatched) != 1 || delMatched[0].ObjectID != "rec-del-1" {
		t.Fatalf("expected only rec-del-1, got %+v", delMatched)
	}
}


