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
