package blobkit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/cache"
	"github.com/suhwr/blobkit/key"
	"github.com/suhwr/blobkit/observer"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
	"github.com/suhwr/blobkit/router"
)

func TestClient_StandaloneDirectUpload(t *testing.T) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{
		Name:          "mem-local",
		Bucket:        "test-bucket",
		PublicBaseURL: "https://cdn.example.com",
	})

	client, err := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithKeyGenerator(key.NewDatePrefixGenerator()),
	)
	if err != nil {
		t.Fatalf("failed to init blobkit client: %v", err)
	}
	defer client.Close()

	// Upload sample PNG
	pngHeader := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}
	payload := append(pngHeader, bytes.Repeat([]byte{0x01}, 100)...)

	// 1. Put without setting ContentType or Key (auto-sniff & auto-key)
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "avatars",
		Filename:  "user_avatar.png",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if obj.ID == "" {
		t.Fatal("expected generated ObjectID")
	}
	if obj.ContentType != "image/png" {
		t.Fatalf("expected auto-sniffed 'image/png', got %q", obj.ContentType)
	}
	if !strings.HasPrefix(obj.Key, "avatars/") {
		t.Fatalf("expected key prefixed with namespace, got %q", obj.Key)
	}

	// 2. Get by physical key
	reader, err := client.Get(ctx, obj.Key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("downloaded data mismatch")
	}

	// 3. Resolve URL
	url, err := client.ResolveURL(ctx, obj.Key)
	if err != nil {
		t.Fatalf("ResolveURL failed: %v", err)
	}
	expectedURL := "https://cdn.example.com/" + obj.Key
	if url != expectedURL {
		t.Fatalf("expected %q, got %q", expectedURL, url)
	}
}

func TestClient_WithRegistryDiscoveryAndAccess(t *testing.T) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{
		Name:          "r2-primary",
		Bucket:        "shiro-storage",
		PublicBaseURL: "https://media.shiroine.com",
	})
	regStore := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithRegistry(regStore),
		blobkit.WithKeyGenerator(key.NewUUIDv7Generator()),
	)
	if err != nil {
		t.Fatalf("failed to init blobkit client: %v", err)
	}
	defer client.Close()

	audioData := []byte("OGGS_AUDIO_MOCK_STREAM_BYTES")

	// 1. Upload declaring semantic intent (no raw key, no bucket, no provider, no URL)
	uploaded, err := client.Put(ctx, bytes.NewReader(audioData), blobkit.PutOptions{
		Namespace: "bots/autorespon",
		OwnerID:   "bot_123",
		Filename:  "welcome.ogg",
		Metadata: map[string]string{
			"trigger": "!halo",
		},
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if uploaded.Status != blobkit.StateCommitted {
		t.Fatalf("expected status committed, got %s", uploaded.Status)
	}

	// 2. Application discovers object by semantic query (namespace, owner_id) WITHOUT knowing URL or raw key!
	found, err := client.Find(ctx, registry.Filter{
		Namespace: "bots/autorespon",
		OwnerID:   "bot_123",
		Status:    blobkit.StateCommitted,
	})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("expected 1 result, got %d", len(found))
	}

	targetID := found[0].ID
	if targetID != uploaded.ID {
		t.Fatalf("expected ObjectID %s, got %s", uploaded.ID, targetID)
	}

	// 3. Application retrieves blob purely by logical ObjectID
	stream, err := client.Get(ctx, targetID, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get by ObjectID failed: %v", err)
	}
	downloaded, err := io.ReadAll(stream)
	stream.Close()
	if err != nil || !bytes.Equal(downloaded, audioData) {
		t.Fatalf("data mismatch downloading by ObjectID")
	}

	// 4. Resolve delivery URL dynamically by ObjectID (CDN domain dynamically formatted)
	deliveryURL, err := client.ResolveURL(ctx, targetID)
	if err != nil {
		t.Fatalf("ResolveURL failed: %v", err)
	}
	if !strings.HasPrefix(deliveryURL, "https://media.shiroine.com/bots/autorespon/") {
		t.Fatalf("unexpected delivery URL: %s", deliveryURL)
	}

	// 5. Presigned URL access by ObjectID
	presigned, err := client.PresignGet(ctx, targetID, blobkit.PresignOptions{Expiry: 10 * time.Minute})
	if err != nil {
		t.Fatalf("PresignGet failed: %v", err)
	}
	if presigned.URL == "" || presigned.Method != "GET" {
		t.Fatalf("invalid presigned response: %+v", presigned)
	}

	// 6. Delete by ObjectID
	err = client.Delete(ctx, targetID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it can no longer be retrieved by ObjectID
	_, err = client.Get(ctx, targetID, blobkit.GetOptions{})
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound after deletion, got %v", err)
	}
}

func TestClient_MultiProviderRouting(t *testing.T) {
	ctx := context.Background()
	r2Driver := memory.NewDriver(memory.Config{Name: "r2-avatars", Bucket: "b-avatars"})
	s3Driver := memory.NewDriver(memory.Config{Name: "s3-backups", Bucket: "b-backups"})

	nsRouter := router.NewNamespaceRouter(r2Driver)
	nsRouter.Register("avatars", r2Driver)
	nsRouter.Register("backups", s3Driver)

	client, err := blobkit.New(blobkit.WithRouter(nsRouter))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	// Upload to avatars
	avatarObj, err := client.Put(ctx, strings.NewReader("avatar"), blobkit.PutOptions{
		Namespace: "avatars",
		Filename:  "pfp.jpg",
	})
	if err != nil || avatarObj.Provider != "r2-avatars" {
		t.Fatalf("expected r2-avatars provider, got %v (err: %v)", avatarObj.Provider, err)
	}

	// Upload to backups
	backupObj, err := client.Put(ctx, strings.NewReader("backup"), blobkit.PutOptions{
		Namespace: "backups",
		Filename:  "db.sql",
	})
	if err != nil || backupObj.Provider != "s3-backups" {
		t.Fatalf("expected s3-backups provider, got %v (err: %v)", backupObj.Provider, err)
	}
}

func TestClient_CopyMoveRenameExists(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-store"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("init client failed: %v", err)
	}
	defer client.Close()

	// 1. Put initial object
	original, err := client.Put(ctx, strings.NewReader("original file content"), blobkit.PutOptions{
		Namespace: "docs",
		Filename:  "readme.txt",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Exists
	exists, err := client.Exists(ctx, original.ID)
	if err != nil || !exists {
		t.Fatalf("expected object to exist, got %v (err: %v)", exists, err)
	}

	// 3. Rename
	err = client.Rename(ctx, original.ID, "documentation.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	renamed, err := store.GetByID(ctx, original.ID)
	if err != nil || renamed.OriginalFilename != "documentation.txt" {
		t.Fatalf("expected renamed filename 'documentation.txt', got %v", renamed.OriginalFilename)
	}

	// 4. Copy
	copied, err := client.Copy(ctx, original.ID, blobkit.PutOptions{
		Namespace: "backups",
		Filename:  "readme_backup.txt",
	})
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	if copied.ID == original.ID {
		t.Fatal("expected new ObjectID for copied blob")
	}

	copyReader, err := client.Get(ctx, copied.ID, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get on copied object failed: %v", err)
	}
	data, _ := io.ReadAll(copyReader)
	copyReader.Close()
	if string(data) != "original file content" {
		t.Fatalf("copied content mismatch: %s", string(data))
	}

	// 5. Move
	moved, err := client.Move(ctx, original.ID, blobkit.PutOptions{
		Namespace: "archive",
		Filename:  "readme_archived.txt",
	})
	if err != nil {
		t.Fatalf("Move failed: %v", err)
	}

	// Original should no longer exist
	existsOld, err := client.Exists(ctx, original.ID)
	if err != nil || existsOld {
		t.Fatalf("expected old object to not exist after Move, got %v", existsOld)
	}

	// Moved object should exist
	existsNew, err := client.Exists(ctx, moved.ID)
	if err != nil || !existsNew {
		t.Fatalf("expected moved object to exist, got %v", existsNew)
	}
}

func TestClient_RetentionAndLegalHold(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-retention"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("init client failed: %v", err)
	}
	defer client.Close()

	future := time.Now().Add(24 * time.Hour)
	lockedObj, err := client.Put(ctx, strings.NewReader("immutable contract"), blobkit.PutOptions{
		Namespace:      "legal",
		Filename:       "contract.pdf",
		RetentionUntil: &future,
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Attempt deletion -> should be blocked by retention policy
	err = client.Delete(ctx, lockedObj.ID)
	if !errors.Is(err, blobkit.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked, got %v", err)
	}

	// Verify object still exists and readable
	exists, err := client.Exists(ctx, lockedObj.ID)
	if err != nil || !exists {
		t.Fatalf("expected object to still exist under retention, got %v", exists)
	}
}

func TestClient_ResumableUpload(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-resumable"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("init client failed: %v", err)
	}
	defer client.Close()

	// 1. Initiate resumable upload session
	sess, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "media",
		Filename:  "recording.mp4",
	}, 1024, 2*time.Hour)
	if err != nil {
		t.Fatalf("InitiateResumableUpload failed: %v", err)
	}
	if sess.ID == "" || sess.Status != blobkit.SessionActive {
		t.Fatalf("unexpected session state: %+v", sess)
	}

	// 2. Upload part 1 & part 2
	part1Data := "first chunk of media - "
	p1, err := client.UploadPart(ctx, sess.ID, 1, strings.NewReader(part1Data), int64(len(part1Data)))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}
	if p1.PartNumber != 1 || p1.ETag == "" {
		t.Fatalf("invalid part 1: %+v", p1)
	}

	part2Data := "second chunk of media"
	p2, err := client.UploadPart(ctx, sess.ID, 2, strings.NewReader(part2Data), int64(len(part2Data)))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}
	if p2.PartNumber != 2 || p2.ETag == "" {
		t.Fatalf("invalid part 2: %+v", p2)
	}

	// 3. List parts
	parts, err := client.ListSessionParts(ctx, sess.ID)
	if err != nil {
		t.Fatalf("ListSessionParts failed: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}

	// 4. Commit session
	finalObj, err := client.CommitResumableUpload(ctx, sess.ID)
	if err != nil {
		t.Fatalf("CommitResumableUpload failed: %v", err)
	}
	expectedTotalSize := int64(len(part1Data) + len(part2Data))
	if finalObj.Size != expectedTotalSize {
		t.Fatalf("expected total size %d, got %d", expectedTotalSize, finalObj.Size)
	}

	// Verify content read
	reader, err := client.Get(ctx, finalObj.ID, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get final object failed: %v", err)
	}
	body, _ := io.ReadAll(reader)
	reader.Close()
	if string(body) != part1Data+part2Data {
		t.Fatalf("unexpected content: %s", string(body))
	}

	// 5. Test Abort session
	sess2, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "media",
		Filename:  "abandoned.mp4",
	}, 1024, time.Hour)
	if err != nil {
		t.Fatalf("Initiate second session failed: %v", err)
	}

	_, err = client.UploadPart(ctx, sess2.ID, 1, strings.NewReader("partial"), 7)
	if err != nil {
		t.Fatalf("UploadPart on sess2 failed: %v", err)
	}

	if err = client.AbortResumableUpload(ctx, sess2.ID); err != nil {
		t.Fatalf("AbortResumableUpload failed: %v", err)
	}

	// Querying aborted session should fail with ErrSessionNotFound
	_, err = client.ListSessionParts(ctx, sess2.ID)
	if !errors.Is(err, blobkit.ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound for aborted session, got %v", err)
	}
}

func TestClient_SoftDeleteAndRestore(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-lifecycle"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("init client failed: %v", err)
	}
	defer client.Close()

	// 1. Put object
	obj, err := client.Put(ctx, strings.NewReader("temporary data"), blobkit.PutOptions{
		Namespace: "temp",
		Filename:  "temp.txt",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. SoftDelete
	err = client.SoftDelete(ctx, obj.ID)
	if err != nil {
		t.Fatalf("SoftDelete failed: %v", err)
	}

	// Should not be visible to Head or Get
	_, err = client.Head(ctx, obj.ID)
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound for soft-deleted object, got %v", err)
	}
	_, err = client.Get(ctx, obj.ID, blobkit.GetOptions{})
	if !errors.Is(err, blobkit.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound for soft-deleted object, got %v", err)
	}

	// 3. Restore
	err = client.Restore(ctx, obj.ID)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Now it should be visible again
	restored, err := client.Head(ctx, obj.ID)
	if err != nil || restored.Status != blobkit.StateCommitted {
		t.Fatalf("expected restored object in StateCommitted, got %+v (err: %v)", restored, err)
	}

	// 4. PermanentDelete
	err = client.PermanentDelete(ctx, obj.ID)
	if err != nil {
		t.Fatalf("PermanentDelete failed: %v", err)
	}

	exists, err := client.Exists(ctx, obj.ID)
	if err != nil || exists {
		t.Fatalf("expected object to not exist after PermanentDelete, got %v", exists)
	}
}

func TestClient_ObserverAndCache(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-obs-cache"})
	store := registry.NewMemoryStore()
	collector := observer.NewMetricsCollector()
	lru := cache.NewLRUCache(100)

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
		blobkit.WithObserver(collector),
		blobkit.WithCache(lru),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	// 1. Put object
	obj, err := client.Put(ctx, strings.NewReader("cached-data"), blobkit.PutOptions{
		Namespace: "cached",
		Filename:  "item.json",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. First Head -> populates cache
	head1, err := client.Head(ctx, obj.ID)
	if err != nil {
		t.Fatalf("First Head failed: %v", err)
	}
	if head1.ID != obj.ID {
		t.Fatalf("unexpected ID: %s", head1.ID)
	}

	// Verify item is now in cache
	cached, ok := lru.Get(obj.ID)
	if !ok || cached.ID != obj.ID {
		t.Fatalf("expected item to be in LRU cache, got %v", ok)
	}

	// 3. Second Head -> served from cache
	head2, err := client.Head(ctx, obj.ID)
	if err != nil || head2.ID != obj.ID {
		t.Fatalf("Second Head failed: %v", err)
	}

	// 4. Verify metrics collected
	snap := collector.Snapshot()
	if snap.OpCounts[blobkit.OpPut] != 1 {
		t.Fatalf("expected 1 Put op recorded, got %d", snap.OpCounts[blobkit.OpPut])
	}
	if snap.OpCounts[blobkit.OpHead] != 2 {
		t.Fatalf("expected 2 Head ops recorded, got %d", snap.OpCounts[blobkit.OpHead])
	}
	if snap.BytesTransferred[blobkit.OpPut] != int64(len("cached-data")) {
		t.Fatalf("expected %d bytes transferred, got %d", len("cached-data"), snap.BytesTransferred[blobkit.OpPut])
	}

	// 5. Delete -> invalidates cache
	if err := client.Delete(ctx, obj.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, ok := lru.Get(obj.ID); ok {
		t.Fatal("expected item to be evicted from cache on Delete")
	}
}
