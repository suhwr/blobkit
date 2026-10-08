package blobkit_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/cache"
	"github.com/suhwr/blobkit/key"
	"github.com/suhwr/blobkit/lifecycle"
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
		Name:          "primary-storage",
		Bucket:        "app-storage",
		PublicBaseURL: "https://media.example.com",
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
		Namespace: "media/audio",
		OwnerID:   "tenant_123",
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
		Namespace: "media/audio",
		OwnerID:   "tenant_123",
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
	if !strings.HasPrefix(deliveryURL, "https://media.example.com/media/audio/") {
		t.Fatalf("unexpected delivery URL: %s", deliveryURL)
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

	// Attempt move -> should be blocked before copy without creating orphan
	_, err = client.Move(ctx, lockedObj.ID, blobkit.PutOptions{
		Namespace: "legal",
		Filename:  "contract_moved.pdf",
	})
	if !errors.Is(err, blobkit.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked on Move, got %v", err)
	}

	// Verify object still exists and readable
	exists, err := client.Exists(ctx, lockedObj.ID)
	if err != nil || !exists {
		t.Fatalf("expected object to still exist under retention, got %v", exists)
	}

	// Overwrite via explicit Key should be blocked if locked
	_, err = client.Put(ctx, strings.NewReader("overwrite attempt"), blobkit.PutOptions{
		Key: lockedObj.Key,
	})
	if !errors.Is(err, blobkit.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked on Put with locked Key, got %v", err)
	}

	// Resumable upload initiation via explicit Key should also be blocked if locked
	_, err = client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Key: lockedObj.Key,
	}, 1024, time.Hour)
	if !errors.Is(err, blobkit.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked on InitiateResumableUpload with locked Key, got %v", err)
	}

	// Test unlocked object overwrite succeeds cleanly without collision
	unlockedObj, err := client.Put(ctx, strings.NewReader("initial content"), blobkit.PutOptions{
		Namespace: "docs",
		Filename:  "doc.txt",
	})
	if err != nil {
		t.Fatalf("Put initial unlocked failed: %v", err)
	}

	overwrittenObj, err := client.Put(ctx, strings.NewReader("updated content"), blobkit.PutOptions{
		Key: unlockedObj.Key,
	})
	if err != nil {
		t.Fatalf("Put overwrite failed: %v", err)
	}
	if overwrittenObj.ID != unlockedObj.ID {
		t.Fatalf("expected object ID to be preserved on overwrite without explicit ID, got %s vs %s", overwrittenObj.ID, unlockedObj.ID)
	}

	// Test Copy to locked destination key is blocked
	srcObj, err := client.Put(ctx, strings.NewReader("source data"), blobkit.PutOptions{
		Namespace: "docs",
		Filename:  "source.txt",
	})
	if err != nil {
		t.Fatalf("Put source failed: %v", err)
	}

	_, err = client.Copy(ctx, srcObj.ID, blobkit.PutOptions{
		Key: lockedObj.Key,
	})
	if !errors.Is(err, blobkit.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked on Copy to locked destination key, got %v", err)
	}

	// Test Copy to unlocked destination key overwrites cleanly and preserves destination ID
	copiedOverwritten, err := client.Copy(ctx, srcObj.ID, blobkit.PutOptions{
		Key: unlockedObj.Key,
	})
	if err != nil {
		t.Fatalf("Copy overwrite failed: %v", err)
	}
	if copiedOverwritten.ID != unlockedObj.ID {
		t.Fatalf("expected destination ID to be preserved on Copy overwrite, got %s vs %s", copiedOverwritten.ID, unlockedObj.ID)
	}

	// Test Move to same key does NOT delete the object
	movedObj, err := client.Move(ctx, srcObj.ID, blobkit.PutOptions{
		Key: srcObj.Key,
	})
	if err != nil {
		t.Fatalf("Move to same key failed: %v", err)
	}
	exists, err = client.Exists(ctx, srcObj.ID)
	if err != nil || !exists {
		t.Fatalf("expected object to still exist after Move to same key, exists=%v, err=%v", exists, err)
	}
	if movedObj.Key != srcObj.Key {
		t.Fatalf("expected moved key %s, got %s", srcObj.Key, movedObj.Key)
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

func TestSanitizeErrorMessage(t *testing.T) {
	raw := `request failed token=supersecret123 and api_key=secretkey999 and key=avatars/john.png with Bearer ya29.a0AfH6_xyz and Basic dXNlcjpwYXNz proxyconnect tcp: dial tcp 10.0.0.5:8080: timeout and dial tcp [::1]:9000: refused and payload {"client_secret":"my_secret_token"}`
	sanitized := blobkit.SanitizeErrorMessage(raw)

	if strings.Contains(sanitized, "supersecret123") {
		t.Fatalf("token secret was not scrubbed: %s", sanitized)
	}
	if strings.Contains(sanitized, "secretkey999") {
		t.Fatalf("api_key secret was not scrubbed: %s", sanitized)
	}
	if strings.Contains(sanitized, "my_secret_token") {
		t.Fatalf("JSON secret was not scrubbed: %s", sanitized)
	}
	if strings.Contains(sanitized, "$1=[redacted]") {
		t.Fatalf("regex group $1 was emitted literally: %s", sanitized)
	}
	if !strings.Contains(sanitized, "token=[redacted]") {
		t.Fatalf("expected token=[redacted], got: %s", sanitized)
	}
	if !strings.Contains(sanitized, "api_key=[redacted]") {
		t.Fatalf("expected api_key=[redacted], got: %s", sanitized)
	}
	// Verify normal object key is preserved without over-redaction
	if !strings.Contains(sanitized, "key=avatars/john.png") {
		t.Fatalf("harmless object key was incorrectly redacted: %s", sanitized)
	}
	if strings.Contains(sanitized, "ya29.a0AfH6_xyz") {
		t.Fatalf("bearer token not scrubbed: %s", sanitized)
	}
	if strings.Contains(sanitized, "dXNlcjpwYXNz") {
		t.Fatalf("basic auth not scrubbed: %s", sanitized)
	}
	if strings.Contains(sanitized, "10.0.0.5:8080") {
		t.Fatalf("internal IPv4 not scrubbed: %s", sanitized)
	}
	if strings.Contains(sanitized, "[::1]:9000") {
		t.Fatalf("internal IPv6 not scrubbed: %s", sanitized)
	}
}

func TestPreserveSentinel(t *testing.T) {
	if !blobkit.PreserveSentinel(blobkit.ErrObjectNotFound) {
		t.Fatal("expected ErrObjectNotFound to be preserved")
	}
	if !blobkit.PreserveSentinel(context.Canceled) {
		t.Fatal("expected context.Canceled to be preserved")
	}
	if !blobkit.PreserveSentinel(context.DeadlineExceeded) {
		t.Fatal("expected context.DeadlineExceeded to be preserved")
	}
	if blobkit.PreserveSentinel(errors.New("random error")) {
		t.Fatal("expected arbitrary error NOT to be preserved")
	}
}

type headCountingDriver struct {
	blobkit.Driver
	headCalls int64
}

func (d *headCountingDriver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
	atomic.AddInt64(&d.headCalls, 1)
	time.Sleep(50 * time.Millisecond) // ensure concurrent callers overlap in singleflight window
	return d.Driver.Head(ctx, key)
}

func TestClient_CacheStampedeSingleflight(t *testing.T) {
	ctx := context.Background()
	baseDriver := memory.NewDriver(memory.Config{
		Bucket: "stampede-bucket",
	})
	driver := &headCountingDriver{Driver: baseDriver}
	lru := cache.NewLRUCache(10)

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithCache(lru),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// 1. Put an object directly
	data := []byte("stampede-data")
	_, err = client.Put(ctx, bytes.NewReader(data), blobkit.PutOptions{
		Key: "stampede.txt",
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Evict from cache to simulate cold cache / cache expiration
	lru.Delete("stampede.txt")
	atomic.StoreInt64(&driver.headCalls, 0)

	// 2. Launch 20 concurrent goroutines calling Head
	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			obj, err := client.Head(ctx, "stampede.txt")
			if err != nil {
				errCh <- err
				return
			}
			if obj.Size != int64(len(data)) {
				errCh <- errors.New("size mismatch")
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent Head failed: %v", err)
	}

	// 3. Assert that singleflight collapsed all 20 calls into exactly 1 driver Head call
	calls := atomic.LoadInt64(&driver.headCalls)
	if calls != 1 {
		t.Fatalf("expected exactly 1 driver Head invocation, got %d", calls)
	}

	// 4. Subsequent Head should hit cache directly (0 additional driver calls)
	_, err = client.Head(ctx, "stampede.txt")
	if err != nil {
		t.Fatalf("subsequent Head failed: %v", err)
	}
	if atomic.LoadInt64(&driver.headCalls) != 1 {
		t.Fatalf("subsequent Head hit driver instead of cache")
	}
}

func TestErrorClassification_PermanentVsTransient(t *testing.T) {
	permanentErrors := []error{
		blobkit.ErrObjectNotFound,
		blobkit.ErrBucketNotFound,
		blobkit.ErrInvalidKey,
		blobkit.ErrInvalidID,
		blobkit.ErrInvalidFilename,
		blobkit.ErrPreconditionFailed,
		blobkit.ErrPermissionDenied,
		blobkit.ErrInvalidCredentials,
		blobkit.ErrUploadTooLarge,
		blobkit.ErrChecksumMismatch,
		blobkit.ErrSizeMismatch,
		blobkit.ErrSecurityViolation,
		blobkit.ErrMIMEMismatch,
		blobkit.ErrNilReader,
		blobkit.ErrObjectLocked,
		blobkit.ErrMultipartInvalidState,
		blobkit.ErrUnsupportedOperation,
	}

	for _, err := range permanentErrors {
		if !blobkit.IsPermanent(err) {
			t.Errorf("expected %v to be classified as permanent", err)
		}
		if blobkit.IsTransient(err) {
			t.Errorf("expected %v NOT to be classified as transient", err)
		}
	}

	transientErrors := []error{
		blobkit.ErrRateLimited,
		context.DeadlineExceeded,
	}

	for _, err := range transientErrors {
		if !blobkit.IsTransient(err) {
			t.Errorf("expected %v to be classified as transient", err)
		}
		if blobkit.IsPermanent(err) {
			t.Errorf("expected %v NOT to be classified as permanent", err)
		}
	}
}

func TestCopy_DestinationCollisionPreflight(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-copy", Bucket: "test"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	// 1. Put source object
	srcObj, err := client.Put(ctx, strings.NewReader("source content"), blobkit.PutOptions{
		Namespace: "src",
		Filename:  "source.txt",
		Metadata:  map[string]string{"type": "source"},
	})
	if err != nil {
		t.Fatalf("Put source failed: %v", err)
	}

	// 2. Put pre-existing destination object
	dstKey := "dst/target.txt"
	origDstPayload := []byte("original target content")
	_, err = driver.Put(ctx, &blobkit.Object{Key: dstKey}, bytes.NewReader(origDstPayload), blobkit.PutOptions{
		Size: int64(len(origDstPayload)),
	})
	if err != nil {
		t.Fatalf("Driver Put destination failed: %v", err)
	}

	// Copy to dstKey
	copiedObj, err := client.Copy(ctx, srcObj.ID, blobkit.PutOptions{
		Key: dstKey,
	})
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	if copiedObj == nil || copiedObj.Key != dstKey {
		t.Fatalf("invalid copied object: %+v", copiedObj)
	}

	// Verify copied object has source contents
	r, err := client.Get(ctx, copiedObj.ID, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get copied failed: %v", err)
	}
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if string(data) != "source content" {
		t.Fatalf("expected copied content 'source content', got %q", string(data))
	}
}

func TestCopy_CompliancePreservation(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-compliance", Bucket: "test"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	retention := time.Now().Add(24 * time.Hour)
	expires := time.Now().Add(48 * time.Hour)

	srcObj, err := client.Put(ctx, strings.NewReader("compliance payload"), blobkit.PutOptions{
		Namespace:      "legal",
		Filename:       "doc.pdf",
		RetentionUntil: &retention,
		ExpiresAt:      &expires,
		LegalHold:      true,
		Visibility:     blobkit.VisibilityPrivate,
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Copy to new destination key
	copiedObj, err := client.Copy(ctx, srcObj.ID, blobkit.PutOptions{
		Key: "legal/copy.pdf",
	})
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	rec, err := reg.GetByID(ctx, copiedObj.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if !rec.LegalHold {
		t.Fatal("expected LegalHold to be preserved")
	}
	if rec.Visibility != blobkit.VisibilityPrivate {
		t.Fatalf("expected VisibilityPrivate, got %s", rec.Visibility)
	}
	if rec.RetentionUntil == nil || !rec.RetentionUntil.Equal(retention) {
		t.Fatalf("expected RetentionUntil preserved, got %v", rec.RetentionUntil)
	}
	if rec.ExpiresAt == nil || !rec.ExpiresAt.Equal(expires) {
		t.Fatalf("expected ExpiresAt preserved, got %v", rec.ExpiresAt)
	}
}

func TestCopy_HeterogeneousFallback(t *testing.T) {
	ctx := context.Background()
	driver1 := memory.NewDriver(memory.Config{Name: "mem-src", Bucket: "b1"})
	driver2 := memory.NewDriver(memory.Config{Name: "mem-dst", Bucket: "b2"})

	r := router.NewNamespaceRouter(driver1)
	r.Register("ns2", driver2)

	client, err := blobkit.New(
		blobkit.WithRouter(r),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	// Put in ns1
	_, err = driver1.Put(ctx, &blobkit.Object{Key: "ns1/item.txt"}, strings.NewReader("cross-provider payload"), blobkit.PutOptions{
		Size: int64(len("cross-provider payload")),
	})
	if err != nil {
		t.Fatalf("driver1 Put failed: %v", err)
	}

	// Copy from ns1 to ns2 (cross-provider)
	copiedObj, err := client.Copy(ctx, "ns1/item.txt", blobkit.PutOptions{
		Namespace: "ns2",
		Key:       "ns2/item_copied.txt",
	})
	if err != nil {
		t.Fatalf("Cross-provider Copy failed: %v", err)
	}

	// Verify item exists on driver2
	rObj, err := driver2.Get(ctx, "ns2/item_copied.txt", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("driver2 Get failed: %v", err)
	}
	data, _ := io.ReadAll(rObj.Body)
	rObj.Body.Close()
	if string(data) != "cross-provider payload" {
		t.Fatalf("expected 'cross-provider payload', got %q", string(data))
	}
	if copiedObj.Size != int64(len("cross-provider payload")) {
		t.Fatalf("expected size %d, got %d", len("cross-provider payload"), copiedObj.Size)
	}
}

func TestResumableUpload_ConcurrentUploadPart(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-resumable", Bucket: "b"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	session, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "resumable",
		Filename:  "large.dat",
	}, 1024, time.Hour)
	if err != nil {
		t.Fatalf("InitiateResumableUpload failed: %v", err)
	}

	const partCount = 5
	var wg sync.WaitGroup
	errCh := make(chan error, partCount)

	for i := 1; i <= partCount; i++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte(partNum)}, 1024)
			_, pErr := client.UploadPart(ctx, session.ID, partNum, bytes.NewReader(payload), int64(len(payload)))
			if pErr != nil {
				errCh <- pErr
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent UploadPart failed: %v", err)
	}

	// Verify all parts recorded in session
	sess, err := reg.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if len(sess.Parts) != partCount {
		t.Fatalf("lost update detected! expected %d parts, got %d", partCount, len(sess.Parts))
	}

	committedObj, err := client.CommitResumableUpload(ctx, session.ID)
	if err != nil {
		t.Fatalf("CommitResumableUpload failed: %v", err)
	}
	if committedObj.Size != int64(partCount*1024) {
		t.Fatalf("expected total size %d, got %d", partCount*1024, committedObj.Size)
	}
}

func TestResumableCommit_Recovery(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-recover", Bucket: "b"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	session, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "recovery",
		Filename:  "file.dat",
	}, 512, time.Hour)
	if err != nil {
		t.Fatalf("InitiateResumableUpload failed: %v", err)
	}

	payload := bytes.Repeat([]byte("A"), 512)
	_, err = client.UploadPart(ctx, session.ID, 1, bytes.NewReader(payload), 512)
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	obj1, err := client.CommitResumableUpload(ctx, session.ID)
	if err != nil {
		t.Fatalf("first CommitResumableUpload failed: %v", err)
	}

	// Second commit of the same session ID should recover idempotently
	obj2, err := client.CommitResumableUpload(ctx, session.ID)
	if err != nil {
		t.Fatalf("second CommitResumableUpload failed to recover: %v", err)
	}
	if obj1.ID != obj2.ID {
		t.Fatalf("expected identical object ID on recovery, got %s and %s", obj1.ID, obj2.ID)
	}
}

func TestPolicy_SanitizeFilename_WindowsPaths(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{`C:\Users\admin\Documents\secret.pdf`, "secret.pdf"},
		{`..\..\etc\passwd`, "passwd"},
		{`folder\subfolder\avatar.png`, "avatar.png"},
		{`normal_file.txt`, "normal_file.txt"},
		{`..\..\`, ""},
	}

	for _, tc := range cases {
		actual := blobkit.SanitizeFilename(tc.input)
		if actual != tc.expected {
			t.Errorf("SanitizeFilename(%q) = %q, expected %q", tc.input, actual, tc.expected)
		}
	}
}

func TestSweeper_MultipartStaleTTL(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-sweeper", Bucket: "b"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	session, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "active",
		Filename:  "active.dat",
	}, 1024, time.Hour)
	if err != nil {
		t.Fatalf("InitiateResumableUpload failed: %v", err)
	}

	sweeper, err := lifecycle.NewSweeper(client, lifecycle.SweeperConfig{
		MultipartStaleTTL: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSweeper failed: %v", err)
	}

	res, err := sweeper.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}
	if res.SessionsAborted != 0 {
		t.Fatalf("expected 0 sessions aborted because session is fresh, got %d", res.SessionsAborted)
	}

	// Session should still exist and be active
	sess, err := reg.GetSession(ctx, session.ID)
	if err != nil || sess.Status != blobkit.SessionActive {
		t.Fatalf("expected session to remain active, err: %v", err)
	}
}

func TestSessionLocks_PurgedOnCommit(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-lock-test", Bucket: "b"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	session, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
		Namespace: "docs",
		Filename:  "contract.pdf",
	}, 1024, time.Hour)
	if err != nil {
		t.Fatalf("InitiateResumableUpload failed: %v", err)
	}

	partData := bytes.Repeat([]byte("A"), 1024)
	_, err = client.UploadPart(ctx, session.ID, 1, bytes.NewReader(partData), 1024)
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// At this point, sessionLock should exist for session.ID
	if !client.HasSessionLock(session.ID) {
		t.Fatalf("expected active session lock during upload")
	}

	// Commit the resumable upload
	_, err = client.CommitResumableUpload(ctx, session.ID)
	if err != nil {
		t.Fatalf("CommitResumableUpload failed: %v", err)
	}

	// The session lock MUST be evicted from sync.Map upon commit
	if client.HasSessionLock(session.ID) {
		t.Fatalf("session lock leaked in sync.Map after successful commit")
	}
	if client.SessionLockCount() != 0 {
		t.Fatalf("expected 0 active session locks, got %d", client.SessionLockCount())
	}

	// Idempotent second commit should also not leave any lock behind
	_, err = client.CommitResumableUpload(ctx, session.ID)
	if err != nil {
		t.Fatalf("idempotent commit failed: %v", err)
	}
	if client.HasSessionLock(session.ID) {
		t.Fatalf("session lock leaked after idempotent commit")
	}
}

func TestBatchUpdateMetadata_ResolutionByKey(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-meta-test", Bucket: "b"})
	reg := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	_, err = client.Put(ctx, bytes.NewReader([]byte("data1")), blobkit.PutOptions{
		Key: "assets/img1.png",
		Metadata: map[string]string{
			"initial": "v1",
		},
	})
	if err != nil {
		t.Fatalf("Put obj1 failed: %v", err)
	}

	obj2, err := client.Put(ctx, bytes.NewReader([]byte("data2")), blobkit.PutOptions{
		Key: "assets/img2.png",
		Metadata: map[string]string{
			"initial": "v1",
		},
	})
	if err != nil {
		t.Fatalf("Put obj2 failed: %v", err)
	}

	// Call BatchUpdateMetadata passing storage KEYS (not ObjectIDs)
	updates := map[string]map[string]string{
		"assets/img1.png": {"tag": "sunset", "author": "alice"},
		obj2.ID:           {"tag": "forest", "author": "bob"}, // Mix of key and objectID
	}

	if err := client.BatchUpdateMetadata(ctx, updates); err != nil {
		t.Fatalf("BatchUpdateMetadata failed: %v", err)
	}

	// Verify head by key
	h1, err := client.Head(ctx, "assets/img1.png")
	if err != nil {
		t.Fatalf("Head obj1 failed: %v", err)
	}
	if h1.Metadata["tag"] != "sunset" || h1.Metadata["author"] != "alice" {
		t.Fatalf("unexpected metadata for obj1: %+v", h1.Metadata)
	}

	h2, err := client.Head(ctx, "assets/img2.png")
	if err != nil {
		t.Fatalf("Head obj2 failed: %v", err)
	}
	if h2.Metadata["tag"] != "forest" || h2.Metadata["author"] != "bob" {
		t.Fatalf("unexpected metadata for obj2: %+v", h2.Metadata)
	}
}

func TestList_RoutesByPrefixNamespace(t *testing.T) {
	ctx := context.Background()
	driverA := memory.NewDriver(memory.Config{Name: "mem-media", Bucket: "media-b"})
	driverB := memory.NewDriver(memory.Config{Name: "mem-docs", Bucket: "docs-b"})

	// Put distinct data directly into each driver
	_, err := driverA.Put(ctx, &blobkit.Object{Key: "media/photo.jpg"}, bytes.NewReader([]byte("photo")), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("put driverA failed: %v", err)
	}
	_, err = driverB.Put(ctx, &blobkit.Object{Key: "docs/report.pdf"}, bytes.NewReader([]byte("report")), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("put driverB failed: %v", err)
	}

	nsRouter := router.NewNamespaceRouter(driverA)
	nsRouter.Register("media", driverA)
	nsRouter.Register("docs", driverB)

	client, err := blobkit.New(
		blobkit.WithRouter(nsRouter),
	)
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	defer client.Close()

	// List with Prefix: "media/"
	resMedia, err := client.List(ctx, blobkit.ListOptions{Prefix: "media/"})
	if err != nil {
		t.Fatalf("List media failed: %v", err)
	}
	if len(resMedia.Objects) != 1 || resMedia.Objects[0].Key != "media/photo.jpg" {
		t.Fatalf("expected media/photo.jpg from driverA, got %+v", resMedia.Objects)
	}

	// List with Prefix: "docs/"
	resDocs, err := client.List(ctx, blobkit.ListOptions{Prefix: "docs/"})
	if err != nil {
		t.Fatalf("List docs failed: %v", err)
	}
	if len(resDocs.Objects) != 1 || resDocs.Objects[0].Key != "docs/report.pdf" {
		t.Fatalf("expected docs/report.pdf from driverB, got %+v", resDocs.Objects)
	}
}

func TestReconcile_PaginationOver100Records(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-reconcile-pagination"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	defer client.Close()

	// Seed 150 objects: 145 exist physically, 5 are ghost records (present in registry only)
	for i := 1; i <= 150; i++ {
		key := fmt.Sprintf("items/file_%03d.txt", i)
		objID := fmt.Sprintf("obj-reconcile-%03d", i)

		rec := &blobkit.Record{
			ObjectID: objID,
			Key:      key,
			Size:     10,
			Status:   blobkit.StateCommitted,
			Provider: driver.Name(),
		}
		if err := store.Save(ctx, rec); err != nil {
			t.Fatalf("save record %d failed: %v", i, err)
		}

		// Only put in driver if not among the 5 ghosts (e.g. 101..105)
		if i < 101 || i > 105 {
			_, err := driver.Put(ctx, &blobkit.Object{Key: key, Size: 10}, strings.NewReader("1234567890"), blobkit.PutOptions{})
			if err != nil {
				t.Fatalf("driver Put %d failed: %v", i, err)
			}
		}
	}

	report, err := client.Reconcile(ctx, false)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Must have audited all 150 records, not just the first 100
	if report.TotalChecked < 150 {
		t.Fatalf("expected TotalChecked >= 150, got %d", report.TotalChecked)
	}
	if len(report.GhostRecords) != 5 {
		t.Fatalf("expected 5 ghost records found across batches, got %d", len(report.GhostRecords))
	}
	if report.Repaired != 5 {
		t.Fatalf("expected 5 repaired records, got %d", report.Repaired)
	}
}

func TestFindSoftDeletedObjects_PaginationAndCutoff(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-softdel"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	defer client.Close()

	now := time.Now().UTC()

	// 100 soft-deleted records deleted 1 minute ago (recent)
	for i := 1; i <= 100; i++ {
		delTime := now.Add(-1 * time.Minute)
		_ = store.Save(ctx, &blobkit.Record{
			ObjectID:  fmt.Sprintf("recent-del-%03d", i),
			Key:       fmt.Sprintf("recent/%03d.txt", i),
			Status:    blobkit.StateDeleted,
			DeletedAt: &delTime,
		})
	}

	// 50 soft-deleted records deleted 2 hours ago (old)
	for i := 1; i <= 50; i++ {
		delTime := now.Add(-2 * time.Hour)
		_ = store.Save(ctx, &blobkit.Record{
			ObjectID:  fmt.Sprintf("old-del-%03d", i),
			Key:       fmt.Sprintf("old/%03d.txt", i),
			Status:    blobkit.StateDeleted,
			DeletedAt: &delTime,
		})
	}

	// Query for soft-deleted objects older than 1 hour ago with limit 100
	cutoff := now.Add(-1 * time.Hour)
	results, err := client.FindSoftDeletedObjects(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("FindSoftDeletedObjects failed: %v", err)
	}

	if len(results) != 50 {
		t.Fatalf("expected 50 old soft-deleted objects, got %d", len(results))
	}
}

func TestClient_KeyCollisionPreflight(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{Name: "mem-preflight"})
	store := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(driver),
		blobkit.WithRegistry(store),
	)
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	defer client.Close()

	// Initial upload: ID="obj-initial", Key="docs/spec.pdf"
	_, err = client.Put(ctx, strings.NewReader("spec content"), blobkit.PutOptions{
		ID:  "obj-initial",
		Key: "docs/spec.pdf",
	})
	if err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	// Attempt Put with a different explicit ID on the same storage key -> preflight collision rejection
	_, err = client.Put(ctx, strings.NewReader("conflict content"), blobkit.PutOptions{
		ID:  "obj-different",
		Key: "docs/spec.pdf",
	})
	if err == nil || !strings.Contains(err.Error(), "already belongs to object") {
		t.Fatalf("expected put_collision error on conflicting explicit ID, got %v", err)
	}

	// Attempt Copy with a different explicit ID on the same destination key -> preflight collision rejection
	_, err = client.Put(ctx, strings.NewReader("source content"), blobkit.PutOptions{
		ID:  "obj-source",
		Key: "docs/source.pdf",
	})
	if err != nil {
		t.Fatalf("Put source failed: %v", err)
	}

	_, err = client.Copy(ctx, "obj-source", blobkit.PutOptions{
		ID:  "obj-different-copy",
		Key: "docs/spec.pdf",
	})
	if err == nil || !strings.Contains(err.Error(), "already belongs to object") {
		t.Fatalf("expected copy_collision error on conflicting explicit ID, got %v", err)
	}
}

