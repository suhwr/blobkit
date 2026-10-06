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
	"github.com/suhwr/blobkit/key"
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
