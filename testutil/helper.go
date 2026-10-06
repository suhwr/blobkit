package testutil

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
)

// MockStorage wraps a BlobKit Client with its underlying MemoryDriver and MemoryStore
// to simplify integration testing in consumer applications without external cloud credentials.
type MockStorage struct {
	Client *blobkit.Client
	Driver *memory.Driver
	Store  *registry.MemoryStore
}

// NewMockStorage constructs an ephemeral in-memory BlobKit client ready for tests.
func NewMockStorage() (*MockStorage, error) {
	memDriver := memory.NewDriver(memory.Config{
		Name:          "test-memory",
		Bucket:        "test-bucket",
		PublicBaseURL: "https://test-media.local",
	})
	memStore := registry.NewMemoryStore()

	client, err := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithRegistry(memStore),
	)
	if err != nil {
		return nil, err
	}

	return &MockStorage{
		Client: client,
		Driver: memDriver,
		Store:  memStore,
	}, nil
}

// Close gracefully shuts down the mock storage.
func (m *MockStorage) Close() error {
	return m.Client.Close()
}

// SeedObject uploads a test object into mock storage and returns the resulting Object.
func (m *MockStorage) SeedObject(ctx context.Context, namespace, filename, content string) (*blobkit.Object, error) {
	return m.Client.Put(ctx, strings.NewReader(content), blobkit.PutOptions{
		Namespace: namespace,
		Filename:  filename,
		Size:      int64(len(content)),
	})
}

// AssertBlobContent retrieves a blob by ID or key and asserts its body matches expected content.
func AssertBlobContent(t testing.TB, client *blobkit.Client, target, expected string) {
	t.Helper()
	reader, err := client.Get(context.Background(), target, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("AssertBlobContent failed to Get %s: %v", target, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("AssertBlobContent failed to read stream %s: %v", target, err)
	}

	if string(data) != expected {
		t.Fatalf("AssertBlobContent mismatch for %s: got %q, want %q", target, string(data), expected)
	}
}
