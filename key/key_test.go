package key_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit/key"
)

func TestNewObjectID(t *testing.T) {
	id1, err := key.NewObjectID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id1 == "" {
		t.Fatal("expected non-empty ID")
	}

	id2, err := key.NewObjectID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("expected unique IDs, got %s == %s", id1, id2)
	}
}

func TestSanitize(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"/", ""},
		{"///", ""},
		{"avatars/user.jpg", "avatars/user.jpg"},
		{"/avatars/user.jpg", "avatars/user.jpg"},
		{"avatars\\user.jpg", "avatars/user.jpg"},
		{"avatars/../secret.jpg", "secret.jpg"},
		{"avatars/./sub/user.jpg", "avatars/sub/user.jpg"},
		{"/foo/bar/../../baz.png", "baz.png"},
	}

	for _, tc := range tests {
		actual := key.Sanitize(tc.input)
		if actual != tc.expected {
			t.Errorf("Sanitize(%q) = %q, expected %q", tc.input, actual, tc.expected)
		}
	}
}

func TestNormalizeExtension(t *testing.T) {
	tests := []struct {
		filename    string
		explicitExt string
		expected    string
	}{
		{"photo.jpg", "", ".jpg"},
		{"photo.PNG", "", ".png"},
		{"photo", "webp", ".webp"},
		{"photo", ".WEBP", ".webp"},
		{"photo.jpeg", ".png", ".png"},
		{"photo", "", ""},
	}

	for _, tc := range tests {
		actual := key.NormalizeExtension(tc.filename, tc.explicitExt)
		if actual != tc.expected {
			t.Errorf("NormalizeExtension(%q, %q) = %q, expected %q", tc.filename, tc.explicitExt, actual, tc.expected)
		}
	}
}

func TestUUIDv7Generator(t *testing.T) {
	ctx := context.Background()
	gen := key.NewUUIDv7Generator()

	k, err := gen.Generate(ctx, key.KeyInput{
		ID:        "01925b3a-7f28-7102-8f92-9428ad0e451b",
		Namespace: "avatars",
		Filename:  "photo.jpg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "avatars/01925b3a-7f28-7102-8f92-9428ad0e451b.jpg"
	if k != expected {
		t.Fatalf("expected %q, got %q", expected, k)
	}

	// Without namespace
	k2, err := gen.Generate(ctx, key.KeyInput{
		ID:       "test-id",
		Filename: "doc.pdf",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if k2 != "test-id.pdf" {
		t.Fatalf("expected 'test-id.pdf', got %q", k2)
	}
}

func TestDatePrefixGenerator(t *testing.T) {
	ctx := context.Background()
	gen := key.NewDatePrefixGenerator()

	fixedTime := time.Date(2026, 10, 6, 14, 30, 0, 0, time.UTC)
	k, err := gen.Generate(ctx, key.KeyInput{
		ID:        "obj-123",
		Namespace: "media/recordings",
		Filename:  "audio.ogg",
		CreatedAt: fixedTime,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "media/recordings/2026/10/06/obj-123.ogg"
	if k != expected {
		t.Fatalf("expected %q, got %q", expected, k)
	}
}

func TestHashShardedGenerator(t *testing.T) {
	ctx := context.Background()
	gen := key.NewHashShardedGenerator(2)

	k, err := gen.Generate(ctx, key.KeyInput{
		ID:        "obj-sharded",
		Namespace: "store/items",
		Filename:  "item.png",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Path should match store/items/xx/yy/obj-sharded.png
	parts := strings.Split(k, "/")
	if len(parts) != 5 {
		t.Fatalf("expected 5 segments, got %d in %q", len(parts), k)
	}
	if parts[0] != "store" || parts[1] != "items" {
		t.Fatalf("unexpected namespace prefix in %q", k)
	}
	if len(parts[2]) != 2 || len(parts[3]) != 2 {
		t.Fatalf("shards must be 2 chars each in %q", k)
	}
	if parts[4] != "obj-sharded.png" {
		t.Fatalf("expected filename 'obj-sharded.png', got %s", parts[4])
	}
}
