package mime_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/suhwr/blobkit/mime"
)

func TestSniffPNG(t *testing.T) {
	// PNG signature: \x89PNG\r\n\x1a\n
	pngHeader := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D}
	payload := append(pngHeader, bytes.Repeat([]byte{0xFF}, 1000)...)

	reader := bytes.NewReader(payload)
	reconstructed, contentType, err := mime.Sniff(reader, "image.png", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if contentType != "image/png" {
		t.Fatalf("expected 'image/png', got %q", contentType)
	}

	// Verify all bytes are preserved
	all, err := io.ReadAll(reconstructed)
	if err != nil {
		t.Fatalf("error reading reconstructed stream: %v", err)
	}
	if !bytes.Equal(all, payload) {
		t.Fatal("reconstructed stream payload mismatch")
	}
}

func TestSniffWebP(t *testing.T) {
	// RIFF....WEBP
	webpHeader := []byte("RIFF\x00\x00\x00\x00WEBPVP8 ")
	reader := bytes.NewReader(webpHeader)

	reconstructed, contentType, err := mime.Sniff(reader, "photo.webp", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if contentType != "image/webp" {
		t.Fatalf("expected 'image/webp', got %q", contentType)
	}

	all, _ := io.ReadAll(reconstructed)
	if !bytes.Equal(all, webpHeader) {
		t.Fatal("stream corrupted")
	}
}

func TestSniffJSON(t *testing.T) {
	jsonData := []byte(`{"status": "ok", "count": 42}`)
	reader := bytes.NewReader(jsonData)

	reconstructed, contentType, err := mime.Sniff(reader, "data.json", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if contentType != "application/json" {
		t.Fatalf("expected 'application/json', got %q", contentType)
	}

	all, _ := io.ReadAll(reconstructed)
	if !bytes.Equal(all, jsonData) {
		t.Fatal("stream corrupted")
	}
}

func TestSniffNonSeeker(t *testing.T) {
	text := "Hello World from BlobKit non-seeker stream"
	// strings.Reader implements io.ReadSeeker, so wrap it in a struct that only implements io.Reader
	pureReader := io.NopCloser(strings.NewReader(text))

	reconstructed, contentType, err := mime.Sniff(pureReader, "note.txt", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if contentType != "text/plain" {
		t.Fatalf("expected 'text/plain', got %q", contentType)
	}

	all, err := io.ReadAll(reconstructed)
	if err != nil {
		t.Fatalf("error reading reconstructed stream: %v", err)
	}
	if string(all) != text {
		t.Fatalf("payload mismatch: expected %q, got %q", text, string(all))
	}
}

func TestSniffHintOverride(t *testing.T) {
	text := "binary payload"
	reader := strings.NewReader(text)

	reconstructed, contentType, err := mime.Sniff(reader, "file.bin", "custom/mime-type")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if contentType != "custom/mime-type" {
		t.Fatalf("expected hint 'custom/mime-type', got %q", contentType)
	}

	all, _ := io.ReadAll(reconstructed)
	if string(all) != text {
		t.Fatal("stream mismatch")
	}
}
