package key

import (
	"context"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// KeyInput supplies contextual metadata to a Generator to determine physical object paths.
type KeyInput struct {
	// ID is the logical identifier (typically UUIDv7).
	ID string

	// Namespace is the logical bucket partition (e.g. "avatars", "audio", "backups").
	Namespace string

	// Filename is the original client file name (e.g. "sample.jpg").
	Filename string

	// Ext is the sanitized file extension (e.g. ".jpg"). If empty, extracted from Filename.
	Ext string

	// CreatedAt is the reference timestamp (defaults to time.Now().UTC() if zero).
	CreatedAt time.Time
}

// Generator defines how physical object keys are generated.
type Generator interface {
	Generate(ctx context.Context, in KeyInput) (string, error)
}

// Sanitize cleans up path delimiters, resolves relative dots, and removes leading slashes.
func Sanitize(raw string) string {
	if raw == "" {
		return ""
	}
	cleaned := strings.ReplaceAll(raw, "\\", "/")
	cleaned = path.Clean("/" + cleaned)
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." {
		return ""
	}
	return cleaned
}

// NormalizeExtension ensures a consistent lowercase dot-prefixed extension.
func NormalizeExtension(filename, explicitExt string) string {
	ext := explicitExt
	if ext == "" && filename != "" {
		ext = filepath.Ext(filename)
	}
	if ext == "" {
		return ""
	}
	ext = strings.ToLower(strings.TrimSpace(ext))
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	return ext
}
