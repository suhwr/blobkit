package key

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

// NewObjectID generates a time-sortable canonical UUIDv7 string.
func NewObjectID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate uuidv7: %w", err)
	}
	return id.String(), nil
}

// UUIDv7Generator formats keys directly by namespace and logical ID:
// [namespace/]<id><ext>
type UUIDv7Generator struct{}

// NewUUIDv7Generator returns a default UUIDv7Generator.
func NewUUIDv7Generator() *UUIDv7Generator {
	return &UUIDv7Generator{}
}

// Generate formats a clean storage key: "{namespace}/{id}{ext}".
func (g *UUIDv7Generator) Generate(ctx context.Context, in KeyInput) (string, error) {
	id := in.ID
	if id == "" {
		var err error
		id, err = NewObjectID()
		if err != nil {
			return "", err
		}
	}

	ext := NormalizeExtension(in.Filename, in.Ext)
	filename := id + ext

	var rawPath string
	if in.Namespace != "" {
		rawPath = path.Join(in.Namespace, filename)
	} else {
		rawPath = filename
	}

	return Sanitize(rawPath), nil
}

// DatePrefixGenerator prepends a UTC date path hierarchy:
// [namespace/]<YYYY>/<MM>/<DD>/<id><ext>
type DatePrefixGenerator struct{}

// NewDatePrefixGenerator returns a DatePrefixGenerator.
func NewDatePrefixGenerator() *DatePrefixGenerator {
	return &DatePrefixGenerator{}
}

// Generate produces a date-partitioned key.
func (g *DatePrefixGenerator) Generate(ctx context.Context, in KeyInput) (string, error) {
	id := in.ID
	if id == "" {
		var err error
		id, err = NewObjectID()
		if err != nil {
			return "", err
		}
	}

	ext := NormalizeExtension(in.Filename, in.Ext)
	filename := id + ext

	ts := in.CreatedAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	} else {
		ts = ts.UTC()
	}

	datePart := ts.Format("2006/01/02")
	var rawPath string
	if in.Namespace != "" {
		rawPath = path.Join(in.Namespace, datePart, filename)
	} else {
		rawPath = path.Join(datePart, filename)
	}

	return Sanitize(rawPath), nil
}

// HashShardedGenerator shards keys across hash prefix subdirectories to distribute write load:
// [namespace/]<shard1>/<shard2>/<id><ext>
type HashShardedGenerator struct {
	// Depth is the number of 2-character hex shard subdirectories (default 2, e.g. "a1/b2").
	Depth int
}

// NewHashShardedGenerator returns a HashShardedGenerator with the specified prefix depth.
func NewHashShardedGenerator(depth int) *HashShardedGenerator {
	if depth <= 0 {
		depth = 2
	}
	if depth > 4 {
		depth = 4
	}
	return &HashShardedGenerator{Depth: depth}
}

// Generate computes a deterministic SHA-256 hash shard prefix based on the ID.
func (g *HashShardedGenerator) Generate(ctx context.Context, in KeyInput) (string, error) {
	id := in.ID
	if id == "" {
		var err error
		id, err = NewObjectID()
		if err != nil {
			return "", err
		}
	}

	ext := NormalizeExtension(in.Filename, in.Ext)
	filename := id + ext

	// Hash the ID to produce a uniform distribution
	h := sha256.Sum256([]byte(id))
	hashHex := hex.EncodeToString(h[:])

	parts := make([]string, 0, g.Depth+2)
	if in.Namespace != "" {
		parts = append(parts, in.Namespace)
	}

	for i := 0; i < g.Depth; i++ {
		start := i * 2
		parts = append(parts, hashHex[start:start+2])
	}
	parts = append(parts, filename)

	return Sanitize(strings.Join(parts, "/")), nil
}

// Func adapts a standard function into a Generator.
type Func func(ctx context.Context, in KeyInput) (string, error)

// Generate delegates to the wrapped function.
func (f Func) Generate(ctx context.Context, in KeyInput) (string, error) {
	return f(ctx, in)
}
