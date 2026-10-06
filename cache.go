package blobkit

import "time"

// Cache defines an optional cache interface for storing object metadata and negative lookups.
type Cache interface {
	// Get retrieves cached metadata for the given storage key or object ID.
	Get(key string) (*Object, bool)

	// Set caches object metadata with a specific TTL.
	Set(key string, obj *Object, ttl time.Duration)

	// SetNegative records a negative cache entry (not found) to shield storage backends.
	SetNegative(key string, ttl time.Duration)

	// IsNegative checks whether a key is currently negative-cached.
	IsNegative(key string) bool

	// Delete invalidates any cached entries for the specified key.
	Delete(key string)

	// Purge clears all entries in the cache.
	Purge()
}
