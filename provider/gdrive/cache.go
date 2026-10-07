package gdrive

import (
	"container/list"
	"sync"
)

type cacheEntry struct {
	key    string
	fileID string
}

// KeyCache is a concurrency-safe bounded LRU cache mapping BlobKit string keys
// to Google Drive file IDs.
type KeyCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]*list.Element
	evict    *list.List
}

// NewKeyCache initializes a KeyCache with the given maximum capacity.
// If capacity <= 0, caching is disabled.
func NewKeyCache(capacity int) *KeyCache {
	if capacity <= 0 {
		return &KeyCache{capacity: 0}
	}
	return &KeyCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		evict:    list.New(),
	}
}

// Get retrieves the fileID for a given key, promoting it to the front of the LRU queue.
func (c *KeyCache) Get(key string) (string, bool) {
	if c.capacity <= 0 {
		return "", false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	elem, found := c.items[key]
	if !found {
		return "", false
	}

	c.evict.MoveToFront(elem)
	entry := elem.Value.(*cacheEntry)
	return entry.fileID, true
}

// Set inserts or updates the fileID for a given key, evicting the least recently used
// entry if capacity is reached.
func (c *KeyCache) Set(key, fileID string) {
	if c.capacity <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.items[key]; found {
		c.evict.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.fileID = fileID
		return
	}

	if c.evict.Len() >= c.capacity {
		oldest := c.evict.Back()
		if oldest != nil {
			c.evict.Remove(oldest)
			oldEntry := oldest.Value.(*cacheEntry)
			delete(c.items, oldEntry.key)
		}
	}

	entry := &cacheEntry{key: key, fileID: fileID}
	elem := c.evict.PushFront(entry)
	c.items[key] = elem
}

// Delete removes a key mapping from the cache.
func (c *KeyCache) Delete(key string) {
	if c.capacity <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, found := c.items[key]; found {
		c.evict.Remove(elem)
		delete(c.items, key)
	}
}

// Clear removes all cached mappings.
func (c *KeyCache) Clear() {
	if c.capacity <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element, c.capacity)
	c.evict.Init()
}

// Len returns the current number of cached mappings.
func (c *KeyCache) Len() int {
	if c.capacity <= 0 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}
