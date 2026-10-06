package cache

import (
	"container/list"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

type cacheItem struct {
	key        string
	obj        *blobkit.Object
	expiresAt  time.Time
	isNegative bool
}

// LRUCache is a bounded, concurrency-safe in-memory cache supporting TTL
// and negative-caching to shield storage drivers.
type LRUCache struct {
	mu        sync.RWMutex
	capacity  int
	items     map[string]*list.Element
	evictList *list.List
}

// NewLRUCache creates an LRU cache with the specified maximum item capacity.
func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		capacity = 1000
	}
	return &LRUCache{
		capacity:  capacity,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

// Get retrieves cached object metadata if present and unexpired.
func (c *LRUCache) Get(key string) (*blobkit.Object, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]
	if !exists {
		return nil, false
	}

	item := elem.Value.(*cacheItem)
	if !item.expiresAt.IsZero() && time.Now().UTC().After(item.expiresAt) {
		c.removeElement(elem)
		return nil, false
	}

	if item.isNegative {
		return nil, false
	}

	c.evictList.MoveToFront(elem)
	cp := *item.obj
	return &cp, true
}

// Set caches an object with a specified TTL.
func (c *LRUCache) Set(key string, obj *blobkit.Object, ttl time.Duration) {
	if obj == nil || key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().UTC().Add(ttl)
	}

	// Update existing element
	if elem, exists := c.items[key]; exists {
		c.evictList.MoveToFront(elem)
		item := elem.Value.(*cacheItem)
		item.obj = obj
		item.expiresAt = exp
		item.isNegative = false
		return
	}

	// Evict oldest if capacity reached
	for c.evictList.Len() >= c.capacity {
		c.removeOldest()
	}

	item := &cacheItem{
		key:        key,
		obj:        obj,
		expiresAt:  exp,
		isNegative: false,
	}
	elem := c.evictList.PushFront(item)
	c.items[key] = elem
}

// SetNegative records a negative lookup (object does not exist) with TTL.
func (c *LRUCache) SetNegative(key string, ttl time.Duration) {
	if key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().UTC().Add(ttl)
	}

	if elem, exists := c.items[key]; exists {
		c.evictList.MoveToFront(elem)
		item := elem.Value.(*cacheItem)
		item.obj = nil
		item.expiresAt = exp
		item.isNegative = true
		return
	}

	for c.evictList.Len() >= c.capacity {
		c.removeOldest()
	}

	item := &cacheItem{
		key:        key,
		obj:        nil,
		expiresAt:  exp,
		isNegative: true,
	}
	elem := c.evictList.PushFront(item)
	c.items[key] = elem
}

// IsNegative checks whether a negative cache entry is active for the key.
func (c *LRUCache) IsNegative(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]
	if !exists {
		return false
	}

	item := elem.Value.(*cacheItem)
	if !item.expiresAt.IsZero() && time.Now().UTC().After(item.expiresAt) {
		c.removeElement(elem)
		return false
	}

	if item.isNegative {
		c.evictList.MoveToFront(elem)
		return true
	}

	return false
}

// Delete invalidates any cached entry for the given key.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[key]; exists {
		c.removeElement(elem)
	}
}

// Purge removes all entries from the cache.
func (c *LRUCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictList.Init()
}

// Len returns the current count of cached items.
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

func (c *LRUCache) removeOldest() {
	elem := c.evictList.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *LRUCache) removeElement(elem *list.Element) {
	c.evictList.Remove(elem)
	item := elem.Value.(*cacheItem)
	delete(c.items, item.key)
}
