package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/cache"
)

func TestLRUCache_BasicAndTTL(t *testing.T) {
	c := cache.NewLRUCache(3)

	obj1 := &blobkit.Object{ID: "obj1", Key: "k1", Size: 100}
	c.Set("k1", obj1, 50*time.Millisecond)

	// Immediate hit
	cached, ok := c.Get("k1")
	if !ok || cached.ID != "obj1" {
		t.Fatalf("expected hit on k1, got %v (%+v)", ok, cached)
	}

	// Wait for TTL expiration
	time.Sleep(70 * time.Millisecond)
	_, ok = c.Get("k1")
	if ok {
		t.Fatal("expected k1 to be expired")
	}
}

func TestLRUCache_NegativeCaching(t *testing.T) {
	c := cache.NewLRUCache(5)

	c.SetNegative("missing.png", 50*time.Millisecond)
	if !c.IsNegative("missing.png") {
		t.Fatal("expected missing.png to be negative cached")
	}

	// Normal Get should return false
	_, ok := c.Get("missing.png")
	if ok {
		t.Fatal("expected Get on negative cached key to return false")
	}

	time.Sleep(60 * time.Millisecond)
	if c.IsNegative("missing.png") {
		t.Fatal("expected negative cache entry to expire")
	}
}

func TestLRUCache_CapacityEviction(t *testing.T) {
	c := cache.NewLRUCache(2)

	c.Set("k1", &blobkit.Object{ID: "1"}, time.Hour)
	c.Set("k2", &blobkit.Object{ID: "2"}, time.Hour)

	// Access k1 to make k2 the least recently used
	_, _ = c.Get("k1")

	// Insert k3 -> should evict k2
	c.Set("k3", &blobkit.Object{ID: "3"}, time.Hour)

	if _, ok := c.Get("k2"); ok {
		t.Fatal("expected k2 to be evicted")
	}
	if _, ok := c.Get("k1"); !ok {
		t.Fatal("expected k1 to still be cached")
	}
	if _, ok := c.Get("k3"); !ok {
		t.Fatal("expected k3 to be cached")
	}
}

func TestLRUCache_Concurrency(t *testing.T) {
	c := cache.NewLRUCache(50)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := string(rune('a' + (j % 26)))
				if j%2 == 0 {
					c.Set(key, &blobkit.Object{ID: key}, time.Minute)
				} else {
					_, _ = c.Get(key)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestLRUCache_DeepCloningMetadata(t *testing.T) {
	c := cache.NewLRUCache(10)

	meta := map[string]string{"foo": "bar", "env": "prod"}
	obj := &blobkit.Object{
		ID:       "id-1",
		Key:      "k1",
		Metadata: meta,
	}

	c.Set("k1", obj, time.Hour)

	// Mutate original meta outside cache
	meta["foo"] = "mutated"

	retrieved, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected object to be in cache")
	}
	if retrieved.Metadata["foo"] != "bar" {
		t.Fatalf("expected cached metadata foo to be 'bar', got %q", retrieved.Metadata["foo"])
	}

	// Mutate retrieved metadata
	retrieved.Metadata["foo"] = "retrieved-mutation"

	retrievedAgain, _ := c.Get("k1")
	if retrievedAgain.Metadata["foo"] != "bar" {
		t.Fatalf("expected cached metadata foo to remain 'bar', got %q", retrievedAgain.Metadata["foo"])
	}
}
