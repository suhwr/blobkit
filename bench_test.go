package blobkit_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/cache"
	"github.com/suhwr/blobkit/key"
	"github.com/suhwr/blobkit/mime"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/registry"
	"github.com/suhwr/blobkit/router"
)

func BenchmarkMIMESniff(b *testing.B) {
	pngHeader := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}
	payload := append(pngHeader, bytes.Repeat([]byte{0x00}, 1024)...)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(payload)
		r, _, err := mime.Sniff(reader, "photo.png", "")
		if err != nil {
			b.Fatal(err)
		}
		// Read a few bytes to simulate stream consumption
		var tmp [64]byte
		_, _ = r.Read(tmp[:])
	}
}

func BenchmarkKeyGeneration_UUIDv7(b *testing.B) {
	ctx := context.Background()
	gen := key.NewUUIDv7Generator()
	in := key.KeyInput{
		ID:        "01925b3a-7f28-7102-8f92-9428ad0e451b",
		Namespace: "avatars",
		Filename:  "user_avatar.jpg",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := gen.Generate(ctx, in)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyGeneration_DatePrefix(b *testing.B) {
	ctx := context.Background()
	gen := key.NewDatePrefixGenerator()
	in := key.KeyInput{
		ID:        "01925b3a-7f28-7102-8f92-9428ad0e451b",
		Namespace: "media/daily",
		Filename:  "recording.mp4",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := gen.Generate(ctx, in)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyGeneration_HashSharded(b *testing.B) {
	ctx := context.Background()
	gen := key.NewHashShardedGenerator(2)
	in := key.KeyInput{
		ID:        "01925b3a-7f28-7102-8f92-9428ad0e451b",
		Namespace: "store/items",
		Filename:  "item.webp",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := gen.Generate(ctx, in)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_Put_Standalone(b *testing.B) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{Bucket: "bench"})
	client, err := blobkit.New(blobkit.WithDriver(memDriver))
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	payload := []byte("Benchmark payload 1KB data block lorem ipsum dolor sit amet")
	opts := blobkit.PutOptions{
		Namespace: "bench",
		Filename:  "bench.txt",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Put(ctx, bytes.NewReader(payload), opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_Put_WithRegistry(b *testing.B) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{Bucket: "bench"})
	reg := registry.NewMemoryStore()
	client, err := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	payload := []byte("Benchmark payload 1KB data block lorem ipsum dolor sit amet")
	opts := blobkit.PutOptions{
		Namespace: "bench",
		OwnerID:   "usr_bench",
		Filename:  "bench.txt",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Put(ctx, bytes.NewReader(payload), opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClient_Get_ByObjectID(b *testing.B) {
	ctx := context.Background()
	memDriver := memory.NewDriver(memory.Config{Bucket: "bench"})
	reg := registry.NewMemoryStore()
	client, err := blobkit.New(
		blobkit.WithDriver(memDriver),
		blobkit.WithRegistry(reg),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	payload := []byte("Benchmark payload 1KB data block lorem ipsum dolor sit amet")
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "bench",
		Filename:  "bench.txt",
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := client.Get(ctx, obj.ID, blobkit.GetOptions{})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, reader)
		_ = reader.Close()
	}
}

func BenchmarkLRUCache_Hit(b *testing.B) {
	c := cache.NewLRUCache(1000)
	obj := &blobkit.Object{ID: "bench-obj", Key: "bench-key", Size: 1024}
	c.Set("bench-obj", obj, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, ok := c.Get("bench-obj")
		if !ok {
			b.Fatal("cache miss")
		}
	}
}

func BenchmarkCircuitBreaker_Select(b *testing.B) {
	ctx := context.Background()
	primary := memory.NewDriver(memory.Config{Name: "bench-primary"})
	fallback := memory.NewDriver(memory.Config{Name: "bench-fallback"})
	cb := router.NewCircuitBreakerRouter(router.CircuitBreakerConfig{
		Primary:  primary,
		Fallback: fallback,
	})
	rc := router.RouteContext{Op: router.OpPut}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cb.Select(ctx, rc)
		if err != nil {
			b.Fatal(err)
		}
	}
}
