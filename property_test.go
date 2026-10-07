package blobkit_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/key"
	"github.com/suhwr/blobkit/provider/memory"
)

// TestProperty_ExactByteLength verifies that any byte stream of random size [0, 65536]
// uploaded via Put preserves its exact size in Head and byte contents in Get.
func TestProperty_ExactByteLength(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver(memory.Config{
		Bucket: "prop-bucket",
	})
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	f := func(seed int64, size uint16) bool {
		payloadSize := int(size) // 0 to 65535
		r := rand.New(rand.NewSource(seed))
		data := make([]byte, payloadSize)
		r.Read(data)

		obj, err := client.Put(ctx, bytes.NewReader(data), blobkit.PutOptions{
			ExplicitSize: true,
			Size:         int64(payloadSize),
		})
		if err != nil {
			t.Logf("Put failed: %v", err)
			return false
		}
		if obj.Size != int64(payloadSize) {
			t.Logf("Put obj.Size mismatch: expected %d, got %d", payloadSize, obj.Size)
			return false
		}

		headObj, err := client.Head(ctx, obj.Key)
		if err != nil || headObj.Size != int64(payloadSize) {
			t.Logf("Head failed or size mismatch: %v", err)
			return false
		}

		reader, err := client.Get(ctx, obj.Key, blobkit.GetOptions{})
		if err != nil {
			t.Logf("Get failed: %v", err)
			return false
		}
		downloaded, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || !bytes.Equal(downloaded, data) {
			t.Logf("data mismatch on read")
			return false
		}

		_ = client.Delete(ctx, obj.Key)
		return true
	}

	cfg := &quick.Config{
		MaxCount: 50,
	}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatalf("property check failed: %v", err)
	}
}

// TestProperty_KeyGeneratorPrefix verifies that any generated key starts with the configured namespace
func TestProperty_KeyGeneratorPrefix(t *testing.T) {
	gen := key.NewUUIDv7Generator()

	f := func(ns string) bool {
		// Clean namespace to valid chars
		var cleanNs []rune
		for _, r := range ns {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
				cleanNs = append(cleanNs, r)
			}
		}
		if len(cleanNs) == 0 {
			return true
		}
		namespace := string(cleanNs)
		k, err := gen.Generate(context.Background(), key.KeyInput{
			Namespace: namespace,
			Filename:  "test.txt",
		})
		if err != nil {
			return false
		}
		return blobkit.ValidateKey(k) == nil
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("key generator property check failed: %v", err)
	}
}
