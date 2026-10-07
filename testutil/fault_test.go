package testutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
)

func newTestDriver() blobkit.Driver {
	return memory.NewDriver(memory.Config{
		Bucket: "fault-bucket",
	})
}

func TestFaultDriver_InjectedErrors(t *testing.T) {
	ctx := context.Background()
	mock := newTestDriver()
	testErr := errors.New("injected disk failure")

	faulty := NewFaultDriver(mock, FaultConfig{
		PutErr: testErr,
	})

	// Put should fail with testErr
	_, err := faulty.Put(ctx, &blobkit.Object{Key: "test.txt"}, bytes.NewReader([]byte("data")), blobkit.PutOptions{})
	if !errors.Is(err, testErr) {
		t.Fatalf("expected injected error, got: %v", err)
	}
	if faulty.FaultsInjected() != 1 {
		t.Fatalf("expected 1 fault injected, got %d", faulty.FaultsInjected())
	}
	if faulty.TotalCalls() != 1 {
		t.Fatalf("expected 1 total call, got %d", faulty.TotalCalls())
	}

	// Dynamically switch config
	faulty.SetConfig(FaultConfig{
		HeadErr: blobkit.ErrProviderUnavailable,
	})
	_, err = faulty.Head(ctx, "test.txt")
	if !errors.Is(err, blobkit.ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable, got: %v", err)
	}
	if faulty.FaultsInjected() != 2 {
		t.Fatalf("expected 2 faults injected, got %d", faulty.FaultsInjected())
	}
}

func TestFaultDriver_FailAfterCalls(t *testing.T) {
	ctx := context.Background()
	mock := newTestDriver()
	failErr := errors.New("quota limit reached")

	faulty := NewFaultDriver(mock, FaultConfig{
		FailAfterCalls: 3,
		FailAfterErr:   failErr,
	})

	// Call 1 & 2 succeed
	_, err := faulty.Put(ctx, &blobkit.Object{Key: "1.txt"}, bytes.NewReader([]byte("1")), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("call 1 failed unexpectedly: %v", err)
	}
	_, err = faulty.Put(ctx, &blobkit.Object{Key: "2.txt"}, bytes.NewReader([]byte("2")), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("call 2 failed unexpectedly: %v", err)
	}

	// Call 3 should fail
	_, err = faulty.Put(ctx, &blobkit.Object{Key: "3.txt"}, bytes.NewReader([]byte("3")), blobkit.PutOptions{})
	if !errors.Is(err, failErr) {
		t.Fatalf("expected call 3 to fail with %v, got %v", failErr, err)
	}
}

func TestFaultDriver_PartialAndCorruptedRead(t *testing.T) {
	ctx := context.Background()
	mock := newTestDriver()
	payload := []byte("0123456789abcdefghij") // 20 bytes

	_, err := mock.Put(ctx, &blobkit.Object{Key: "data.bin"}, bytes.NewReader(payload), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("setup Put failed: %v", err)
	}

	// 1. Partial Read (truncate at 10 bytes)
	faultyPartial := NewFaultDriver(mock, FaultConfig{
		PartialReadBytes: 10,
	})
	reader, err := faultyPartial.Get(ctx, "data.bin", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(data) != 10 || !bytes.Equal(data, payload[:10]) {
		t.Fatalf("expected 10 bytes partial read, got %d bytes: %q", len(data), data)
	}

	// 2. Corrupted Read (bit flip at byte index 5)
	faultyCorrupt := NewFaultDriver(mock, FaultConfig{
		CorruptBytesAt: 5,
	})
	reader2, err := faultyCorrupt.Get(ctx, "data.bin", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	data2, err := io.ReadAll(reader2)
	reader2.Close()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(data2) != 20 {
		t.Fatalf("expected 20 bytes, got %d", len(data2))
	}
	if data2[5] == payload[5] {
		t.Fatalf("expected byte at index 5 to be corrupted, got original %x", data2[5])
	}
	// Verify bits flipped
	if data2[5] != (payload[5] ^ 0xFF) {
		t.Fatalf("expected byte to be inverted %x, got %x", payload[5]^0xFF, data2[5])
	}
	// Verify other bytes untouched
	if !bytes.Equal(data2[:5], payload[:5]) || !bytes.Equal(data2[6:], payload[6:]) {
		t.Fatalf("non-targeted bytes were unexpectedly modified")
	}

	// 3. Drop connection (ErrUnexpectedEOF)
	faultyDrop := NewFaultDriver(mock, FaultConfig{
		DropConnection: true,
	})
	reader3, err := faultyDrop.Get(ctx, "data.bin", blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	buf := make([]byte, 10)
	n, err := reader3.Read(buf)
	if err != nil {
		t.Fatalf("first chunk failed: %v", err)
	}
	if n != 10 {
		t.Fatalf("expected 10 bytes read, got %d", n)
	}
	_, err = reader3.Read(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF on dropped connection, got %v", err)
	}
	reader3.Close()
}

func TestFaultDriver_Latency(t *testing.T) {
	ctx := context.Background()
	mock := newTestDriver()
	faulty := NewFaultDriver(mock, FaultConfig{
		Latency: 30 * time.Millisecond,
	})

	start := time.Now()
	_, _ = faulty.Head(ctx, "nonexistent")
	elapsed := time.Since(start)

	if elapsed < 25*time.Millisecond {
		t.Fatalf("expected latency injection >= 25ms, got %v", elapsed)
	}
}
