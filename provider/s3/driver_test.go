package s3_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/s3"
)

func TestConfigValidation(t *testing.T) {
	cfg := s3.Config{}
	err := cfg.Validate()
	if !errors.Is(err, blobkit.ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}

	cfg.Bucket = "test-bucket"
	err = cfg.Validate()
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if cfg.Region != "auto" {
		t.Fatalf("expected default region 'auto', got %s", cfg.Region)
	}
	if cfg.MultipartThreshold != s3.DefaultMultipartThreshold {
		t.Fatalf("expected default threshold %d, got %d", s3.DefaultMultipartThreshold, cfg.MultipartThreshold)
	}
	if cfg.GlobalMemoryLimiter == nil {
		t.Fatal("expected non-nil default GlobalMemoryLimiter")
	}
}

func TestMemoryLimiter(t *testing.T) {
	limiter := s3.NewMemoryLimiter(100) // 100 bytes limit
	ctx := context.Background()

	// 1. Successful Acquire
	err := limiter.Acquire(ctx, 60)
	if err != nil {
		t.Fatalf("unexpected acquire error: %v", err)
	}
	if limiter.Allocated() != 60 {
		t.Fatalf("expected 60 allocated, got %d", limiter.Allocated())
	}

	// 2. TryAcquire exceeding limit
	ok := limiter.TryAcquire(50)
	if ok {
		t.Fatal("expected TryAcquire to fail for 50 bytes (total 110 > 100)")
	}

	// 3. TryAcquire within limit
	ok = limiter.TryAcquire(40)
	if !ok {
		t.Fatal("expected TryAcquire to succeed for 40 bytes (total 100 == 100)")
	}
	if limiter.Allocated() != 100 {
		t.Fatalf("expected 100 allocated, got %d", limiter.Allocated())
	}

	// 4. Release
	limiter.Release(60)
	if limiter.Allocated() != 40 {
		t.Fatalf("expected 40 allocated, got %d", limiter.Allocated())
	}

	// 5. Exceeding maxBytes outright fails immediately
	err = limiter.Acquire(ctx, 150)
	if !errors.Is(err, blobkit.ErrMemoryBudgetExceeded) {
		t.Fatalf("expected ErrMemoryBudgetExceeded, got %v", err)
	}

	// 6. Concurrency test
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			subCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if err := limiter.Acquire(subCtx, 10); err == nil {
				time.Sleep(5 * time.Millisecond)
				limiter.Release(10)
			}
		}()
	}
	wg.Wait()
}

func TestURLResolution(t *testing.T) {
	// CDN configured
	driverCDN, err := s3.NewDriver(s3.Config{
		Bucket:        "assets",
		PublicBaseURL: "https://cdn.example.com",
	})
	if err != nil {
		t.Fatalf("unexpected driver init error: %v", err)
	}
	defer driverCDN.Close()

	u, err := driverCDN.ResolveURL("avatars/user.png")
	if err != nil {
		t.Fatalf("unexpected ResolveURL error: %v", err)
	}
	if u != "https://cdn.example.com/avatars/user.png" {
		t.Fatalf("unexpected CDN URL: %s", u)
	}

	// MinIO path-style configured
	driverMinIO, err := s3.NewDriver(s3.Config{
		Bucket:       "mybucket",
		Endpoint:     "http://localhost:9000",
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("unexpected driver init error: %v", err)
	}
	defer driverMinIO.Close()

	uMinIO, err := driverMinIO.ResolveURL("files/doc.pdf")
	if err != nil {
		t.Fatalf("unexpected ResolveURL error: %v", err)
	}
	if uMinIO != "http://localhost:9000/mybucket/files/doc.pdf" {
		t.Fatalf("unexpected path-style URL: %s", uMinIO)
	}
}

type mockAPIError struct {
	code    string
	message string
}

func (m mockAPIError) Error() string { return m.message }
func (m mockAPIError) ErrorCode() string { return m.code }
func (m mockAPIError) ErrorMessage() string { return m.message }
func (m mockAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func TestErrorScrubbing(t *testing.T) {
	// Verify raw proxy connect error is scrubbed
	rawMsg := "operation error S3: PutObject: proxyconnect tcp: dial tcp 127.0.0.1:37355: connect: connection refused"
	sanitized := blobkit.SanitizeErrorMessage(rawMsg)
	if sanitized == rawMsg {
		t.Fatal("expected sanitized message to differ from raw sensitive message")
	}
	if contains(sanitized, "127.0.0.1:37355") {
		t.Fatalf("sanitized message still contains internal IP and port: %s", sanitized)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && searchSubstr(s, substr))
}

func searchSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
