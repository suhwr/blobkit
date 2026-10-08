package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
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

	// 6. Over-release clamp (must never go negative)
	limiter.Release(1000)
	if limiter.Allocated() != 0 {
		t.Fatalf("expected 0 allocated after over-release, got %d", limiter.Allocated())
	}

	// 7. Concurrent acquisitions and releases
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			subCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			if err := limiter.Acquire(subCtx, 10); err == nil {
				time.Sleep(2 * time.Millisecond)
				limiter.Release(10)
			}
		}()
	}
	wg.Wait()

	// 8. Cancellation while waiting
	_ = limiter.Acquire(ctx, 100) // allocate full 100 bytes
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled
	if err := limiter.Acquire(cancelCtx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// 9. Close wakes waiters and blocks new acquisitions
	limiter.Close()
	if ok := limiter.TryAcquire(1); ok {
		t.Fatal("expected TryAcquire to fail on closed limiter")
	}
	if err := limiter.Acquire(ctx, 1); !errors.Is(err, blobkit.ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable on closed limiter, got %v", err)
	}
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

func (m mockAPIError) Error() string                 { return m.message }
func (m mockAPIError) ErrorCode() string             { return m.code }
func (m mockAPIError) ErrorMessage() string          { return m.message }
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

func TestS3_ListParts_Pagination(t *testing.T) {
	bucket := "pagination-bucket"
	mockServer := newMockS3Server(bucket)
	mockServer.maxParts = 4 // force 4 parts per page
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	driver, err := s3.NewDriver(s3.Config{
		Bucket:    bucket,
		Endpoint:  ts.URL,
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	ctx := context.Background()
	key := "large-multipart.bin"
	uploadID, err := driver.CreateMultipart(ctx, &blobkit.Object{Key: key}, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}

	// Upload 10 parts (will span 3 pages: 4 + 4 + 2)
	const totalParts = 10
	for i := int32(1); i <= totalParts; i++ {
		data := []byte(fmt.Sprintf("part-%02d-content", i))
		_, err := driver.UploadPart(ctx, key, uploadID, i, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", i, err)
		}
	}

	parts, err := driver.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}

	if len(parts) != totalParts {
		t.Fatalf("expected %d parts after paginated listing, got %d", totalParts, len(parts))
	}

	for i, p := range parts {
		expectedNum := int32(i + 1)
		if p.PartNumber != expectedNum {
			t.Errorf("part index %d: expected PartNumber %d, got %d", i, expectedNum, p.PartNumber)
		}
	}
}

func TestS3_UploadMultipart_SizeMismatch(t *testing.T) {
	bucket := "mismatch-bucket"
	mockServer := newMockS3Server(bucket)
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	driver, err := s3.NewDriver(s3.Config{
		Bucket:              bucket,
		Endpoint:            ts.URL,
		UsePathStyle:        true,
		MultipartThreshold: 1024,
		MultipartPartSize:  1024,
	})
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	ctx := context.Background()
	key := "mismatch.bin"
	actualData := bytes.Repeat([]byte("X"), 3000)

	// Declare explicit size of 2000, but stream yields 3000 bytes
	_, err = driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(actualData), blobkit.PutOptions{
		Size:         2000,
		ExplicitSize: true,
	})
	if !errors.Is(err, blobkit.ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
}

func TestS3_CompleteMultipart_HeadFallbackSize(t *testing.T) {
	bucket := "head-fallback-bucket"
	mockServer := newMockS3Server(bucket)
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	driver, err := s3.NewDriver(s3.Config{
		Bucket:       bucket,
		Endpoint:     ts.URL,
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewDriver failed: %v", err)
	}
	defer driver.Close()

	ctx := context.Background()
	key := "multipart_head_fallback.bin"
	uploadID, err := driver.CreateMultipart(ctx, &blobkit.Object{Key: key}, blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("CreateMultipart failed: %v", err)
	}

	partData := []byte("hello world multipart s3 fallback")
	etag, err := driver.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(partData), int64(len(partData)))
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Caller provides standard CompletedPart without Size (Size: 0)
	parts := []blobkit.CompletedPart{
		{PartNumber: 1, ETag: etag, Size: 0},
	}

	completedObj, err := driver.CompleteMultipart(ctx, &blobkit.Object{Key: key}, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipart failed: %v", err)
	}

	if completedObj.Size != int64(len(partData)) {
		t.Fatalf("expected completedObj.Size %d via Head fallback, got %d", len(partData), completedObj.Size)
	}
}

