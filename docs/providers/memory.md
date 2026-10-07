# In-Memory Storage Driver & Test Harness

The `provider/memory` package implements an in-memory, zero-dependency storage driver for BlobKit. It is designed for **fast unit testing**, **offline CI/CD pipelines**, and **local service development** without requiring Docker, cloud emulators, or network connectivity.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Zero Cloud Dependencies** | Yes | Runs entirely in RAM; requires no AWS, Azure, GCP, or SSH credentials. |
| **Thread-Safe Concurrency** | Yes | Protected by `sync.RWMutex` for reliable multi-threaded test execution under `go test -race`. |
| **Full Driver Interface Parity**| Yes | Implements all driver operations: Put, Get, Range Reads, Head, Delete, Batch Delete, Copy, List, Presign, and Multipart. |
| **Resumable Multipart Simulation** | Yes | Fully simulates multi-part uploads with part-by-part chunk assembly and ETags. |
| **Standardized Conformance** | Yes | Fully verified by the `testutil.RunDriverContractTests` conformance test harness. |
| **Sub-Millisecond Execution** | Yes | Operations complete in microseconds, enabling thousands of unit tests per second. |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (default "memory").
    Name string

    // Bucket is the simulated bucket name (default "test-bucket").
    Bucket string

    // PublicBaseURL specifies an optional mock CDN URL for testing URL resolution.
    PublicBaseURL string
}
```

---

## Unit Testing Recipes with `testutil`

BlobKit includes a high-level test harness in `testutil` that pairs the memory driver with an in-memory metadata registry for testing consumer services.

### 1. Simple Unit Test with `MockStorage`

```go
package myapp_test

import (
    "context"
    "testing"

    "github.com/suhwr/blobkit/testutil"
)

func TestUserService_UploadAvatar(t *testing.T) {
    // Setup isolated in-memory storage instance
    mock, err := testutil.NewMockStorage()
    if err != nil {
        t.Fatalf("failed to initialize mock storage: %v", err)
    }
    defer mock.Close()

    ctx := context.Background()

    // Seed test fixture
    obj, err := mock.SeedObject(ctx, "avatars", "user_1.png", "fake image bytes")
    if err != nil {
        t.Fatalf("SeedObject failed: %v", err)
    }

    // Assert stored payload matches expectation
    testutil.AssertBlobContent(t, mock.Client, obj.ID, "fake image bytes")
}
```

### 2. Standalone Driver Conformance Testing

If you develop custom middleware or wrappers around `blobkit.Driver`, verify them using BlobKit's official contract suite:

```go
func TestCustomDriver_Conformance(t *testing.T) {
    testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
        d := memory.NewDriver(memory.Config{
            Name:   "custom-test",
            Bucket: "test-bucket",
        })
        return d, func() {
            _ = d.Close()
        }
    })
}
```

---

## Contract Conformance & Error Behavior

The memory driver behaves identically to production cloud storage drivers:
- **404 Not Found**: `Head` or `Get` on non-existent keys returns `blobkit.ErrObjectNotFound`.
- **Precondition Failed**: Requesting invalid byte ranges returns `blobkit.ErrPreconditionFailed`.
- **Path Traversal Security**: Malicious keys containing `..`, null bytes, or leading `/` return `blobkit.ErrSecurityViolation`.
- **Context Cancellation**: Cancelled contexts return `context.Canceled` immediately.
