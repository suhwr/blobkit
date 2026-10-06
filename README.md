# BlobKit

[![Go Reference](https://pkg.go.dev/badge/github.com/suhwr/blobkit.svg)](https://pkg.go.dev/github.com/suhwr/blobkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/suhwr/blobkit)](https://goreportcard.com/report/github.com/suhwr/blobkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

BlobKit is a high-performance, resilient object storage infrastructure library for Go. Built on top of AWS SDK v2, it provides a clean, provider-agnostic infrastructure layer for modern applications interfacing with S3-compatible backends—including **Cloudflare R2**, **AWS S3**, **MinIO**, **Wasabi**, and **Backblaze B2**.

BlobKit is deliberately engineered as an **infrastructure orchestration layer**, not a superficial SDK wrapper. It decouples application business intent from raw storage wire details, manages canonical object identity, enforces strict streaming memory budgets, routes across multiple cloud providers with circuit breaking and load balancing, safeguards against security vulnerabilities, and coordinates comprehensive object lifecycles.

---

## Architectural Principles & Strict Separation of Concerns

BlobKit enforces a strict three-tier architecture:

```
+-----------------------------------------------------------------------------------+
| 1. APPLICATION LAYER (e.g., bot engines, web backends, microservices)             |
|    - Declares semantic intent: Namespace, OwnerID, Original Filename, Visibility |
|    - Passes streaming data (io.Reader)                                            |
|    - Discovers objects by metadata attributes without knowing raw bucket keys     |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 2. BLOBKIT INFRASTRUCTURE LAYER (Orchestration Engine)                            |
|    - Logical ObjectID generation (UUIDv7) & structured key generation             |
|    - Zero-rewind 512B MIME sniffing & streaming SHA-256 integrity verification     |
|    - Security Policy Engine (MIME/extension whitelist, path & CRLF sanitization)  |
|    - Resumable multipart upload session lifecycle & state tracking                |
|    - Retention policies, legal hold enforcement & decoupled background sweeper    |
|    - Advanced Routing (Weighted traffic, 3-state Circuit Breaker, Capability bits) |
|    - Bounded LRU caching with negative caching & vendor-neutral telemetry         |
|    - Database Metadata Registry coordination (Database source of truth)           |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 3. PROVIDER DRIVER LAYER (Storage Driver Abstraction)                             |
|    - AWS SDK v2 Driver (R2, S3, MinIO) with persistent pooled HTTP/2 transport   |
|    - Pure In-Memory Driver for offline unit tests and ephemeral workloads         |
|    - Error scrubbing (sanitizes internal proxy dials and secret tokens)           |
|    - Native server-side copy & driver multipart chunking                          |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 4. PHYSICAL STORAGE BACKENDS                                                      |
|    Cloudflare R2  /  AWS S3  /  MinIO  /  Wasabi  /  Backblaze B2                 |
+-----------------------------------------------------------------------------------+
```

---

## Identity & Delivery Separation

BlobKit strictly decouples four distinct concepts:

| Concept | Representation | Example | Canonical DB Field? |
| :--- | :--- | :--- | :--- |
| **Logical Identity** | `ObjectID` | `01925b3a-7f28-7102-8f92-9428ad0e451b` (UUIDv7) | **Yes** (Primary Key) |
| **Physical Storage Key** | `ObjectKey` | `bots/autorespon/2026/10/06/01925b3a...png` | **Yes** (Storage Index) |
| **Client Presentation** | `Original Filename` | `voice_note.ogg` | **Yes** (Header/Audit) |
| **Delivery URL** | `Access URL` | `https://cdn.example.com/...` or signed presigned URL | **NO** (Generated on-demand) |

> [!IMPORTANT]
> **URLs are delivery details, never canonical identifiers.** Storing full URLs in databases causes schema rot when domains migrate, buckets change, or certificates rotate. In BlobKit, URLs are resolved dynamically via `ResolveURL` or `PresignGet`.

---

## Core Features Across 9 Categories

### 1. Lifecycle Management & Background Automation
- **Retention Policies & Legal Hold**: Protect objects against premature deletion (`RetentionUntil`, `LegalHold`). Attempting to delete a locked object returns `ErrObjectLocked`.
- **Expiration**: Ephemeral blobs auto-expire based on `ExpiresAt`.
- **Soft Delete & Restore**: Mark records as deleted (`SoftDelete`) while keeping physical bytes, or recover them (`Restore`). Soft-deleted objects are shielded from regular `Get`/`Head` queries.
- **Permanent Purge**: Cleanly wipe physical blobs from storage drivers and delete registry records via `PermanentDelete`.
- **Decoupled Background Sweeper**: `lifecycle.NewSweeper` periodically reclaims expired objects, purges soft-deleted objects older than `SoftDeleteTTL`, and aborts abandoned multipart uploads.

### 2. High-Level Object Operations
- **Server-Side & Cross-Driver Copy**: Fast server-side copy when source and destination share the same provider supporting `CapCopy`; seamless streaming fallback across different drivers.
- **Move**: Atomic copy-and-delete pipeline.
- **Rename**: Instant logical filename updates in metadata registry without touching physical storage keys.
- **Batch Metadata Updates**: Multi-key metadata updates (`BatchUpdateMetadata`).
- **Bounded Concurrency DeleteBatch**: Efficient bulk deletion with chunked provider calls.
- **Existence Checks & Head**: `Exists` and `Head` inspect metadata without transferring body payloads.

### 3. Integrity & Verification
- **Streaming SHA-256 Calculation**: Computes SHA-256 digests on-the-fly during upload via `io.TeeReader` with zero file buffering in RAM.
- **Client Checksum Verification**: If caller provides an expected hash (`ClientChecksum`), BlobKit aborts and purges the blob if the computed hash mismatches (`ErrChecksumMismatch`).
- **ETag & Size Verification**: Strict bounds checking prevents corrupted or truncated uploads.

### 4. Resumable Multipart Uploads
- **Upload Sessions**: Long-running uploads track lifecycle states (`SessionActive`, `SessionCommitted`, `SessionAborted`).
- **Reliable Part Chunking**: Upload individual parts (`UploadPart`), list completed chunks (`ListSessionParts`), and commit into a finalized object (`CommitResumableUpload`).
- **Stale Session Abort**: Automatically aborts abandoned sessions to prevent runaway cloud storage costs.

### 5. Vendor-Neutral Observability
- **Pluggable Observer Interface**: Implement `blobkit.Observer` to export metrics to Prometheus, OpenTelemetry, Datadog, or Zap.
- **MetricsCollector**: Thread-safe in-memory metrics collector tracking operation latencies, error counts, byte volumes, and limiter contention.
- **Zero Credential Leaks**: Sensitive proxy endpoints, tokens, and query parameters are scrubbed before reaching telemetry spans or logs.

### 6. Modular Caching Layer
- **Bounded LRU Cache**: Memory-efficient `cache.NewLRUCache(capacity)` stores hot object metadata.
- **Negative Caching**: Cache object-not-found misses (`SetNegative`) with short TTLs to shield downstream storage backends from stampedes.
- **Race-Safe Invalidation**: Mutex-guarded cache invalidation triggers automatically on `Put`, `Delete`, `Copy`, `Move`, and `PermanentDelete`.

### 7. Advanced Multi-Provider Routing
- **Fixed Router**: Directs traffic to a single primary backend.
- **Namespace Router**: Routes by domain or category (e.g. `avatars` to R2, `backups` to S3).
- **Failover Router**: Automatic active-passive failover with consecutive failure thresholds and cooldown canary probes.
- **Weighted Router**: Proportional load balancing across multiple providers based on integer weights.
- **3-State Circuit Breaker**: Wraps drivers in `Closed`, `Open`, and `HalfOpen` states with failure thresholds and canary probes.
- **Capability-Based Router**: Filters candidate drivers by required capability bitmasks (`CapDirectPut`, `CapMultipartSession`, `CapCopy`, `CapByteRangeGet`).

### 8. Security Policy & Sanitization
- **Policy Engine**: Configurable MIME type and extension whitelists/blacklists and file size ceilings.
- **Content/MIME Mismatch Defense**: Detects executable files or discrepancies between claimed file extensions and byte magic signatures (`ErrMIMEMismatch`).
- **Filename Sanitization**: Automatically scrubs path traversal sequences (`../`), null bytes (`\x00`), and control characters.
- **Header Injection Defense**: Strips CRLF (`\r\n`) injection attempts from `Content-Disposition` and `Cache-Control` headers.

### 9. Developer Experience
- **Typed Sentinel Errors**: Idiomatic error predicates: `IsNotFound`, `IsSecurityViolation`, `IsObjectLocked`, `IsChecksumMismatch`, `IsSizeMismatch`.
- **Test Fixtures**: `testutil.NewMockStorage()` provides an ephemeral in-memory BlobKit environment for unit testing without cloud credentials.

---

## Installation

```bash
go get github.com/suhwr/blobkit
```

---

## Quick Start Examples

### 1. Direct Upload with Security Policy & Observability

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "log"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/observer"
    "github.com/suhwr/blobkit/policy"
    "github.com/suhwr/blobkit/provider/s3"
)

func main() {
    ctx := context.Background()

    // 1. Configure Cloudflare R2 / S3 driver
    driver, err := s3.NewDriver(s3.Config{
        Name:            "r2-primary",
        Endpoint:        "https://<account_id>.r2.cloudflarestorage.com",
        Bucket:          "my-app-storage",
        AccessKeyID:     "R2_ACCESS_KEY_ID",
        SecretAccessKey: "R2_SECRET_ACCESS_KEY",
        PublicBaseURL:   "https://cdn.example.com",
    })
    if err != nil {
        log.Fatal(err)
    }

    // 2. Define upload security policy
    uploadPolicy := policy.NewBuilder().
        AllowExtensions(".jpg", ".jpeg", ".png", ".webp").
        AllowMIME("image/jpeg", "image/png", "image/webp").
        MaxSize(10 * 1024 * 1024). // 10MB
        DisallowMIMEMismatch(true).
        Build()

    // 3. Attach telemetry collector
    collector := observer.NewMetricsCollector()

    // 4. Initialize BlobKit client
    client, err := blobkit.New(
        blobkit.WithDriver(driver),
        blobkit.WithPolicy(uploadPolicy),
        blobkit.WithObserver(collector),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // 5. Upload stream
    data := bytes.NewReader([]byte("fake image data"))
    obj, err := client.Put(ctx, data, blobkit.PutOptions{
        Namespace: "avatars",
        OwnerID:   "user_123",
        Filename:  "profile.png",
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Created ObjectID: %s\n", obj.ID)
    fmt.Printf("Storage Key:      %s\n", obj.Key)
    fmt.Printf("SHA-256 Digest:   %s\n", obj.ChecksumSHA256)
}
```

### 2. Resumable Multipart Upload

```go
// 1. Initiate session
session, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
    Namespace: "recordings",
    Filename:  "video.mp4",
}, 5*1024*1024, 24*time.Hour)

// 2. Upload chunks concurrently or sequentially
part1, err := client.UploadPart(ctx, session.ID, 1, chunk1Reader, chunk1Size)
part2, err := client.UploadPart(ctx, session.ID, 2, chunk2Reader, chunk2Size)

// 3. Finalize into committed object
finalObj, err := client.CommitResumableUpload(ctx, session.ID)
fmt.Printf("Uploaded size: %d bytes\n", finalObj.Size)
```

### 3. Automated Lifecycle Sweeper

```go
import "github.com/suhwr/blobkit/lifecycle"

// Initialize background sweeper
sweeper, err := lifecycle.NewSweeper(client, lifecycle.SweeperConfig{
    Interval:          1 * time.Hour,
    SoftDeleteTTL:     30 * 24 * time.Hour, // purge after 30 days
    MultipartStaleTTL: 24 * time.Hour,      // abort abandoned sessions after 24 hours
    BatchSize:         100,
})
if err != nil {
    log.Fatal(err)
}

// Start in background goroutine
_ = sweeper.Start(ctx)
defer sweeper.Stop()
```

### 4. Advanced Circuit Breaker & Weighted Routing

```go
import "github.com/suhwr/blobkit/router"

// 3-State Circuit Breaker router
cbRouter := router.NewCircuitBreakerRouter(router.CircuitBreakerConfig{
    Primary:          r2PrimaryDriver,
    Fallback:         s3BackupDriver,
    FailureThreshold: 3,
    SuccessThreshold: 2,
    Cooldown:         30 * time.Second,
})

client, _ := blobkit.New(blobkit.WithRouter(cbRouter))
```

---

## Benchmarks

Measured on AMD EPYC 7C13 64-Core Processor (Linux x86_64, Go 1.24):

| Benchmark | Operations | Latency (ns/op) | Memory (B/op) | Allocs/op |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkCircuitBreaker_Select` | 75,700,000+ | **14.68 ns/op** | **0 B/op** | **0 allocs/op** |
| `BenchmarkLRUCache_Hit` | 3,030,000+ | **402.7 ns/op** | **320 B/op** | **1 allocs/op** |
| `BenchmarkKeyGeneration_UUIDv7` | 2,420,000+ | **530.6 ns/op** | **208 B/op** | **4 allocs/op** |
| `BenchmarkMIMESniff` | 1,340,000+ | **894.8 ns/op** | **624 B/op** | **3 allocs/op** |
| `BenchmarkKeyGeneration_DatePrefix` | 1,430,000+ | **855.1 ns/op** | **256 B/op** | **5 allocs/op** |
| `BenchmarkKeyGeneration_HashSharded`| 1,480,000+ | **874.9 ns/op** | **416 B/op** | **7 allocs/op** |
| `BenchmarkClient_Get_ByObjectID` | 1,000,000+ | **1,457 ns/op** | **707 B/op** | **4 allocs/op** |
| `BenchmarkClient_Put_Standalone` | 163,000+ | **7,347 ns/op** | **3,019 B/op** | **27 allocs/op** |
| `BenchmarkClient_Put_WithRegistry` | 122,000+ | **10,795 ns/op** | **4,290 B/op** | **30 allocs/op** |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
