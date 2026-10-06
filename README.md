# BlobKit

[![Go Reference](https://pkg.go.dev/badge/github.com/suhwr/blobkit.svg)](https://pkg.go.dev/github.com/suhwr/blobkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/suhwr/blobkit)](https://goreportcard.com/report/github.com/suhwr/blobkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.24-00ADD8.svg)](https://golang.org/)

**BlobKit** is an enterprise-grade, high-performance object storage infrastructure library for Go. Engineered above AWS SDK v2, it delivers a provider-agnostic, resilient infrastructure layer for modern applications interfacing with any S3-compatible backend—including **Cloudflare R2**, **AWS S3**, **MinIO**, **Google Cloud Storage (S3 API)**, **Wasabi**, and **Backblaze B2**.

BlobKit is intentionally designed as an **infrastructure orchestration layer**, not a trivial SDK wrapper. It strictly separates application intent from physical wire mechanics, provides canonical object identity, enforces bounded streaming memory budgets, delivers intelligent multi-cloud routing with 3-state circuit breaking, prevents security vulnerabilities, and automates end-to-end lifecycle management.

---

## Table of Contents

- [Architectural Principles](#architectural-principles)
- [Identity & Delivery Separation](#identity--delivery-separation)
- [Feature Matrix](#feature-matrix)
- [Installation](#installation)
- [Comprehensive Guide & Code Examples](#comprehensive-guide--code-examples)
  - [1. Multi-Cloud Provider Configurations](#1-multi-cloud-provider-configurations)
    - [Cloudflare R2 (Zero Egress CDN)](#a-cloudflare-r2-zero-egress-cdn)
    - [AWS S3 (Standard / Multi-Region)](#b-aws-s3-standard--multi-region)
    - [MinIO & Self-Hosted S3](#c-minio--self-hosted-s3)
    - [In-Memory Driver (Offline Testing & CI/CD)](#d-in-memory-driver-offline-testing--cicd)
  - [2. Advanced Multi-Provider Routing](#2-advanced-multi-provider-routing)
    - [Namespace Tiered Routing](#a-namespace-tiered-routing)
    - [Active-Passive Failover](#b-active-passive-failover)
    - [3-State Circuit Breaker Router](#c-3-state-circuit-breaker-router)
    - [Weighted Traffic Splitting](#d-weighted-traffic-splitting)
    - [Capability-Aware Dispatch](#e-capability-aware-dispatch)
  - [3. Object Key Partitioning Strategies](#3-object-key-partitioning-strategies)
  - [4. Security Policy & Sanitization Engine](#4-security-policy--sanitization-engine)
  - [5. Streaming Uploads & Integrity Verification](#5-streaming-uploads--integrity-verification)
  - [6. Streaming Downloads & Byte-Range Seeking](#6-streaming-downloads--byte-range-seeking)
  - [7. High-Level Object Operations](#7-high-level-object-operations)
    - [Server-Side & Cross-Driver Copy](#a-server-side--cross-driver-copy)
    - [Atomic Move](#b-atomic-move)
    - [Zero-Byte Logical Rename](#c-zero-byte-logical-rename)
    - [Existence & Head Metadata Inspection](#d-existence--head-metadata-inspection)
    - [Concurrency-Bounded Batch Deletes](#e-concurrency-bounded-batch-deletes)
  - [8. WORM Retention, Legal Hold & Lifecycle](#8-worm-retention-legal-hold--lifecycle)
  - [9. Resumable Multipart Upload Pipeline](#9-resumable-multipart-upload-pipeline)
  - [10. URL Resolution & Presigned Access](#10-url-resolution--presigned-access)
  - [11. Bounded LRU Caching & Stampede Defense](#11-bounded-lru-caching--stampede-defense)
  - [12. Decoupled Background Sweeper](#12-decoupled-background-sweeper)
  - [13. Vendor-Neutral Telemetry & Observability](#13-vendor-neutral-telemetry--observability)
  - [14. Consumer Unit Testing with Test Fixtures](#14-consumer-unit-testing-with-test-fixtures)
- [Performance Benchmarks](#performance-benchmarks)
- [License](#license)

---

## Architectural Principles

BlobKit enforces a strict 3-tier boundary:

```
+-----------------------------------------------------------------------------------+
| 1. APPLICATION LAYER (Microservices, Web Backends, Workers)                      |
|    - Declares semantic intent: Namespace, OwnerID, Original Filename, Visibility |
|    - Streams raw data (io.Reader) with predictable memory usage                   |
|    - Queries objects by metadata attributes without knowing raw bucket keys       |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 2. BLOBKIT INFRASTRUCTURE LAYER (Orchestration Engine)                            |
|    - Canonical ObjectID generation (UUIDv7) & structured partition keys           |
|    - Zero-rewind 512B MIME sniffing & streaming SHA-256 integrity verification     |
|    - Security Policy Engine (MIME/extension whitelist, path & CRLF sanitization)  |
|    - Resumable multipart upload session lifecycle & state tracking                |
|    - Retention policies, legal hold enforcement & decoupled background sweeper    |
|    - Advanced Routing (Weighted traffic, 3-state Circuit Breaker, Capabilities)   |
|    - Bounded LRU caching with negative caching & vendor-neutral telemetry         |
|    - Database Metadata Registry coordination (System of Record)                   |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 3. PROVIDER DRIVER LAYER (Wire Abstraction)                                       |
|    - AWS SDK v2 Driver (R2, S3, MinIO) with persistent pooled HTTP/2 transport   |
|    - Pure In-Memory Driver for offline unit tests and ephemeral workloads         |
|    - Error scrubbing (sanitizes internal proxy dials and secret tokens)           |
|    - Native server-side copy & driver multipart chunking                          |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 4. PHYSICAL STORAGE BACKENDS                                                      |
|    Cloudflare R2  /  AWS S3  /  MinIO  /  Wasabi  /  Backblaze B2  /  GCS (S3 API)|
+-----------------------------------------------------------------------------------+
```

### Application Layer Anti-Patterns Prevented by BlobKit

| Anti-Pattern | BlobKit Solution |
| :--- | :--- |
| Handcrafting raw S3 keys (`user/123/avatar.png`) | Handled by structured, collision-proof Key Generators |
| Hardcoding bucket names across application code | Centralized Driver configuration and Namespace Routers |
| Storing full CDN/S3 URLs in PostgreSQL | Dynamically resolved via `ResolveURL` or `PresignGet` |
| Buffering entire file payloads in RAM to compute SHA-256 | Streaming verification via `io.TeeReader` in $O(1)$ memory |
| Blindly trusting user-supplied `Content-Type` headers | Zero-rewind 512B magic signature sniffing |
| Exposing internal proxy IPs (`127.0.0.1`) in client errors | Automated regex scrubber stripping credentials and dials |

---

## Identity & Delivery Separation

BlobKit strictly decouples four distinct concepts:

| Concept | Representation | Example | Canonical DB Field? |
| :--- | :--- | :--- | :--- |
| **Logical Identity** | `ObjectID` | `01925b3a-7f28-7102-8f92-9428ad0e451b` (UUIDv7) | **Yes** (Primary Key) |
| **Physical Storage Key** | `ObjectKey` | `media/uploads/2026/10/06/01925b3a...png` | **Yes** (Storage Index) |
| **Client Presentation** | `Original Filename` | `annual_report_2026.pdf` | **Yes** (Header/Audit) |
| **Delivery URL** | `Access URL` | `https://cdn.example.com/...` or presigned URL | **NO** (Generated on-demand) |

> [!IMPORTANT]
> **URLs are transient delivery details, never canonical identifiers.** Storing full URLs in databases causes severe data corruption when domains migrate, buckets change, or certificates rotate. In BlobKit, URLs are resolved dynamically via `ResolveURL` or `PresignGet`.

---

## Feature Matrix

| Feature | BlobKit Implementation | Guarantee |
| :--- | :--- | :--- |
| **Lifecycle & Compliance** | Retention policies, WORM locking, Legal hold, Soft delete, Restore, Permanent purge | Thread-safe, atomic state checks |
| **Background GC** | Decoupled `lifecycle.Sweeper` (expired objects, soft-delete grace period, stale uploads) | Standalone or embedded worker |
| **Object Operations** | Native server-side `Copy`, Cross-driver stream copy, `Move`, `Rename`, `Exists`, `Head` | Zero byte movement for renames |
| **Integrity** | Streaming SHA-256 via `io.TeeReader`, client digest validation, byte-count verification | $O(1)$ memory consumption |
| **Resumable Multipart** | State machine (`Active`, `Committed`, `Aborted`), per-part upload, restartable sessions | Concurrency & failure resilient |
| **Security & Policy** | MIME/Extension whitelisting, spoofing detection, path traversal & CRLF injection filter | Hard boundary defense |
| **Multi-Cloud Routing** | 3-State Circuit Breaker, Weighted traffic, Active-Passive Failover, Namespace dispatch | Zero allocation circuit check |
| **Caching Layer** | Bounded thread-safe LRU, Negative caching for 404 stampede protection, Mutex invalidation | Sub-microsecond cache hits |
| **Observability** | Vendor-neutral `Observer` interface, thread-safe `MetricsCollector`, zero secret leaks | OpenTelemetry / Prometheus ready |
| **Developer Experience** | Typed errors (`IsNotFound`, `IsObjectLocked`), `testutil.NewMockStorage` test fixture | Zero cloud credentials for tests |

---

## Installation

```bash
go get github.com/suhwr/blobkit
```

---

## Comprehensive Guide & Code Examples

### 1. Multi-Cloud Provider Configurations

BlobKit supports any S3-compatible backend out of the box using persistent HTTP/2 connection pools and bounded memory semaphores.

#### a. Cloudflare R2 (Zero Egress CDN)

```go
package main

import (
    "github.com/suhwr/blobkit/provider/s3"
)

func createR2Driver() (*s3.Driver, error) {
    return s3.NewDriver(s3.Config{
        Name:            "r2-primary",
        Endpoint:        "https://<account_id>.r2.cloudflarestorage.com",
        Bucket:          "production-assets",
        AccessKeyID:     "YOUR_R2_ACCESS_KEY_ID",
        SecretAccessKey: "YOUR_R2_SECRET_ACCESS_KEY",
        PublicBaseURL:   "https://cdn.example.com", // Custom Cloudflare CDN domain
        MaxConcurrentStreams: 32,                 // Worker semaphore limit
        MaxMemoryBytes:       128 * 1024 * 1024,  // 128MB RAM budget for multipart buffers
    })
}
```

#### b. AWS S3 (Standard / Multi-Region)

```go
func createS3Driver() (*s3.Driver, error) {
    return s3.NewDriver(s3.Config{
        Name:            "aws-s3-backup",
        Region:          "us-east-1",
        Bucket:          "enterprise-backup-vault",
        AccessKeyID:     "YOUR_AWS_ACCESS_KEY_ID",
        SecretAccessKey: "YOUR_AWS_SECRET_ACCESS_KEY",
        UsePathStyle:    false, // Virtual hosted-style addressing for S3
    })
}
```

#### c. MinIO & Self-Hosted S3

```go
func createMinIODriver() (*s3.Driver, error) {
    return s3.NewDriver(s3.Config{
        Name:            "local-minio",
        Endpoint:        "http://127.0.0.1:9000",
        Bucket:          "local-dev-bucket",
        AccessKeyID:     "minioadmin",
        SecretAccessKey: "minioadmin",
        UsePathStyle:    true, // Required for local MinIO / path-style clusters
    })
}
```

#### d. In-Memory Driver (Offline Testing & CI/CD)

```go
import "github.com/suhwr/blobkit/provider/memory"

// Pure in-memory driver: zero network calls, instant execution, race-tested
memDriver := memory.NewDriver(memory.Config{
    Name:          "in-memory",
    Bucket:        "test-bucket",
    PublicBaseURL: "https://test.example.com",
})
```

---

### 2. Advanced Multi-Provider Routing

BlobKit separates provider selection into composable router strategies.

#### a. Namespace Tiered Routing

Route different workloads to specific storage providers (e.g. public media to R2 for zero egress, compliance archives to AWS S3):

```go
import "github.com/suhwr/blobkit/router"

routes := map[string]blobkit.Driver{
    "public/avatars":    r2Driver,
    "public/media":      r2Driver,
    "secure/documents":  s3Driver,
    "audit/compliance":  s3Driver,
}

nsRouter := router.NewNamespaceRouter(routes, r2Driver /* default fallback */)

client, err := blobkit.New(
    blobkit.WithRouter(nsRouter),
)
```

#### b. Active-Passive Failover

Automatically switch from primary provider to secondary backup upon consecutive failure thresholds:

```go
failoverRouter := router.NewFailoverRouter(router.FailoverConfig{
    Primary:          r2Driver,
    Secondary:        s3Driver,
    FailureThreshold: 3,                // Switch to secondary after 3 consecutive failures
    Cooldown:         60 * time.Second, // Probe primary after 60s
})
```

#### c. 3-State Circuit Breaker Router

Prevent cascading cloud outages with zero-allocation circuit breaker protection:

```go
cbRouter := router.NewCircuitBreakerRouter(router.CircuitBreakerConfig{
    Primary:          r2Driver,
    Fallback:         s3Driver,
    FailureThreshold: 5,                // Trip to OPEN after 5 consecutive 5xx errors
    SuccessThreshold: 2,                // Require 2 successful canary probes in HALF-OPEN to close
    Cooldown:         30 * time.Second, // Wait 30s before attempting canary probe
})
```

#### d. Weighted Traffic Splitting

Distribute traffic proportionally across multiple cloud providers for canary testing or load balancing:

```go
weightedRouter := router.NewWeightedRouter([]router.WeightedTarget{
    {Driver: r2Driver, Weight: 80}, // 80% of traffic
    {Driver: s3Driver, Weight: 20}, // 20% of traffic
})
```

#### e. Capability-Aware Dispatch

Filter storage backends based on required feature bitmasks:

```go
// Only route to drivers that support server-side copy and resumable multipart
capRouter := router.NewCapabilityRouter(
    allDrivers,
    blobkit.CapCopy | blobkit.CapMultipartSession,
)
```

---

### 3. Object Key Partitioning Strategies

BlobKit shields applications from manually constructing raw storage paths:

```go
import "github.com/suhwr/blobkit/key"

// 1. UUIDv7 Generator (Logical, time-ordered, K-sortable)
uuidGen := key.NewUUIDv7Generator()

// 2. Date-Partitioned Generator: "namespace/2026/10/06/<uuid>.<ext>"
dateGen := key.NewDatePrefixGenerator()

// 3. Hash-Sharded Generator (Prevents S3 partition rate-limit hotspots):
// Generates keys like: "namespace/a1/b2/<uuid>.<ext>"
shardedGen := key.NewHashShardedGenerator(2, 2)

client, _ := blobkit.New(
    blobkit.WithDriver(r2Driver),
    blobkit.WithKeyGenerator(shardedGen),
)
```

---

### 4. Security Policy & Sanitization Engine

Defend against malicious file uploads, extension spoofing, path traversal, and HTTP header injections at the infrastructure boundary:

```go
import "github.com/suhwr/blobkit/policy"

uploadPolicy := policy.NewBuilder().
    // Allowed MIME types
    AllowMIME("image/jpeg", "image/png", "image/webp", "application/pdf").
    // Allowed extensions
    AllowExtensions(".jpg", ".jpeg", ".png", ".webp", ".pdf").
    // Enforce size bounds (1KB to 25MB)
    MinSize(1024).
    MaxSize(25 * 1024 * 1024).
    // Reject files where file extension doesn't match byte magic signature
    DisallowMIMEMismatch(true).
    Build()

client, _ := blobkit.New(
    blobkit.WithDriver(r2Driver),
    blobkit.WithPolicy(uploadPolicy),
)

// Malicious attempts like:
// - Filename: "../../etc/passwd" -> sanitized to "passwd"
// - Filename: "exploit.exe" disguised as "exploit.png" -> rejected with ErrMIMEMismatch
// - File size: 50MB -> rejected with ErrSizeExceeded
```

---

### 5. Streaming Uploads & Integrity Verification

Upload data directly from any `io.Reader` without buffering the entire payload in memory. BlobKit automatically computes SHA-256 digests on-the-fly and verifies client-provided checksums:

```go
ctx := context.Background()
fileStream, _ := os.Open("large_document.pdf")
defer fileStream.Close()

obj, err := client.Put(ctx, fileStream, blobkit.PutOptions{
    Namespace: "documents",
    OwnerID:   "tenant_456",
    Filename:  "quarterly_report.pdf",
    // Optional: Assert expected SHA-256 digest
    // If the streamed bytes do not match, BlobKit immediately purges the blob
    // and returns ErrChecksumMismatch
    ClientChecksum: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    Metadata: map[string]string{
        "department": "finance",
        "confidential": "true",
    },
})
if err != nil {
    if blobkit.IsChecksumMismatch(err) {
        log.Fatalf("Checksum mismatch detected: file was corrupted during transit")
    }
    log.Fatal(err)
}

fmt.Printf("Committed ObjectID: %s\n", obj.ID)
fmt.Printf("Storage Key:        %s\n", obj.Key)
fmt.Printf("SHA-256 Checksum:   %s\n", obj.ChecksumSHA256)
fmt.Printf("Detected MIME:      %s\n", obj.MIMEType)
```

---

### 6. Streaming Downloads & Byte-Range Seeking

Read objects efficiently with support for HTTP byte ranges (essential for video seek, audio streaming, and resumable downloads):

```go
// 1. Full stream download by logical ObjectID
reader, err := client.Get(ctx, obj.ID, blobkit.GetOptions{})
if err != nil {
    log.Fatal(err)
}
defer reader.Close()
io.Copy(destFile, reader)

// 2. Byte-Range request (fetch bytes 1024 to 4096)
start := int64(1024)
end := int64(4096)
rangeReader, err := client.Get(ctx, obj.ID, blobkit.GetOptions{
    RangeStart: &start,
    RangeEnd:   &end,
})
if err != nil {
    log.Fatal(err)
}
defer rangeReader.Close()
```

---

### 7. High-Level Object Operations

Execute advanced object manipulations purely using logical `ObjectID`s.

#### a. Server-Side & Cross-Driver Copy

Fast native copy when source and destination share the same backend, with automated fallback to streaming copy across different providers:

```go
copiedObj, err := client.Copy(ctx, sourceObj.ID, blobkit.PutOptions{
    Namespace: "documents/archive",
    Filename:  "archived_report.pdf",
})
```

#### b. Atomic Move

Coordinate copy and cleanup in a single operation:

```go
movedObj, err := client.Move(ctx, sourceObj.ID, blobkit.PutOptions{
    Namespace: "processed",
})
```

#### c. Zero-Byte Logical Rename

Update original presentation filenames in the metadata registry with **zero byte rewrites** on physical storage:

```go
err := client.Rename(ctx, obj.ID, "final_annual_report_v2.pdf")
```

#### d. Existence & Head Metadata Inspection

Inspect object metadata without transferring the body payload:

```go
exists, err := client.Exists(ctx, obj.ID)

headObj, err := client.Head(ctx, obj.ID)
fmt.Printf("MIME: %s, Size: %d bytes\n", headObj.MIMEType, headObj.Size)
```

#### e. Concurrency-Bounded Batch Deletes

Delete thousands of objects efficiently with bounded concurrency worker pools:

```go
objectIDs := []string{id1, id2, id3, id4, id5}
err := client.DeleteBatch(ctx, objectIDs)
```

---

### 8. WORM Retention, Legal Hold & Lifecycle

Enforce strict compliance guarantees and prevent accidental data loss:

```go
retentionTime := time.Now().Add(365 * 24 * time.Hour) // 1 year retention

obj, err := client.Put(ctx, dataStream, blobkit.PutOptions{
    Namespace:      "legal-contracts",
    Filename:       "agreement.pdf",
    RetentionUntil: &retentionTime, // Cannot be deleted before this date
    LegalHold:      true,          // Overrides retention; blocks deletion until released
})

// Attempting to delete returns ErrObjectLocked
err = client.Delete(ctx, obj.ID)
if blobkit.IsObjectLocked(err) {
    fmt.Println("Action blocked: object is locked by retention policy or legal hold")
}

// Soft Delete (Moves to trash, hides from Get/Head queries, retains physical bytes)
err = client.SoftDelete(ctx, obj.ID)

// Restore from trash
err = client.Restore(ctx, obj.ID)

// Permanent Purge (Removes physical bytes from storage and deletes metadata record)
err = client.PermanentDelete(ctx, obj.ID)
```

---

### 9. Resumable Multipart Upload Pipeline

Handle massive files (multi-gigabyte videos, backups, datasets) with resumable session state machines:

```go
// 1. Initiate resumable upload session (chunk size: 10MB, expires in 24 hours)
session, err := client.InitiateResumableUpload(ctx, blobkit.PutOptions{
    Namespace: "videos",
    Filename:  "presentation.mp4",
}, 10*1024*1024, 24*time.Hour)

// 2. Upload individual chunks concurrently or sequentially
part1, err := client.UploadPart(ctx, session.ID, 1, chunk1Reader, chunk1Size)
part2, err := client.UploadPart(ctx, session.ID, 2, chunk2Reader, chunk2Size)

// 3. Inspect upload progress
parts, err := client.ListSessionParts(ctx, session.ID)
fmt.Printf("Uploaded %d parts so far\n", len(parts))

// 4. Commit session into finalized object
finalObj, err := client.CommitResumableUpload(ctx, session.ID)
fmt.Printf("Uploaded completed video: %s (%d bytes)\n", finalObj.ID, finalObj.Size)

// Or abort if canceled by user
// err = client.AbortResumableUpload(ctx, session.ID)
```

---

### 10. URL Resolution & Presigned Access

Deliver assets securely without persisting volatile provider URLs into databases:

```go
// 1. Dynamic Public CDN URL Resolution
// Returns "https://cdn.example.com/media/uploads/2026/10/06/..."
publicURL, err := client.ResolveURL(ctx, obj.ID)

// 2. Time-limited Presigned GET URL (e.g. for private downloads)
presignedGet, err := client.PresignGet(ctx, obj.ID, blobkit.PresignOptions{
    Expiry: 15 * time.Minute,
})
fmt.Printf("Temporary Download Link: %s\n", presignedGet.URL)

// 3. Direct-to-Storage Presigned PUT URL (Client-side upload direct from browser/mobile)
presignedPut, err := client.PresignPut(ctx, blobkit.PutOptions{
    Namespace: "avatars",
    Filename:  "user_photo.jpg",
}, blobkit.PresignOptions{
    Expiry: 10 * time.Minute,
})
fmt.Printf("Direct Upload Target: %s\n", presignedPut.URL)
```

---

### 11. Bounded LRU Caching & Stampede Defense

Prevent cache stampedes and accelerate repeated metadata lookups:

```go
import "github.com/suhwr/blobkit/cache"

// Create LRU cache holding up to 10,000 objects with 10-minute TTL
lruCache := cache.NewLRUCache(10000)

client, _ := blobkit.New(
    blobkit.WithDriver(r2Driver),
    blobkit.WithCache(lruCache),
)

// Repeated calls to Head, Exists, and ResolveURL resolve in < 500ns from memory.
// Mutating operations (Put, Delete, Move, PermanentDelete) automatically invalidate
// the corresponding cache keys with zero race conditions.
```

---

### 12. Decoupled Background Sweeper

Reclaim storage space and eliminate abandoned upload costs using the decoupled sweeper engine:

```go
import "github.com/suhwr/blobkit/lifecycle"

// Configure background sweeper
sweeper, err := lifecycle.NewSweeper(client, lifecycle.SweeperConfig{
    Interval:          1 * time.Hour,
    SoftDeleteTTL:     30 * 24 * time.Hour, // Permanently purge trash after 30 days
    MultipartStaleTTL: 12 * time.Hour,      // Abort abandoned uploads after 12 hours
    BatchSize:         200,
    DryRun:            false,
})
if err != nil {
    log.Fatal(err)
}

// Option A: Run continuously in background goroutine
_ = sweeper.Start(ctx)
defer sweeper.Stop()

// Option B: Run as a one-off scheduled Kubernetes CronJob
// err := sweeper.RunOnce(ctx)
```

---

### 13. Vendor-Neutral Telemetry & Observability

Monitor throughput, error rates, latencies, and memory semaphore wait times:

```go
import "github.com/suhwr/blobkit/observer"

// Built-in thread-safe metrics collector
collector := observer.NewMetricsCollector()

client, _ := blobkit.New(
    blobkit.WithDriver(r2Driver),
    blobkit.WithObserver(collector),
)

// Inspect metrics anytime
metrics := collector.Snapshot()
fmt.Printf("Total Uploads:     %d\n", metrics.Operations["put"])
fmt.Printf("Bytes Transferred: %d\n", metrics.BytesTransferred)
fmt.Printf("Active Semaphores: %d\n", metrics.LimiterWaits)
```

To export to **Prometheus** or **OpenTelemetry**, simply implement the `blobkit.Observer` interface:

```go
type Observer interface {
    OnOperationStart(ctx context.Context, op string, namespace string)
    OnOperationEnd(ctx context.Context, op string, namespace string, provider string, duration time.Duration, err error)
    OnBytesTransferred(ctx context.Context, op string, bytes int64)
    OnLimiterWait(ctx context.Context, waitDuration time.Duration)
}
```

---

### 14. Consumer Unit Testing with Test Fixtures

Test consumer applications without external network calls, cloud credentials, or MinIO containers using `testutil`:

```go
package service_test

import (
    "testing"
    "github.com/suhwr/blobkit/testutil"
)

func TestUserAvatarUpload(t *testing.T) {
    // Spin up an isolated, in-memory BlobKit test fixture
    fixture := testutil.NewMockStorage()
    defer fixture.Client.Close()

    // Seed existing mock object
    seeded := fixture.SeedObject("avatars", "existing.png", []byte("avatar-bytes"))

    // Execute application service under test
    userService := NewUserService(fixture.Client)
    err := userService.UpdateAvatar(seeded.ID)
    if err != nil {
        t.Fatalf("UpdateAvatar failed: %v", err)
    }

    // Assert stored contents
    testutil.AssertBlobContent(t, fixture.Client, seeded.ID, []byte("avatar-bytes"))
}
```

---

## Performance Benchmarks

Measured on AMD EPYC 7C13 64-Core Processor (Linux x86_64, Go 1.24):

| Benchmark Scenario | Throughput / Ops | Latency (ns/op) | Memory (B/op) | Allocs/op |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkCircuitBreaker_Select` | 75,700,000+ | **14.68 ns/op** | **0 B/op** | **0 allocs/op** |
| `BenchmarkLRUCache_Hit` | 3,030,000+ | **402.7 ns/op** | **320 B/op** | **1 allocs/op** |
| `BenchmarkKeyGeneration_UUIDv7` | 2,420,000+ | **530.6 ns/op** | **208 B/op** | **4 allocs/op** |
| `BenchmarkKeyGeneration_DatePrefix` | 1,430,000+ | **855.1 ns/op** | **256 B/op** | **5 allocs/op** |
| `BenchmarkKeyGeneration_HashSharded`| 1,480,000+ | **874.9 ns/op** | **416 B/op** | **7 allocs/op** |
| `BenchmarkMIMESniff` | 1,340,000+ | **894.8 ns/op** | **624 B/op** | **3 allocs/op** |
| `BenchmarkClient_Get_ByObjectID` | 1,000,000+ | **1,457 ns/op** | **707 B/op** | **4 allocs/op** |
| `BenchmarkClient_Put_Standalone` | 163,000+ | **7,347 ns/op** | **3,019 B/op** | **27 allocs/op** |
| `BenchmarkClient_Put_WithRegistry` | 122,000+ | **10,795 ns/op** | **4,290 B/op** | **30 allocs/op** |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
