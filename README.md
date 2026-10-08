# BlobKit

[![Go Reference](https://pkg.go.dev/badge/github.com/suhwr/blobkit.svg)](https://pkg.go.dev/github.com/suhwr/blobkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/suhwr/blobkit)](https://goreportcard.com/report/github.com/suhwr/blobkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.24-00ADD8.svg)](https://golang.org/)

**BlobKit** is a modular, high-performance object storage library for Go. Engineered with 8 native, production-hardened storage engines with zero external SDK bloat, it provides a clean, unified storage layer for applications interfacing with **AWS S3**, **Cloudflare R2**, **MinIO**, **Wasabi**, **Backblaze B2**, **Azure Blob Storage**, **Google Cloud Storage (Native JSON API)**, **WebDAV (Nextcloud / TrueNAS)**, **Google Drive (Shared Drives)**, **Local POSIX Filesystem**, **Remote SFTP / SSH**, and **Ephemeral In-Memory** storage.

BlobKit offers two distinct operating modes depending on your architecture:
1. **Low-Level Core (`Bucket`)**: A fast, stateless, key-value storage abstraction with direct streaming (`io.WriteCloser`), zero database dependencies, and sub-microsecond throughput.
2. **High-Level Orchestrator (`Client`)**: A full-featured storage engine providing canonical object identities (UUIDv7), database metadata registries, policy verification, intelligent multi-cloud routing (circuit breakers, failover), and automated lifecycle management.

---

## Table of Contents

- [Architectural Principles](#architectural-principles)
- [Identity & Delivery Separation](#identity--delivery-separation)
- [Feature Matrix](#feature-matrix)
- [Installation](#installation)
- [Choosing Your API Tier: Low-Level `Bucket` vs. High-Level `Client`](#choosing-your-api-tier-low-level-bucket-vs-high-level-client)
- [Low-Level Storage Engine & Wire-Level Architecture](#low-level-storage-engine--wire-level-architecture)
  - [1. Stateless `Bucket` Architecture & Direct Key I/O](#1-stateless-bucket-architecture--direct-key-io)
  - [2. Asynchronous Streaming Writes (`bucket.NewWriter`)](#2-asynchronous-streaming-writes-bucketnewwriter)
  - [3. Kernel Zero-Copy Streaming (`ObjectReader.WriteTo`)](#3-kernel-zero-copy-streaming-objectreaderwriteto)
  - [4. Random-Access Seeking (`SeekableReader` & `zip.NewReader`)](#4-random-access-seeking-seekablereader--zipnewreader)
  - [5. Building Custom Storage Drivers (`BaseDriver` SPI)](#5-building-custom-storage-drivers-basedriver-spi)
  - [6. Driver Middleware & Interceptor Pipeline](#6-driver-middleware--interceptor-pipeline)
  - [7. Low-Level Error Taxonomy & Resilient Retries](#7-low-level-error-taxonomy--resilient-retries)
  - [8. Wire-Level Protocols & Concurrency Internals Deep Dive](#8-wire-level-protocols--concurrency-internals-deep-dive)
- [High-Level Orchestrator & Enterprise Features](#high-level-orchestrator--enterprise-features)
  - [1. Multi-Cloud Provider Configurations](#1-multi-cloud-provider-configurations)
    - [Cloudflare R2 (Zero Egress CDN)](#a-cloudflare-r2-zero-egress-cdn)
    - [AWS S3 (Standard / Multi-Region)](#b-aws-s3-standard--multi-region)
    - [MinIO & Self-Hosted S3](#c-minio--self-hosted-s3)
    - [In-Memory Driver (Offline Testing & CI/CD)](#d-in-memory-driver-offline-testing--cicd)
    - [Google Drive Driver (Cloud & Shared Drives)](#e-google-drive-driver-cloud--shared-drives)
    - [Local POSIX Filesystem Driver (Zero Cost & Edge Storage)](#f-local-posix-filesystem-driver-zero-cost--edge-storage)
    - [WebDAV Protocol Driver (Nextcloud, ownCloud, TrueNAS, NAS)](#g-webdav-protocol-driver-nextcloud-owncloud-truenas-nas)
    - [Azure Blob Storage Driver (Native Block Blobs & SAS Presigning)](#h-azure-blob-storage-driver-native-block-blobs--sas-presigning)
    - [Google Cloud Storage Driver (Native JSON API & V4 Signed URLs)](#i-google-cloud-storage-driver-native-json-api--v4-signed-urls)
    - [SFTP / SSH Storage Driver (Remote Linux/Unix Server Storage)](#j-sftp--ssh-storage-driver-remote-linuxunix-server-storage)
  - [2. Advanced Multi-Provider Routing](#2-advanced-multi-provider-routing)
    - [Namespace Tiered Routing](#a-namespace-tiered-routing)
    - [Active-Passive Failover](#b-active-passive-failover)
    - [3-State Circuit Breaker Router](#c-3-state-circuit-breaker-router)
    - [Weighted Traffic Splitting](#d-weighted-traffic-splitting)
    - [Capability-Aware Dispatch](#e-capability-aware-dispatch)
    - [Declarative Multi-Account Fleet Loader (`router/fleet`)](#f-declarative-multi-account-fleet-loader-routerfleet)
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
  - [11. Pluggable Metadata Registries (Memory, SQLite3, PostgreSQL)](#11-pluggable-metadata-registries-memory-sqlite3-postgresql)
  - [12. Bounded LRU Caching & Stampede Defense](#12-bounded-lru-caching--stampede-defense)
  - [13. Decoupled Background Sweeper](#13-decoupled-background-sweeper)
  - [14. Vendor-Neutral Telemetry & Observability](#14-vendor-neutral-telemetry--observability)
  - [15. Consumer Unit Testing with Test Fixtures](#15-consumer-unit-testing-with-test-fixtures)
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
|    - AWS SDK v2 Driver (AWS S3, Cloudflare R2, MinIO, Wasabi, Backblaze B2)       |
|    - Native Azure Blob Storage Driver (Block Blobs, SAS, Azurite emulator)        |
|    - Native Google Cloud Storage Driver (JSON API v1, 256KB Resumable, V4 Sign)   |
|    - WebDAV Protocol Driver (Nextcloud, ownCloud, TrueNAS, Apache/Nginx)          |
|    - Google Drive Driver (Personal & Enterprise Shared Drives, appProperties)     |
|    - Local POSIX Filesystem Driver (Atomic swaps, Sidecar metadata, Edge)         |
|    - Remote SFTP / SSH Driver (Connection pooling, Remote seeks, Sidecars)        |
|    - Pure In-Memory Driver for offline unit tests and ephemeral workloads         |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 4. PHYSICAL STORAGE BACKENDS                                                      |
|    AWS S3 / R2 / MinIO / Azure Blob / GCS / WebDAV / GDrive / Local NVMe / SFTP   |
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

## Choosing Your API Tier: Low-Level `Bucket` vs. High-Level `Client`

BlobKit is architected to give developers full flexibility. Choose the layer that matches your system requirements:

| Capability | Low-Level Core (`Bucket`) | High-Level Orchestrator (`Client`) |
| :--- | :--- | :--- |
| **Primary Identity** | Direct Physical Key (`"reports/2026.pdf"`) | Canonical `ObjectID` (UUIDv7) + Key |
| **Database Dependency** | **None (Zero Database, Stateless)** | Pluggable Metadata Registry (Postgres, SQLite, Memory) |
| **Object State Machine** | Direct I/O (`Put`, `Get`, `Delete`) | States: `Pending` ➔ `Committed` ➔ `Deleted` |
| **Streaming Primitives** | `io.WriteCloser` via `bucket.NewWriter()` | Streaming with MIME sniff & SHA-256 verification |
| **Routing & Topologies** | Single Driver / Bucket | Multi-driver Failover, Circuit Breaking & Tiering |
| **Lifecycle & Compliance**| Provider-native lifecycle | Soft-delete, WORM legal hold, automated background sweeper |
| **Best For** | Microservices, background workers, pure storage I/O | Central storage platforms, multi-tenant SaaS, audit systems |

### 🚀 Quick Start: Low-Level `Bucket` (Stateless Direct I/O)

Ideal for microservices or tasks where you simply want universal storage without running a database:

```go
package main

import (
    "context"
    "fmt"
    "io"
    "strings"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/s3"
)

func main() {
    ctx := context.Background()

    // 1. Initialize any storage driver
    driver, _ := s3.NewDriver(s3.Config{
        Region:          "us-east-1",
        Bucket:          "my-app-uploads",
        AccessKeyID:     "MY_KEY",
        SecretAccessKey: "MY_SECRET",
    })

    // 2. Wrap into a stateless Bucket (no database required!)
    bucket, _ := blobkit.NewBucket(driver)
    defer bucket.Close()

    // 3. Put an object stream
    key := "docs/invoice_001.txt"
    _, _ = bucket.Put(ctx, key, strings.NewReader("Invoice Total: $120.00"), blobkit.PutOptions{
        ContentType: "text/plain",
    })

    // 4. Convenience helpers: PutBytes & GetBytes
    _ = bucket.PutBytes(ctx, "cache/flag.bin", []byte{0x01, 0x02}, blobkit.PutOptions{})
    data, _, _ := bucket.GetBytes(ctx, "cache/flag.bin", blobkit.GetOptions{})
    fmt.Printf("Read %d bytes\n", len(data))

    // 5. Streaming io.WriteCloser (zero memory buffering)
    writer, _ := bucket.NewWriter(ctx, "logs/app.log", blobkit.PutOptions{
        ContentType: "text/plain",
    })
    writer.Write([]byte("Application booted successfully\n"))
    writer.Close() // Automatically finalizes upload to S3

    // 6. Inspect & Delete
    exists, _ := bucket.Exists(ctx, key)
    if exists {
        _ = bucket.Delete(ctx, key)
    }
}
```

---

## Low-Level Storage Engine & Wire-Level Architecture

The Low-Level Core in BlobKit (`blobkit.Bucket`) is engineered for maximum throughput, predictable memory footprint, and architectural minimalism. When building microservices, background ingestion workers, data pipelines, high-performance reverse proxies, or edge runtimes, you don't need a relational database, UUIDv7 object identity mappings, or complex lifecycle state machines. You simply need fast, reliable, zero-overhead storage I/O directly on the wire.

```
+-----------------------------------------------------------------------------+
|                         APPLICATION CODE / PIPELINE                         |
|   (io.Reader / io.Writer / io.ReaderAt / io.ReadSeeker / []byte payloads)   |
+-----------------------------------------------------------------------------+
                                      |
              +-----------------------+-----------------------+
              |                                               |
              v                                               v
+-------------------------------+             +-------------------------------+
|      blobkit.Bucket           |             |      SeekableReader           |
|  - Direct Key-Value Storage   |             |  - io.ReadSeekCloser          |
|  - Asynchronous NewWriter()   |             |  - io.ReaderAt (Concurrency)  |
|  - Fast PutBytes / GetBytes   |             |  - HTTP Byte-Range Seeks      |
|  - Low-level Multipart SPI    |             |  - ZIP / Parquet / Media      |
+-------------------------------+             +-------------------------------+
              |                                               |
              +-----------------------+-----------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                         DRIVER INTERCEPTOR PIPELINE                         |
|   WrapDriver(driver, LatencyMiddleware, CustomRateLimiter, AuditLogger)     |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                     NORMATIVE DRIVER CONTRACT & SPI                         |
|   blobkit.Driver interface  <--- embedded by --->  blobkit.BaseDriver       |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                     ZERO-COPY KERNEL STREAMING                              |
|   ObjectReader.WriteTo(w) -> Linux sendfile(2) / splice(2) fast path        |
+-----------------------------------------------------------------------------+
```

### 1. Stateless `Bucket` Architecture & Direct Key I/O

A `Bucket` wraps any `blobkit.Driver` directly. It communicates using physical storage keys without touching an external database:

- **Ultra-low latency**: Reads resolve in **~735 ns/op** and uploads in **~2.3 µs/op** (over **3.3x faster** than full client orchestration).
- **Direct key addressing**: Store and retrieve using exact paths (e.g. `media/2026/avatar.png`).
- **Memory-efficient helpers**: In-memory byte slices can be saved and retrieved via `PutBytes` and `GetBytes` without manual buffer management.
- **Direct provider multipart**: Initiate, upload parts, and complete provider multipart sessions directly using raw keys (`CreateMultipart`, `UploadPart`, `CompleteMultipart`).

```go
package main

import (
    "context"
    "fmt"
    "strings"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/s3"
)

func main() {
    ctx := context.Background()

    // 1. Initialize any storage driver
    driver, err := s3.NewDriver(s3.Config{
        Region:          "us-east-1",
        Bucket:          "my-app-uploads",
        AccessKeyID:     "MY_KEY",
        SecretAccessKey: "MY_SECRET",
    })
    if err != nil {
        panic(err)
    }

    // 2. Wrap into a stateless Bucket (no database required!)
    bucket, err := blobkit.NewBucket(driver)
    if err != nil {
        panic(err)
    }
    defer bucket.Close()

    // 3. Put an object stream directly to a key
    key := "docs/invoice_001.txt"
    _, err = bucket.Put(ctx, key, strings.NewReader("Invoice Total: $120.00"), blobkit.PutOptions{
        ContentType: "text/plain",
        Metadata: map[string]string{
            "tier": "enterprise",
        },
    })

    // 4. Convenience fast paths: PutBytes & GetBytes
    _ = bucket.PutBytes(ctx, "cache/flags.bin", []byte{0x01, 0x02, 0x03}, blobkit.PutOptions{})
    data, meta, _ := bucket.GetBytes(ctx, "cache/flags.bin", blobkit.GetOptions{})
    fmt.Printf("Read %d bytes (content-type: %s)\n", len(data), meta.ContentType)

    // 5. Inspect metadata (Head) & Existence check
    exists, _ := bucket.Exists(ctx, key)
    if exists {
        head, _ := bucket.Head(ctx, key)
        fmt.Printf("Key: %s, Size: %d bytes\n", head.Key, head.Size)
    }

    // 6. Direct server-side Copy & Delete
    _ = bucket.Copy(ctx, key, "docs/invoice_001_backup.txt")
    _ = bucket.Delete(ctx, key)

    // 7. Paginated listing by key prefix
    result, _ := bucket.List(ctx, blobkit.ListOptions{
        Prefix: "docs/",
        Limit:  50,
    })
    for _, obj := range result.Objects {
        fmt.Printf("- %s (%d bytes)\n", obj.Key, obj.Size)
    }
}
```

### 2. Asynchronous Streaming Writes (`bucket.NewWriter`)

When streaming dynamically generated content (e.g. database dumps, telemetry streams, audio encodes, or compressed archives), buffering the entire output in memory or temporary disk files wastes memory and adds disk I/O latency.

`bucket.NewWriter(ctx, key, opts)` returns a standard `io.WriteCloser` backed by an internal asynchronous pipe (`io.Pipe`). As bytes are written into the writer, they are streamed concurrently to the remote storage driver. Calling `.Close()` automatically flushes the pipe and awaits provider confirmation:

```go
package main

import (
    "compress/gzip"
    "context"
    "log"

    "github.com/suhwr/blobkit"
)

// Stream dynamically compressed gzip data directly to object storage with O(1) memory
func StreamArchive(ctx context.Context, bucket *blobkit.Bucket, key string) error {
    // 1. Open an asynchronous streaming writer
    writer, err := bucket.NewWriter(ctx, key, blobkit.PutOptions{
        ContentType: "application/gzip",
    })
    if err != nil {
        return err
    }

    // 2. Wrap writer in standard streaming compressor
    gz := gzip.NewWriter(writer)

    // 3. Write data chunks on-the-fly
    payload := []byte("streamed log entry line 1\nstreamed log entry line 2\n")
    if _, err := gz.Write(payload); err != nil {
        // Abort the pipe immediately on error to terminate the background upload
        _ = writer.CloseWithError(err)
        return err
    }

    // 4. Flush and close gzip framing blocks first
    if err := gz.Close(); err != nil {
        _ = writer.CloseWithError(err)
        return err
    }

    // 5. Close the BucketWriter to finalize upload and receive remote persistence error if any
    if err := writer.Close(); err != nil {
        log.Printf("Upload failed on storage provider: %v", err)
        return err
    }

    log.Println("Streaming upload successfully committed")
    return nil
}
```

### 3. Kernel Zero-Copy Streaming (`ObjectReader.WriteTo`)

BlobKit's `ObjectReader` implements standard Go `io.WriterTo`:

```go
func (r *ObjectReader) WriteTo(w io.Writer) (int64, error)
```

When serving objects over HTTP handlers, reverse proxies, or TCP connections (`*net.TCPConn`), Go runtime's `io.Copy` detects `io.WriterTo`. If the underlying driver stream is backed by an OS file descriptor or socket, the transfer engages Linux kernel zero-copy mechanisms like `sendfile(2)` or `splice(2)`. This moves data straight across kernel buffers without allocating heap memory or copying bytes into userspace Go application memory:

```go
package main

import (
    "fmt"
    "net/http"

    "github.com/suhwr/blobkit"
)

// High-throughput HTTP Asset Server leveraging kernel zero-copy transfer
func BlobServerHandler(bucket *blobkit.Bucket) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Path[1:] // e.g. "assets/video.mp4"

        // Open object stream
        reader, err := bucket.Get(r.Context(), key, blobkit.GetOptions{})
        if err != nil {
            if blobkit.IsNotFound(err) {
                http.NotFound(w, r)
                return
            }
            http.Error(w, "storage error", http.StatusInternalServerError)
            return
        }
        defer reader.Close()

        // Set response metadata headers
        if reader.ContentType != "" {
            w.Header().Set("Content-Type", reader.ContentType)
        }
        if reader.Size > 0 {
            w.Header().Set("Content-Length", fmt.Sprintf("%d", reader.Size))
        }

        // Fast-Path: WriteTo streams directly to http.ResponseWriter
        // Utilizing sendfile(2)/splice(2) fast paths with zero heap allocations
        _, _ = reader.WriteTo(w)
    }
}
```

### 4. Random-Access Seeking (`SeekableReader` & `zip.NewReader`)

Cloud object storage (AWS S3, Cloudflare R2, Google Cloud Storage, Azure Blob Storage) operates on HTTP byte ranges (`Range: bytes=start-end`), whereas Go standard library packages (such as `archive/zip.NewReader`, Parquet table readers, video atom decoders, or PDF parsers) require `io.ReaderAt` or `io.ReadSeekCloser`.

`bucket.OpenSeeker(ctx, key)` creates an adapter that bridges this gap seamlessly:
- Implements `io.ReadSeekCloser` and `io.ReaderAt`.
- Thread-safe `ReadAt` calls execute range GET requests concurrently across goroutines.
- Reuses active body streams when sequential reads continue from current cursor.

#### Example: Extracting a File from a 50GB Remote ZIP Without Downloading It

In the ZIP file specification, the **Central Directory** index is located at the very **end** of the archive. A standard downloader would need to download all 50GB. With `SeekableReader`, Go's `zip.NewReader` only fetches the trailing metadata bytes over Range GET, then jumps directly to the compressed offset of the requested file:

```go
package main

import (
    "archive/zip"
    "context"
    "fmt"
    "io"

    "github.com/suhwr/blobkit"
)

func ExtractZipEntry(ctx context.Context, bucket *blobkit.Bucket, zipKey string, targetFile string) ([]byte, error) {
    // 1. Open random-access seeker over remote cloud blob
    seeker, err := bucket.OpenSeeker(ctx, zipKey)
    if err != nil {
        return nil, err
    }
    defer seeker.Close()

    // 2. Mount directly into archive/zip.NewReader
    // Only reads the end-of-archive Central Directory bytes over HTTP Range!
    zipReader, err := zip.NewReader(seeker, seeker.Size())
    if err != nil {
        return nil, fmt.Errorf("failed to parse zip central directory: %w", err)
    }

    // 3. Locate and stream only the specific file
    for _, file := range zipReader.File {
        if file.Name == targetFile {
            rc, err := file.Open()
            if err != nil {
                return nil, err
            }
            defer rc.Close()

            // Stream and read ONLY the target file bytes!
            return io.ReadAll(rc)
        }
    }

    return nil, fmt.Errorf("entry %q not found in remote archive", targetFile)
}
```

### 5. Building Custom Storage Drivers (`BaseDriver` SPI)

BlobKit is completely extensible. You can write your own storage driver (e.g. for Redis, IPFS, Ceph RADOS, Couchbase, or internal corporate storage) in under 50 lines of Go code by embedding `blobkit.BaseDriver`.

#### Why Embed `blobkit.BaseDriver`?
The normative BlobKit Driver Contract specifies that any capability not supported by a driver must return `blobkit.ErrUnsupportedOperation`. Embedding `BaseDriver` provides safe, contract-compliant stub implementations for all optional methods (`Copy`, `List`, `PresignGet`, `CreateMultipart`, etc.), so you only need to implement the core methods your backend supports:

```go
package mydriver

import (
    "bytes"
    "context"
    "io"
    "sync"

    "github.com/suhwr/blobkit"
)

// CustomDriver implements blobkit.Driver for an in-memory key-value engine.
type CustomDriver struct {
    blobkit.BaseDriver // Provides compliant stubs for all unadvertised capabilities
    data map[string][]byte
    mu   sync.RWMutex
}

func NewCustomDriver(name string) *CustomDriver {
    return &CustomDriver{
        BaseDriver: blobkit.NewBaseDriver(
            name,
            blobkit.CapCore|blobkit.CapByteRangeGet, // Advertised capability bitmask
        ),
        data: make(map[string][]byte),
    }
}

// 1. Put stores a blob stream
func (d *CustomDriver) Put(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
    payload, err := io.ReadAll(r)
    if err != nil {
        return nil, blobkit.WrapError("put", obj.Key, d.Name(), err)
    }

    d.mu.Lock()
    d.data[obj.Key] = payload
    d.mu.Unlock()

    res := *obj
    res.Size = int64(len(payload))
    return &res, nil
}

// 2. Get retrieves a blob stream
func (d *CustomDriver) Get(ctx context.Context, key string, opts blobkit.GetOptions) (*blobkit.ObjectReader, error) {
    d.mu.RLock()
    payload, exists := d.data[key]
    d.mu.RUnlock()

    if !exists {
        return nil, blobkit.WrapError("get", key, d.Name(), blobkit.ErrObjectNotFound)
    }

    return &blobkit.ObjectReader{
        Object: blobkit.Object{Key: key, Size: int64(len(payload))},
        Body:   io.NopCloser(bytes.NewReader(payload)),
    }, nil
}

// 3. Head inspects blob metadata
func (d *CustomDriver) Head(ctx context.Context, key string) (*blobkit.Object, error) {
    d.mu.RLock()
    payload, exists := d.data[key]
    d.mu.RUnlock()

    if !exists {
        return nil, blobkit.WrapError("head", key, d.Name(), blobkit.ErrObjectNotFound)
    }
    return &blobkit.Object{Key: key, Size: int64(len(payload))}, nil
}

// 4. Delete removes a blob
func (d *CustomDriver) Delete(ctx context.Context, key string) error {
    d.mu.Lock()
    delete(d.data, key)
    d.mu.Unlock()
    return nil
}

// 5. Close releases driver resources
func (d *CustomDriver) Close() error {
    return nil
}
```

#### Verifying Custom Drivers with the Contract Test Suite
You can verify your custom driver against BlobKit's normative contract suite in a single line:

```go
package mydriver_test

import (
    "testing"
    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/testutil"
)

func TestCustomDriverContract(t *testing.T) {
    testutil.RunDriverContractTests(t, func() blobkit.Driver {
        return NewCustomDriver("test-custom")
    })
}
```

### 6. Driver Middleware & Interceptor Pipeline

BlobKit provides a composable middleware architecture inspired by HTTP middleware. Middlewares intercept wire-level driver operations to inject observability, chaos testing, audit trails, encryption, or rate limiting.

#### Architecture: `WrapDriver` & `DelegateDriver`
- `blobkit.WrapDriver(driver, middlewares...)`: Composes multiple middlewares outside-in.
- `blobkit.DelegateDriver`: A transparent proxy struct that implements `blobkit.Driver`. Middleware authors embed `DelegateDriver` and selectively override only the methods they want to intercept.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/s3"
)

// Example 1: Built-in Latency Middleware (Measures exact wire duration)
func withLatencyInstrumentation(rawDriver blobkit.Driver) blobkit.Driver {
    return blobkit.WrapDriver(rawDriver, blobkit.LatencyMiddleware(func(op blobkit.Operation, key string, duration time.Duration, err error) {
        log.Printf("[METRICS] Op=%s Key=%s Latency=%v Err=%v", op, key, duration, err)
    }))
}

// Example 2: Custom Security Audit Middleware
type SecurityAuditMiddleware struct {
    blobkit.DelegateDriver
    logger *log.Logger
}

func NewSecurityAuditMiddleware(logger *log.Logger) blobkit.DriverMiddleware {
    return func(next blobkit.Driver) blobkit.Driver {
        return &SecurityAuditMiddleware{
            DelegateDriver: blobkit.NewDelegateDriver(next),
            logger:         logger,
        }
    }
}

// Intercept only Delete operations
func (m *SecurityAuditMiddleware) Delete(ctx context.Context, key string) error {
    m.logger.Printf("[SECURITY AUDIT] Permanent delete requested for key=%s", key)
    return m.DelegateDriver.Delete(ctx, key)
}

func main() {
    rawDriver, _ := s3.NewDriver(s3.Config{
        Region: "us-east-1",
        Bucket: "app-bucket",
    })

    // Chain middlewares: Audit runs first, then Latency, then the raw Driver
    instrumentedDriver := blobkit.WrapDriver(
        rawDriver,
        blobkit.LatencyMiddleware(func(op blobkit.Operation, key string, duration time.Duration, err error) {
            log.Printf("Op %s took %v", op, duration)
        }),
        NewSecurityAuditMiddleware(log.Default()),
    )

    bucket, _ := blobkit.NewBucket(instrumentedDriver)
    // All bucket operations pass through the middleware chain
}
```

### 7. Low-Level Error Taxonomy & Resilient Retries

BlobKit standardizes all errors across cloud providers into a unified taxonomy wrapped in `blobkit.StorageError`:

```go
type StorageError struct {
    Op       string // "get", "put", "head", "delete", etc.
    Key      string // Object key that triggered the error
    Provider string // Provider identifier (e.g. "s3-primary", "azure-archive")
    Err      error  // Underlying sentinel error
}
```

#### Error Classification Predicates

| Helper Function | Indicates | Safe to Retry? | Example Sentinel Errors |
| :--- | :--- | :---: | :--- |
| `blobkit.IsNotFound(err)` | Object or bucket does not exist | **No** | `ErrObjectNotFound`, `ErrBucketNotFound` |
| `blobkit.IsPermanent(err)` | Non-recoverable client errors | **No** | `ErrInvalidKey`, `ErrPermissionDenied`, `ErrChecksumMismatch`, `ErrSecurityViolation` |
| `blobkit.IsTransient(err)` | Temporary network / server outages | **Yes** | `ErrProviderUnavailable`, `ErrRateLimited`, 502/503/504, connection reset |
| `blobkit.IsSecurityViolation(err)` | Security / MIME policy blocked | **No** | `ErrSecurityViolation`, `ErrMIMEMismatch` |
| `blobkit.IsObjectLocked(err)` | Object locked by WORM / legal hold | **No** | `ErrObjectLocked` |
| `blobkit.IsRateLimited(err)` | Throttling / 429 Too Many Requests | **Yes** (with backoff) | `ErrRateLimited` |

#### Resilient Retry Loop Example

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/suhwr/blobkit"
)

func FetchWithRetry(ctx context.Context, bucket *blobkit.Bucket, key string) ([]byte, error) {
    maxRetries := 3
    backoff := 150 * time.Millisecond

    for attempt := 1; attempt <= maxRetries; attempt++ {
        data, _, err := bucket.GetBytes(ctx, key, blobkit.GetOptions{})
        if err == nil {
            return data, nil
        }

        // 1. Immediately abort on permanent non-retryable errors
        if blobkit.IsPermanent(err) {
            if blobkit.IsNotFound(err) {
                log.Printf("Object not found: %s", key)
            } else if blobkit.IsPermissionDenied(err) {
                log.Printf("Access forbidden: check IAM permissions")
            }
            return nil, err
        }

        // 2. Retry only transient recoverable errors
        if blobkit.IsTransient(err) {
            log.Printf("Attempt %d transiently failed (%v). Retrying in %v...", attempt, err, backoff)
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(backoff):
                backoff *= 2 // Exponential backoff
                continue
            }
        }

        return nil, err
    }

    return nil, errors.New("maximum retries exceeded")
}
```

#### Automated Credential Scrubbing
To prevent accidental data leakage into log ingestion services (Sentry, Datadog, CloudWatch), all errors returned by BlobKit automatically scrub:
- AWS, Azure, and Google Cloud HMAC signatures (`X-Amz-Signature`, `X-Goog-Signature`, `sig=...`)
- Bearer tokens, Basic Auth headers, and OAuth refresh tokens
- RSA and Ed25519 private key PEM headers
- Internal RFC 1918 IP addresses (`10.x.x.x`, `172.16-31.x.x`, `192.168.x.x`, `127.0.0.1`)

### 8. Wire-Level Protocols & Concurrency Internals Deep Dive

BlobKit interfaces directly with storage backends at the raw wire level without heavy, monolithic vendor SDKs. Understanding how BlobKit manages chunking, concurrency, atomic state transitions, and memory allocations allows you to build mission-critical infrastructure with deterministic behavior.

#### a. Provider Wire Protocols for Resumable & Chunked Uploads

Each storage provider implements its own chunking specification. BlobKit unifies these protocols behind the normative `Driver` interface:

| Provider | Protocol Architecture | Wire Mechanics & Finalization | Guarantees & Edge Cases |
| :--- | :--- | :--- | :--- |
| **AWS S3 / R2 / MinIO** | AWS Multipart Upload | `POST /?uploads` $\rightarrow$ `PUT /?partNumber=N&uploadId=ID` $\rightarrow$ `POST /?uploadId=ID` (XML block list) | MD5 / SHA-256 etag verification per part. Same-key copy uses `MetadataDirectiveReplace`. |
| **Azure Blob Storage** | Azure Block Blobs | `PUT /blob?comp=block&blockid=ID` (base64) $\rightarrow$ `PUT /blob?comp=blocklist` (XML block list) | Preserves `stored.Size` on unknown-size streams. Appends SAS tokens to `x-ms-copy-source` on internal Copy. |
| **Google Cloud Storage** | GCS Resumable Upload | `POST /upload/storage/v1/b/...` (session URI) $\rightarrow$ `PUT <session_uri>` with `Content-Range: bytes START-END/TOTAL` | Aborts session on client context cancellation or size mismatch (`ErrSizeMismatch`). |
| **Google Drive** | Resumable Drive v3 API | `POST /upload/drive/v3/files?uploadType=resumable` $\rightarrow$ `PUT <uri>` with 256 KiB chunks $\rightarrow$ `bytes */uploaded` finalizer | Ref-counted per-key mutex locks prevent duplicate files on race conditions. Explicit boundary finalization prevents 308 hang. |
| **Local POSIX Filesystem** | Staged Temporary Files | Chunks written to `tmp_<uploadID>` $\rightarrow$ atomic rename `os.Rename(tmp, dst)` + sidecar `.meta.json` | Atomic rename ensures zero partial reads. Suffix range queries (`bytes=-N`) parse from EOF. |
| **Remote SFTP / SSH** | Remote Atomic Staging | Staged via `sftp.Client` $\rightarrow$ atomic remote rename $\rightarrow$ sidecar `.meta.json` | Non-destructive atomic move first, avoiding premature deletion of existing target files. |

#### b. Thread Safety & Race-Free Concurrency Model

BlobKit is verified with the Go race detector (`go test -race ./...`) under high concurrent workloads:

1. **Per-Session Lock Serialization**:
   Concurrent calls to `client.UploadPart` targeting the same resumable session are serialized through `sessionLocks sync.Map` with per-session mutexes. This prevents lost updates where two concurrent worker goroutines simultaneously fetch and overwrite the session's uploaded parts slice.

2. **Stampede Defense with Panic-Safe Singleflight**:
   The `singleflightGroup` collapses concurrent identical `Head` or metadata queries into a single driver lookup. If the leader goroutine panics, the panic is caught, the error is safely propagated to all awaiting follower goroutines, and the leader re-panics—preventing deadlocks, memory leaks, or followers receiving phantom `(nil, nil)` results.

3. **Immutable Deep Cloning**:
   Metadata maps and session parts retrieved from or saved into in-memory caches (`cache/lru.go`) and registries (`registry/memory.go`) are deeply cloned. External modifications to returned structs will never mutate internal cache entries or cause concurrent map read/write panics.

4. **Non-Destructive Rollback Compensation**:
   In `client.Copy`, destination pre-existence is audited prior to initiating the copy. If a post-copy metadata save fails, BlobKit's rollback compensation only deletes the destination file if it did **not** exist prior to the operation. Pre-existing files are never deleted on rollback.

#### c. Kernel Zero-Copy & RFC 9110 Byte-Range Streaming

- **Linux Zero-Copy (`sendfile` / `splice`)**:
  BlobKit's `ObjectReader` exposes `WriteTo(w io.Writer)`. When streaming to a network socket (such as an `http.ResponseWriter` backed by `net.TCPConn`), Go's internal runtime engages `sendfile(2)` or `splice(2)`. Bytes stream directly from file descriptors or driver sockets to the network card without touching application user-space memory buffers.
- **RFC 9110 Range Compliance**:
  All drivers declaring `CapByteRangeGet` strictly parse HTTP Range headers, including:
  - Standard ranges: `bytes=0-1024` (inclusive offsets 0 through 1024)
  - Open-ended ranges: `bytes=2048-` (offset 2048 through EOF)
  - Suffix ranges: `bytes=-500` (trailing 500 bytes of the object)
  - Exact EOF behavior: `SeekableReader.ReadAt` returns `io.EOF` whenever bytes read are less than the requested buffer at object boundaries, guaranteeing 100% compliance with Go's standard library `io.ReaderAt` contracts.

---

## High-Level Orchestrator & Enterprise Features

### 1. Multi-Cloud Provider Configurations

BlobKit natively supports 8 production-grade storage engines and declarative fleet management. For in-depth architecture, configuration options, memory bounds, and security practices, refer to the dedicated provider guides:

| Provider | Package | Supported Backends / Protocols | Dedicated Guide |
| :--- | :--- | :--- | :---: |
| **AWS S3 & Compatible** | `provider/s3` | AWS S3, Cloudflare R2, MinIO, Wasabi, Backblaze B2 | [Read S3 Guide ↗](docs/providers/s3.md) |
| **Azure Blob Storage** | `provider/azure` | Native Azure Block Blobs, SAS Presigning, Azurite emulator | [Read Azure Guide ↗](docs/providers/azure.md) |
| **Google Cloud Storage** | `provider/gcs` | Native GCS JSON API v1, 256KB Resumable, V4 Signed URLs | [Read GCS Guide ↗](docs/providers/gcs.md) |
| **WebDAV Protocol** | `provider/webdav` | Nextcloud, ownCloud, TrueNAS, Apache/Nginx, Synology/QNAP | [Read WebDAV Guide ↗](docs/providers/webdav.md) |
| **Google Drive** | `provider/gdrive` | Google Drive v3, Personal & Enterprise Shared Drives | [Read Google Drive Guide ↗](docs/providers/gdrive.md) |
| **Local POSIX Filesystem**| `provider/fs` | NVMe, SSD, EBS, Edge Storage, Atomic Swaps, Sidecar Meta | [Read FS Guide ↗](docs/providers/fs.md) |
| **Remote SFTP / SSH** | `provider/sftp` | Remote Linux/Unix Servers, SSH Keys, Connection Pooling | [Read SFTP Guide ↗](docs/providers/sftp.md) |
| **In-Memory Harness** | `provider/memory` | Pure RAM Ephemeral, Zero-Credential Unit Testing | [Read Memory Guide ↗](docs/providers/memory.md) |
| **Multi-Cloud Fleet** | `router/fleet` | Declarative JSON/YAML Multi-Cloud Fleet & Topologies | [Read Fleet Guide ↗](docs/providers/fleet.md) |

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
        Bucket:          "production-backup-vault",
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

#### e. Google Drive Driver (Cloud & Shared Drives)

Native Google Drive REST API v3 implementation adhering to pure object storage semantics with resumable chunked uploads, byte-range streaming, in-memory key indexing, and Shared Drive support:

```go
import "github.com/suhwr/blobkit/provider/gdrive"

gdriveDriver, err := gdrive.NewDriver(gdrive.Config{
    Name:              "gdrive-primary",
    FolderID:          "1A2B3C4D5E6F7G8H9I0J", // Target Google Drive folder or Shared Drive ID
    SupportsAllDrives: true,                   // Enable Google Workspace Shared Drives
    TokenFunc: func(ctx context.Context) (string, error) {
        // Return fresh OAuth2 token or use Service Account JWT
        return oauthTokenSource.Token(ctx)
    },
    ChunkSize:        8 * 1024 * 1024, // 8 MiB chunks (must be multiple of 256 KiB)
    KeyCacheCapacity: 10000,           // Concurrency-safe LRU key -> fileID cache
})
```

#### f. Local POSIX Filesystem Driver (Zero Cost & Edge Storage)

Microsecond-latency, zero-cost persistent disk storage with atomic temporary file commits, byte-range seeking (`os.File.Seek`), sidecar metadata persistence, and strict path traversal protection:

```go
import "github.com/suhwr/blobkit/provider/fs"

fsDriver, err := fs.NewDriver(fs.Config{
    Name:              "fs-local",
    RootDir:           "/var/data/blobs",               // Dedicated directory path
    PublicBaseURL:     "https://cdn.example.com/blobs", // Optional public URL prefix
    DirMode:           0755,
    FileMode:          0644,
    EnableSidecarMeta: true,                            // Atomically persist .meta.json sidecars
    StagingDir:        ".staging",                      // Chunk staging directory for multipart
})
```

#### g. WebDAV Protocol Driver (Nextcloud, ownCloud, TrueNAS, NAS)

Native RFC 4918 implementation connecting BlobKit to self-hosted cloud platforms (**Nextcloud**, **ownCloud**), **TrueNAS (ZFS pools)**, Apache/Nginx WebDAV, and private NAS appliances with streaming PUT/GET, byte-range seeks, and automatic `MKCOL` collection provisioning:

```go
import "github.com/suhwr/blobkit/provider/webdav"

webdavDriver, err := webdav.NewDriver(webdav.Config{
    Name:          "nextcloud-primary",
    Endpoint:      "https://cloud.example.com/remote.php/dav/files/admin",
    Username:      "admin",
    Password:      "app-password-or-token",
    PublicBaseURL: "https://cloud.example.com/s", // Optional public link prefix
})
```

#### h. Azure Blob Storage Driver (Native Block Blobs & SAS Presigning)

Native Azure Blob Storage driver adhering to BlobKit's pure object storage abstraction. Supports Block Blob uploads, server-side block staging (`comp=block`) and commits (`comp=blocklist`), SharedKey HMAC-SHA256 authentication, Service SAS token generation for presigned URLs, byte-range streaming, server-side synchronous copy, batch deletes, and credential scrubbing:

```go
import "github.com/suhwr/blobkit/provider/azure"

azureDriver, err := azure.NewDriver(azure.Config{
    Name:           "azure-primary",
    AccountName:    "mystorageaccount",
    AccountKey:     "base64-encoded-storage-key",
    ContainerName:  "my-container",
    // Optional: custom endpoint for Azurite emulator or sovereign clouds
    // CustomEndpoint: "http://127.0.0.1:10000/mystorageaccount",
    PublicBaseURL:  "https://cdn.example.com", // Optional public CDN prefix
})
```

#### i. Google Cloud Storage Driver (Native JSON API & V4 Signed URLs)

Native Google Cloud Storage driver interfacing directly with the GCS JSON API v1 and Resumable Upload protocol. Supports single-shot media uploads, multipart/related uploads with custom metadata, 256 KiB-aligned chunked resumable sessions, byte-range streaming, server-side rewrite/copy, concurrent batch deletion, and client-side Google Cloud V4 Signed URLs (`GOOG4-RSA-SHA256`) with zero external Google Cloud SDK dependencies:

```go
import "github.com/suhwr/blobkit/provider/gcs"

gcsDriver, err := gcs.NewDriver(gcs.Config{
    Name:                "gcs-primary",
    Bucket:              "my-cloud-bucket",
    TokenFunc: func(ctx context.Context) (string, error) {
        // Return fresh OAuth2 token or use Compute Engine / Workload Identity token source
        return tokenSource.Token(ctx)
    },
    // Optional: for Google Cloud V4 Signed URLs (PresignGet & PresignPut)
    ServiceAccountEmail: "service-account@project.iam.gserviceaccount.com",
    PrivateKeyPEM:       rsaPrivateKeyPEMBytes,
    ChunkSize:           8 * 1024 * 1024, // 8 MiB chunks (multiple of 256 KiB)
    PublicBaseURL:       "https://cdn.example.com", // Optional public CDN prefix
})
```

#### j. SFTP / SSH Storage Driver (Remote Linux/Unix Server Storage)

Native SFTP / SSH storage driver connecting BlobKit to remote Linux/Unix file servers, private NAS appliances, and secure file transfer clusters. Supports SSH password or private key authentication (with optional passphrase), atomic temporary file staging, byte-range seeks, sidecar metadata persistence, chroot containment defenses, and multiplexed SSH sessions:

```go
import "github.com/suhwr/blobkit/provider/sftp"

sftpDriver, err := sftp.NewDriver(sftp.Config{
    Name:              "sftp-archive",
    Host:              "storage.internal.corp",
    Port:              22,
    User:              "blobkit-service",
    PrivateKeyPEM:     sshPrivateKeyPEMBytes,
    BaseDir:           "/var/data/blobs",
    EnableSidecarMeta: true,                     // Persist .meta.json sidecars for custom metadata
    PublicBaseURL:     "https://cdn.example.com", // Optional public HTTP gateway prefix
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

Prevent cascading cloud outages with automatic 3-state circuit breaker protection:

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

#### f. Declarative Multi-Account Fleet Loader (`router/fleet`)

Orchestrate complete multi-provider fleets and routing topologies declaratively from JSON, YAML, config files, or Go structs without manual wiring. Automatically initializes and wires any combination of all 8 storage drivers (`s3`, `r2`, `azure`, `gcs`, `webdav`, `gdrive`, `fs`, `sftp`, `memory`) with namespace, failover, circuit breaker, or weighted routing:

```go
import "github.com/suhwr/blobkit/router/fleet"

fleetConfigJSON := []byte(`{
  "providers": [
    {
      "type": "s3",
      "name": "r2-public",
      "s3": {
        "endpoint": "https://<account>.r2.cloudflarestorage.com",
        "bucket": "public-media",
        "access_key_id": "...",
        "secret_access_key": "..."
      }
    },
    {
      "type": "azure",
      "name": "azure-archive",
      "azure": {
        "account_name": "prodarchive",
        "account_key": "...",
        "container_name": "archives"
      }
    },
    {
      "type": "fs",
      "name": "local-cache",
      "fs": {
        "root_dir": "/var/data/blobs"
      }
    }
  ],
  "routing": {
    "strategy": "namespace",
    "default_provider": "local-cache",
    "namespaces": {
      "public/media": "r2-public",
      "secure/archive": "azure-archive"
    }
  }
}`)

// Load fleet from JSON or Go struct
storageFleet, err := fleet.LoadFromJSON(fleetConfigJSON)
if err != nil {
    log.Fatalf("failed to initialize storage fleet: %v", err)
}
defer storageFleet.Close()

// Create fully wired BlobKit client directly
client, err := storageFleet.NewClient()
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
rangeReader, err := client.Get(ctx, obj.ID, blobkit.GetOptions{
    Range: "bytes=1024-4096",
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

### 11. Pluggable Metadata Registries (Memory, SQLite3, PostgreSQL)

BlobKit provides first-class, swappable metadata store implementations for `blobkit.MetadataStore`. Applications can select the storage engine that matches their deployment architecture:

```go
// Option A: In-Memory Store (Zero setup, ephemeral, perfect for tests & workers)
import "github.com/suhwr/blobkit/registry"
memStore := registry.NewMemoryStore()

// Option B: SQLite3 Store (Embedded local ACID database, zero external dependencies)
import "github.com/suhwr/blobkit/registry/sqlite"
sqliteStore, err := sqlite.New(sqlite.Config{
    FilePath:    "metadata.db", // Or ":memory:" for tests
    BusyTimeout: 5 * time.Second,
    WAL:         true,
    AutoMigrate: true, // Automatically creates tables and indexes
})

// Option C: PostgreSQL Store (Production relational database)
import "github.com/suhwr/blobkit/registry/postgres"
pgStore, err := postgres.New(postgres.Config{
    DSN:         "postgres://user:pass@localhost:5432/app_db?sslmode=disable",
    MaxOpenConns: 25,
    AutoMigrate:  true, // Automatically creates tables and indexes
})

// Initialize BlobKit with any chosen store:
client, _ := blobkit.New(
    blobkit.WithDriver(r2Driver),
    blobkit.WithRegistry(sqliteStore), // Or memStore, pgStore
)
```

---

### 12. Bounded LRU Caching & Stampede Defense

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

### 13. Decoupled Background Sweeper

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

### 14. Vendor-Neutral Telemetry & Observability

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

### 15. Consumer Unit Testing with Test Fixtures

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

Measured on AMD EPYC 7C13 Processor (8 CPU cores allocated, Linux x86_64, Go 1.27):

| Benchmark Scenario | Throughput / Ops | Latency (ns/op) | Memory (B/op) | Allocs/op | Architectural Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `BenchmarkCircuitBreaker_Select-8` | 86,390,000+ | **13.92 ns/op** | **0 B/op** | **0 allocs/op** | Zero-allocation route decision |
| `BenchmarkLRUCache_Hit-8` | 4,640,000+ | **279.4 ns/op** | **320 B/op** | **1 allocs/op** | Sub-microsecond metadata cache |
| `BenchmarkKeyGeneration_UUIDv7-8` | 2,270,000+ | **441.3 ns/op** | **208 B/op** | **4 allocs/op** | Time-ordered collision-proof keys |
| `BenchmarkMIMESniff-8` | 2,050,000+ | **593.7 ns/op** | **624 B/op** | **3 allocs/op** | 512B zero-rewind magic signature check |
| `BenchmarkKeyGeneration_HashSharded-8` | 1,720,000+ | **696.2 ns/op** | **416 B/op** | **7 allocs/op** | High-cardinality directory partitioning |
| `BenchmarkKeyGeneration_DatePrefix-8` | 1,760,000+ | **711.6 ns/op** | **256 B/op** | **5 allocs/op** | YYYY/MM/DD prefix layout |
| `BenchmarkBucket_Get-8` | 1,530,000+ | **735.4 ns/op** | **450 B/op** | **5 allocs/op** | **Low-level direct key read (Bucket)** |
| `BenchmarkClient_Get_ByObjectID-8` | 1,000,000+ | **1,124 ns/op** | **739 B/op** | **5 allocs/op** | Logical ID resolution + read (Client) |
| `BenchmarkBucket_Put-8` | 484,000+ | **2,335 ns/op** | **1,537 B/op** | **12 allocs/op** | **Low-level direct wire upload (3.3x faster)** |
| `BenchmarkClient_Put_Standalone-8` | 163,000+ | **7,706 ns/op** | **3,234 B/op** | **33 allocs/op** | MIME sniff + SHA256 TeeReader + KeyGen |
| `BenchmarkClient_Put_WithRegistry-8` | 120,000+ | **9,905 ns/op** | **4,509 B/op** | **36 allocs/op** | Full pipeline + in-memory SQL state machine |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
