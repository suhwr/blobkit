# BlobKit

[![Go Reference](https://pkg.go.dev/badge/github.com/suhwr/blobkit.svg)](https://pkg.go.dev/github.com/suhwr/blobkit)
[![Go Report Card](https://goreportcard.com/badge/github.com/suhwr/blobkit)](https://goreportcard.com/report/github.com/suhwr/blobkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

BlobKit is a high-performance, high-level object storage infrastructure library for Go. Built on top of AWS SDK v2, it provides an idiomatic, provider-agnostic infrastructure layer for applications interfacing with S3-compatible backends—including **Cloudflare R2**, **AWS S3**, **MinIO**, **Wasabi**, and **Backblaze B2**.

BlobKit is intentionally designed as an **infrastructure orchestrator**, not a superficial SDK wrapper. It decouples application business domains from physical storage wire details, manages canonical object identity, enforces bounded memory budgets, routes across multiple providers, and separates physical storage from delivery access.

---

## Architecture & Responsibilities

BlobKit enforces a strict three-tier separation of concerns:

```
+-----------------------------------------------------------------------------------+
| 1. APPLICATION LAYER (Consumer: shiro, shiroine-web, microservices)              |
|    - Declares semantic intent: Namespace, OwnerID, Original Filename, Visibility |
|    - Passes streaming data (io.Reader)                                            |
|    - Discovers objects by metadata attributes without knowing URLs or raw keys    |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 2. BLOBKIT INFRASTRUCTURE LAYER (Orchestration Engine)                            |
|    - Logical ObjectID generation (UUIDv7)                                         |
|    - Structured physical ObjectKey generation (date-prefix, sharded, namespaced)  |
|    - Zero-rewind 512-byte MIME sniffing (io.MultiReader)                          |
|    - Strict bounded memory enforcement (per-upload concurrency & global limiter)  |
|    - Deterministic provider routing & circuit failover                            |
|    - Dynamic access URL delivery (CDN resolver vs presigned URLs)                 |
|    - Metadata Registry coordination (Database source of truth for indexes)        |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| 3. PROVIDER DRIVER LAYER (Storage Abstraction)                                    |
|    - AWS SDK v2 Driver (R2, S3, MinIO) with persistent pooled HTTP/2 transport   |
|    - Pure In-Memory Driver for offline testing and ephemeral workloads            |
|    - Error scrubbing (sanitizes internal proxy dials and secret tokens)           |
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
| **Delivery URL** | `Access URL` | `https://cdn.example.com/...` (CDN) or `https://...?X-Amz-...` | **NO** (Generated on-demand) |

> [!IMPORTANT]
> **URLs are delivery details, never canonical identifiers.** Storing full URLs in databases causes schema rot when domains migrate, buckets change, or certificates rotate. In BlobKit, URLs are resolved dynamically via `ResolveURL` or `PresignGet`.

---

## Features

- **Streaming-First & Zero-Rewind Sniffing**: Uses `io.ReadFull(512B)` + `io.MultiReader` to detect MIME types without buffering entire files in RAM.
- **Bounded Memory Budgets**: Multipart streaming uploads enforce per-upload worker concurrency (default 2 workers, 5MB chunks) and a client-wide byte semaphore ceiling (default 64MB) to prevent OOM spikes.
- **Persistent Connection Pooling**: The S3 driver configures a tuned HTTP/2 `*http.Transport` with connection pooling, avoiding connection churn under high concurrency.
- **Sanitized Error Boundary**: Internal network topology, private proxy dial failures, and credential tokens are scrubbed from error messages automatically.
- **Multi-Provider Routing**: Supports single primary (`Fixed`), active-passive circuit failover with cooldown probing (`Failover`), and namespace-based routing (`Namespace`).
- **Optional Metadata Registry**: Coordinate two-phase uploads or rich metadata queries (`Find`) by owner, namespace, or custom tags while maintaining the ability to run standalone without a database.

---

## Installation

```bash
go get github.com/suhwr/blobkit
```

---

## Quick Start

### 1. Standalone Direct Upload & Retrieval

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "log"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/key"
    "github.com/suhwr/blobkit/provider/s3"
)

func main() {
    ctx := context.Background()

    // 1. Initialize S3 / Cloudflare R2 driver with connection pooling
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

    // 2. Initialize BlobKit client
    client, err := blobkit.New(
        blobkit.WithDriver(driver),
        blobkit.WithKeyGenerator(key.NewDatePrefixGenerator()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // 3. Application declares semantic intent
    data := bytes.NewReader([]byte("Hello from BlobKit!"))
    obj, err := client.Put(ctx, data, blobkit.PutOptions{
        Namespace: "documents",
        OwnerID:   "usr_123",
        Filename:  "welcome.txt",
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Uploaded ObjectID: %s\n", obj.ID)
    fmt.Printf("Physical Key:      %s\n", obj.Key)
    fmt.Printf("Detected MIME:     %s\n", obj.ContentType)

    // 4. Resolve delivery URL dynamically
    url, err := client.ResolveURL(ctx, obj.Key)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Delivery URL:      %s\n", url)
}
```

---

### 2. Metadata Discovery & Access by ObjectID

When using an enabled metadata registry, applications discover and access blobs by logical attributes without ever managing physical bucket paths:

```go
// 1. Find objects by semantic criteria
objects, err := client.Find(ctx, blobkit.Filter{
    Namespace: "bots/autorespon",
    OwnerID:   "bot_123",
    Status:    blobkit.StateCommitted,
})
if err != nil {
    log.Fatal(err)
}

for _, obj := range objects {
    // 2. Download directly by logical ObjectID
    reader, err := client.Get(ctx, obj.ID, blobkit.GetOptions{})
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    // 3. Or generate a secure time-limited presigned URL
    presigned, err := client.PresignGet(ctx, obj.ID, blobkit.PresignOptions{
        Expiry: 15 * time.Minute,
    })
    fmt.Printf("Presigned URL: %s\n", presigned.URL)
}
```

---

### 3. Namespace-Based Provider Routing

Route different types of objects across different storage backends automatically:

```go
import (
    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/s3"
    "github.com/suhwr/blobkit/router"
)

// Primary fast storage (Cloudflare R2)
r2Driver, _ := s3.NewDriver(s3.Config{Name: "r2-hot", Bucket: "hot-media", ...})

// Cold archival storage (AWS S3 Glacier / Backblaze B2)
b2Driver, _ := s3.NewDriver(s3.Config{Name: "b2-cold", Bucket: "cold-backups", ...})

// Configure namespace router
nsRouter := router.NewNamespaceRouter(r2Driver)
nsRouter.Register("backups", b2Driver)
nsRouter.Register("archives", b2Driver)

client, _ := blobkit.New(blobkit.WithRouter(nsRouter))
```

---

## Benchmarks

Measured on Go 1.27 (Linux x86_64):

| Benchmark | Operations | Speed (ns/op) | Memory (B/op) | Allocs/op |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkMIMESniff` | 1,400,000+ | 765 ns/op | 624 B/op | 3 allocs/op |
| `BenchmarkKeyGeneration_UUIDv7` | 2,500,000+ | 482 ns/op | 208 B/op | 4 allocs/op |
| `BenchmarkKeyGeneration_DatePrefix` | 1,500,000+ | 817 ns/op | 256 B/op | 5 allocs/op |
| `BenchmarkKeyGeneration_HashSharded` | 1,600,000+ | 857 ns/op | 416 B/op | 7 allocs/op |
| `BenchmarkClient_Get_ByObjectID` | 1,500,000+ | 1,040 ns/op | 610 B/op | 4 allocs/op |
| `BenchmarkClient_Put_Standalone` | 250,000+ | 4,420 ns/op | 2,429 B/op | 16 allocs/op |
| `BenchmarkClient_Put_WithRegistry` | 170,000+ | 6,763 ns/op | 3,374 B/op | 19 allocs/op |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
