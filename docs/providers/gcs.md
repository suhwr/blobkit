# Google Cloud Storage (GCS) Driver

The `provider/gcs` package implements a native, zero-SDK Go driver for **Google Cloud Storage** using the official Google Cloud Storage JSON API v1 (`storage.googleapis.com/storage/v1`).

Engineered without Google Cloud client library dependencies, it provides automated Service Account JWT token management, aligned resumable chunking, RSA V4 Signed URLs, and memory-bounded uploads.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Native JSON API v1** | Yes | Communicates directly with Google's HTTP REST endpoints; zero external GCP dependencies. |
| **Service Account JWT Auth** | Yes | Parses GCP Service Account JSON keys and exchanges RSA-signed JWTs with Google OAuth2 endpoints. |
| **Aligned Resumable Uploads** | Yes | Implements Google's 256 KiB chunk alignment requirement for reliable resumable streams. |
| **V4 Signed URLs** | Yes | Generates standard GCP V4 signed download and upload URLs using RSA-SHA256 PKCS#1 v1.5. |
| **Direct Put Streaming** | Yes | Streams smaller payloads directly across the wire without buffering entire files in memory. |
| **Server-Side Rewrite/Copy** | Yes | Replicates objects across buckets or keys using Google's native `rewriteTo` API. |
| **Byte-Range Retrieval** | Yes | Standard HTTP `Range: bytes=start-end` requests for media streaming and resumed downloads. |
| **Automatic Token Refresh** | Yes | Re-authenticates and caches OAuth2 access tokens ahead of their 1-hour expiry. |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (e.g. "gcs-prod").
    Name string

    // Bucket is the GCS bucket identifier.
    Bucket string

    // ServiceAccountJSON contains raw JSON content from a GCP Service Account key.
    ServiceAccountJSON []byte

    // ServiceAccountFile is the filesystem path to a GCP Service Account key file.
    ServiceAccountFile string

    // PublicBaseURL specifies a custom domain or Cloud CDN URL for public asset delivery.
    PublicBaseURL string

    // ChunkSize specifies the chunk buffer size for resumable uploads (must be a multiple of 256 KiB, default 8 MiB).
    ChunkSize int64

    // MultipartThreshold is the byte size threshold triggering resumable uploads (default 16 MiB).
    MultipartThreshold int64

    // HTTPClient is an optional custom HTTP client.
    HTTPClient *http.Client
}
```

---

## Production Setup & Code Examples

### 1. Initializing the Driver with Service Account Key

```go
package main

import (
    "context"
    "fmt"
    "strings"
    "time"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/gcs"
)

func main() {
    ctx := context.Background()

    driver, err := gcs.NewDriver(gcs.Config{
        Name:               "gcs-us-central1",
        Bucket:             "my-company-gcs-bucket",
        ServiceAccountFile: "/etc/secrets/gcp-service-account.json",
        MultipartThreshold: 16 * 1024 * 1024,      // 16 MiB
        ChunkSize:          8 * 1024 * 1024,       // 8 MiB (aligned to 256 KiB)
        PublicBaseURL:      "https://cdn.example.com",
    })
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Upload an object
    payload := "Enterprise report stored natively on Google Cloud Storage."
    obj, err := client.Put(ctx, strings.NewReader(payload), blobkit.PutOptions{
        Namespace: "analytics",
        Filename:  "report.csv",
        Size:      int64(len(payload)),
        ContentType: "text/csv",
        Metadata: map[string]string{
            "department": "data-platform",
        },
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Uploaded Object: %s (Key: %s)\n", obj.ID, obj.Key)
}
```

---

## Memory Bounded Resumable Upload Architecture

Google Cloud Storage strictly requires that every chunk uploaded via a resumable upload session must have a size divisible by **256 KiB (262,144 bytes)**, except for the final terminating chunk.

BlobKit manages this lifecycle automatically:
1. **Payload $\le$ MultipartThreshold with known size & no custom metadata**:
   - Streamed via standard direct upload: `POST https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={key}`.
2. **Payload $>$ MultipartThreshold, unknown size, or containing custom metadata**:
   - Initiates a resumable upload session: `POST ...?uploadType=resumable`.
   - Google responds with a dedicated `Location` session URI.
   - The stream is consumed in chunks aligned to `ChunkSize` (default 8 MiB = 32 $\times$ 256 KiB).
   - Each chunk sends a `Content-Range: bytes START-END/TOTAL` header.
   - Peak RAM usage never exceeds `ChunkSize`.

---

## Google V4 Signed URLs

BlobKit generates official Google V4 Signed URLs by creating a canonical request and signing it with the Service Account's RSA private key:

```go
// Generate download URL valid for 30 minutes
presignedGet, err := client.PresignGet(ctx, obj.ID, blobkit.PresignOptions{
    Expiry: 30 * time.Minute,
})
if err != nil {
    return err
}
// Outputs: https://storage.googleapis.com/my-bucket/analytics/report.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=...&X-Goog-Date=...&X-Goog-Expires=1800&X-Goog-SignedHeaders=host&X-Goog-Signature=...

// Generate upload URL valid for 15 minutes
presignedPut, err := client.PresignPut(ctx, "uploads/dataset.parquet", blobkit.PresignOptions{
    Expiry:      15 * time.Minute,
    ContentType: "application/octet-stream",
})
```

---

## Error Normalization & Security

The GCS driver maps Google API HTTP error responses to standardized sentinels:
- `404 Not Found`: `blobkit.ErrObjectNotFound` or `blobkit.ErrBucketNotFound`.
- `412 Precondition Failed`: `blobkit.ErrPreconditionFailed`.
- `403 Forbidden` / `401 Unauthorized`: Mapped to `blobkit.WrapError(...)`.
- `Context Cancellation`: If the caller context expires, active HTTP requests and socket connections terminate immediately.
- `Key Traversal Protection`: `..`, `\r`, `\x00`, and leading `/` characters are blocked before making remote HTTP calls.
