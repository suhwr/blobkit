# Azure Blob Storage Driver

The `provider/azure` package implements a lightweight, native Go driver for **Microsoft Azure Blob Storage**. It connects directly to Azure's REST API without requiring heavy, transitive vendor SDK dependencies.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Native Block Blobs** | Yes | Full support for Azure Block Blobs, including streaming single-part and staged chunked uploads. |
| **Zero Heavy SDKs** | Yes | Engineered with pure Go standard library HTTP clients; zero external Azure SDK bloat. |
| **Auto Block Chunking** | Yes | Automatically stages chunked blocks (`Put Block`) and commits them (`Put Block List`) for large payloads. |
| **SharedKey HMAC-SHA256** | Yes | Cryptographically authenticates every REST call using Azure SharedKey HMAC-SHA256 signatures. |
| **SAS URL Presigning** | Yes | Native Account SAS token generator for secure time-limited direct client uploads and downloads. |
| **Byte-Range Retrieval** | Yes | Native HTTP `x-ms-range` support for partial reads and high-speed multi-threaded downloads. |
| **Server-Side Copy** | Yes | Direct cloud-side replication using the Azure `x-ms-copy-source` header. |
| **Azurite Emulator Support** | Yes | Out-of-the-box local testing with Microsoft's official Azurite emulator. |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (e.g. "azure-westeurope").
    Name string

    // AccountName is the Azure Storage Account identifier.
    AccountName string

    // AccountKey is the Base64-encoded primary or secondary account access key.
    AccountKey string

    // Container is the target blob container name.
    Container string

    // CustomEndpoint allows overriding the default *.blob.core.windows.net endpoint.
    // Essential for Azure China, Azure Government, or local Azurite emulator.
    CustomEndpoint string

    // PublicBaseURL specifies an Azure Front Door or custom CDN URL for public delivery.
    PublicBaseURL string

    // PartSize is the chunk size in bytes for staging blocks (default 8 MiB).
    PartSize int64

    // MultipartThreshold is the byte size threshold triggering block staging (default 32 MiB).
    MultipartThreshold int64

    // HTTPClient is an optional custom HTTP client with configured proxies or TLS certificates.
    HTTPClient *http.Client
}
```

---

## Production Setup & Code Examples

### 1. Connecting to Azure Cloud

```go
package main

import (
    "context"
    "fmt"
    "strings"
    "time"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/azure"
)

func main() {
    ctx := context.Background()

    driver, err := azure.NewDriver(azure.Config{
        Name:               "azure-primary",
        AccountName:        "mystorageaccount",
        AccountKey:         "kE4x...==", // Base64 Account Key
        Container:          "app-blobs",
        MultipartThreshold: 32 * 1024 * 1024, // 32 MiB
        PartSize:           8 * 1024 * 1024,  // 8 MiB
        PublicBaseURL:      "https://media.mycompany.com", // Optional CDN/Front Door
    })
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Direct stream upload
    payload := "Enterprise document stored securely on Microsoft Azure Blob Storage."
    obj, err := client.Put(ctx, strings.NewReader(payload), blobkit.PutOptions{
        Namespace: "legal",
        Filename:  "contract.pdf",
        Size:      int64(len(payload)),
        Metadata: map[string]string{
            "tier": "confidential",
        },
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Uploaded Blob: %s (Key: %s, ETag: %s)\n", obj.ID, obj.Key, obj.ETag)
}
```

### 2. Local Development with Azurite Emulator

You can develop and run end-to-end integration tests without cloud credentials using Azurite:

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

Configure BlobKit to point to the local emulator:

```go
driver, err := azure.NewDriver(azure.Config{
    Name:           "azurite-dev",
    AccountName:    "devstoreaccount1",
    AccountKey:     "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", // Well-known Azurite key
    Container:      "dev-container",
    CustomEndpoint: "http://127.0.0.1:10000/devstoreaccount1",
})
```

---

## Memory Bounded Streaming Architecture

Unlike simple wrappers that read the entire payload into a memory buffer before uploading, BlobKit implements memory-bounded streaming:
1. **Known Size $\le$ MultipartThreshold**: Streamed directly across the wire in a single HTTP `PUT` request with `Content-Length`.
2. **Payload $>$ MultipartThreshold or Unknown Size**: Uploaded via auto-chunking using Azure Block Blobs:
   - Parts are read in fixed `PartSize` chunks (default 8 MiB).
   - Each chunk is committed via `PUT /container/blob?comp=block&blockid=...`.
   - Once the stream finishes, all block IDs are committed in a single atomic `PUT /container/blob?comp=blocklist` call.
   - Peak RAM usage is strictly bounded to `PartSize`, regardless of whether the file is 100 MB or 1 TB.

---

## Shared Access Signatures (SAS) Presigning

BlobKit computes RFC-compliant Account SAS tokens directly in Go using HMAC-SHA256:

```go
// Generate download URL valid for 1 hour
presignedGet, err := client.PresignGet(ctx, obj.ID, blobkit.PresignOptions{
    Expiry: 1 * time.Hour,
})
if err != nil {
    return err
}
// Returns: https://mystorageaccount.blob.core.windows.net/app-blobs/legal/contract.pdf?sp=r&se=2026-10-07T12:00:00Z&sv=2020-10-04&sr=b&sig=...

// Generate upload URL valid for 20 minutes
presignedPut, err := client.PresignPut(ctx, "uploads/avatar.png", blobkit.PresignOptions{
    Expiry:      20 * time.Minute,
    ContentType: "image/png",
})
```

---

## Error Normalization & Security

The Azure driver converts native XML/HTTP status codes to standard BlobKit errors:
- `404 ResourceNotFound` / `BlobNotFound` $\rightarrow$ `blobkit.ErrObjectNotFound`.
- `404 ContainerNotFound` $\rightarrow$ `blobkit.ErrBucketNotFound`.
- `412 ConditionNotMet` $\rightarrow$ `blobkit.ErrPreconditionFailed`.
- `403 AuthenticationFailed` $\rightarrow$ `blobkit.WrapError(...)` (sensitive signature query params redacted).
- `Path Traversal Attempts` $\rightarrow$ Keys containing `..`, null bytes, or leading slashes are immediately rejected with `blobkit.ErrSecurityViolation`.
