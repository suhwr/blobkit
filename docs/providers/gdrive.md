# Google Drive Storage Driver

The `provider/gdrive` package implements a native Go storage driver for the **Google Drive API v3**. It allows applications to utilize Google Drive—both **Personal Google Drive accounts (e.g. Google One 2TB)** and **Enterprise Shared Drives (Team Drives)**—as an elastic object storage backend.

Engineered with direct HTTP REST calls without large external SDK dependencies, it supports OAuth2 User Token refresh functions, Service Account JWT authentication, in-memory LRU key caching, `appProperties` metadata storage, byte-range streaming, and resumable multipart uploads.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Native Drive API v3** | Yes | Communicates directly with `www.googleapis.com/drive/v3`; zero external Google SDK dependencies. |
| **Personal Google Drive (Google One)** | Yes | Fully supported via OAuth2 User Refresh Token (`TokenFunc`), utilizing the user's personal quota (e.g. 2TB). |
| **Shared Drives (Team Drives)** | Yes | Fully supports Google Workspace Shared Drives with `supportsAllDrives=true` and `includeItemsFromAllDrives=true`. |
| **Flat Key Mapping via `appProperties`** | Yes | Maps arbitrary storage paths (`media/2026/10/video.mp4`) inside `FolderID` via native `blobkit_key` appProperties. |
| **Logical Object ID Preservation** | Yes | Preserves BlobKit's logical `ObjectID` (`blobkit_id` in `appProperties`) to prevent registry conflicts in SQL databases. |
| **Resumable Multipart Uploads** | Yes | Implements Google Drive's chunked resumable upload protocol (multiples of 256 KiB) with chunk status recovery. |
| **In-Memory LRU Key Cache** | Yes | Caches key-to-fileID mappings with configurable capacity and automatic stale-cache 404 recovery. |
| **Byte-Range Retrieval** | Yes | Supports partial media downloads and seeking via standard HTTP `Range` headers. |

---

## Configuration Reference

```go
type Config struct {
    // Name is the driver identifier (e.g. "gdrive", "gdrive-backup"). Defaults to "gdrive".
    Name string

    // FolderID is the ID of the Google Drive folder or Shared Drive where all objects reside.
    // Required.
    FolderID string

    // HTTPClient is an optional preconfigured HTTP client. If nil, a client with Timeout=0 is used.
    HTTPClient *http.Client

    // TokenFunc is a function that supplies fresh OAuth2 bearer tokens (recommended for production).
    TokenFunc func(ctx context.Context) (string, error)

    // BearerToken is a static OAuth2 token (primarily for testing or short-lived CLI tasks).
    BearerToken string

    // ChunkSize is the chunk size in bytes for resumable multipart uploads.
    // Must be an exact multiple of 256 KiB (MinChunkSize). Defaults to 8 MiB (DefaultChunkSize).
    ChunkSize int

    // SupportsAllDrives indicates whether the driver should interact with Google Workspace Shared Drives.
    // Defaults to true.
    SupportsAllDrives bool

    // PublicBaseURL is an optional base URL for public CDN/proxy access (e.g. "https://cdn.example.com").
    PublicBaseURL string

    // KeyCacheCapacity is the capacity of the in-memory key-to-fileID cache.
    // Defaults to 10,000. Set to -1 to disable caching.
    KeyCacheCapacity int
}
```

---

## Production Setup & Code Examples

### 1. Personal Google Drive (Google One 2TB) via OAuth2 Refresh Token

> [!IMPORTANT]
> When uploading to a personal `@gmail.com` Google Drive, you **must** authenticate via an OAuth2 User Token (using a refresh token from the Google account owning the quota). Google Cloud Service Accounts have **0 MB** storage quota by default in personal drives, which causes `403 storageQuotaExceeded` errors even if the folder is shared with the service account.

```go
package main

import (
    "context"
    "fmt"
    "strings"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/gdrive"
)

func main() {
    ctx := context.Background()

    // Create a dynamic token provider using OAuth2 user credentials
    tokenSource := NewOAuthTokenSource(clientID, clientSecret, refreshToken)

    driver, err := gdrive.NewDriver(gdrive.Config{
        Name:              "gdrive-personal",
        FolderID:          "1NAmklWOFHvDEJ1HV5jAeSbKmFpYfRZ5z", // Target folder ID
        TokenFunc:         tokenSource.Token,
        SupportsAllDrives: true,
        ChunkSize:         8 * 1024 * 1024, // 8 MiB chunks
    })
    if err != nil {
        panic(err)
    }

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Upload object
    obj, err := client.Put(ctx, strings.NewReader("Hello BlobKit!"), blobkit.PutOptions{
        Namespace: "uploads",
        Filename:  "sample.txt",
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Uploaded: %s (Key: %s)\n", obj.ID, obj.Key)
}
```

---

### 2. Google Workspace Shared Drive (Team Drive) via Service Account

In Google Workspace Shared Drives, files belong to the organization rather than individual uploaders. A Google Cloud Service Account can be used directly without personal quota restrictions:

1. Create a Service Account in the Google Cloud Console.
2. Grant the Service Account email **Content Manager** or **Contributor** permission inside the target Shared Drive.
3. Use the Shared Drive ID (or a folder ID within it) as `FolderID`.

```go
package main

import (
    "context"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/gdrive"
)

func main() {
    ctx := context.Background()

    saTokenSource := NewServiceAccountTokenSource("path/to/service-account.json")

    driver, err := gdrive.NewDriver(gdrive.Config{
        Name:              "gdrive-shared",
        FolderID:          "0AJk2...ABC", // Google Shared Drive ID
        TokenFunc:         saTokenSource.Token,
        SupportsAllDrives: true,
    })
    if err != nil {
        panic(err)
    }

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }
    _ = client
}
```

---

## Architecture: Flat Key Mapping & `appProperties`

Unlike hierarchical file systems, BlobKit stores all objects directly inside `FolderID` using Google Drive's native `appProperties`:
- `blobkit_key`: The canonical storage key (e.g. `media/2026/10/video.mp4`).
- `blobkit_id`: The canonical logical `ObjectID` assigned by BlobKit (UUIDv7).

This flat architecture offers several major advantages:
1. **O(1) Direct Key Lookup**: Files are discovered via exact search `appProperties has { key='blobkit_key' and value='...' }` without recursive folder tree traversal.
2. **In-Memory LRU Cache**: Avoids repeated Google Drive API list calls on high-frequency reads (`Get` and `Head`).
3. **Registry Consistency**: Retaining `blobkit_id` ensures relational database metadata stores (PostgreSQL, SQLite) maintain primary key integrity across pending and committed states.

---

## Error Handling & Quota Limits

- `404 Not Found` $\rightarrow$ Converted to `blobkit.ErrObjectNotFound`. Stale cache entries are automatically evicted and retried once.
- `403 storageQuotaExceeded` $\rightarrow$ Returned when uploading with a Service Account into a personal drive. Switch to OAuth2 User Token.
- `403 rateLimitExceeded` / `userRateLimitExceeded` $\rightarrow$ Google Drive enforces standard per-user request rate limits (queries per minute). Wrap with backoff retry if necessary.
- `Byte-Range Streaming` $\rightarrow$ Full support for `Range: bytes=start-end` enables video seeking and partial downloads without downloading the whole object.
