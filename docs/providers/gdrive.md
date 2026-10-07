# Google Drive Storage Driver

The `provider/gdrive` package implements a native Go driver for **Google Drive API v3**. It allows applications to utilize Google Drive—including **Enterprise Shared Drives (Team Drives)**—as an elastic object storage backend.

Engineered with direct HTTP REST calls without large external SDKs, it supports OAuth2 Service Account authentication, hierarchical folder resolution, `appProperties` metadata storage, and resumable multipart uploads.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Native Drive API v3** | Yes | Communicates directly with `www.googleapis.com/drive/v3`; zero Google SDK dependencies. |
| **Shared Drives (Team Drives)** | Yes | Fully supports Google Workspace Shared Drives with `supportsAllDrives=true` and `includeItemsFromAllDrives=true`. |
| **Folder Hierarchy Auto-Resolution** | Yes | Automatically maps path keys (e.g. `media/2026/avatar.png`) to Drive folder hierarchies, creating missing folders on demand. |
| **Resumable Multipart Uploads** | Yes | Implements Google Drive's chunked resumable upload protocol for large files. |
| **Custom Metadata via `appProperties`**| Yes | Stores user metadata inside Google Drive's native `appProperties` dictionary. |
| **OAuth2 & Service Accounts** | Yes | Supports both static OAuth2 Access Tokens and automated Service Account JWT token exchange. |
| **Byte-Range Retrieval** | Yes | Supports partial media downloads via standard HTTP `Range` headers. |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (e.g. "gdrive-shared").
    Name string

    // ServiceAccountJSON contains raw JSON content from a GCP Service Account key.
    ServiceAccountJSON []byte

    // ServiceAccountFile is the filesystem path to a GCP Service Account key file.
    ServiceAccountFile string

    // AccessToken is an optional pre-generated OAuth2 Bearer token (alternative to Service Account).
    AccessToken string

    // RootFolderID is the Google Drive Folder ID used as the storage root.
    // In Shared Drives, this is the top-level Shared Drive ID or a dedicated folder ID within it.
    RootFolderID string

    // ChunkSize specifies chunk size in bytes for resumable uploads (default 8 MiB, multiple of 256 KiB).
    ChunkSize int64

    // PublicBaseURL provides a custom CDN or proxy URL for public file access.
    PublicBaseURL string

    // HTTPClient is an optional custom HTTP client.
    HTTPClient *http.Client
}
```

---

## Production Setup & Code Examples

### 1. Connecting to an Enterprise Shared Drive (Team Drive)

To store objects inside a Google Workspace Shared Drive:
1. Create a Service Account in the Google Cloud Console.
2. Grant the Service Account email address **Content Manager** or **Contributor** permission inside the target Shared Drive.
3. Configure the driver with the Shared Drive's Folder ID:

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

    driver, err := gdrive.NewDriver(gdrive.Config{
        Name:               "gdrive-team",
        ServiceAccountFile: "/secrets/gsuite-service-account.json",
        RootFolderID:       "0AJk2...ABC", // Google Shared Drive ID from browser URL
        ChunkSize:          8 * 1024 * 1024,
    })
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Upload an object into a structured namespace
    payload := "Confidential sales report stored in Google Shared Drive."
    obj, err := client.Put(ctx, strings.NewReader(payload), blobkit.PutOptions{
        Namespace: "finance/q4",
        Filename:  "sales.xlsx",
        Size:      int64(len(payload)),
        ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        Metadata: map[string]string{
            "department": "sales",
            "confidential": "true",
        },
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("File Stored in Google Drive: %s (Key: %s, Drive File ID: %s)\n", obj.ID, obj.Key, obj.ETag)
}
```

---

## Folder Tree Resolution & In-Memory Caching

Google Drive is not a key-value object store; it is a graph of files and folders linked by parent IDs.

BlobKit translates standard object keys (e.g. `avatars/users/2026/img.jpg`) into Drive hierarchy automatically:
1. It splits the path into components: `avatars` $\rightarrow$ `users` $\rightarrow$ `2026`.
2. Checks an internal thread-safe cache for the folder ID of each directory level.
3. If an intermediate directory does not exist, it queries Drive via `files.list` with `mimeType = 'application/vnd.google-apps.folder'` and `trashed = false`.
4. If missing, it creates the folder via `files.create` and caches the resulting folder ID.
5. The final file is uploaded with `parents = [folderID]`.

---

## User Metadata via `appProperties`

Google Drive supports private application-specific metadata via the `appProperties` JSON field.
BlobKit automatically maps `PutOptions.Metadata` into `appProperties`, allowing metadata inspection and querying without modifying file content or creating auxiliary database entries.

---

## Error Handling & Rate Limiting

- `404 Not Found` $\rightarrow$ `blobkit.ErrObjectNotFound`.
- `403 rateLimitExceeded` / `userRateLimitExceeded` $\rightarrow$ Returns wrapped error indicating temporary quota exhaustion.
- `Context Cancellation` $\rightarrow$ Ongoing resumable chunks terminate immediately when the caller context expires.
- `Key Traversal Protection` $\rightarrow$ Keys containing `..`, null bytes, or carriage returns are blocked locally.
