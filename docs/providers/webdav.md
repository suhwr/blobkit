# WebDAV Protocol Storage Driver

The `provider/webdav` package implements an enterprise-grade RFC 4918 WebDAV storage driver. It enables modern Go applications to treat **Nextcloud**, **ownCloud**, **TrueNAS CORE/SCALE**, **Synology NAS**, **QNAP**, and self-hosted **Nginx / Apache WebDAV** instances as native object storage backends.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **RFC 4918 Compliance** | Yes | Implements standard WebDAV methods: `PUT`, `GET`, `HEAD`, `DELETE`, `MKCOL`, `PROPFIND`, `COPY`, `MOVE`. |
| **Recursive Collection Auto-Creation** | Yes | Automatically executes `MKCOL` to construct missing parent folders before uploading nested objects. |
| **MKCOL Hierarchy Cache** | Yes | Remembers known directory trees in memory to eliminate redundant `MKCOL` roundtrips. |
| **Authentication Modes** | Yes | Supports standard HTTP Basic authentication as well as modern OAuth2 / App Bearer Tokens. |
| **PROPFIND XML Parsing** | Yes | Parses WebDAV `207 Multi-Status` XML envelopes to extract metadata, sizes, ETags, and timestamps. |
| **Native Remote Copy** | Yes | Replicates files using the WebDAV `COPY` method and `Destination` header without local downloads. |
| **Byte-Range Retrieval** | Yes | Supports standard HTTP byte ranges for streaming media and seeking large files. |
| **Path Traversal Defense** | Yes | Sanitizes keys and blocks directory traversal escapes (`../`). |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (e.g. "nextcloud-main").
    Name string

    // Endpoint is the full WebDAV root URL (e.g. "https://cloud.example.com/remote.php/dav/files/user").
    Endpoint string

    // Username for HTTP Basic Authentication.
    Username string

    // Password or App Token for HTTP Basic Authentication.
    Password string

    // BearerToken for token-based authentication (alternative to Username/Password).
    BearerToken string

    // Prefix is an optional folder path prepended to all keys (e.g. "blobkit-data/").
    Prefix string

    // PublicBaseURL provides a custom CDN or public download endpoint.
    PublicBaseURL string

    // HTTPClient is an optional custom HTTP client with custom timeouts or TLS certificates.
    HTTPClient *http.Client
}
```

---

## Production Setup & Code Examples

### 1. Connecting to Nextcloud / ownCloud

Nextcloud exposes WebDAV endpoints under `/remote.php/dav/files/{username}/`. Use an App Password generated under *Personal Settings $\rightarrow$ Security* instead of your primary user password.

```go
package main

import (
    "context"
    "fmt"
    "strings"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/webdav"
)

func main() {
    ctx := context.Background()

    driver, err := webdav.NewDriver(webdav.Config{
        Name:     "nextcloud-storage",
        Endpoint: "https://nextcloud.mycompany.com/remote.php/dav/files/admin/",
        Username: "admin",
        Password: "app-token-generated-in-settings",
        Prefix:   "ProductionBlobs",
    })
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Upload an object nested deeply inside folders
    // WebDAV driver will automatically create "ProductionBlobs/invoices/2026/10/" via MKCOL
    payload := "Invoice #10294 Details..."
    obj, err := client.Put(ctx, strings.NewReader(payload), blobkit.PutOptions{
        Namespace: "invoices/2026/10",
        Filename:  "INV-10294.txt",
        Size:      int64(len(payload)),
        ContentType: "text/plain",
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Stored in Nextcloud: %s (Key: %s)\n", obj.ID, obj.Key)
}
```

### 2. Connecting to TrueNAS / ZFS WebDAV Share

TrueNAS CORE and SCALE include built-in WebDAV service sharing ZFS datasets:

```go
driver, err := webdav.NewDriver(webdav.Config{
    Name:     "truenas-zfs",
    Endpoint: "http://nas.local:8080/tank/blobs",
    Username: "backup_user",
    Password: "secure_password",
})
```

---

## Directory Auto-Creation (`MKCOL`)

Standard HTTP `PUT` requests to WebDAV servers fail with `409 Conflict` if parent collections do not exist.

BlobKit eliminates this operational issue:
1. When uploading key `reports/q3/financial.pdf`, BlobKit checks its in-memory collection cache.
2. If intermediate folders are not yet confirmed, it issues sequential `MKCOL` requests starting from the top level:
   - `MKCOL /reports/`
   - `MKCOL /reports/q3/`
3. Successfully created collections are cached in an internal thread-safe hash map, ensuring subsequent uploads to `reports/q3/*` require zero redundant HTTP calls.

---

## Error Handling & Sanitization

- `404 Not Found` $\rightarrow$ `blobkit.ErrObjectNotFound`.
- `412 Precondition Failed` $\rightarrow$ `blobkit.ErrPreconditionFailed`.
- `401 Unauthorized` / `403 Forbidden` $\rightarrow$ Wrapped without leaking credentials.
- `Authentication Scrubbing` $\rightarrow$ Passwords and `Authorization: Basic ...` headers are scrubbed from error logs.
- `Context Cancellation` $\rightarrow$ Context timeouts cleanly close open HTTP sockets.
