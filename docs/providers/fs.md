# Local POSIX Filesystem Storage Driver

The `provider/fs` package implements a production-grade local filesystem driver for Go. It allows applications to utilize local NVMe disks, block volumes (AWS EBS, GCP Persistent Disks), Network File Systems (NFS), or edge compute storage as standard BlobKit object storage backends.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Atomic Writes** | Yes | Writes incoming streams to temporary files before performing atomic rename swaps to prevent partial/corrupt files. |
| **Strict Chroot Containment** | Yes | Rejects path traversal escapes (`../`), root escapes, null bytes, and non-canonical paths. |
| **Sidecar Metadata** | Yes | Optional `.meta.json` sidecar files persist Content-Type, ETags, SHA-256 hashes, and user metadata alongside blobs. |
| **Fast Byte-Range Seeking** | Yes | Utilizes OS-level `io.ReadSeeker` and file seeks for zero-overhead HTTP Range requests. |
| **Chunked Staging Multipart** | Yes | Stages multipart parts into isolated staging folders before sequentially concatenating them into final files. |
| **Thread-Safe Worker Pool** | Yes | Concurrently deletes keys using a bounded worker pool (up to 16 parallel goroutines). |
| **Zero External Dependencies**| Yes | Pure Go standard library (`os`, `io`, `filepath`); zero runtime bloat. |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (e.g. "fs-local", "edge-storage").
    Name string

    // RootDir is the absolute or relative base directory on disk where blobs are stored.
    RootDir string

    // StagingDir is the folder name inside RootDir used for multipart chunks (default ".staging").
    StagingDir string

    // DirMode specifies UNIX directory permission bits (default 0755).
    DirMode os.FileMode

    // FileMode specifies UNIX file permission bits (default 0644).
    FileMode os.FileMode

    // EnableSidecarMeta enables persistence of object metadata in *.meta.json files.
    EnableSidecarMeta bool

    // PublicBaseURL provides an HTTP base URL for public download resolution (e.g. "https://static.example.com").
    PublicBaseURL string
}
```

---

## Production Setup & Code Examples

### 1. Initializing Local Storage with Sidecar Metadata

```go
package main

import (
    "context"
    "fmt"
    "strings"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/fs"
)

func main() {
    ctx := context.Background()

    driver, err := fs.NewDriver(fs.Config{
        Name:              "local-nvme",
        RootDir:           "/data/storage",
        EnableSidecarMeta: true,
        DirMode:           0755,
        FileMode:          0644,
        PublicBaseURL:     "https://media.local.net",
    })
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Atomic stream upload
    payload := "Locally stored mission-critical payload on NVMe drive."
    obj, err := client.Put(ctx, strings.NewReader(payload), blobkit.PutOptions{
        Namespace: "firmware/v2",
        Filename:  "update.bin",
        Size:      int64(len(payload)),
        ContentType: "application/octet-stream",
        Metadata: map[string]string{
            "release": "stable",
            "build":   "20261007",
        },
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("File saved atomically: %s (Path: %s)\n", obj.ID, obj.Key)
}
```

---

## Atomic Write Guarantee

Network interruptions, power losses, or ungraceful service terminations during standard file writing often leave zero-byte or corrupt files on disk.

BlobKit protects your data integrity:
1. Streams are written into a hidden temporary file in the destination folder (`.tmp_put_*`).
2. MD5 and SHA-256 hashes are calculated on-the-fly during write.
3. The temporary file is flushed, closed, and permissions set (`FileMode`).
4. `os.Rename(tmpFile, targetPath)` performs an atomic filesystem inode swap.
5. If the upload is cancelled or encounters an error, the temporary file is deleted immediately.

---

## Security & Strict Chroot Containment

The filesystem driver enforces hard boundaries to ensure untrusted user inputs never escape `RootDir`:
- **Path Cleaning**: Normalizes delimiters (`/` and `\`) and cleans double-slashes.
- **Prefix Confinement**: Computes the absolute path and verifies that it strictly begins with `RootDir + Separator`.
- **Directory Traversal**: Keys containing `..`, `\x00`, or `\r` are immediately blocked with `blobkit.ErrSecurityViolation`.
- **System File Protection**: Direct user access to `.meta.json` files or `.staging` directories is rejected.

---

## Sidecar Metadata Architecture

When `EnableSidecarMeta: true` is configured, saving key `photos/cat.jpg` automatically produces:
- Physical Blob: `/data/storage/photos/cat.jpg`
- Metadata JSON: `/data/storage/photos/cat.jpg.meta.json`

The sidecar file stores:
```json
{
  "id": "01925b3a-...",
  "key": "photos/cat.jpg",
  "content_type": "image/jpeg",
  "etag": "\"a1b2c3d4...\"",
  "checksum_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "metadata": {
    "release": "stable"
  },
  "created_at": "2026-10-07T08:00:00Z",
  "updated_at": "2026-10-07T08:00:00Z"
}
```
When `Head` or `Get` is called, BlobKit reads this sidecar file to serve cached ETags, MIME types, and user metadata instantly without re-hashing the payload.
When `Delete` or `Copy` is executed, the sidecar metadata file is cleaned up or duplicated automatically.
