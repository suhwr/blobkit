# SFTP / Remote SSH Storage Driver

The `provider/sftp` package implements an enterprise-grade driver for remote **SFTP (SSH File Transfer Protocol)** servers. It enables Go microservices to read and write objects to remote Linux/Unix hosts, storage appliances, and secure enterprise jumpboxes as native BlobKit storage.

Built on `golang.org/x/crypto/ssh` and `github.com/pkg/sftp`, it provides connection pooling, remote atomic file writes, sidecar metadata persistence, and byte-range seeking.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **SSH Key & Password Auth** | Yes | Supports OpenSSH/RSA/ED25519 private keys (with passphrase support) and password authentication. |
| **Resilient Connection Pooling** | Yes | Thread-safe connection pool manages persistent SSH channels and reconnects upon transient drops. |
| **Remote Atomic Writes** | Yes | Streams incoming files to remote temporary files before executing atomic remote renames. |
| **Remote Sidecar Metadata** | Yes | Persists `.meta.json` sidecar files on the remote server for MIME type, hash, and metadata retention. |
| **Remote Byte-Range Seeking** | Yes | Uses SFTP `Seek` calls for partial content reads and video streaming without downloading full files. |
| **Recursive Directory Creation** | Yes | Automatically creates missing remote directories before saving nested files. |
| **Strict Remote Path Containment**| Yes | Prevents remote directory escape attacks (`../../etc/passwd`). |

---

## Configuration Reference

```go
type Config struct {
    // Name is the unique identifier for this driver instance (e.g. "sftp-backup-dc1").
    Name string

    // Host specifies the remote SSH hostname or IP address.
    Host string

    // Port specifies the SSH port (default 22).
    Port int

    // User specifies the SSH username.
    User string

    // Password specifies an optional SSH login password.
    Password string

    // PrivateKey contains raw PEM-encoded private key data (RSA, ECDSA, ED25519).
    PrivateKey []byte

    // PrivateKeyFile specifies the filesystem path to an SSH private key.
    PrivateKeyFile string

    // KeyPassphrase decrypts password-protected private keys.
    KeyPassphrase string

    // BaseDir specifies the remote root directory for blob storage (e.g. "/var/data/blobs").
    BaseDir string

    // MaxIdleConns sets the maximum number of idle SFTP connections in the pool (default 4).
    MaxIdleConns int

    // ConnectTimeout specifies connection timeout (default 10s).
    ConnectTimeout time.Duration

    // InsecureIgnoreHostKey allows skipping SSH host key verification (testing only).
    InsecureIgnoreHostKey bool

    // EnableSidecarMeta enables remote persistence of *.meta.json files.
    EnableSidecarMeta bool

    // PublicBaseURL provides an optional HTTP CDN or reverse-proxy URL.
    PublicBaseURL string
}
```

---

## Production Setup & Code Examples

### 1. Connecting via SSH Private Key (ED25519 / RSA)

```go
package main

import (
    "context"
    "fmt"
    "strings"
    "time"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/provider/sftp"
)

func main() {
    ctx := context.Background()

    driver, err := sftp.NewDriver(sftp.Config{
        Name:              "sftp-datacenter",
        Host:              "backup.corp.internal",
        Port:              22,
        User:              "blob_operator",
        PrivateKeyFile:    "/etc/ssh/id_ed25519",
        BaseDir:           "/mnt/backups/blobkit",
        EnableSidecarMeta: true,
        MaxIdleConns:      8,
        ConnectTimeout:    10 * time.Second,
    })
    if err != nil {
        panic(err)
    }
    defer driver.Close()

    client, err := blobkit.New(blobkit.WithDriver(driver))
    if err != nil {
        panic(err)
    }

    // Stream upload directly to remote host
    payload := "Encrypted database backup stream transferred via secure SFTP."
    obj, err := client.Put(ctx, strings.NewReader(payload), blobkit.PutOptions{
        Namespace: "postgres/daily",
        Filename:  "backup_20261007.sql.gz",
        Size:      int64(len(payload)),
        ContentType: "application/gzip",
        Metadata: map[string]string{
            "database": "production",
        },
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("File Saved via SFTP: %s (Remote Key: %s)\n", obj.ID, obj.Key)
}
```

---

## Remote Connection Pooling & Reconnect Resiliency

Establishing SSH handshakes and cryptographic key exchanges for every storage operation introduces unacceptable latency (typically 100–300ms per call).

BlobKit maintains an active pool of open SFTP clients:
- High-concurrency operations lease connections from the pool.
- Health checks verify socket liveness before returning connections to callers.
- If a remote server closes an SSH session due to an idle timeout, the pool automatically evicts the broken client and re-establishes a fresh SSH handshake.

---

## Remote Atomic File Commits

To ensure network drops mid-stream do not corrupt remote files:
1. BlobKit creates `.tmp_put_*` in the target directory on the remote server.
2. The payload is streamed and cryptographic hashes calculated.
3. Upon stream completion, an atomic remote `client.Rename(tmpPath, finalPath)` is executed.
4. If an error or context cancellation occurs, the remote temporary file is removed immediately.

---

## Error Handling & Security

- `404 Not Found`: SFTP `os.ErrNotExist` is normalized into `blobkit.ErrObjectNotFound`.
- `Path Traversal`: Remote keys are resolved strictly within `BaseDir`. Traversal attempts (`../`) are blocked locally before executing SFTP commands.
- `Credential Scrubbing`: SSH private keys, passphrases, and passwords are automatically scrubbed from error logs and stack traces.
