# BlobKit Driver Contract (Normative RFC 2119 Specification)

## 1. Scope & Purpose

This document defines the normative contract and conformance requirements for all storage drivers implemented in `github.com/suhwr/blobkit`. The key words **"MUST"**, **"MUST NOT"**, **"REQUIRED"**, **"SHALL"**, **"SHALL NOT"**, **"SHOULD"**, **"SHOULD NOT"**, **"RECOMMENDED"**, **"MAY"**, and **"OPTIONAL"** in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

All `blobkit.Driver` implementations—including Amazon S3, POSIX Filesystem, Azure Blob Storage, Google Cloud Storage, Google Drive, SFTP, WebDAV, and In-Memory—**MUST** conform strictly to these semantics. Implementations **MUST NOT** redefine, weaken, or omit guarantees based on underlying backend idiosyncrasies.

---

## 2. Capabilities Declaration

1. Each driver **MUST** accurately report supported capabilities via `Capabilities() Capability`.
2. A driver **MUST NOT** advertise a capability bitmask flag that it cannot fulfill in strict accordance with this contract.
3. If an optional operation (e.g. `Copy`, `DeleteBatch`, `PresignGet`, `PresignPut`, `CreateMultipart`) is invoked on a driver that lacks the corresponding capability, the driver **MUST** return `blobkit.ErrUnsupportedOperation`.

---

## 3. Storage Primitives & Operational Semantics

### 3.1 Put (`Put(ctx, obj, r, opts)`)

- **Input Validation**:
  - `obj` **MUST** be non-nil and contain a valid storage key conforming to `blobkit.ValidateKey`.
  - `r` **MUST** be non-nil. If `r` is nil, `Put` **MUST** return `blobkit.ErrNilReader`.
- **Exact Size Enforcing**:
  - If `opts.ExplicitSize` is `true` or `opts.Size >= 0`, the stream **MUST** yield exactly `opts.Size` bytes.
  - If `r` reaches EOF before `opts.Size` bytes are read (short read), `Put` **MUST** fail and return `blobkit.ErrSizeMismatch`.
  - If `r` yields surplus bytes beyond `opts.Size`, `Put` **MUST** fail and return `blobkit.ErrSizeMismatch`.
- **Zero-Byte Invariant**:
  - Drivers **MUST** support creating, overwriting, and retrieving valid 0-byte objects (`Size == 0`).
- **Atomic Overwrites & Rollback**:
  - If an object already exists at `obj.Key`, `Put` **MUST** overwrite it cleanly.
  - On failure, network error, size mismatch, or context cancellation, the driver **MUST NOT** leave partially-created or corrupted objects in storage. Any staged temporary objects **MUST** be rolled back immediately.
  - POSIX filesystem implementations **MUST** execute atomic replacement (e.g. via temporary staging file and atomic rename).
- **Concurrency & Cancellation**:
  - `Put` **MUST** be safe for concurrent execution across identical or distinct keys.
  - If `ctx` is canceled or times out before or during upload, `Put` **MUST** abort immediately and return `ctx.Err()`.

### 3.2 Get (`Get(ctx, key, opts)`)

- **Existence**:
  - If `key` does not exist, `Get` **MUST** return `blobkit.ErrObjectNotFound`.
- **Zero-Byte Objects**:
  - For 0-byte objects, `Get` **MUST** return an `ObjectReader` whose `Body` yields 0 bytes and returns `io.EOF`.
- **HTTP Range Requests**:
  - If `opts.Range` is provided and the driver advertises `CapByteRangeGet`, `Get` **MUST** return the exact byte range requested.
- **Conditional Requests (RFC 7232)**:
  - If `opts.IfMatch`, `opts.IfNoneMatch`, `opts.IfModifiedSince`, or `opts.IfUnmodifiedSince` are set, the driver **MUST** evaluate them against the object's current metadata.
  - If any precondition fails, `Get` **MUST** return `blobkit.ErrPreconditionFailed` without streaming object bytes.
  - ETag matching **MUST** respect strong comparison rules: weak ETags (`W/"..."`) **MUST NOT** match strong `If-Match`.
  - Timestamp comparisons **MUST** be evaluated with 1-second precision truncation.

### 3.3 Head (`Head(ctx, key)`)

- **Metadata Retrieval**:
  - `Head` **MUST** inspect object metadata (`Size`, `ETag`, `ContentType`, `UpdatedAt`) without reading or downloading the payload body.
- **Missing Object**:
  - If `key` does not exist, `Head` **MUST** return `blobkit.ErrObjectNotFound`.
- **Zero-Byte Object**:
  - For a 0-byte object, `Head` **MUST** return `Object` with `Size == 0` and `nil` error.

### 3.4 Delete (`Delete(ctx, key)`)

- **Idempotency**:
  - Deleting an object that does not exist or has already been deleted **MUST** succeed and return `nil` error.
- **Concurrency**:
  - `Delete` **MUST** be thread-safe across concurrent callers.

### 3.5 DeleteBatch (`DeleteBatch(ctx, keys)`)

- **Partial Failure Semantics**:
  - If `keys` is empty, `DeleteBatch` **MUST** return an empty list and `nil` error.
  - If some keys are deleted and others fail, `DeleteBatch` **MUST** return the slice of all successfully deleted keys alongside the aggregated error.

### 3.6 Copy (`Copy(ctx, srcKey, dstKey)`)

- **Source Object**:
  - If `srcKey` does not exist, `Copy` **MUST** return `blobkit.ErrObjectNotFound`.
  - The source object **MUST** remain unchanged.
- **Destination Object**:
  - The destination `dstKey` **MUST** be created or overwritten.

### 3.7 List (`List(ctx, opts)`)

- **Filtering & Grouping**:
  - `opts.Prefix` **MUST** filter objects starting with the specified prefix.
  - If `opts.Delimiter` is set (e.g. `/`), matching keys containing the delimiter beyond the prefix **MUST** be grouped in `CommonPrefixes`.
- **Pagination**:
  - If more objects remain beyond `opts.Limit`, `IsTruncated` **MUST** be `true` and `NextCursor` **MUST** be set. Subsequent calls passing `opts.Cursor = NextCursor` **MUST** resume pagination deterministically without skipping or duplicating items.

---

## 4. Multipart State Machine

Drivers supporting `CapMultipartSession` **MUST** enforce the following lifecycle state transitions:

```
                  ┌──────────────┐
                  │   Created    │
                  └──────┬───────┘
                         │ UploadPart(...) [partNumber >= 1]
                         ▼
                  ┌──────────────┐
       ┌──────────┤  In-Progress │──────────┐
       │          └──────┬───────┘          │
       │                 │                  │
       │ Abort           │ Complete         │ Abort
       ▼                 ▼                  ▼
┌──────────────┐  ┌──────────────┐   ┌──────────────┐
│   Aborted    │  │  Completed   │   │   Aborted    │
└──────────────┘  └──────────────┘   └──────────────┘
```

1. **State Isolation**: Calling `UploadPart`, `CompleteMultipart`, `AbortMultipart`, or `ListParts` with an invalid, expired, or already-finalized `uploadID` **MUST** return `blobkit.ErrSessionNotFound` or `blobkit.ErrMultipartInvalidState`.
2. **Part Number Invariants**: Part numbers **MUST** be $\ge 1$.
3. **Part Size Verification**: Each uploaded part **MUST** yield exactly `size` bytes. Short or overflow reads **MUST** fail immediately with `blobkit.ErrSizeMismatch`.
4. **Completion Validation**: When calling `CompleteMultipart`, parts **MUST** be validated:
   - Parts list **MUST NOT** be empty.
   - Parts **MUST NOT** contain duplicate part numbers.
   - All part numbers **MUST** be positive integers.
5. **Staging Cleanup**: On completion or abort, all temporary chunk files and staging buffers **MUST** be cleaned up completely.

---

## 5. Security & Isolation Invariants

1. **Key Confinement**: Every driver **MUST** invoke `blobkit.ValidateKey(key)`. Keys containing path traversal sequences (`..`), leading slashes, null bytes, backslashes, control characters, Unicode bidi overrides, or percent-encoded variations **MUST** be rejected with `blobkit.ErrInvalidKey` or `blobkit.ErrSecurityViolation`.
2. **Filesystem Confinement**: Local POSIX drivers **MUST** evaluate symlinks using `filepath.EvalSymlinks` and confirm that target paths resolve within the configured root directory. Any attempt to escape the root boundary via symlinks or path manipulation **MUST** return `blobkit.ErrSecurityViolation`.
3. **Credential Sanitization**: Driver error messages **MUST NOT** leak credentials, AWS keys, secret tokens, private keys, bearer headers, or internal IP addresses. All errors returned to clients **MUST** be processed through `blobkit.SanitizeErrorMessage`.

---

## 6. Error Classification

Drivers **MUST** map native SDK errors to standardized BlobKit sentinels:

| Category | Sentinels | `IsPermanent(err)` | Failover Action |
|---|---|:---:|---|
| **Client Input** | `ErrObjectNotFound`, `ErrInvalidKey`, `ErrInvalidID`, `ErrInvalidFilename`, `ErrUploadTooLarge`, `ErrNilReader`, `ErrUnsupportedOperation` | `true` | Do **NOT** retry; do **NOT** failover |
| **Integrity & Preconditions** | `ErrPreconditionFailed`, `ErrChecksumMismatch`, `ErrSizeMismatch`, `ErrSecurityViolation`, `ErrMIMEMismatch` | `true` | Do **NOT** retry; do **NOT** failover |
| **Authentication & AuthZ** | `ErrPermissionDenied`, `ErrInvalidCredentials`, `ErrObjectLocked`, `ErrMultipartInvalidState` | `true` | Do **NOT** retry; do **NOT** failover |
| **Transient & Infrastructure** | `ErrRateLimited`, `ErrProviderUnavailable`, `ErrQuotaExceeded`, network timeouts, 5xx server errors | `false` | **Retry** with backoff; trigger router **failover** |
