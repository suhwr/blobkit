# BlobKit Enterprise Guarantees & Correctness Model

This document outlines the enterprise-grade reliability, security, correctness, and contract guarantees provided by BlobKit across all operations.

---

## 1. Data Integrity & Exact Byte Accounting

BlobKit guarantees zero silent data truncation, zero data corruption, and exact byte accounting across all streaming and single-part uploads:

- **SizeReader Invariant**: When an upload declares an explicit payload size (`opts.Size >= 0`), BlobKit streams through a verified `SizeReader`. If the stream produces fewer bytes than declared, the upload is aborted immediately with `blobkit.ErrSizeMismatch`. If surplus bytes remain after reading the declared length, the upload fails with `blobkit.ErrSizeMismatch`.
- **Zero Partial Artifacts**: On any upload failure, size mismatch, or context cancellation, drivers execute automatic rollback routines (e.g. `driver.Delete` or filesystem removal) ensuring no orphaned partial objects remain in storage.
- **Client & Transport Checksums**: BlobKit provides streaming SHA-256 and MD5 validation. Uploads configured with `VerifyIntegrity` or `ClientChecksum` verify cryptographic hashes before committing records in the registry.

---

## 2. Path Traversal & File System Confinement

BlobKit enforces a multi-layered defense-in-depth shield against directory traversal and path injection attacks:

- **Strict Key Validation**: Every key passed to `Put`, `Get`, `Head`, `Delete`, `Copy`, or `CreateMultipart` is validated by `blobkit.ValidateKey`.
- **Recursive Percent-Encoding Defense**: Path traversal attacks employing single, double, or multi-encoded patterns (e.g. `%2e%2e`, `%252e%252e`, `%2f`, `%5c`) are recursively decoded up to 3 layers and rejected with `blobkit.ErrSecurityViolation`.
- **Character Filtering**: Keys containing ASCII control characters (`0x00`-`0x1F`, `0x7F`), Unicode bidi override formatting characters (`\u202A`–`\u202E`, `\u2066`–`\u2069`), and fullwidth punctuation (`\uFF0F`, `\uFF3C`) are rejected immediately.
- **POSIX Symlink Confinement**: The POSIX filesystem driver resolves symlinks via `filepath.EvalSymlinks` against the canonical repository root, ensuring symlink escapes outside the configured storage directory are strictly blocked.

---

## 3. High Availability & Concurrency Correctness

- **Cache Stampede Prevention**: BlobKit integrates an in-process, zero-dependency `singleflightGroup` into `Client.Head`. Concurrent read requests targeting cold or expired cache entries are collapsed into a single backend lookup, shielding object storage drivers from thundering herds.
- **Resilient Circuit Breakers & Failover**: BlobKit routers (`FailoverRouter`, `CircuitBreakerRouter`, `TieredRouter`) distinguish between transient infrastructure errors and permanent client errors via `blobkit.IsPermanent(err)`:
  - **Permanent Client Errors** (`ErrObjectNotFound`, `ErrInvalidKey`, `ErrPreconditionFailed`, `ErrPermissionDenied`, `ErrInvalidCredentials`, `ErrSizeMismatch`) do **not** trip circuit breakers or trigger failovers to secondary backends.
  - **Transient Errors** (rate limits, 5xx server errors, timeouts) trip circuits and cleanly fail over to healthy replica backends.
- **Deadlock-Free Concurrency**: All state locks across registry stores, multipart session trackers, and caches maintain strict acquisition orders and short critical sections.

---

## 4. State Machine & Registry Reconciliation

- **Lifecycle Transition Integrity**: Objects progress through a deterministic state machine (`StateCreated` -> `StateUploading` -> `StateCommitted`). Read operations (`resolveTarget`) reject uncommitted states (`StatePending`, `StateAborted`, `StateFailed`).
- **Reconciliation Engine**: `Client.Reconcile(ctx, dryRun)` provides enterprise-grade data auditing:
  - **Ghost Detection**: Identifies records marked committed in the database but missing from physical storage backends, and optionally transitions them to `StateDeleted`.
  - **Orphan Detection**: Identifies physical objects stored on disk or cloud backends that lack committed database records.
  - **Size Mismatch Auditing**: Flags inconsistencies where physical object byte size contradicts the database registry.

---

## 5. Security & Credential Confidentiality

- **Automated Error Sanitization**: All error messages returned by `blobkit.StorageError` and drivers pass through `blobkit.SanitizeErrorMessage`:
  - **Secret Tokens**: AWS access keys, Bearer tokens, HTTP Basic authentication headers, and URL signatures (`token=`, `sig=`, `api_key=`) are redacted with `[redacted]`.
  - **Private Keys**: PEM private key blocks (`-----BEGIN ... PRIVATE KEY-----`) are redacted with `[redacted private key]`.
  - **Network Topology**: Internal RFC 1918 IPv4 addresses (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), loopback addresses (`127.0.0.1`, `[::1]`), and private IPv6 addresses are scrubbed with `[scrubbed-address]`.

---

## 6. Contract Compliance & Conformance Suite

BlobKit ships with a comprehensive test utility package (`testutil`) ensuring 100% contract compliance across all drivers:
- **Contract Conformance Suite**: `testutil.RunDriverContractTests` evaluates 13 core operational contracts across CRUD, Range, Copy, Batch, List, Security, Cancellation, Multipart, Empty Objects, Size Mismatch Rollback, and RFC 7232 Preconditions.
- **Differential Testing**: `testutil.TestDifferential_MemoryVsFS` guarantees cross-provider parity between memory and filesystem drivers.
- **Fault Injection Driver**: `testutil.FaultDriver` provides deterministic latency, partial read, bit flip, and error injection for resilience verification.
- **Property & Fuzz Testing**: Comprehensive fuzz testing covering `ValidateKey`, `SanitizeErrorMessage`, and `CheckPreconditions`, alongside property tests enforcing exact size invariants.
