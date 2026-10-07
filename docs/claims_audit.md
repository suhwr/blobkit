# BlobKit Documentation & Guarantees Claims Audit

This document audits all strong technical claims made across BlobKit's documentation, specifications, and code comments. Every claim has been extracted, mapped to its underlying implementation, evaluated against independent test evidence, and rigorously classified.

---

## 1. Classification Taxonomy

- **PROVEN**: Verified by automated behavioral tests, deterministic test oracles, and the Go race detector (`-race`).
- **CONDITIONALLY TRUE**: The claim holds under specific OS, filesystem, or provider API conditions, but is not unconditional.
- **PARTIALLY PROVEN**: Verified for common failure modes, but subject to fundamental distributed systems boundaries (e.g. CAP theorem, dual-write limits).
- **IMPLEMENTATION-DEPENDENT**: Depends entirely on the underlying storage provider engine.
- **UNPROVEN**: The code intends this behavior, but independent automated behavioral evidence is incomplete.
- **FALSE**: Marketing exaggeration or technically inaccurate claim (must be eliminated or rewritten).

---

## 2. Comprehensive Claims Matrix

| Claim ID | Claimed Statement | Subsystem / Location | Classification | Implementation & Verification Evidence | Audit Finding & Action Taken |
|:---:|---|---|:---:|---|---|
| **CLM-001** | Atomic file replacement on overwrite | POSIX FS Driver (`provider/fs`) | **PROVEN** | Staged in `os.CreateTemp` and swapped via `os.Rename`. Verified by `Contract_Put_Get_Head_Delete` and `TestDifferential_MemoryVsFS`. | POSIX guarantees `rename(2)` atomicity within the same filesystem mount. Validated. |
| **CLM-002** | Remote atomic file writes | SFTP Driver (`provider/sftp`) | **CONDITIONALLY TRUE** | Staged in `.tmp.*` and renamed via `client.Rename`. Verified by `TestSFTPDriver_Contract`. | Atomic on OpenSSH/Linux SFTP servers supporting POSIX rename extension. May fail on Windows SFTP servers if target exists. Downgraded in SFTP docs. |
| **CLM-003** | Atomic multipart commit | Azure Blob Storage (`provider/azure`) | **PROVEN** | All staged blocks committed in single `PUT /container/blob?comp=blocklist` call. Verified by `Contract_MultipartSession`. | Azure Storage engine guarantees all-or-nothing transactional block commit. Validated. |
| **CLM-004** | Atomic multi-key batch deletion | Batch Deletion (`driver.go`, `blobkit.go`) | **FALSE / WEAKENED** | S3 `DeleteObjects` or loop over drivers. | Batch deletes in cloud object stores are **not** ACID transactions; individual key deletions can fail independently. Updated contract and `blobkit.DeleteBatch` to handle partial failures explicitly. |
| **CLM-005** | Idempotent object deletion | All Drivers (`driver.go:Delete`) | **PROVEN** | Deleting non-existent or already-deleted objects returns `nil` error without error. Verified by `Contract_DeleteIdempotent`. | Universal across all drivers. Validated. |
| **CLM-006** | Concurrency safety and thread safety | Memory Registry & Cache (`registry`, `cache`) | **PROVEN** | Synchronized with `sync.RWMutex`. Verified by `go test -race ./...` across all 22 packages with zero data races. | Validated under full race detector execution. |
| **CLM-007** | Cache stampede suppression | Client Read Layer (`singleflight.go`, `blobkit.go`) | **PROVEN** | `singleflightGroup` collapses concurrent cache misses. Verified by `TestClient_CacheStampedeSingleflight` (20 concurrent goroutines -> 1 backend lookup). | Validated. |
| **CLM-008** | Cancellation safety and stream abort | Stream Ingestion (`testutil/contract.go`) | **PROVEN** | Canceled context returns `ctx.Err()` immediately and rolls back partial storage objects. Verified by `Contract_ContextCancellation`. | Validated. |
| **CLM-009** | Dual-write registry & storage consistency | Client Lifecycle (`blobkit.go:Put`, `reconcile.go`) | **PARTIALLY PROVEN** | Two-phase state machine (`StatePending` -> `driver.Put` -> `StateCommitted`). On commit failure, physical blob is rolled back and pending state aborted. Verified by `TestRollback_Put_RegistrySaveFailure`. | CAP theorem prevents strict linearizability across non-transactional dual writes. Eventual consistency is achieved via `Client.Reconcile`. |
| **CLM-010** | Failover resilience to permanent client errors | Router Layer (`router/failover.go`, `router/circuit.go`) | **PROVEN** | Router `ReportFailure` ignores `blobkit.IsPermanent(err)`. Client 404s, invalid keys, and precondition errors do not trip circuits. Verified by `TestFailoverRouter` and `TestCircuitBreakerRouter`. | Validated. |
| **CLM-011** | Zero-allocation circuit breaker | Router Documentation (`README.md:400`) | **FALSE** | `router/circuit.go` uses mutex synchronization and dynamic error wrappers. | Corrected: Phrasing updated to "Automatic 3-state circuit breaker protection". |
| **CLM-012** | Strict symlink confinement | POSIX FS Driver (`provider/fs/path.go`) | **PROVEN** | Canonical path resolved with `filepath.EvalSymlinks` against `realRoot`. Symlinks escaping root return `ErrSecurityViolation`. Verified by `Contract_PathTraversalAndSecurity`. | Validated. |
| **CLM-013** | Credential scrubbing from logs and errors | Error Subsystem (`errors.go`) | **PROVEN** | `SanitizeErrorMessage` redacts AWS keys, tokens, basic/bearer headers, PEM private keys, and RFC 1918 internal IPs. Verified by `FuzzSanitizeErrorMessage` (16,634 iterations) and `TestSanitizeErrorMessage`. | Validated. |
| **CLM-014** | Exact size accounting and short-read abort | Driver Ingestion Layer (`errors.go:ErrSizeMismatch`) | **PROVEN** | `SizeReader` enforces exact byte accounting. Short reads or surplus streams fail with `ErrSizeMismatch` and roll back. Verified by `Contract_SizeMismatch` and `TestProperty_ExactByteLength`. | Validated. |
| **CLM-015** | RFC 7232 HTTP precondition parity | Precondition Evaluation (`preconditions.go`) | **PROVEN** | Strong vs weak ETag matching and 1-second timestamp precision matching. Verified by `Contract_ConditionalRequests` and `FuzzCheckPreconditions` (16,166 iterations). | Validated. |
