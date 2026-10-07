# BlobKit Engineering Governance & Quality Standards

This document establishes permanent architectural rules, testing requirements, and governance mechanisms for any contribution to `github.com/suhwr/blobkit`.

---

## 1. New Provider Rules

Implementing `blobkit.Driver` is necessary, but **not sufficient**, to add a storage backend. Every new provider **MUST**:
1. **Pass the Conformance Suite**: Execute `testutil.RunDriverContractTests` without skips for advertised capabilities.
2. **Behavioral Capability Rule**: A provider **MUST NOT** advertise a capability bit in `Capabilities()` until that capability passes positive, negative, edge-case, and cancellation behavioral tests.
3. **Provider-Specific Error Mapping**: Map native errors to BlobKit sentinels (`ErrObjectNotFound`, `ErrPermissionDenied`, `ErrInvalidCredentials`, `ErrRateLimited`, `ErrPreconditionFailed`).
4. **Credential Sanitization**: Pass all native error messages through `blobkit.SanitizeErrorMessage`.
5. **Rollback on Short Reads**: If an incoming stream yields fewer bytes than declared, execute rollback deletion immediately and preserve `ErrSizeMismatch`.
6. **Provider Quirks Documentation**: Document consistency model, metadata limits, multipart chunk size constraints, and retry semantics.

---

## 2. New Feature Rules

Every new feature **MUST** define:
1. **API Design & Syntax**: Idiomatic Go interfaces.
2. **Semantic Specification**: Exact behavioral invariants under RFC 2119.
3. **Failure Semantics**: What happens when an operation fails at each stage?
4. **Concurrency Semantics**: Thread safety guarantees under `-race`.
5. **Cancellation Semantics**: Immediate context propagation and resource cleanup.
6. **Resource Ownership**: Ownership of streams, memory buffers, and temporary files.
7. **Provider Compatibility**: Impact across all supported storage backends.
8. **Registry & Router Impact**: How registry metadata and routing policies interact.
9. **Traceable Test Suite**: Tests mapped to explicit Contract IDs.
10. **Documentation**: Clear guide and API reference.

---

## 3. Router & Failover Rules

1. **Failure Classification**: Routers **MUST** distinguish between transient failures and permanent client errors using `blobkit.IsPermanent(err)`.
2. **No False Failovers**: Client errors (`404 Not Found`, `400 Bad Request`, `412 Precondition Failed`, `403 Forbidden`) **MUST NOT** trip circuit breakers or cascade failover to backup providers.
3. **Ambiguous Write Handling**: A router **MUST NOT** silently convert an ambiguous write (network drop during `Put`) into a definitive failure and retry against another provider without verifying idempotent overwriting.
4. **Cooldown & Canary Probes**: Circuit breakers **MUST** use bounded cooldowns and canary probes (`CircuitHalfOpen`) before resuming traffic to primary backends.

---

## 4. Registry & Persistence Rules

1. **Atomic State Transitions**: Object records **MUST** follow strict lifecycle states: `StatePending` -> `StateCommitted` -> `StateDeleted` / `StateAborted`.
2. **Uniqueness & Collisions**: Key collisions in registries **MUST** be handled deterministically.
3. **Dialect Equivalence**: Different SQL dialects (PostgreSQL, SQLite) **MUST** pass identical test suites without semantic differences.
4. **Reconciliation Compatibility**: Every registry **MUST** support status filtering and reconciliation queries (`Filter{Status: ...}`).

---

## 5. Cache Rules

1. **Non-Authoritative Boundary**: Cache state is strictly ephemeral and **MUST NOT** become authoritative over database or storage state.
2. **Stampede Shield**: Concurrent cache misses on identical keys **MUST** be collapsed via `singleflightGroup`.
3. **Negative Caching**: Missing objects (`ErrObjectNotFound`) should be negatively cached with short TTLs to protect backends against scans.
4. **Explicit Invalidation**: Write, delete, and rollback operations **MUST** invalidate both primary key and logical object ID cache entries.

---

## 6. Contract ID Catalog

Every critical test in BlobKit is traceable to a permanent Contract ID:

| Contract ID | Name | Behavioral Guarantee | Verification Location |
|:---:|---|---|---|
| **PUT-001** | Put Lifecycle | Streams payload, preserves bytes, returns valid ETag and size | `testutil/contract.go:Contract_Put_Get_Head_Delete` |
| **PUT-002** | Zero-Byte Object | Supports creating, storing, and retrieving 0-byte objects | `testutil/contract.go:Contract_EmptyObject` |
| **PUT-003** | Exact Size Accounting | Fails with `ErrSizeMismatch` on short-read or surplus | `testutil/contract.go:Contract_SizeMismatch` |
| **GET-001** | Get Payload | Streams byte-exact content matching upload | `testutil/contract.go:Contract_Put_Get_Head_Delete` |
| **GET-002** | Missing Object | Returns `ErrObjectNotFound` when key does not exist | `testutil/contract.go:Contract_Put_Get_Head_Delete` |
| **RANGE-001** | Byte-Range Get | Retrieves requested subslice for HTTP Range requests | `testutil/contract.go:Contract_ByteRangeGet` |
| **COND-001** | ETag Preconditions | Evaluates `If-Match` / `If-None-Match` with strong comparison | `testutil/contract.go:Contract_ConditionalRequests` |
| **COND-002** | Timestamp Preconditions | Evaluates `If-Modified-Since` with 1-second precision | `testutil/contract.go:Contract_ConditionalRequests` |
| **COPY-001** | Server-Side Copy | Copies object without downloading; source unchanged | `testutil/contract.go:Contract_Copy` |
| **DEL-001** | Idempotent Delete | Deleting non-existent or deleted key returns `nil` | `testutil/contract.go:Contract_DeleteIdempotent` |
| **BATCH-001** | Batch Deletion | Concurrently deletes keys; returns deleted list and error | `testutil/contract.go:Contract_BatchDelete` |
| **LIST-001** | Prefix & Delimiter | Filters by prefix; groups common prefixes by delimiter | `testutil/contract.go:Contract_List` |
| **MP-001** | Multipart Lifecycle | Resumable chunk upload and validation of part numbers | `testutil/contract.go:Contract_MultipartSession` |
| **SEC-001** | Path Traversal Shield | Rejects directory traversal, null bytes, control chars | `testutil/contract.go:Contract_PathTraversalAndSecurity` |
| **SEC-002** | Symlink Confinement | Blocks symlink escapes outside root storage directory | `provider/fs/path.go` |
| **ERR-001** | Error Sanitization | Redacts credentials, tokens, and internal IPs from errors | `fuzz_test.go:FuzzSanitizeErrorMessage` |
| **FAILOVER-001** | Permanent Error Filter | Permanent client errors do not trip router failovers | `blobkit_test.go:TestErrorClassification_PermanentVsTransient` |
| **STAMPEDE-001** | Singleflight Shield | Concurrent cache-miss reads collapse into 1 driver lookup | `blobkit_test.go:TestClient_CacheStampedeSingleflight` |
| **RECON-001** | State Reconciliation | Audits and repairs ghost records and orphan storage objects | `reconcile_test.go:TestClient_Reconcile` |
| **RB-001** | Rollback on Failure | Deletes physical blob on registry failure or checksum mismatch | `rollback_test.go:TestRollback_Put_RegistrySaveFailure` |

---

## 7. No Test Theater Principle

The objective of BlobKit testing is **independently proven behavior**, not test count or line coverage.
- **Reject Constant Assertions**: Tests asserting `assert(caps & CapX != 0)` or `assert(cfg.Val == expected)` are prohibited unless accompanied by behavioral execution.
- **Independent Oracles**: Test oracles must be independent of the code under test (e.g. independently computed SHA-256 hashes, separate client reads, filesystem inode inspection).
- **Verify Failure Paths**: Every feature must test failure, rollback, and cancellation paths alongside happy paths.

---

## 8. Definition of Done for Future Contributions

A contribution is **NOT complete** until:
- [ ] Behavior is formally specified with RFC 2119 terminology.
- [ ] Public contract impact is analyzed and documented.
- [ ] Provider capability impact is updated and behaviorally proven.
- [ ] Multi-step failure and rollback paths are defined and tested.
- [ ] Context cancellation and timeout propagation are verified.
- [ ] Concurrency and thread safety are verified under `go test -race ./...`.
- [ ] Independent test oracles are used for all assertions.
- [ ] Mocks accurately model provider failure and consistency semantics.
- [ ] Fuzzing invariants are meaningful and tested.
- [ ] Documentation claims match actual implementation guarantees.
- [ ] Security implications (path traversal, credential scrubbing) are reviewed.
- [ ] Resource usage is bounded (no unbounded RAM buffering or goroutine leaks).
- [ ] All tests pass without skips or conditional weakening.
- [ ] Contract IDs are assigned to all major tests.
