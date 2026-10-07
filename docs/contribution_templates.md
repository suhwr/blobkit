# BlobKit Contribution Templates

Use these templates when proposing new storage providers, new features, or modifications to the BlobKit Driver/Client contract. Every pull request or design document must adhere to the corresponding template.

---

## 1. New Provider Contribution Template

```markdown
# Provider Proposal: [Provider Name] (e.g. Backblaze B2, Ceph, MinIO, OCI Object Storage)

## 1. Executive Summary & Protocol Overview
- **Storage Service**: [Name of cloud or on-premise storage service]
- **Protocol / SDK**: [Underlying transport or SDK, e.g. REST API, gRPC, native Go SDK]
- **Package Path**: `provider/[provider_name]`

## 2. Capabilities Declaration
List every capability flag from `blobkit.Capabilities` that this driver declares, along with proof of support:
- [ ] `CapByteRangeGet`: [Yes/No - Range HTTP header or offset read support]
- [ ] `CapConditionalGet`: [Yes/No - ETag (If-Match/If-None-Match) or timestamp preconditions]
- [ ] `CapConditionalPut`: [Yes/No - If-None-Match: * or upload preconditions]
- [ ] `CapCopy`: [Yes/No - Native server-side copy without local stream round-trip]
- [ ] `CapBatchDelete`: [Yes/No - Atomic or bulk multi-object delete endpoint]
- [ ] `CapList`: [Yes/No - Prefix filtering and delimiter grouping support]
- [ ] `CapMultipart`: [Yes/No - Resumable chunked multipart upload session support]
- [ ] `CapPresignGet`: [Yes/No - Pre-signed download URL generation]
- [ ] `CapPresignPut`: [Yes/No - Pre-signed upload URL generation]

> **Rule**: Do NOT declare a capability flag unless accompanied by behavioral tests verifying both success and failure/unsupported paths.

## 3. Storage Consistency Model
- **Read-After-Write Consistency**: [Strong immediate / Eventual (propagation delay)]
- **Read-After-Delete Consistency**: [Strong immediate / Ghost tombstone window]
- **List Consistency**: [Strong / Eventual]
- **Overwrite Semantics**: [Atomic replacement / Truncate-then-write / In-place mutation]

## 4. Checksums & ETags
- **ETag Semantics**: [Exact hex-encoded MD5 / SHA-256 / Multipart composite with hyphen e.g. `<hash>-<parts>` / Opaque version token]
- **Checksum Verification**: [Native Content-MD5 / SHA-256 header / Client-side manual check]
- **Short-Read Handling**: [Rollback deletion executed if stream yields fewer bytes than declared: Yes/No]

## 5. Multipart Upload Constraints
- **Minimum Part Size**: [e.g. 5 MiB, 8 MiB, or none]
- **Maximum Part Size**: [e.g. 5 GiB]
- **Maximum Number of Parts**: [e.g. 10,000]
- **Finalization Semantics**: [Atomic commit / Manifest compilation]
- **Abort Semantics**: [Physical chunk cleanup on abort: Yes/No]

## 6. Pre-signed URLs
- **Expiry Bounds**: [Minimum and maximum allowed expiry duration]
- **Header Constraints**: [Headers required at pre-sign time vs. execution time]

## 7. Error Mapping & Sanitization
- **Mapping Table**:
  - Native 404 / NoSuchKey -> `blobkit.ErrObjectNotFound`
  - Native 403 / AccessDenied -> `blobkit.ErrPermissionDenied`
  - Native 401 / InvalidCredentials -> `blobkit.ErrInvalidCredentials`
  - Native 429 / 503 SlowDown -> `blobkit.ErrRateLimited`
  - Native 412 / PreconditionFailed -> `blobkit.ErrPreconditionFailed`
- **Credential Scrubbing**: [Passes native errors through `blobkit.SanitizeErrorMessage`: Yes/No]

## 8. Provider Quirks & Behavioral Edge Cases
- Document any platform-specific peculiarities (e.g. leading slash requirements, directory object markers, metadata header size limits, case sensitivity, timestamp resolution).

## 9. Retry & Idempotency Safety
- **Safe Operations**: [e.g. Get, Head, Delete]
- **Non-Idempotent Operations**: [e.g. Multipart Complete if parts expired, Conditional Put]

## 10. Conformance Suite Verification
- [ ] Runs `testutil.RunDriverContractTests(t, driver, advertisedCaps)`.
- [ ] Zero tests skipped (`t.Skip` without documented issue link prohibited).
- [ ] Concurrency verified under `go test -race ./provider/[name]/...`.
```

---

## 2. New Feature Contribution Template

```markdown
# Feature Proposal: [Feature Name]

## 1. Problem Statement & Motivation
- **Problem**: What pain point or capability gap does this feature address?
- **User Story**: As a developer using BlobKit, I want to [...] so that [...].

## 2. Public API Design
- **New Types / Methods**:
```go
// Proposed signatures
```
- **Backward Compatibility**: [Fully backward compatible / Opt-in via Options / Breaking change]

## 3. Semantic Specification (RFC 2119)
- The Client/Driver **MUST** [...]
- The Client/Driver **SHOULD** [...]
- The Client/Driver **MUST NOT** [...]

## 4. Multi-Step Failure & Rollback Paths
- **Failure Matrix**:
  - Stage 1 (Pre-validation): If invalid, returns error before network/driver calls.
  - Stage 2 (Physical I/O): If driver fails, clean up local resources and return mapped error.
  - Stage 3 (Registry / State Commit): If registry commit fails after physical write:
    - How is the physical object rolled back?
    - How is the registry state transitioned to `StateAborted`?
  - Stage 4 (Cache Invalidation): How are stale cache entries purged?

## 5. Cancellation & Resource Semantics
- **Context Cancellation**: Does context cancellation immediately abort active I/O, goroutines, and network calls?
- **Resource Ownership**: Who closes readers? Are temp buffers returned to `sync.Pool`?

## 6. Concurrency & Race Safety
- **Goroutine Safety**: Describe concurrent access patterns.
- **Race Verification**: Must pass `go test -race -count=5` with zero data races.

## 7. Security Implications
- **Path Traversal**: Does this handle keys? Are paths sanitized against `../`, null bytes, and control chars?
- **Error Redaction**: Are any secrets, tokens, or endpoints potentially leaked in errors?

## 8. Performance & Resource Boundaries
- **Memory Overhead**: Is memory consumption bounded (no `io.ReadAll` on arbitrary streams)?
- **Goroutine Bounding**: Are concurrent workers bounded by worker pools or semaphores?

## 9. Contract ID & Test Plan
- **Contract IDs**: Assign Contract IDs for new behavioral guarantees (e.g. `FEAT-001`).
- **Tests**:
  - Positive functional tests
  - Negative error handling tests
  - Timeout and context cancellation tests
  - Concurrent access tests
```

---

## 3. Contract Change Contribution Template

```markdown
# Contract Change Proposal: [Contract Change Title]

## 1. Target Contract ID(s)
- **Affected Contract ID(s)**: [e.g. PUT-001, RANGE-001, BATCH-001]
- **Target Interface**: [`blobkit.Driver`, `blobkit.Registry`, `blobkit.Router`, or `blobkit.Cache`]

## 2. Motivation & Architectural Rationale
- Why is the existing contract insufficient or flawed?
- What real-world failure mode or limitation necessitates this change?

## 3. Proposed Specification Delta (RFC 2119)
- **Current Behavior**: [...]
- **Proposed Behavior**: [...]

## 4. Impact Analysis
- **Breaking Change Classification**: [Major breaking change / Minor additive change]
- **Affected Providers**: List all providers that must update their implementations:
  - `provider/s3`: [Impact & required changes]
  - `provider/gcs`: [Impact & required changes]
  - `provider/azure`: [Impact & required changes]
  - `provider/fs`: [Impact & required changes]
  - `provider/memory`: [Impact & required changes]
  - `provider/sftp`: [Impact & required changes]
  - `provider/webdav`: [Impact & required changes]
  - `provider/gdrive`: [Impact & required changes]
- **Registry Impact**: [Postgres, SQLite, Memory]
- **Router Impact**: [Failover, Tiered, Hash, Replicated]

## 5. Migration Strategy & Compatibility Window
- How do existing consumers migrate without data loss or downtime?
- Is there a deprecation period or compatibility shim?

## 6. Proof Tests & Conformance Suite Updates
- Which tests in `testutil/contract.go` are modified or added?
- How is compliance proven across all in-tree providers?
```
