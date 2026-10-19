# Lode Storage Adapter — Contract

This document defines the required semantics for storage adapters.
It is authoritative for any implementation of the `Store` interface.

---

## Goals

1. Safe, immutable writes.
2. Accurate existence checks and listings.
3. Backend-agnostic behavior.

---

## Adapter Obligations

### Put

**Intent**: Put creates a new object. It MUST NOT silently overwrite existing data.

**Behavior**:
- MUST write data to the given path.
- If an existing path is detected, MUST return `ErrPathExists`.
- Detection mechanism and guarantee vary by upload path (see table below).
- On paths with TOCTOU windows, existence may not be detected under concurrent writers.
- MUST be all-or-nothing: on any error (including an error from the supplied
  reader) the path MUST be left as it was before the call. A Put that created
  an object and then failed MUST remove it. Callers never clean up after a
  failed Put, and MUST NOT touch a path whose Put returned `ErrPathExists` —
  that object belongs to another writer.

#### Put Upload Paths

Adapters MAY route Put through different mechanisms based on payload size.
The no-overwrite guarantee strength depends on the path and backend capabilities:

| Path | Trigger | Detection Mechanism | Guarantee |
|------|---------|---------------------|-----------|
| Atomic | payload ≤ threshold | Conditional write | **Atomic** — no TOCTOU |
| Multipart | payload > threshold | Conditional completion (if supported) or preflight check | **Atomic** (conditional) or **Best-effort** (preflight only) |

**Atomic Path** (≤ threshold):
- MUST use atomic conditional-create where backend supports it.
- Duplicate writes MUST return `ErrPathExists` atomically.
- No coordination required beyond the adapter.
- MAY spool to temp file for memory efficiency; cleanup MUST be automatic.

**Multipart Path** (> threshold):
- Used when backend API limits require chunked uploads.
- SHOULD use conditional completion (e.g., S3 `If-None-Match` on `CompleteMultipartUpload`)
  where the backend supports it for atomic no-overwrite guarantee.
- MAY perform preflight existence check as a fail-fast optimization.
- If conditional completion is not supported, MUST perform preflight existence check.
- If preflight detects existing path, MUST return `ErrPathExists`.
- When only preflight is available: **TOCTOU window** exists between check and completion;
  single-writer semantics or external coordination is REQUIRED.

#### Adapter Documentation Requirements

Adapters MUST document:
- The size threshold for atomic vs multipart routing.
- Which path provides atomic vs best-effort detection.
- Backend-specific limitations (e.g., maximum object size, part limits).

### Get
- MUST return a readable stream for an existing path.
- If the path does not exist, MUST return `ErrNotFound` (or an equivalent error).

### Exists
- MUST accurately report existence.
- MUST not create or mutate data.

### List
- MUST return all paths under the given prefix.
- The prefix is a **string prefix over paths**, not a directory: `a/b` matches
  `a/b/x` and `a/bc`. Callers that mean a directory end the prefix with `/`.
- Ordering is unspecified.
- Pagination behavior (if any) MUST be documented by the adapter.

### Delete
- MUST remove the path if it exists.
- MUST be safe to call on a missing path (idempotent or `ErrNotFound`).

### ReadRange
- MUST return bytes from `[offset, offset+length)` for the given path.
- If the path does not exist, MUST return `ErrNotFound`.
- If offset or length is negative, MUST return `ErrInvalidPath`.
- If length exceeds platform `int` capacity, MUST return `ErrInvalidPath`.
- If offset+length would overflow, MUST return `ErrInvalidPath`.
- If the range extends beyond EOF, MUST return available bytes (not an error).
- If offset is beyond EOF, MUST return an empty slice.
- MUST use true range reads (not whole-file read) where the backend supports it.

### ReaderAt
- MUST return an `io.ReaderAt` for random access reads.
- If the path does not exist, MUST return `ErrNotFound`.
- The returned `ReaderAt` MUST support concurrent reads at different offsets.
- Callers are responsible for closing the underlying resource if it implements `io.Closer`.

---

## Commit Semantics

- Manifests (or explicit commit markers) define visibility.
- Writers MUST write data objects before the manifest.
- Readers MUST treat manifest presence as the commit signal.

## Streamed Writes

- Adapters MUST allow data objects to be written before manifest commit.
- Adapters MUST NOT provide any implicit commit signal outside manifest presence.
- Safe write semantics (no overwrite) apply per Put path guarantees (see "Put Upload Paths").
- Adapters MUST allow deletion of partial objects via `Delete` for cleanup.

### Streaming Write Atomicity

For streaming writes that use the multipart/chunked path:
- The no-overwrite guarantee depends on the Put path used (see "Put Upload Paths").
- On backends without conditional multipart completion, concurrent writers may
  create a race condition where both detect "not exists" and proceed to write.
- Callers using streaming writes on such backends MUST ensure single-writer
  semantics or use external coordination.
- Failure during multipart upload SHOULD trigger best-effort abort/cleanup.
- Cleanup MUST use an independent context (not the caller's potentially-canceled
  context) to maximize cleanup success.

---

## ConditionalWriter Capability

`ConditionalWriter` is an optional interface that storage adapters MAY implement
to enable optimistic concurrency (CAS) on the latest pointer.

```go
type ConditionalWriter interface {
    CompareAndSwap(ctx context.Context, path string, expected string, replacement string) error
}
```

### Semantics

| Path State | Expected | Behavior |
|------------|----------|----------|
| Does not exist | `""` (empty) | Create file with `replacement` content |
| Does not exist | Non-empty | Return `ErrSnapshotConflict` |
| Exists, content == expected | Any | Atomically replace content with `replacement` |
| Exists, content != expected | Any | Return `ErrSnapshotConflict` |

- The operation MUST be atomic: concurrent `CompareAndSwap` calls on the same
  path MUST NOT both succeed.
- `expected == ""` means "path must not exist or be empty" — this handles
  the first-commit edge case.

### Implementation Notes

**Filesystem adapter:**
- Acquire `flock` on a companion `.lock` file adjacent to the target path.
- Under lock: read current content, compare, write replacement.
- Release lock.

**S3 adapter:**
- `GetObject` to read current content and capture ETag.
- `PutObject` with `If-Match` (update) or `If-None-Match: *` (create).
- If the ETag has changed, return `ErrSnapshotConflict`.

**Memory adapter:**
- Mutex-protected read-compare-write.

### Adapter Compatibility

Adapters that do not implement `ConditionalWriter` are valid. The commit
path falls back to Delete+Put and callers must ensure single-writer
semantics as before.

When an adapter does implement `ConditionalWriter`, the commit path uses
`CompareAndSwap` automatically. No caller configuration is required.

---

## Consistency Notes

Adapters MUST document:
- Consistency guarantees for `List` and `Exists`
- Any required read-after-write delays or mitigations

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for full definitions.

### Per-Operation Bounds

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Put` | 1 (atomic) or 1 + ⌈size/partSize⌉ (multipart) | O(payload) |
| `Get` | 1 | O(1) streaming |
| `Exists` | 1 | O(1) |
| `Delete` | 1 | O(1) |
| `ReadRange` | 1 | O(L) |
| `ReaderAt` | 1 setup; 1 per ReadAt call | O(L) per call |
| `List` | ⌈N/page⌉ | **O(N) full materialization** |

### List Full Materialization

`Store.List` returns `[]string`. All matching keys are accumulated in memory.
This is a **documented interface constraint**, not an implementation bug.

Adapters MUST paginate the backend (e.g., S3 `ListObjectsV2` continuation tokens)
but MUST return all results in the returned slice.

Callers MUST scope prefixes as narrowly as possible.
Hot-path operations MUST NOT depend on unbounded List results.
