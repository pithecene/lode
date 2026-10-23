# Lode Storage Adapter — Contract

This document defines the required semantics for storage adapters.
It is authoritative for any implementation of the `Store` interface.

---

## Goals

1. Safe, immutable writes.
2. Accurate existence checks and listings.
3. Behavior independent of the backend.

---

## Adapter Obligations

### Put

**Purpose**: `Put` creates a new object. It MUST NOT overwrite existing data
without reporting it.

**Behavior**:
- MUST write data to the given path.
- Paths are slash-separated on every platform. A path that is empty, escapes
  the root, or contains a backslash MUST return `ErrInvalidPath`; adapters
  MUST NOT read a backslash as a separator. A path whose final segment
  begins with the reserved prefix `.lode-tmp-` MUST also return
  `ErrInvalidPath` on every adapter, because the filesystem adapter names
  its temporary files with that prefix (below).
- If it detects an existing path, it MUST return `ErrPathExists`.
- The detection mechanism and its guarantee vary by upload path (see the
  table below).
- On paths with a time-of-check-to-time-of-use (TOCTOU) interval, an
  existing object may go undetected under concurrent writers.
- It MUST be atomic wherever the adapter knows the outcome. On an error that
  the adapter can observe (including an error from the supplied reader), the
  path MUST be left as it was before the call, and a `Put` that created an
  object and then failed MUST remove it. If that removal itself fails, `Put`
  MUST return an error that reports both failures.
- An object MUST become visible to `Get`, `Exists`, and `List` only once it
  is complete: a concurrent reader observes the whole object or none of it.
  A reader that observed a partial manifest would treat an incomplete commit
  as complete (§Commit Semantics). The filesystem adapter writes the object
  under a temporary name in the same directory and publishes it with a hard
  link, which fails if the path exists; the adapter therefore requires a
  filesystem that supports hard links, and on one that does not, `Put`
  returns an error. A published object has the mode that creating it with
  mode 0644 under the process umask gives. Temporary names begin with the
  reserved prefix `.lode-tmp-`, and `List` omits them. The adapter
  removes the temporary name after publishing; if that removal fails, or the
  process ends first, an inert temporary file remains that no Lode operation
  observes, and an operator may delete it.
- A remote backend can accept an object and then lose the response (through
  a network or context timeout). The adapter MUST NOT attempt to resolve
  such an indeterminate outcome by deleting the key, because it cannot know
  whether the object at the key is its own. The object is then orphaned: no
  manifest references it, and readers cannot see it. The retention sweep
  reclaims it (`CONTRACT_RETENTION.md`).
- Callers never remove anything after a failed `Put`: they MUST NOT access a
  path whose `Put` returned an error. A path whose `Put` returned
  `ErrPathExists` belongs to another writer. The single exception is
  retention's creation of a tombstone, which reads the existing tombstone to
  verify that it records the same expiry and never writes or deletes it
  (`CONTRACT_RETENTION.md` §Order of Operations).

#### Put Upload Paths

Adapters MAY route `Put` through different mechanisms according to the
size of the payload. The strength of the guarantee against overwriting
depends on the path and on the capabilities of the backend:

| Path | Trigger | Detection Mechanism | Guarantee |
|------|---------|---------------------|-----------|
| Atomic | Payload at or below the threshold | Conditional write | **Atomic**: no TOCTOU interval |
| Multipart | Payload above the threshold | Conditional completion (if supported) or a preliminary check | **Atomic** (conditional) or **best-effort** (preliminary check only) |

**Atomic Path** (at or below the threshold):
- MUST use an atomic conditional create where the backend supports one.
- A duplicate write MUST return `ErrPathExists` atomically.
- No coordination beyond the adapter is required.
- MAY buffer the payload in a temporary file to conserve memory; the
  temporary file MUST be removed automatically.

**Multipart Path** (above the threshold):
- Used when the limits of the backend's API require uploads in parts.
- SHOULD use conditional completion (for example, S3 `If-None-Match` on
  `CompleteMultipartUpload`) where the backend supports it, to guarantee
  atomically that nothing is overwritten.
- MAY perform a preliminary existence check so that it fails early.
- If conditional completion is not supported, MUST perform a preliminary
  existence check.
- If the preliminary check detects an existing path, MUST return
  `ErrPathExists`.
- When only the preliminary check is available, a **TOCTOU interval**
  exists between the check and completion; single-writer semantics or
  external coordination is REQUIRED.

#### Adapter Documentation Requirements

Adapters MUST document:
- the size threshold that separates the atomic path from the multipart path;
- which path provides atomic detection and which provides best-effort
  detection; and
- limitations specific to the backend (for example, the maximum object size
  and limits on parts).

### Get
- MUST return a readable stream for an existing path.
- If the path does not exist, MUST return an error that matches `ErrNotFound`
  under `errors.Is`. Lode distinguishes an absent object from a failed read
  by this error alone, and it handles each absence as CONTRACT_CORE §Head
  Pointer specifies: for example, when the `latest` pointer is absent,
  `Latest` and parent resolution scan the snapshots. Lode returns any other
  error to the caller. An adapter that reports absence with a different
  error therefore causes `Latest` and commits to fail on a dataset that has
  no pointer.

### Exists
- MUST accurately report existence.
- MUST NOT create or mutate data.

### List
- MUST return all paths under the given prefix.
- The prefix is a **string prefix over paths**, not a directory: `a/b` matches
  `a/b/x` and `a/bc`. Callers that intend a directory end the prefix with
  `/`.
- The order of results is unspecified.
- The adapter MUST document its pagination behavior, if any.

### Delete
- MUST remove the path if it exists.
- MUST be safe to call on a missing path (by succeeding idempotently or
  returning `ErrNotFound`).

### Range Reads (RangeReader)

`ReadRange` and `ReaderAt` form the optional `RangeReader` interface
(V1_API_DECISIONS D5). The built-in stores and the S3 adapter implement it.
An adapter that cannot read ranges MUST NOT implement it, rather than
implement methods that only return `ErrRangeReadNotSupported`.

### ReadRange
- MUST return bytes from `[offset, offset+length)` for the given path.
- If the path does not exist, MUST return `ErrNotFound`.
- If offset or length is negative, MUST return `ErrInvalidPath`.
- If the length exceeds the capacity of the platform's `int`, MUST return
  `ErrInvalidPath`.
- If the sum of offset and length would overflow, MUST return
  `ErrInvalidPath`.
- If the range extends beyond the end of the object, MUST return the
  available bytes (not an error).
- If the offset is beyond the end of the object, MUST return an empty slice.
- Buffer allocation MUST be bounded by the bytes available, not by the
  requested length; a large valid length beyond the end of the object MUST
  NOT exhaust memory.
- MUST use genuine range reads (not a read of the whole object) where the
  backend supports them.

### ReaderAt
- MUST return an `io.ReaderAt` for random access.
- If the path does not exist, MUST return `ErrNotFound`.
- The returned `ReaderAt` MUST support concurrent reads at different offsets.
- Callers are responsible for closing the underlying resource if it
  implements `io.Closer`.

---

## Commit Semantics

The commit protocol, including when a commit is complete, is stated in
CONTRACT_CORE §Head Pointer. An adapter's obligation is that `Put` makes an
object visible only once it is complete (§Put), so that a manifest a reader
observes is whole.

## Streamed Writes

- Adapters MUST allow data objects to be written before the manifest is
  committed.
- Adapters MUST NOT provide any implicit commit signal other than the
  presence of a manifest.
- The guarantee against overwriting applies according to the `Put` path
  (see §Put Upload Paths).
- Adapters MUST allow partial objects to be removed through `Delete`.

### Streaming Write Atomicity

For streaming writes that use the multipart path:
- The guarantee against overwriting depends on the `Put` path used (see
  §Put Upload Paths).
- On backends without conditional completion of multipart uploads,
  concurrent writers can race: both may find the object absent and proceed
  to write.
- Callers that use streaming writes on such backends MUST ensure
  single-writer semantics or use external coordination.
- A failure during a multipart upload SHOULD cause a best-effort abort and
  removal of the partial upload.
- That removal MUST use an independent context (not the caller's context,
  which may have been canceled) to maximize the likelihood of success.

---

## ConditionalWriter Capability

Every adapter MUST implement `ConditionalWriter`: `Store` is `ObjectStore`
(the object operations above) together with `ConditionalWriter`. Every
commit advances the latest pointer through `CompareAndSwap`, which provides
optimistic concurrency control (CAS), and retention writes tombstones
through it.

```go
type ConditionalWriter interface {
    CompareAndSwap(ctx context.Context, path string, expected string, replacement string) error
}
```

### Semantics

| Path State | Expected | Behavior |
|------------|----------|----------|
| Does not exist | `""` (empty) | Create the object with the content `replacement` |
| Does not exist | Non-empty | Return `ErrSnapshotConflict` |
| Exists; content equals `expected` | Any | Atomically replace the content with `replacement` |
| Exists; content differs from `expected` | Any | Return `ErrSnapshotConflict` |

- The operation MUST be atomic: concurrent `CompareAndSwap` calls on the same
  path MUST NOT both succeed.
- `expected == ""` means that the path must not exist or must be empty;
  this rule accommodates the first commit.

### Implementation Notes

**Filesystem adapter:**
- Acquire an exclusive lock on a companion `.lock` file adjacent to the
  target path: `flock` on Unix, `LockFileEx` on Windows. On other platforms,
  such as Plan 9, no such lock exists, and `CompareAndSwap` returns an error
  that matches `errors.ErrUnsupported`.
- While holding the lock, read the current content, compare it, and write
  the replacement.
- Release the lock.

**S3 adapter:**
- The conditional `PutObject` decides every outcome: `If-None-Match: *`
  creates the object, and `If-Match` replaces exactly the version that was
  compared. No outcome is inferred from what the adapter recorded earlier.
- When `expected` is empty, call `PutObject` with `If-None-Match: *` at
  once.
- When the adapter itself replaced the object (with `If-Match`) and
  `expected` equals the content it wrote, call `PutObject` with `If-Match`
  on the ETag that S3 returned for that write, without reading the object
  first. The adapter records only versions that it wrote by replacement,
  one per object and identified by a digest of the content, so objects
  that are created once and never replaced, such as tombstones, leave no
  record. It holds a bounded number of such records, evicting one when the
  limit is reached, and `Delete` discards the record of the deleted
  object; neither affects any outcome.
- Otherwise, or when either of those writes reports that its precondition
  failed or that the object no longer exists, call `GetObject` to read the current content and its ETag,
  compare the content with `expected`, and then call `PutObject` with
  `If-Match` on that ETag, or with `If-None-Match: *` if the object does not
  exist. The outcome is therefore the one that reading first would give: an
  empty existing object satisfies an empty `expected`, and an object that
  another writer replaced yields `ErrSnapshotConflict`.
- A refused precondition on the final write returns `ErrSnapshotConflict`,
  as does an `If-Match` write that finds the object deleted since it was
  read, which S3 reports as not found.
- After any failed write the adapter discards what it knew of the object's
  version, because a lost response may hide a completed write.

**Memory adapter:**
- Read, compare, and write under a mutex.

### Backends without a Conditional Write

A backend that offers no conditional write cannot implement
`CompareAndSwap` atomically across processes. `lode.SingleProcess(store)`
turns its `ObjectStore` into a `Store` by serializing `CompareAndSwap` calls
under a lock held by the process; a store that already implements
`ConditionalWriter` is returned unchanged. Its `CompareAndSwap` MUST follow
the semantics table above. It serializes only callers that share the
returned value within one process, and its replacement is a `Delete`
followed by a `Put`, so a failed `Put` leaves the path absent. Reads
through the returned value (`Get`, `Exists`, `List`, `ReadRange`, and
`ReaderAt`) MUST wait for a replacement in progress, so that a reader in the
process observes the old or the new content, never the absence between the
`Delete` and the `Put`. A nil store MUST be returned as nil.

---

## Consistency Notes

Adapters MUST document:
- the consistency guarantees of `List` and `Exists`; and
- any delays or mitigations required for read-after-write consistency.

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for complete
definitions.

### Per-Operation Bounds

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Put` | 1 (atomic) or 1 + ⌈size/partSize⌉ (multipart) | O(payload) |
| `Get` | 1 | O(1) streaming |
| `Exists` | 1 | O(1) |
| `Delete` | 1 | O(1) |
| `ReadRange` | 1 | O(L) |
| `ReaderAt` | 1 to open; 1 per `ReadAt` call | O(L) per call |
| `List` | ⌈N/page⌉ | **O(N), fully materialized** |

### Full Materialization of List

`Store.List` returns a `[]string`, so all matching keys accumulate in
memory. This is a **documented constraint of the interface**, not a defect
of an implementation.

Adapters MUST paginate requests to the backend (for example, with S3
`ListObjectsV2` continuation tokens) but MUST return all results in the
returned slice.

Callers MUST restrict prefixes as narrowly as possible. Hot-path operations
MUST NOT depend on unbounded `List` results.
