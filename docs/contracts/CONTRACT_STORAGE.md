# Lode Storage Adapter Contract

This document defines the required semantics for storage adapters.
It is authoritative for any implementation of the `Store` interface.

---

## Goals

1. Safe, immutable writes.
2. Accurate existence checks and listings.
3. Behavior independent of the backend.

---

## Adapter Obligations

### `Put`

**Purpose**: `Put` creates a new object, and it MUST NOT overwrite existing
data without reporting the conflict.

**Behavior**:
- MUST write data to the given path.
- Paths are slash-separated on every platform. A path that is empty, escapes
  the root, or contains a backslash MUST cause `Put` to return
  `ErrInvalidPath`. Adapters MUST NOT read a backslash as a separator. If
  a path's final segment begins with the reserved prefix `.lode-tmp-`, `Put`
  MUST also return `ErrInvalidPath` on every adapter, because the filesystem adapter names
  its temporary files with that prefix (below).
- If `Put` detects an existing path, it MUST return `ErrPathExists`.
- The detection mechanism and its guarantee vary by upload path (see the
  table below).
- On upload paths with a time-of-check-to-time-of-use (TOCTOU) interval, a
  concurrent writer can overwrite an existing object without detection.
- `Put` MUST be atomic wherever the adapter knows the outcome. On an error that
  the adapter can observe, including an error from the supplied reader, the
  adapter MUST leave the path as it was before the call. A `Put` that
  created an object and then failed MUST remove it. If that removal itself fails, `Put`
  MUST return an error that reports both failures.
- An object MUST become visible to `Get`, `Exists`, and `List` only once it
  is complete: a concurrent reader observes the whole object or none of it.
  A reader that observed a partial manifest would treat an incomplete commit
  as complete (§Commit Semantics). The filesystem adapter writes the object
  under a temporary name in the same directory and publishes it with a hard
  link, which fails if the path exists. The adapter therefore requires a
  filesystem that supports hard links, and on one that does not, `Put`
  returns an error. A published object has mode 0644, which the process
  umask then modifies. Temporary names begin with the
  reserved prefix `.lode-tmp-`, and `List` omits them. The adapter
  removes the temporary name after publishing. If that removal fails, or the
  process ends first, an inert temporary file remains. No Lode operation
  observes it, and an operator may delete it.
- A remote backend can accept an object and then lose the response (through
  a network or context timeout). The adapter MUST NOT attempt to resolve
  such an indeterminate outcome by deleting the key, because it cannot know
  whether the object at the key is its own. The object is then orphaned: no
  manifest references it, and readers cannot see it. The retention sweep
  reclaims it ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)).
- Callers never remove anything after a failed `Put`: they MUST NOT access a
  path whose `Put` returned an error. A path whose `Put` returned
  `ErrPathExists` belongs to another writer. The single exception arises
  when retention creates a tombstone. Retention then reads the existing
  tombstone to verify that it records the same expiry, and it never writes
  or deletes that tombstone
  ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md) §Order of Operations).

#### Put Upload Paths

Adapters MAY route `Put` through different mechanisms according to the
size of the payload. The overwrite guarantee depends on the upload path
and the backend's capabilities:

| Path | Trigger | Detection Mechanism | Guarantee |
|------|---------|---------------------|-----------|
| Atomic | Payload at or below the threshold | Conditional write | **Atomic**: no TOCTOU interval |
| Multipart | Payload above the threshold | Conditional completion (if supported) or a preliminary check | **Atomic** (conditional) or **best-effort** (preliminary check only) |

**Atomic Path** (at or below the threshold):
- MUST use an atomic conditional create where the backend supports one.
- A duplicate write MUST return `ErrPathExists` atomically.
- The atomic path requires no coordination beyond the adapter.
- MAY buffer the payload in a temporary file to conserve memory, and MUST
  then remove that temporary file automatically.

**Multipart Path** (above the threshold):
- Applies when the backend's API limits require uploads in parts.
- SHOULD use conditional completion (for example, S3 `If-None-Match` on
  `CompleteMultipartUpload`) where the backend supports it, to guarantee
  atomically that the upload overwrites no existing object.
- MAY perform a preliminary existence check so that `Put` fails before the
  upload when the path exists.
- If the backend does not support conditional completion, MUST perform a
  preliminary existence check.
- If the preliminary check detects an existing path, MUST return
  `ErrPathExists`.
- When only the preliminary check is available, a **TOCTOU interval**
  separates the check from completion, so callers MUST ensure single-writer
  semantics or external coordination.

#### Adapter Documentation Requirements

Adapters MUST document:
- the size threshold that separates the atomic path from the multipart path
- the path that provides atomic detection and the path that provides
  best-effort detection
- limitations specific to the backend (for example, the maximum object size
  and limits on parts)

### `Get`
- MUST return a readable stream for an existing path.
- If the path does not exist, MUST return an error that matches `ErrNotFound`
  under `errors.Is`. Lode distinguishes an absent object from a failed read
  by this error alone, and it handles each absence as CONTRACT_CORE §Head
  Pointer specifies. For example, when the `latest` pointer is absent,
  `Latest` and parent resolution scan the snapshots. Lode returns any other
  error to the caller. An adapter that reports absence with a different
  error therefore causes `Latest` and commits to fail on a dataset that has
  no pointer.

### `Exists`
- MUST accurately report existence.
- MUST NOT create or mutate data.

### `List`
- MUST return all paths under the given prefix.
- The prefix is a **string prefix over paths**, not a directory: `a/b` matches
  `a/b/x` and `a/bc`. Callers that intend a directory end the prefix with
  `/`.
- The order of results is unspecified.
- The adapter MUST document its pagination behavior, if any.

### `Delete`
- MUST remove the path if it exists.
- MUST be safe to call on a missing path (by succeeding idempotently or
  returning `ErrNotFound`).

### Range Reads (RangeReader)

`ReadRange` and `ReaderAt` form the optional `RangeReader` interface
(V1_API_DECISIONS D5). `RangeStore` is a `Store` that implements it. The
built-in stores and the S3 adapter are `RangeStore`s, and their
constructors and factories return that type. An adapter that cannot read
ranges MUST omit the `RangeReader` interface rather than implement methods
that only return `ErrRangeReadNotSupported`.

Where a store's capability is static, the store's type expresses it, and a
function that requires range reads takes a `RangeStore` parameter. Only
code that accepts an arbitrary `Store` discovers the capability at run
time. Such code MUST use a checked type assertion, never one that can
panic, and MUST report `ErrRangeReadNotSupported` when the assertion
fails.

### `ReadRange`
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
- The adapter MUST bound its buffer by the available bytes, not by the
  requested length. A large valid length beyond the end of the object MUST
  NOT exhaust memory.
- MUST use genuine range reads (not a read of the whole object) where the
  backend supports them.

### `ReaderAt`
- MUST return an `io.ReaderAt` for random access.
- If the path does not exist, MUST return `ErrNotFound`.
- The returned `ReaderAt` MUST support concurrent reads at different offsets.
- Callers close the underlying resource if it implements `io.Closer`.

---

## Names in Keys

Lode builds keys from names and normalizes each key as a path, so a name
that is not one path segment redirects its key to another object. Through the dataset `a`, the
snapshot ID `../../b/snapshots/<id>` would address a snapshot of the dataset
`b`. The key rules of §Put cannot detect such a key, because it stays within
the store.

Every name that Lode builds into a key from a caller's value MUST therefore
be one canonical path segment. Such a segment is not empty, `.`, or `..`,
contains no `/` or backslash, and does not begin with the reserved prefix
`.lode-tmp-`. Lode
checks each name before any store call:

| Name | Where Lode checks it | When the name is not one segment |
|------|----------------------|----------------------------------|
| Dataset ID | `Root.Records`, `Root.Blobs`, `ReadRoot.Records`, `ReadRoot.Blobs` | `ErrInvalidArgument` |
| Volume ID | `volume.New` | `ErrInvalidArgument` |
| Path name | Each path-name root option, such as `WithDatasetsDir`, which also excludes `=` | `ErrInvalidArgument` |
| Hive key | `Hive`, which also excludes `=` | `ErrInvalidArgument` |
| Compressor extension | `WithCompressor` on a writer, which names each data file by its role and the extension | `ErrInvalidArgument` |
| Snapshot ID | `Snapshot`, `Read`, and `GetManifest`, and Volume's `Snapshot` and `ReadAt` | `ErrNotFound`, because no snapshot has such an ID |

Separate constraints bound the remaining values that reach a key:

- Lode escapes a Hive partition value and places it after its key and
  `=`, so its segment is never `.` or `..`.
- `Expire` derives paths only from IDs that its own listing parsed, or from
  IDs in the generated form (CONTRACT_RETENTION §Selection).
- `OpenObject` and `ReaderAt` read only a data object of the handle's
  dataset (CONTRACT_READ_API §Storage Adapter Interface).

---

## Prefixed Stores

`Prefixed(factory, prefix)` returns a factory of a `RangeStore` that keeps
every object of the store `factory` creates under `prefix`. A caller uses it
to place a container under a location of its own choice in a store it
already holds, on any backend.

- The key `k` addresses the inner object `prefix/k`. `List` lists the inner
  store under the prefix and returns each key without the prefix, so the
  store never returns or touches an object outside the prefix. A sibling
  location that shares the prefix as a string, such as `a/b2` beside
  `a/b`, is outside it.
- Each operation applies the key rules of §Put to its key before it adds
  the prefix, and returns `ErrInvalidPath` for a key that they reject. A key
  such as `../x` therefore never reaches the inner store.
- The prefix MUST name a location inside the root: a prefix that is empty,
  that cleans to the root, or that escapes it yields `ErrInvalidArgument`
  when the factory is called, before the factory calls `factory`. A nil
  `factory`, or one that returns a nil store, yields `ErrInvalidArgument`
  too, and an error from `factory` is returned unchanged.
- Every other obligation of this contract, such as immutable `Put`, range
  reads and `CompareAndSwap`, is the inner store's, so the prefixed store
  holds each of them exactly when the inner store does.

---

## Commit Semantics

CONTRACT_CORE §Head Pointer states the commit protocol, including the point
at which a commit is complete. `Put` MUST make each object visible only
once it is complete (§Put). A reader therefore never observes a partial
manifest.

## Streamed Writes

- Adapters MUST allow a caller to write data objects before the caller
  commits the manifest.
- Adapters MUST NOT provide any implicit commit signal beyond the commit
  protocol of CONTRACT_CORE §Head Pointer.
- The guarantee against overwriting depends on the `Put` upload path (see
  §Put Upload Paths).
- Adapters MUST allow callers to remove partial objects through `Delete`.

### Streaming Write Atomicity

For streaming writes that use the multipart path:
- The guarantee against overwriting depends on the `Put` path used (see
  §Put Upload Paths).
- On backends without conditional completion of multipart uploads,
  concurrent writers can race: both may find the object absent and proceed
  to write.
- Callers that use streaming writes on such backends MUST ensure
  single-writer semantics or use external coordination.
- When a multipart upload fails, the adapter SHOULD abort it and remove the
  partial upload on a best-effort basis.
- The adapter MUST perform that removal with an independent context rather
  than the caller's context, which the caller may already have canceled.

---

## ConditionalWriter Capability

Every adapter MUST implement `ConditionalWriter`: `Store` is `ObjectStore`
(the object operations above) together with `ConditionalWriter`. Each
commit advances the latest pointer through `CompareAndSwap`, a
compare-and-swap (CAS) operation that provides optimistic concurrency
control, and retention writes tombstones
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
| Exists, and the content equals `expected` | Any | Atomically replace the content with `replacement` |
| Exists, and the content differs from `expected` | Any | Return `ErrSnapshotConflict` |

- The operation MUST be atomic: concurrent `CompareAndSwap` calls on the same
  path MUST NOT both succeed.
- An empty `expected` matches a path that does not exist or an object whose
  content is empty. The first commit relies on this rule to create the
  latest pointer.

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
  creates the object, and `If-Match` replaces exactly the version whose ETag
  the adapter compared. The adapter infers no outcome from its earlier records.
- When `expected` is empty, call `PutObject` with `If-None-Match: *`
  immediately, without reading the object first.
- When the adapter itself replaced the object with `If-Match`, it records
  the ETag that S3 returned. If `expected` equals the content of that
  write, call `PutObject` with `If-Match` on that ETag without reading the
  object first. The adapter records only versions that it wrote by replacement,
  one per object, each identified by a digest of the content. Objects that
  the adapter creates once and never replaces, such as tombstones,
  therefore leave no record. The adapter holds a bounded number of such records and evicts
  one when it reaches the limit. `Delete` discards the record of the
  deleted object. Neither eviction nor discarding affects any outcome.
- Otherwise, call `GetObject` to read the current content and its ETag.
  Also call `GetObject` when either conditional write reports a failed
  precondition or a missing object. Then compare
  the content with `expected`, and call `PutObject` with `If-Match` on that
  ETag, or with `If-None-Match: *` if the object does not exist. The
  outcome therefore matches the outcome of a read before every write: an
  empty existing object satisfies an empty `expected`, and an object that
  another writer replaced yields `ErrSnapshotConflict`.
- The adapter returns `ErrSnapshotConflict` when S3 refuses the
  precondition on the final write. It also returns `ErrSnapshotConflict`
  when an `If-Match` write finds the object deleted after the read, a
  condition that S3 reports as not found.
- After any failed write, the adapter discards its record of the object's
  ETag, because a lost response may hide a completed write.

**Memory adapter:**
- Read, compare, and write under a mutex.

### Backends without a Conditional Write

A backend that offers no conditional write cannot implement
`CompareAndSwap` atomically across processes. `lode.SingleProcess(store)`
converts an `ObjectStore` into a `Store` by serializing `CompareAndSwap`
calls under a process-local lock. `lode.SingleProcess` returns unchanged
any store that already implements `ConditionalWriter`. The
`CompareAndSwap` of the returned value MUST follow the semantics table
above. It serializes only callers that share the
returned value within one process, and its replacement is a `Delete`
followed by a `Put`, so a failed `Put` leaves the path absent. Reads
through the returned value (`Get`, `Exists`, `List`, `ReadRange`, and
`ReaderAt`) MUST wait for a replacement in progress. A reader in the
process then observes the old or the new content, never the absence
between the `Delete` and the `Put`. `lode.SingleProcess` MUST return nil
for a nil store.

---

## Consistency Notes

Adapters MUST document:
- the consistency guarantees of `List` and `Exists`
- any delays or mitigations required for read-after-write consistency

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
| `ReaderAt` | 1 to open, and 1 per `ReadAt` call | O(L) per call |
| `List` | ⌈N/page⌉ | **O(N), fully materialized** |

### Full `List` Materialization

`Store.List` returns a `[]string`, so all matching keys accumulate in
memory. Full materialization is a **documented constraint of the
interface**, not a defect of any implementation.

Adapters MUST paginate requests to the backend (for example, with S3
`ListObjectsV2` continuation tokens) but MUST return all results in the
returned slice.

Callers MUST restrict prefixes as narrowly as possible. Hot-path operations
MUST NOT depend on unbounded `List` results.
