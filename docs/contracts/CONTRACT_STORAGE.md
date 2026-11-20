# Lode Storage Adapter Contract

This document defines the required semantics for storage adapters implementing
`Store`, `ObjectStore`, `PrefixLister`, `RangeReader`, and `RangeStore`.

---

## Goals

1. Safe, immutable object creation (`Put` never silently overwrites).
2. Accurate existence checks, full prefix listings, and optional shallow
   prefix enumeration.
3. Atomic conditional pointer updates (`CompareAndSwap`) independent of the
   backend.

---

## Adapter Obligations

### `Put`

**Purpose**: create a new immutable object at `path` without overwriting an
existing object.

**Behavior**:
- Paths are canonical slash-separated relative keys on every platform. Empty
  paths, rooted paths, trailing slashes, empty segments (`"//"`), paths
  containing `\`, `.` or `..` segments, or paths whose final segment begins
  with `.lode-tmp-` MUST return `ErrInvalidPath`.
- If `path` already exists, `Put` MUST return `ErrPathExists`, with one
  idempotency exception for lost-response transport retries: where the
  backend exposes a content digest comparable to the payload's, the adapter
  MUST compare the stored object's byte length and digest with the payload's
  and return `nil` when both match (`ErrPathExists` otherwise). On single-part
  S3 uploads (`≤5 GiB`), `s3.Store` compares `ContentLength` and `ETag`
  against the payload's byte length and MD5 digest.
  - **SSE-KMS and SSE-C note**: on S3 buckets encrypted with SSE-KMS or
    SSE-C, single-part `ETag` values are not MD5 digests of the plaintext
    payload, so a lost-response SDK retry of `PutObject` cannot confirm
    payload identity via `HeadObject` and returns `ErrPathExists`.
- `Put` MUST publish objects atomically so concurrent `Get`, `Exists`, and
  `List` calls observe either the complete object or `ErrNotFound`, never a
  partially written object.
  - `FS` (`NewFS`) writes to a `.lode-tmp-*` file in the target directory and
    publishes via `os.Link` (failing with `ErrPathExists` if the target
    exists, and requiring a filesystem that supports hard links). When
    configured with `WithSync()` (`FSOption`), `FS` `fsync`s the temporary
    file before linking and `fsync`s the parent directory after unlinking the
    temporary name, adding power-loss durability to crash-consistent
    atomicity.
- On an observable failure (such as a reader error), `Put` MUST leave the
  target path absent and clean up temporary staging. On an indeterminate
  remote timeout, the adapter MUST NOT delete the target key (which may
  belong to another writer); unreferenced orphans are reclaimed later by
  `Sweep` ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)).

#### `Put` Upload Paths

| Path | Trigger | Overwrite Detection | Guarantee |
|------|---------|---------------------|-----------|
| Atomic | Payload `≤` adapter threshold (`5 GiB` on S3) | Conditional write (`If-None-Match: "*"`, `os.Link`, mutex) | **Atomic**: zero TOCTOU window |
| Multipart | Payload `>` adapter threshold (`>5 GiB` up to `5 TiB` on S3) | Conditional completion (`CompleteMultipartUpload` with `If-None-Match: "*"`) + preflight check | **Atomic** where supported; **best-effort** preflight where backend ignores `If-None-Match` on multipart completion (such as Hetzner) |

Adapters MUST document their atomic threshold, maximum object size, and any
backend-specific multipart caveats.

### `Get` and `Exists`
- `Get` MUST return a readable `io.ReadCloser` for an existing object key and
  an error matching `ErrNotFound` for a missing key. Intermediate directory
  prefixes (such as `"parent"` when `"parent/child"` is stored on `FS`) are not
  stored objects: `Get` MUST return `ErrNotFound` and `Exists` MUST return
  `false, nil`. Any non-`ErrNotFound` I/O error MUST be returned as-is so Lode
  never mistakes a transient read error for an absent pointer
  ([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Head Pointer).
- `Exists` MUST report key existence without mutating storage.

### `List`
- `List(ctx, prefix)` MUST return all stored object keys matching the raw
  string `prefix` (paginating through all backend pages, such as S3
  `ListObjectsV2` continuation tokens) and MUST omit `.lode-tmp-*` staging
  files and `.lock` files where applicable (note: `.lock` companion files are
  filtered by Lode's layout parsers).
- Result order is unspecified; callers sort as needed. Invalid prefixes
  (rooted, empty segments `"//"`, `\`, `.` or `..` segments) MUST return
  `ErrInvalidPath`.

### `PrefixLister` (Optional Capability)

```go
type PrefixLister interface {
    ListPrefixes(ctx context.Context, prefix string) ([]string, error)
}
```

- `ListPrefixes(ctx, prefix)` returns the immediate child prefixes (each
  ending in `"/"`) beneath `prefix` that contain at least one stored object,
  sorted lexicographically and deduplicated, without enumerating every
  descendant key. Intermediate segments starting with `.lode-tmp-` are valid
  directory segments (only the final object-key segment reserves `.lode-tmp-`)
  and MUST be included when they contain stored objects.
- Invalid prefixes MUST return `ErrInvalidPath`. When no child prefixes
  exist, `ListPrefixes` returns an empty slice without error.
- `FS`, `Memory`, and `s3.Store` (via `ListObjectsV2` with `Delimiter="/"`)
  implement `PrefixLister`.
- Store wrappers (`Prefixed`, `SingleProcess`, and `OpenReadOnly`'s wrapper)
  MUST preserve `PrefixLister` whenever the inner store implements it.

### `Delete`
- `Delete` MUST remove the stored object at `path` if present and succeed
  idempotently if `path` does not name a stored object (including when `path`
  is an intermediate directory on `FS`). Invalid paths MUST return
  `ErrInvalidPath`.

### `RangeReader` and `RangeStore`

`ReadRange` and `ReaderAt` form the optional `RangeReader` interface;
`RangeStore` composes `Store` and `RangeReader`:
- `FS`, `Memory`, `Prefixed`, and `s3.Store` implement `RangeStore`.
- `ReadRange(ctx, path, offset, length)`:
  - Returns `ErrInvalidPath` when `offset < 0`, `length < 0`, `length`
    exceeds platform `int` capacity, or `offset + length` overflows `64`-bit
    signed bounds, and when `path` is invalid.
  - Returns `ErrNotFound` if `path` does not name a stored object (including
    when `length == 0` or when `path` is an intermediate directory on `FS`).
  - Clamps buffer allocation to available bytes when `offset + length`
    extends past EOF, and returns `[]byte{}` when `offset >= size`.
  - Uses genuine range requests (such as HTTP `Range` headers on S3).
- `ReaderAt(ctx, path)` returns an `io.ReaderAt` safe for concurrent reads at
  different offsets, or `ErrNotFound` if `path` does not name a stored object.

---

## Names in Keys

Every caller-supplied identifier embedded into a storage key MUST be
validated as a single canonical path segment (non-empty, not `.` or `..`, no
`/` or `\`, and not starting with `.lode-tmp-`) before any store call:

| Value | Validated By | On Failure |
|-------|--------------|------------|
| `DatasetID` | `Root.Records`, `Root.Blobs`, `Root.RemoveDataset`, `ReadRoot.Records`, `ReadRoot.Blobs` | `ErrInvalidArgument` |
| `volume.ID` | `volume.New`, `volume.Remove`, `volume.Head` | `ErrInvalidArgument` |
| Hive key | `Hive(keys...)` (also forbids `=`) | `ErrInvalidArgument` |
| Compressor extension | `WithCompressor` on a writer (`data<ext>` must be a valid segment) | `ErrInvalidArgument` |
| `WithAnchor` `SnapshotID` | `WithAnchor(id)` | `ErrInvalidArgument` |
| `SnapshotID` / `volume.SnapshotID` lookup | `Snapshot`, `Read`, `volume.Reader.Snapshot`, `volume.Reader.ReadAt` | `ErrNotFound` |

---

## Prefixed and Single-Process Wrappers

### `Prefixed(factory, prefix)`
- Scopes every key `k` of the inner `RangeStore` to `prefix/k` and strips
  `prefix/` from `List` and `ListPrefixes` results so objects outside
  `prefix/` are unreachable and invisible.
- Validates keys before prepending `prefix` (`ErrInvalidPath` on invalid
  keys) and validates `prefix` as a non-empty relative prefix inside the root
  (`ErrInvalidArgument` when `factory` or `prefix` is invalid, before calling
  `factory`).
- Preserves `RangeStore` and, when the inner store implements it,
  `PrefixLister`.

### `SingleProcess(store)`
- Wraps an `ObjectStore` that lacks conditional writes with a process-local
  `sync.RWMutex` implementing `CompareAndSwap`. Before deleting `<path>`,
  `CompareAndSwap` writes the verified prior content to `<path>.lode-cas`,
  restores `<path>` on replacement `Put` failure, and serves `<path>.lode-cas`
  across `Get`, `Exists`, `List`, `ReadRange`, `ReaderAt`, and subsequent
  `CompareAndSwap` calls whenever `<path>` is absent so neither replacement
  failures nor process termination lose the committed head. Keys ending in
  `.lode-cas` are reserved on a `SingleProcess`-wrapped store and return
  `ErrInvalidPath`.
- `Get`, `Exists`, `List`, `ReadRange`, `ListPrefixes`, and every `ReadAt` call
  on a returned `ReaderAt` hold the read lock so in-process readers never
  observe the transient replacement gap.
- Returns stores that already satisfy `Store` unchanged, and returns `nil`
  for `nil` and typed-nil stores. Preserves `RangeReader` and `PrefixLister`.

---

## `ConditionalWriter` (`CompareAndSwap`)

```go
type ConditionalWriter interface {
    CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}
```

| Path State | `expected` | Behavior |
|------------|------------|----------|
| Absent | `""` | Create `path` with `replacement` |
| Absent | Non-empty | Return `ErrSnapshotConflict` |
| Exists, content `== expected` | Any | Atomically replace content with `replacement` |
| Exists, content `!= expected` | Any | Return `ErrSnapshotConflict` |

- **Cancellation**: `CompareAndSwap` MUST honor `ctx` cancellation while
  waiting to acquire contention locks and MUST NOT publish `replacement` if
  `ctx` is already canceled before the atomic publication step.
- **Filesystem (`FS` / `NewFS`)**: acquires an exclusive advisory lock on
  `<path>.lock` (`flock` on Unix, `LockFileEx` on Windows, honoring `ctx`
  cancellation while waiting; `errors.ErrUnsupported` on platforms without file
  locking), reads and compares `<path>`, writes a `.lode-tmp-cas-*` file
  (`fsync`ing it when `WithSync()` is set), checks `ctx.Err()` before atomic
  replacement, replaces `<path>`, and `fsync`s the parent directory when
  `WithSync()` is set.
- **S3 (`s3.New` / `s3.Bucket`)**: uses `PutObject` with `If-None-Match: "*"`
  when `expected == ""` and `If-Match` (with bare unquoted `ETag`) when
  replacing a known version, falling back to `GetObject` + conditional
  `PutObject` when no cached `ETag` matches or a precondition is stale.
  Bounded `ETag` caching saves a `GetObject` on steady-state single-writer
  commits without affecting correctness.
- **Memory (`Memory` / `NewMemory`)**: executes read-compare-write under
  `sync.RWMutex`.

---

## Complexity Bounds

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Put` | 1 (atomic) or `2 + ⌈size/partSize⌉` (multipart) | O(1) spooled on `FS`/`s3.Store`, O(payload) on `Memory` |
| `Get` | 1 | O(1) streaming |
| `Exists` | 1 | O(1) |
| `Delete` | 1 | O(1) |
| `ReadRange` | 1 | O(min(L, available)) |
| `ReaderAt` | 1 to open + 1 per `ReadAt` | O(L) per read |
| `List` | `⌈N/page⌉` | O(N) materialized |
| `ListPrefixes` | `⌈P_child/page⌉` | O(P_child) |
