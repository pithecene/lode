# V1_API_DECISIONS.md — Breaking API Candidates Held for v1.0

This document records the public-API changes that the v0.10.0 hygiene pass
found worth making but that **break callers**, so they are decided once,
before the v1.0 surface freeze (`V1_READINESS.md` §1), instead of being
re-litigated per release.

Each candidate states what exists, why it is wrong, the proposed shape, and
who it breaks. The *Status* line is the decision. Until a candidate is
accepted and shipped, the current shape in `PUBLIC_API.md` is the contract.

Additive fixes from the same pass (`ErrInvalidArgument`,
`DatasetListOptions.StartAfter`, sentinel consolidation) shipped in v0.10.0
(`CHANGELOG.md` §0.10.0) and are not listed here.

D9 comes from dogfooding rather than the hygiene pass: it changes where a
part of the surface lives and what v1.0 promises about it, so it is decided
here with the rest. D10 comes from the investigation of the history fork
observed in production; most of its parts change contracts rather than
signatures, but they define what v1.0 promises about commits and listing.
D11 comes from a review of the constructors against the call sites of
Quarry and the examples; it would resolve D1 and D3 if accepted.

---

## D1 — `Dataset.Write` is type-erased

**Current.** `Write(ctx, data []any, metadata)` means "records" when a codec
is configured and "exactly one `[]byte`" when it is not. The mode is fixed at
construction by whether `WithCodec` was given; in blob mode the call then
checks `len(data) == 1` and `data[0].([]byte)` at runtime, with two
`ErrInvalidArgument` messages for the two ways to get it wrong.

**Problem.** The payload type is erased: a blob dataset accepts any `[]any`
and rejects almost all of them at runtime, and a records dataset accepts a
bare `[]byte` it will try to encode as a record. The mode itself is a runtime
property — `WithCodec` is an `Option` and `NewDataset` returns the same
`Dataset` either way — so the signature cannot carry it; it can carry the
payload shape.

**Proposed.** `WriteBlob(ctx, blob []byte, metadata)` for datasets without a
codec and `Write(ctx, records []any, metadata)` for datasets with one.
Calling the wrong one returns `ErrCodecConfigured` / a new sentinel, as
`StreamWrite` already does. This fixes the payload typing, not mode
selection: a wrong-mode call still fails at runtime. Compile-time mode
selection would need distinct constructors returning distinct interfaces
(`NewBlobDataset` / `NewRecordDataset`), a larger surface change recorded
here as the alternative, not the proposal.

**Breaks.** Every blob-mode `Write` caller: `examples/blob_upload`,
`examples/vector_artifacts` (the embedding blobs; its index blobs go through
`StreamWrite`, which is unaffected), the README's blob example, and any
downstream dataset built without a codec. Quarry writes records through the
JSONL codec and is unaffected. Mechanical: pass the `[]byte` directly.

**Status.** Open.

---

## D2 — `Layout` is public in signatures but not nameable

**Current.** `WithLayout(l layout)` takes the unexported `layout` interface;
`NewDefaultLayout()` and `NewFlatLayout()` return it; `NewHiveLayout(keys...)`
returns `(layout, error)` while `WithHiveLayout(keys...)` exists as the
option form.

**Problem.** Callers can hold a layout only as the result of a constructor
call; they cannot declare a variable, field, or parameter of its type. Two
spellings exist for Hive (`WithLayout(NewHiveLayout(...))` cannot even be
written because of the error return), one for the others.

**Proposed.** Export `Layout` as an opaque interface (no methods callers can
implement) and keep the constructors. For Hive, either keep `WithHiveLayout`
and remove `NewHiveLayout` from the public surface, or keep `NewHiveLayout`
as is and add `MustHiveLayout(keys...)` (panics on no keys) so
`WithLayout(lode.MustHiveLayout(...))` is writable. Changing `NewHiveLayout`
itself to panic is not an option: it would drop the error result, so every
`l, err := NewHiveLayout(...)` stops compiling, and it would replace a
classified `ErrInvalidArgument` with a panic.

**Breaks.** Nothing that compiles today if `Layout` is added and `layout`
removed in the same release; removing either Hive spelling breaks its users;
`MustHiveLayout` is additive.

**Status.** Open. Decide which Hive spelling survives.

---

## D3 — `Option` applicability is N×M

**Current.** `Option` has `applyDataset` and `applyReader`. Every option
implements both; the ones that do not apply to a reader return
`ErrOptionNotValidForDatasetReader`. A second sentinel,
`ErrOptionNotValidForDataset`, is declared and documented but no option
produces it: every current option applies to `NewDataset`. (Until D9 shipped
there was a third target, `applyVolume`, and a third sentinel,
`ErrOptionNotValidForVolume`; Volume now has its own option type.)

**Problem.** Adding a constructor touches every option; adding an option
touches every constructor. Two sentinels name one condition ("this option
does not apply here"), one of them unproduced, so callers that want to
classify need two checks.

**Proposed.** One `ErrOptionNotApplicable` sentinel whose message names the
option and the constructor; internally, options declare the targets they
support as data (a set) and a single applicability check replaces the
per-target methods. Public surface: two sentinels become one.

**Breaks.** Callers matching the two sentinels by identity. Grep shows
only Lode's own tests do.

**Status.** Open.

---

## D4 — Partition paths are raw `string`

**Current.** `ListManifests(…, partition string, …)`, `PartitionRef.Path`,
`ManifestRef.Partition`, `PartitionListOptions.Prefix` and `.StartAfter` are
all `string`. `CONTRACT_READ_API.md` named a `PartitionPath` type for the
`ListManifests` parameter before any code existed; v0.10.0 aligned the
contract text with the code and recorded the type here.

**Problem.** A partition path has a grammar (canonical relative fragment, no
`.`/`..`/empty segments) that is validated by a free function at one call
site (`Prefix`) and not at all elsewhere. Nothing in the types says which
strings are partition paths.

**Proposed.** An opaque `PartitionPath` (struct with an unexported field)
built only by `ParsePartitionPath(string) (PartitionPath, error)`, which
returns `ErrInvalidPath`, with `String()` for display; the zero value is the
root (no partition). Used for the `ListManifests` parameter, `PartitionRef.Path`,
`ManifestRef.Partition` and `PartitionListOptions.StartAfter` — four of the
five. `PartitionListOptions.Prefix` stays a raw `string`: it is a string
prefix, not a path, and the distinction matters (`day=a/` ends at a segment
boundary, `day=a` also matches `day=a-b`); a path grammar that forbids empty
segments would have to reject or strip that trailing slash and silently
change the listing. A plain `type PartitionPath
string` is not enough: `PartitionPath("../x")` is a legal conversion, so the
type would document the grammar without enforcing it. This was scoped for the
additive slice of v0.10.0 and found not to be additive: changing parameter
and field types breaks any caller holding a `string` and every implementer
of `DatasetReader`.

**Breaks.** `DatasetReader` implementers (mocks); every caller that passes or
reads a partition path, including string literals (an opaque type has no
untyped-constant conversion).

**Status.** Open. Recommended accept.

---

## D5 — Range reads are a required method that may refuse

**Current.** `ReadRange` and `ReaderAt` are required methods of `Store`, and
their doc comments (and `DatasetReader.ReaderAt`'s) say a store that does not
support range reads returns `ErrRangeReadNotSupported`. No in-tree adapter
returns it; a downstream adapter that cannot do range reads must implement
both methods and return the sentinel from them.

**Problem.** Capability is expressed as an error from a method the store was
forced to implement, while the other optional capability, conditional
writes, is expressed as an interface the store may implement
(`ConditionalWriter`). Callers cannot tell up front whether range reads work;
they find out per call.

**Proposed.** Move `ReadRange` and `ReaderAt` out of `Store` into an optional
`RangeReader` interface. `DatasetReader.ReaderAt` returns
`ErrRangeReadNotSupported` when the store does not implement it. `Volume`
requires range reads (`Volume.ReadAt` is built on `ReadRange`;
`CONTRACT_VOLUME.md`), so `NewVolume` checks the capability once and returns
`ErrInvalidArgument` naming the store when it is absent. A type assertion
cannot see through a legacy adapter whose `ReadRange` / `ReaderAt` still
exist only to return the sentinel, so the migration note for adapters is:
remove those stubs (a store that cannot range-read must not look like one
that can), and the per-call `ErrRangeReadNotSupported` stays as the backstop
for adapters that do not. Adapters that only implement `Store` lose two
methods they could only ever stub.

**Breaks.** Code that calls `ReadRange` / `ReaderAt` on a value typed as
`Store` (type-assert to `RangeReader` instead); `NewVolume` over a store
without range reads, which today constructs and then fails on read. Adapters
are not broken: the methods they already have satisfy the new interface.
Quarry references neither the methods nor the sentinel.

**Status.** Open.

---

## D6 — Error message prefix has no rule

**Current.** Messages mix `lode: …` (most), bare (the sentinels, such as
`option not valid for volume`), `s3: …` and `parquet: …`.

**Problem.** Callers that surface Lode errors to users get inconsistent
text; wrapped chains can read `lode: failed to write data: lode: …`.

**Proposed.** Sentinels carry no prefix (they are matched, not read);
wrapping sites add `lode: ` exactly once, at the public boundary. Adapters
keep their package prefix (`s3:`), codecs theirs (`parquet:`).

**Breaks.** Nothing under `errors.Is`; substring matches on full messages.

**Status.** Open. Not strictly breaking; listed so it is decided with the
rest rather than drifting.

---

## D7 — The S3 test double is compiled into the adapter

**Current.** `s3.MockS3Client` / `s3.NewMockS3Client` are exported from the
built `lode/s3` package: an in-memory `API` implementation that only Lode's
own tests use. The hygiene pass moved it into a `_test.go` file and had to
move it back: the symbols are exported, so removing them from the compiled
package is an API removal, not a refactor.

**Problem.** ~250 lines of test scaffolding ship in the adapter, are measured
as production code, and are a public surface nobody has committed to.

**Proposed.** Move the mock to `lode/s3/mock_test.go` (unexported from the
built package). Downstream tests that want an in-memory `API` can vendor it
or use a real emulator (`examples/s3` already targets LocalStack and MinIO).

**Breaks.** Any downstream test importing the mock. Quarry does not.

**Status.** Open. Recommended accept.

---

## D8 — On-disk path compatibility has no stated policy

**Current.** `V1_READINESS.md` §2 freezes manifest *fields*; nothing freezes
the directory names a layout writes (`datasets/`, `partitions/`, `segments/`
under Hive partitions, `snapshots/` in the default layout, `data/`,
`manifest.json`, `latest`). `CONTRACT_LAYOUT.md` describes topology as a
configurable abstraction. The v0.10.0 pass renamed *internal identifiers* to
say snapshot and left every byte on disk unchanged — because readers of
existing stores depend on those names, not because a document says so.

**Problem.** Without a policy, a future path change could be treated as
compatible when it would orphan every existing dataset.

**Proposed.** State in `CONTRACT_LAYOUT.md` that the path grammar of each
built-in layout is part of the persisted format: names may be added (a new
layout) but never changed or removed for an existing layout, and
`V1_READINESS.md` §2 gains the corresponding checkbox. `Manifest` JSON field
names (`snapshot_id`, `parent_snapshot_id`) fall under the same rule.

Volume does not use `Layout` (`PUBLIC_API.md`). Its fixed grammar is named
explicitly so the rule covers it:
- `volumes/<id>/snapshots/<snapshot>/manifest.json`
- `volumes/<id>/data/<offset>-<length>.bin`
- `volumes/<id>/latest`

Volume being experimental (D9) does not exempt its persisted data: a
Volume written before the boundary moves must stay readable after it.

**Breaks.** Nothing; it forbids a class of future changes.

**Status.** Open. Recommended accept before v1.0.

---

## D9 — Volume enters v1.0 as experimental

**Current.** `Volume` (`NewVolume`, `Volume`, `VolumeWriter`, `VolumeReader`,
`VolumeID`, `VolumeSnapshotID`, `VolumeSnapshot`, `VolumeManifest`,
`BlockRef`, `ErrRangeMissing`, `ErrOverlappingBlocks`,
`ErrOptionNotValidForVolume`) is exported from `lode` next to `Dataset` and
documented as a coequal persistence paradigm. `V1_READINESS.md` §4 gates
v1.0 on Volume dogfooding, and §2 on `VolumeManifest` stability.

**Problem.** No dogfooding consumer writes or reads a Volume, so §4 and
the `VolumeManifest` freeze have no evidence and none is in reach.

The workloads Volume is for share one shape: a byte object of known length,
immutable once written, that is filled in pieces. The pieces arrive in
parallel, out of order, or across process restarts, and a partly filled
object is useful as long as its gaps are explicit. Examples: resumable
mirroring of large remote files; range-subsetting of chunked scientific and
geospatial formats; sector imaging with a map of recovered and missing
ranges; and sharded writers that each fill a range of a fixed layout.

Committed ranges never overlap (`CONTRACT_VOLUME.md` §Write Lifecycle, §API Constraints), so
workloads that rewrite regions do not fit, and neither do streams of unknown
length (`TotalLength` is required). The candidate workloads surveyed for
dogfooding are all of the fill-once shape, and none is implemented. The
nearest is a proposal to move Quarry's artifact bytes into Volumes
(pithecene/quarry#255), which would fill ranges in order.

Holding v1.0 for this evidence would hold the Dataset surface, which has
eight months of production evidence, behind a container nobody uses yet.

**Two containers, not a family.** `Dataset` and `Volume` share one internal
core, `History[ID, M]` (`internal/history`: the snapshot chain, the latest
pointer, CAS). Each container supplies its own `Kind` (its path
grammar) and its own manifest type, and the manifests differ in kind:

| Container | What a snapshot's manifest describes |
|---|---|
| `Dataset` | The files of that one write |
| `Volume` | Every block committed so far (cumulative) |

The shared seam is internal, which is the right level. No public "container"
abstraction should be drawn from two instances. Reconsider only if a third
container appears with real demand.

**Proposed.** Volume is excluded from the v1.0 compatibility promise until it
passes its own gate: `V1_READINESS.md` §4, which now also holds
`VolumeManifest` stability and the Volume halves of §6 (AWS S3) and §10
(checksum-verified corruption). How the exclusion is expressed:

| Option | Shape | Enforced by | Cost |
|---|---|---|---|
| A. Label only | `Experimental:` doc comments; exclusion stated in `PUBLIC_API.md` | Documentation only. The v1 module version covers every exported name by Go convention, and nothing in the version or import path tells a caller that Volume is different | None |
| B. Subpackage | Directory `lode/volume/` beside `lode/s3/`, in the same module; package doc and `PUBLIC_API.md` state that it is experimental | A separate import path, so every caller opts in visibly, plus documentation. It still ships at v1 | Shared internals move to `internal/`. Volume options leave `lode.Option` |
| C. Nested module | Directory `lode/volume/` with its own `go.mod`, versioned v0.x | Semantic versioning: v0 may break | As B, plus the problems below |

Both B and C keep the public API under `/lode` (`AGENTS.md` §Public API
boundaries); the package lives in the `lode/` directory, as `lode/s3` does.

C's costs make it the wrong choice:
- **Cross-module internals.** The `volume` module would import the parent's
  `internal/` packages. A `go.mod` requirement is a minimum, not a pin, so
  minimal version selection can pair an older `volume` with a newer `lode`
  whose internals changed. That combination stops compiling. Avoiding it
  needs either a public seam for the history core (new v1 surface) or a
  duplicate of it.
- **CI.** Nested modules sit outside the root module's `./...`, so `task test`
  and `task build` would silently stop covering Volume. They would need
  their own jobs or a workspace.

Recommended: **B**. It marks Volume's boundary in the import path without
cross-module coupling. "Experimental" is stated in the package and in
`PUBLIC_API.md`, not encoded in a version.

Work required for B (and C):
- Move the history core and the retry configuration into `internal/`
  (`internal/history`, `internal/retry`). Checksums need no move:
  `lode.Checksum` is public and Volume uses it as is.
- Give Volume its own `Option` type. `lode.Option` carried an unexported
  `applyVolume` method (`lode/dataset.go`), which another package cannot
  implement or call. Volume uses only the checksum and retry options.
- `ErrOptionNotValidForVolume` leaves `lode`. This shrinks D3 to two targets.

The on-disk Volume path grammar does not change. Volume does not use
`Layout`, so D8 names its fixed grammar explicitly.

**Breaks.**
- Under A: nothing.
- Under B or C: every Volume caller changes its import path and option
  constructors (`examples/volume_sparse`; no downstream consumer known), and
  callers matching `ErrOptionNotValidForVolume` lose that sentinel.
- `Dataset` callers are unaffected.

**Status.** Accepted and shipped (owner decisions, 2026-10-20): Volume is
experimental for v1.0, structured as **B**, package `lode/volume`, with
unprefixed names (`volume.New`, `volume.ID`, `volume.SnapshotID`,
`volume.Snapshot`, `volume.Manifest`, `volume.BlockRef`, `volume.Writer`,
`volume.Reader`, `volume.Volume`, `volume.Option`, `volume.ErrRangeMissing`,
`volume.ErrOverlappingBlocks`). Shipping met the conditions this entry set:
- the boundary exists: package `lode/volume`, its own `Option` type, and
  `ErrOptionNotValidForVolume` removed from `lode`;
- `PUBLIC_API.md` (§Volume, §Compatibility scope) and `CONTRACT_VOLUME.md`
  mark Volume experimental;
- `PUBLIC_API.md` §Compatibility scope states which packages the v1.0
  promise covers.

Volume dogfooding evidence (§4) no longer blocks v1.0; D9 itself is
settled.

---

## D10 — Commit semantics left open by the head-pointer protocol

**Current.** CONTRACT_CORE §Head Pointer (#225) defines how a commit
advances the `latest` pointer and how readers and writers treat a pointer
that names a snapshot without a manifest. The investigation of
[#208](https://github.com/pithecene/lode/issues/208)
(`dogfood_audit_2026-10.md` F3) identified six further questions that the
contracts either leave open or answer inconsistently. Each is decided
separately below. Only D10d changes the commit protocol: it removes the
implicit pointer replacement by `Delete` and `Put`, which remains only in
`lode.SingleProcess`, a wrapper the caller selects explicitly.

### D10a — A listing cursor is not a commit-order cursor

**Current.** `ListManifests` orders snapshots by `(Partition, ID)`, and
`StartAfter` resumes after a given reference (CONTRACT_READ_API §Listing
Order and Cursors). A snapshot ID records when its write *began*
(CONTRACT_CORE §Snapshot ID), and writes commit in a different order: in
the production fork, a child's ID preceded its parent's.

**Problem.** A consumer that reads new snapshots by passing its last
reference as `StartAfter` permanently skips a snapshot that commits after a
snapshot with a larger ID. Nothing in the contracts warns against this
pattern.

**Options.**
1. Document the limit. CONTRACT_READ_API states that `StartAfter` is a
   position in `(Partition, ID)` order, which within one partition is ID
   order, not commit order, and that v1.0 offers no cursor over commits. A
   consumer that must observe every commit reconciles against a full
   listing, which on a Hive layout omits a canonical-only snapshot until
   D10b is resolved.
2. Assign each snapshot an ID at commit time that exceeds its parent's ID,
   so that ID order equals commit order along the history. This changes the
   meaning of the ID, which retention reads as the write's start instant
   (CONTRACT_RETENTION §Grace Period).

A cursor over commits is a feature, not a correction of the listing
contract, and can be added after v1.0 without breaking callers. It is
therefore not designed here.

**Proposed.** Option 1.

**Breaks.** Nothing; it documents an existing limit.

**Status.** Open.

### D10b — On a Hive layout, listing cannot see a canonical-only snapshot

**Current.** A commit persists the canonical manifest first and the
partition manifests after it. `Latest` and `Snapshot(id)` use the canonical
manifest; `ListManifests`, `ListPartitions`, and expiry by `Partition` find
snapshots only through partition manifests. CONTRACT_LAYOUT requires Hive
layouts to place manifests under partition prefixes so that a
partition-filtered listing cannot miss a committed snapshot.

**Problem.** A writer that terminates after the canonical manifest leaves
the partition index incomplete, which contradicts CONTRACT_LAYOUT. If it
terminates before the first partition manifest, the snapshot is
*canonical-only*: `Latest` and `Dataset.Snapshots` return it, but
`DatasetReader.ListManifests`, `ListPartitions`, and expiry by `Partition`
do not. If it terminates later, the snapshot is missing only from the
partition-filtered results of the partitions whose manifests were not
persisted. The audit's open question
F6 (six production snapshots that `ListManifests` does not return) is
consistent with the canonical-only case, though not yet confirmed.

**Options.**
1. Define the canonical manifest as the commit and partition manifests as
   an index. An unfiltered listing, which already enumerates canonical
   manifests and currently discards them, reports a canonical-only snapshot
   under the empty partition. A snapshot that has any partition manifest is
   reported through its partition manifests as before, so the references
   and `StartAfter` positions of existing snapshots do not change. A repair, for example as part of `Sweep`,
   persists any missing partition manifest from the canonical manifest,
   whose `Files` name every partition; this creates objects and modifies
   none. Until the repair runs, a partition-filtered listing documents that
   it reflects the index.
2. Persist the partition manifests before the pointer and the canonical
   manifest, so that the index is complete whenever the commit is. Listing
   would then report partition manifests of uncommitted snapshots unless it
   confirmed each canonical manifest (one more `Exists` per snapshot).

**Proposed.** Option 1, after the read-only check that the six F6
snapshots are canonical-only.

**Breaks.** Nothing for callers; unfiltered listings may report snapshots
they previously omitted.

**Status.** Open.

### D10c — Dataset and Volume retries differ in snapshot ID

**Current.** A Dataset retry keeps its snapshot ID (CONTRACT_WRITE_API
§Optimistic Concurrency (CAS)); a Volume retry draws a new one (CONTRACT_VOLUME
§Optimistic Concurrency (CAS)). Both are documented, but CONTRACT_VOLUME also
describes its mechanism as identical to the Dataset's.

**Problem.** Neither contract states why the retries differ, so the
asymmetry reads as an accident beside the claim that the mechanisms are
identical.

**Proposed.** Retain both behaviors and state the reason in the contracts:
a Dataset's data paths contain the snapshot ID, so the ID is fixed once
data is written, whereas a Volume's block paths do not
(`volumes/<id>/data/<offset>-<length>.bin`).

**Breaks.** Nothing.

**Status.** Open.

### D10d — Commits on a store without CAS fall back to Delete+Put without notice

**Current.** When the store does not implement `ConditionalWriter`, a
commit replaces the pointer with Delete+Put, and concurrent writers can
fork the history (CONTRACT_WRITE_API §Concurrency). Among the bundled
adapters, only the filesystem adapter on platforms without a file lock,
such as Plan 9, lacks CAS; it supports CAS on Unix and on Windows.

**Problem.** AGENTS.md prohibits silent fallbacks, and the fallback is the
only remaining path by which Lode itself can fork a history.

**Options.**
1. Implement `CompareAndSwap` for the filesystem adapter on the remaining
   platforms, so that every bundled adapter supports CAS, and keep the
   documented fallback for custom adapters.
2. Require a construction option that acknowledges single-writer use on a
   store without CAS, and fail construction without it. This adds a
   configuration option and breaks callers of custom adapters.
3. Require `CompareAndSwap` of every `Store`, and remove the fallback. A
   backend without a conditional write is wrapped explicitly with
   `lode.SingleProcess`, which serializes the commits of one process and
   states that it does not coordinate other processes.

**Decided.** Option 3. Option 1 leaves the fallback, and with it the fork,
for every custom adapter. Option 2 accepts the fork once the caller
acknowledges it. Under option 3 a store that cannot coordinate writers does
not compile as a `Store`, and the single-process scope is chosen in the
code rather than in an option.

**Breaks.**
- Every custom adapter that lacks `CompareAndSwap`: it implements the
  method, or its caller wraps it with `lode.SingleProcess`.
- Callers that type-assert `ConditionalWriter` on a `Store`: the assertion
  always succeeds.

**Status.** Accepted and shipped (owner decision, 2026-10-22). The
filesystem adapter implements CAS on Unix and Windows; on platforms without
a file lock, such as Plan 9, its `CompareAndSwap` returns an error that
matches `errors.ErrUnsupported`.

### D10e — Volume has no repair operation for an abandoned commit

**Current.** After an abandoned commit, every Volume commit returns
`ErrSnapshotConflict` until the missing manifest is persisted or an
operator sets the pointer to an earlier snapshot (CONTRACT_VOLUME §Error
Semantics). No API performs the repair.

**Options.**
1. Document the manual procedure in `PUBLIC_API.md`, to be performed while
   no writer is active. Afterward, a `Volume` value that remembers a head
   other than the restored one refreshes it as CONTRACT_VOLUME §Error
   Semantics requires; after the repair of a first commit, that `Latest`
   call returns `lode.ErrNoSnapshots`. The procedure:
   - If the Volume has an earlier snapshot, replace the pointer content,
     the abandoned ID, with the ID of the previous head through
     `CompareAndSwap(ctx, "volumes/<id>/latest", <abandoned ID>, <previous
     head>)`; `Put` cannot replace an existing object. The previous head is
     the snapshot with a manifest that no other snapshot names as its
     parent; in a linear history exactly one exists. The newest ID is not a
     safe choice, because IDs record when a write began, not commit order.
   - If the abandoned commit was the Volume's first, no previous head
     exists: `Delete` the pointer, after which the next commit creates it.
2. Add a repair operation to `lode/volume`, which is experimental (D9) and
   can therefore change without a compatibility cost.

**Proposed.** Option 1 now; reconsider option 2 when Volume passes its
stability gate (`V1_READINESS.md` §4).

**Breaks.** Nothing.

**Status.** Open.

### D10f — Commit rules are stated in several contracts

**Current.** CONTRACT_CORE, CONTRACT_WRITE_API, CONTRACT_STORAGE,
CONTRACT_READ_API, CONTRACT_RETENTION, and CONTRACT_VOLUME each restate part
of the commit protocol.

**Problem.** The restatements drifted before #225 (`dogfood_audit_2026-10.md`
F3) and can drift again.

**Proposed.** Make CONTRACT_CORE §Head Pointer the only normative statement
of the commit protocol; the other contracts state their own obligations and
refer to it instead of restating it.

**Breaks.** Nothing.

**Status.** Open.

---

## D11 — Construction and record types

**Current.** Each handle is constructed separately from a `StoreFactory`
and a list of `Option` values: `NewDataset(id, factory, opts...)`,
`NewDatasetReader(factory, opts...)`, and `volume.New(id, factory, length,
opts...)`. A factory defers the store's construction error into the
constructor's error, so a caller checks one error. Records are `[]any` on
`Write` and on `Read`. The layout is given to each constructor and is
persisted nowhere.

**Problem.** The call sites show five costs:
1. **Repeated configuration.** Quarry constructs the same dataset in three
   places (its filesystem client, its S3 client, and its read path), each
   with `WithHiveLayout("source", "category", "day", "run_id",
   "event_type")`, `WithCodec(NewJSONLCodec())`, and `WithRetryCount(3)`.
   The examples pass `WithHiveLayout("day")` to both the writer and the
   reader of one dataset. A reader configured with another layout finds no
   manifests and returns no error.
2. **A second store value.** Quarry retains the factory and calls it again
   to obtain a store for files outside any dataset (sidecar files), so the
   process holds two store values for one root. Two `lode.SingleProcess`
   values do not serialize each other (CONTRACT_STORAGE §ConditionalWriter
   Capability). Quarry also builds the sidecar path from Lode's Hive path
   grammar (`datasets/<id>/partitions/.../files/<name>`), which couples it
   to the on-disk layout (D8).
3. **Untyped records.** Quarry writes `map[string]any` records and reads
   `[]any`, asserting each element's type; numbers in snapshot metadata
   return as `float64` after the JSON round trip, and Quarry converts them
   back. The examples assert `[]byte` (blob mode, D1) and `map[string]any`
   (Parquet).
4. **A reader that requires CAS.** `NewDatasetReader` takes a `StoreFactory`
   and therefore requires `CompareAndSwap`, although a reader never writes
   (D10d).
5. **Options for two targets.** Writer-only options passed to
   `NewDatasetReader` fail at run time with
   `ErrOptionNotValidForDatasetReader` (D3).

**Options.** Each option is shown as Quarry's writer construction would
read under it.

1. **Typed factory with the current constructors.** The factory carries the
   store's type, and each constructor states what it requires:

   ```go
   type Factory[S ObjectStore] func() (S, error)

   func NewDatasetReader[S ObjectStore](f Factory[S], opts ...Option) (DatasetReader, error)
   func NewDataset[S Store](id DatasetID, f Factory[S], opts ...Option) (Dataset, error)
   ```

   Type inference requires no annotation at existing call sites, and a
   read-only factory passed to `NewDataset` fails to compile. This resolves
   cost 4 and retains the single error check, but leaves costs 1, 2, 3,
   and 5.

2. **A root handle with package-level typed datasets.** A handle is opened
   once per store root, with the store-level options:

   ```go
   repo, err := lode.Open(ctx, lode.FS(root), lode.WithHiveLayout("source", "category", "day", "run_id", "event_type"))
   if err != nil { ... }
   events, err := lode.DatasetOf[Record](repo, "events", lode.WithCodec(lode.JSONL[Record]()), lode.WithRetryCount(3))
   ```

   The first `Open` of a root persists a root descriptor that names the
   layout, created with `CompareAndSwap`; a later `Open` reads it and
   rejects a conflicting layout, and `OpenReadOnly` takes an `ObjectStore`
   and needs no layout option. Options divide into store-level options on
   `Open` and dataset-level options on the dataset. This resolves costs 1,
   3, 4, and 5, and two error checks remain.

3. **A root handle with generic methods.** As option 2, with the dataset
   obtained through a generic method of a concrete `Repo` (Go 1.27). `Open`
   records its error and returns it from every method of the handle, so a
   chained construction checks one error, as a factory does today:

   ```go
   events, err := lode.Open(ctx, lode.FS(root), lode.WithHiveLayout("source", "category", "day", "run_id", "event_type")).
   	Dataset("events", lode.WithCodec(lode.JSONL[Record]()), lode.WithRetryCount(3))
   ```

   The record type is inferred from the typed codec; a dataset without a
   codec states it (`Dataset[[]byte]`). A prototype compiled with Go 1.27.3
   confirms both the chain and the inference. `Repo` must be a concrete
   type, because interface methods cannot declare type parameters; the
   returned `Dataset[T]` remains an interface.

None of the options resolves cost 2. Whether the root handle exposes its
store for objects outside any dataset, or whether such objects become a
part of the layout, is a separate question that this entry records but does
not decide.

**Proposed.** Option 3, provided that the Go 1.27.3 minimum (#261) is
accepted; otherwise, option 2. Options 2 and 3 persist the layout, which
`AGENTS.md` requires of metadata, and give each option one target. Option 3
also retains the single error check of the current factory. If option 2
or 3 is accepted, it decides D1 (the payload type is the dataset's type
parameter) and D3 (options divide by level); D2 remains, because the layout
option still takes a layout value.

**Breaks.**
- Every constructor call: the examples, the README, `PUBLIC_API.md`, and
  Quarry's three constructions and two query functions.
- Under options 2 and 3, a root written before the change has no
  descriptor. The first `Open` with an explicit layout creates it; until
  then, `OpenReadOnly` requires the layout option. The descriptor is a new
  object at the root, which D8 must name; a consumer that reads objects by
  path is otherwise unaffected.
- Under option 3, the minimum Go version (#261).

**Status.** Open.
