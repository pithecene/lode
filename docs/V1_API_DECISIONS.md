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
D11 decides the v1.0 construction and record surface, judged at the call
sites of Quarry and the examples; it supersedes D1, D2, D3, and the
fixed-name part of D8.

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

**Status.** Decided by D11 (accepted 2026-10-22): records and blobs are
separate handles, `Records[T]` and `Blobs`.

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

**Status.** Decided by D11 (accepted 2026-10-22): `Layout` is exported,
with one spelling per layout, `Unpartitioned()` and `Hive(keys...)`, passed
with `WithLayout`; path topology moved to the root's path options.

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

**Status.** Decided by D11 (accepted 2026-10-22): each option type has one
target (`RootOption`, `DatasetOption`, `ViewOption`), so an option that
does not apply does not compile, and the `ErrOptionNotValidFor...`
sentinels are removed.

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

**Status.** Accepted as revised by D11 (2026-10-22). The names within a
dataset's paths are root options; the default names and the flat paths,
the order, placement, and parsing of path segments, the manifest's field
names, and the Volume grammar are persisted format that no release
changes (CONTRACT_LAYOUT §Path Names).

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
separately below. D10d changes the commit protocol: it removes the
implicit pointer replacement by `Delete` and `Put`, which remains only in
`lode.SingleProcess`, a wrapper the caller selects explicitly. The
resolution of D10e (#260) changes the Volume commit, which now persists a
staged manifest before it advances the pointer.

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

**Status.** Superseded by #260 (2026-10-22). A Volume commit now persists
a staged copy of its manifest before it advances the pointer, so the next
commit adopts an abandoned commit and merges its blocks (CONTRACT_VOLUME
§Error Semantics). The manual repair of option 1 remains necessary only
for a pointer that names a commit persisted by a release earlier than
v0.11.0, or a pointer edited by hand.

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

## D11 — The v1.0 construction and record surface

**Criterion.** The v1.0 surface is chosen for elegance and ergonomics at
real call sites, with the guarantees unchanged: immutable snapshots, the
commit protocol (CONTRACT_CORE §Head Pointer), `CompareAndSwap` on every
pointer write, explicit and persisted metadata, and the bounds of
CONTRACT_COMPLEXITY. Compatibility with the v0 surface is not a criterion:
Lode is not yet at v1.0, and every caller migrates once. Volume is
excluded (D9).

**Current.** Each handle is constructed separately from a `StoreFactory`
and a list of `Option` values: `NewDataset(id, factory, opts...)`,
`NewDatasetReader(factory, opts...)`, and `volume.New(id, factory, length,
opts...)`. A factory defers the store's construction error into the
constructor's error, so a caller checks one error. Records are `[]any` on
`Write` and on `Read`. The caller chooses one of three built-in layouts;
the names within each path (`datasets`, `snapshots`, `partitions`,
`segments`, `data`, `manifest.json`, `latest`, and `expired`) are constants
in `lode/layout.go` that no option can change.

**Problem.** The call sites show five costs:
1. **Repeated configuration.** Quarry constructs the same dataset in three
   places (its filesystem client, its S3 client, and its read path), each
   with `WithHiveLayout("source", "category", "day", "run_id",
   "event_type")` and `WithCodec(NewJSONLCodec())`; the two write
   constructions also pass `WithRetryCount(3)`. The examples pass
   `WithHiveLayout("day")` to both the writer and the reader of one
   dataset. Each manifest already records the codec, the compressor, the
   checksum algorithm, and the partitioner's name, but no reader uses them,
   and the partitioner's name omits the partition keys. A reader configured
   with a layout of another path grammar (flat rather than Hive, for
   example) finds no manifests, and an unfiltered `ListManifests` then
   returns `ErrNotFound`, the error for a dataset that does not exist, so
   the misconfiguration cannot be told from an absent dataset.
2. **Fixed path names.** The caller cannot choose the names within a path,
   so a store whose objects follow another naming convention cannot be
   read or written, and every name is an obligation of Lode's: renaming a
   constant in a later release would make existing datasets unreadable
   (D8).
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

**Proposed.** One root handle per store and path convention, typed record
datasets, a separate blob handle, and settings read from the newest
manifest. Quarry's writer and reader would read as follows:

```go
root := lode.Open(lode.FS("/data"))
events, err := root.Records(ctx, "events", lode.JSONL[Event](),
	lode.WithLayout(lode.Hive("source", "category", "day", "run_id", "event_type").By(Event.Partition)),
	lode.WithRetry(3))
if err != nil { ... }
snap, err := events.Write(ctx, batch, RunInfo{RunID: runID, Files: n})
got, err := events.Read(ctx, snap.ID) // []Event
var info RunInfo
err = snap.Metadata.Decode(&info)

view, err := lode.OpenReadOnly(lode.FS("/data")).Records(ctx, "events", lode.JSONL[Event]())
```

Most callers configure no paths: `lode.Open(lode.FS("/data"))` uses the
default names. A caller with its own naming convention states only the
names it changes, once, on the root:

```go
root := lode.Open(lode.FS("/data"),
	lode.WithDatasetsDir("tables"),
	lode.WithManifestFile("_manifest.json"))
```

| Element | Shape | Resolves |
|---|---|---|
| Factory | `type Factory[S ObjectStore] func() (S, error)`. `lode.FS`, `lode.Memory`, and the S3 adapter return `Factory[Store]`; constructing one performs no I/O and returns no error. | Retains the single error check |
| Root | `func Open[S Store](f Factory[S], opts ...RootOption) *Root` and `func OpenReadOnly[S ObjectStore](f Factory[S], opts ...RootOption) *ReadRoot`. Each calls the factory once and records any error, which every method of the handle returns. Every dataset of a root shares its one store value. | Cost 4 |
| Paths | Without path options, a root uses today's names, and its paths are those of today's default and Hive layouts. Each name has one `RootOption`, which replaces that name only: `WithDatasetsDir`, `WithSnapshotsDir`, `WithPartitionsDir`, `WithSegmentsDir` (the snapshot directory within a partition), `WithManifestFile`, `WithDataDir`, `WithExpiredDir`, and `WithStagedDir` (see Settings). The name of the `latest` pointer is not an option: the pointer is where writers of one dataset coordinate, and two roots that named it differently would commit independent histories over shared snapshot paths. `WithFlatPaths()` selects today's flat paths, which place datasets at the store root and snapshots directly under the dataset, and cannot be combined with `WithDatasetsDir` or `WithSnapshotsDir`. A writer and a reader of one store state a convention once, on their roots. `WithFlatPaths()` omits the datasets and snapshots directories rather than naming them empty. Each name a root uses must be one canonical path segment under CONTRACT_STORAGE (not empty, not `.` or `..`, without `/` or a backslash, and not beginning with the reserved prefix `.lode-tmp-`) that contains no `=`, and the names a root uses must be distinct and must not be `latest`, the pointer's name, so that every path parses to one meaning; the root records an invalid name or combination, as it records a factory error, and every method of the root returns it. Each manifest records the complete convention, every name and whether the paths are flat, so that two roots whose conventions differ never write to one history. A handle whose convention differs from that of the manifest it resolves is rejected. A root whose datasets directory differs addresses another prefix, every path of which differs, and sees another, possibly empty, dataset. A root whose other names differ shares the pointer: if it finds the manifest the pointer names, the recorded convention rejects it, and if it does not, a writer returns `ErrSnapshotConflict`, as for a pending commit, and never appends. A manifest written before the convention was recorded used today's default or flat names, according to where it lies, and a root with any other names is rejected by it. When no pointer exists, the fallback scan lists the dataset's prefix; if the prefix holds objects but no manifest under the root's names, the dataset is not treated as empty: a writer is rejected rather than starting a second history, and a view reports the mismatch. A dataset ID must be one canonical path segment (not `.` or `..`, without `/` or a backslash, and not beginning with `.lode-tmp-`), because Lode cleans the path it builds from an ID, so that `a/../events` would otherwise address `events`; `=` remains permitted. | Cost 2; D8 |
| Record datasets | A generic method, `func (r *Root) Records[T any](ctx context.Context, id DatasetID, codec Codec[T], opts ...DatasetOption) (Records[T], error)`. `Records[T]` embeds the read-only `RecordView[T]`, which is what `ReadRoot.Records` returns. Codec constructors return the interface type, so that `T` is inferred from the codec. | Cost 3; D1 |
| Blob datasets | `func (r *Root) Blobs(ctx context.Context, id DatasetID, opts ...DatasetOption) (Blobs, error)`, whose `Write` takes one `[]byte` and whose `Read` returns one, with streaming writes. `Blobs` embeds the read-only `BlobView`, which `ReadRoot.Blobs(ctx, id, opts ...ViewOption)` returns. | D1 |
| Settings | Constructing a dataset handle resolves the head under `ctx` as the current reader does (CONTRACT_CORE §Head Pointer), including its fallback scan when the pointer is absent and its reading of manifests stored only under a partition; a view never creates a missing pointer, because it never writes, so it needs no `CompareAndSwap`. The manifest gains a `partition_keys` field, so that with the codec, compressor, checksum algorithm, and partitioner it already records, it names every component a writer must match. A view reads each snapshot with the components that snapshot's manifest names, so a history written with different bundled compressors or checksums remains readable without migration; a snapshot that names a custom component other than the one passed to the view fails to read with an error that names the component; a view needs no partition keys, because each manifest lists its files' paths. A writer whose options conflict with them is rejected; a writer of an empty dataset uses its options, and its first commit records them. A manifest written before `partition_keys` existed names a Hive partitioner without keys; for such a dataset a writer supplies the layout, and Lode verifies its keys, by name and in order, against the partition paths of the first snapshot that has files, found by following parents from the resolved head, before the writer commits; the first commit after that records them, so the walk occurs once per dataset. A chain without files back to its root contains no partition path, so any keys are consistent with it; a chain that ends at a missing or expired parent before a snapshot with files cannot verify the keys, and the write is rejected. A writer's settings are checked against one manifest, found in this order: the manifest of the snapshot the pointer names; that snapshot's staged copy; and, when the pointer is absent or names neither, the manifest `Latest` selects. With no pointer and no manifest, the dataset is empty. A pending first commit has no manifest to check against, so a dataset's first commit persists a staged copy of its manifest under `Staged` before it advances the pointer and removes the copy after it persists the canonical manifest, as a Volume commit does (CONTRACT_CORE §Head Pointer, CONTRACT_VOLUME §Error Semantics); this costs one `Put` and one `Delete` on that commit only, because every later commit has a manifest to check against. A writer that finds a pointer naming a snapshot with neither a manifest nor a staged copy, and no other manifest, returns `ErrSnapshotConflict` until an operator repairs the pointer, as a Volume writer does; only a first commit persisted before D11 or a pointer edited by hand produces that state. Two first commits with conflicting settings are serialized by the pointer's `CompareAndSwap`: the losing writer reads the winner's manifest and is rejected rather than retried. | Cost 1 |
| Layout | An exported `Layout` type that chooses partitioning only: `lode.Unpartitioned()` and `lode.Hive(keys...)`; the names within a path come from the root's path options. A Hive layout reads partition values from `map[string]any` records by name; for typed records, it takes a function that returns the values by key name (`lode.Hive("source", "day").By(func(e Event) lode.Partition { return lode.Partition{"source": e.Source, "day": e.Day} })`), so that partitioning uses no reflection and a value cannot be assigned to the wrong key. A write whose partition names other keys than the layout's fails. The function is code, not configuration, and is not persisted; a function that derives values differently writes different data, as a map record with other values does. `By` is a generic method of the concrete Hive layout, and `Records` reports a function written for another record type. | D2 |
| Options | Writer handles take `DatasetOption`. A view takes no layout or retry. The codec is always the caller's, because it fixes the record type. A codec describes its configuration (`Describe() string`, a Parquet schema, for example) in a deterministic canonical form, so that equal configurations always yield equal descriptions, and each manifest records that description with the codec's name. A writer whose codec's name or description differs from the head's is rejected, and a view reports a snapshot whose codec it does not match rather than decode it. A manifest written before descriptions were recorded is checked by name only. Lode reconstructs the bundled compressors and checksums from the names the manifest records; a custom compressor or checksum is passed to the view explicitly, through `ReadRoot.Records(ctx, id, codec, opts ...ViewOption)`, and its `Name` must match the manifest's. `ViewOption` holds only `WithCompressor` and `WithChecksum`. No global registry is introduced. The `ErrOptionNotValidFor...` sentinels are removed. | Cost 5; D3 |
| Metadata | `Write` takes any value that encodes as a JSON object, a struct or a map; nil persists as `{}`, as CONTRACT_CORE §Metadata Rules requires. `Snapshot.Metadata` retains the persisted JSON and decodes it on request (`Decode(&v)`), so a struct target receives its declared field types; a map target receives JSON's types, with numbers as `float64`. | Cost 3 (metadata) |

The surface requires Go 1.27 for generic methods, which is the minimum
since #261. A prototype compiled with Go 1.27.3 confirms the chained
construction, the inference of the record type from the codec, typed
metadata, and a view over a store without `CompareAndSwap`.

**Guarantees.** The CAS requirement, immutability, and the snapshot model
are unchanged. The commit protocol changes in one respect: a dataset's
first commit persists a staged copy of its manifest before it advances the
pointer, so that its settings can be checked while it is pending. The
manifest gains three fields: `partition_keys`, an array of the Hive keys in
order; `paths`, an object that maps each option's name (`datasets`,
`snapshots`, `partitions`, `segments`, `manifest`, `data`, `expired`, and
`staged`) to its value, with `flat` a boolean; and `codec_description`, the
string `Describe` returns. Their names fall under D8's rule for manifest
fields. and the paths a root produces without path options,
or with `WithFlatPaths()`, are byte-identical to today's default, Hive,
and flat layouts, so existing datasets need no migration. Constructing a
dataset handle costs, when the pointer names a snapshot with a canonical
manifest, one `Get` of the pointer and at most two `Get` calls for the
manifest; without such a pointer, it falls back to the scan that resolving
the head performs today, which CONTRACT_COMPLEXITY already bounds. The
second `Get` arises because a handle that is given no layout does not
know whether the manifest lies under the snapshots or the segments
directory. Construction writes nothing. A dataset's first commit costs one additional `Put` and one
`Delete` for its staged manifest, and the first write to a Hive dataset written before
D11 reads one manifest per snapshot back to the first with files.
CONTRACT_COMPLEXITY would state all three as exceptions; later commits, reads, and listings keep their bounds. Generic records add no per-record cost beyond the codec's own
encoding.

**Alternatives considered.**
- **A typed factory with the current constructors.** It removes cost 4 and
  nothing else.
- **Package-level generic constructors** (`lode.RecordsOf[T](root, ...)`).
  They are what Go before 1.27 permits; they break the chain, so a writer
  checks two errors.
- **A dataset descriptor object.** A separate, immutable object recording
  each dataset's settings. It duplicates what the manifest records, needs a
  path reserved from every dataset ID, and needs a migration for every
  existing dataset; the manifest already serves the purpose.
- **A path template** (`"{dataset}/{partitions}/{snapshot}/{file}"`). It
  would permit any order of segments, but it is a small language to parse
  and validate, which `AGENTS.md` excludes, and an order that places the
  snapshot before the dataset cannot be listed by prefix.
- **Versioned path grammars with migration tools.** They keep the names
  Lode's and add a format version and a rewrite operation for each change.
  Names chosen by the caller make the rewrite unnecessary: a change of
  convention is a change of the caller's path options.

**Open questions.** None.

**Supersedes.** If accepted, D11 decides D1 (separate record and blob
handles), D2 (an exported `Layout` with one spelling per layout), D3 (one
option type with one target), and the fixed-name part of D8 for
datasets (names chosen by the caller, with the default names and the flat
paths fixed as part of the API). D8's rule for the order, placement, and
parsing of path segments, and its rule for the Volume grammar, are
unchanged.

**Contracts.** CONTRACT_LAYOUT states that a layout governs both the
topology of paths and the encoding of partitions; acceptance revises it
so that the root's path options govern the names and `Layout` the
partitioning.
CONTRACT_CORE §Head Pointer gains the staged first manifest and
distinguishes a view's resolution, which never creates a missing pointer,
from a writer's, which may; §Manifest gains the partition keys, the path
convention, and the codec description. CONTRACT_COMPLEXITY gains the
first commit's additional `Put`, the construction's second manifest
`Get`, and the walk of a legacy Hive dataset. CONTRACT_ERRORS gains the
dataset ID rule.

**Breaks.** Every caller, once: the examples, the README, `PUBLIC_API.md`,
and Quarry, whose three constructions become one root and one `Records`
call per role and whose queries lose their type assertions. A caller
whose dataset IDs contain `/` or are not canonical renames them. A caller of
`NewFlatLayout()` opens its root with `WithFlatPaths()`.
Every implementer of a custom codec, once: codecs become generic and
implement `Describe`.

**Implementation.** The surface as shipped settles the details this
decision left open:
- `Root.ListDatasets` and `ReadRoot.ListDatasets` list datasets; the
  per-dataset listing methods (`ListPartitions`, `ListManifests`,
  `GetManifest`, `OpenObject`, `ReaderAt`) belong to every handle and view
  (`SnapshotReader`), and retention (`Expire`, `Sweep`) to the writer
  handles (`Retention`).
- `Records.StreamWrite` takes an `iter.Seq2[T, error]`; `Blobs.StreamWrite`
  returns the `StreamWriter`. `WithRetryCount` is `WithRetry`.
- `Use(store)` is the factory of a store the caller holds; `Memory()`
  creates a store per root.
- `DatasetSnapshot` embeds `*Manifest`, so `snap.Metadata.Decode(&v)`
  reads as written above.
- A view's store is wrapped so that a write returns the new `ErrReadOnly`.
- `Partition` is a map of values by key name, each formatted as Hive
  partition values always were.
- Opening a writer checks its settings against the head, so a
  misconfigured writer fails when it opens; the commit checks again
  (CONTRACT_WRITE_API §Settings).
- Lode recognizes another history's manifest by its file name, from a
  listing alone: the root's manifest name or the default name, which every
  manifest persisted before the names became root options bears. A root
  with a custom manifest name thus finds a history that predates the
  pointer. Recognition by content was considered and declined: an object's
  content can be learned only by reading it, so the check's cost would
  grow with the number of objects in the dataset's directory.
- A view matches a custom checksum by name, as it matches a custom
  compressor; reads do not verify checksums.

**Status.** Accepted (owner decision, 2026-10-22) and implemented: the
persisted format in #279, the surface in the pull request that follows
it.
