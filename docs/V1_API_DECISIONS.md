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
here with the rest.

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
