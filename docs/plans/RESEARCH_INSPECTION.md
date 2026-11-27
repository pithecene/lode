# RESEARCH: Inspection Primitives

**Status:** Research notes, 2026-11-27. Nothing here is an approved API, a
command, or a plan of record. It lists what a caller can inspect through the
public API today, what is missing, and which gaps wait on the discovery work
in [`RESEARCH_METADATA_PLANE.md`](RESEARCH_METADATA_PLANE.md).

## 1. Purpose and principle

An operator, an agent and a visual workbench ask the same questions of a
store: what datasets exist, how a dataset is partitioned, which snapshots it
holds, whether its objects match its manifests, and what retention has
removed or left behind.

**Principle.** One set of read-only inspection calls serves the library's
callers, a command-line tool and any graphical workbench. A workbench is a
separate, optional program over those calls. Lode stays an embeddable
library: inspection adds no daemon, no service and no index outside the
store. A scan that reads every object runs only when a caller asks for it.

## 2. What the public API answers today

This table names the call that answers each question. It states no cost:
the contract row named in each line holds the store calls, with their
conditions. Unless a line names another contract, the row is in
`CONTRACT_COMPLEXITY.md`.

| Question | Call | Where its cost is stated |
|---|---|---|
| Which datasets exist? | `ListDatasets` | §Enumeration and Removal, `ListDatasets` |
| Which partitions does a dataset have? | `ListPartitions` | §Enumeration and Removal, `ListPartitions` |
| Which snapshots are committed? | `Snapshots`, or `Manifests` with no partition | §Dataset Read Operations, `Snapshots()`; §Enumeration and Removal, `Manifests` |
| What does one snapshot hold? | `Snapshot(id)` | §Dataset Read Operations, `Snapshot(id)` |
| What are a snapshot's objects, sizes and checksums? | the manifest's object list | none beyond the manifest |
| Do one object's bytes match its checksum? | `OpenDecompressed`, read to a successful `io.EOF` | `CONTRACT_READ_API.md` §Complexity Summary, `OpenDecompressed` |
| What would one expiry remove? | `Expire` with `DryRun`, on a writer | §Dataset Retention, `Expire` with `DryRun` |
| What would one sweep remove? | `Sweep` with `DryRun`, on a writer | §Dataset Retention, `Sweep` |
| Which byte ranges does a Volume snapshot cover? | `Blocks` on a reader that `ReaderAt` opened | `CONTRACT_VOLUME.md` §Complexity Bounds, `ReaderAt`; §Read Semantics describes `Blocks` |

Two limits on the integrity rows. `OpenDecompressed` compares a digest only
when the manifest records a checksum. `Read(id)` checks that every object
path belongs to the snapshot, and a record read must decode, but without a
recorded checksum it makes no digest comparison either.

`ManifestListOptions.Prefetch` bounds how many manifests a listing loads
ahead concurrently. It does not reduce the requests. One measured cold
inventory of 14,834 snapshots took 45 LIST pages and 14,834 GETs, about 60
seconds ([`RESEARCH_METADATA_PLANE.md`](RESEARCH_METADATA_PLANE.md) §2).

## 3. Gaps

1. **Snapshot counts and totals cost a full inventory.** A dataset's snapshot
   count, its count per partition, and its referenced bytes (the sum of the
   sizes that manifests record) each need every snapshot's manifest loaded. A
   summary page of a workbench would pay an inventory of that kind whenever
   it has no cached result. This gap is the discovery problem of the
   metadata research, and inspection does not solve it separately.
2. **A partition's snapshots can be incomplete.** A partition-filtered
   listing can omit a committed snapshot whose partition index is incomplete
   (`CONTRACT_READ_API.md`), so a per-partition view is correct only after
   reconciling against the unfiltered listing. Same dependency as gap 1.
3. **No inventory of retention state.** A dry run of `Expire` or `Sweep`
   previews one request, on a writer handle. Neither handle lists the
   tombstones, the pending commits, or every path of an unreferenced object:
   `SweepReport` counts the orphans it would reclaim and names only those in
   `Unparseable`, and a tombstone that is already settled need not appear in
   it.
4. **No physical usage figure.** Referenced bytes are not stored bytes. The
   October audit ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md))
   found 3.85 GB of data objects, of which manifests referenced 1.78 GB.
   Data objects sit under each snapshot, and a store listing returns keys
   without sizes. Physical usage therefore needs every data key enumerated,
   including those that no manifest references, and each length read from
   the object or from backend metadata. No call reports it.
5. **No integrity report.** A caller can verify one object by reading it, and
   the October audit checked a whole dataset with its own program. No call
   returns, for a dataset or a snapshot, the objects that are missing,
   mis-sized or failing their checksum.
6. **No lineage walk.** A manifest records its parent, but a caller follows
   the chain one `Snapshot` call at a time, and an expired ancestor ends it.
7. **No Lode command.** An operator can read a stored manifest with ordinary
   storage tools, but no supported command exposes Lode's read semantics:
   commitment, deduplication by snapshot, or checksum verification.

## 4. What can start now, and what waits

Independent of the discovery work, because each reads only what a caller
names or explicitly asks to scan:

- An integrity report for one snapshot, and for a dataset on request (gap 5).
- A read-only inventory of retention state (gap 3).
- Physical usage of a dataset on request (gap 4).
- A lineage walk that reports where and why it stops (gap 6).
- Volume coverage across snapshots, from `Blocks`.

Waiting on Lode-owned discovery, so that inspection does not grow a second
discovery path beside the one under research:

- Cheap snapshot counts, per-partition counts and referenced-bytes totals
  (gaps 1 and 2).

## 5. Decisions this needs before any code

1. **Where the calls live.** A new public call restarts the API surface
   window of [`V1_READINESS.md`](V1_READINESS.md) §1. An experimental package,
   as `lode/volume` is, would keep inspection outside the v1.0 promise while
   it settles.
2. **Whether a command-line tool belongs in this repository.** It would be a
   new top-level directory, which needs an entry in `docs/ARCH_INDEX.md`, or
   it could live in a separate module that depends on Lode.
3. **Report shapes.** `SweepReport` and `ExpireReport` already report by
   snapshot ID and count. Inspection reports should use the same vocabulary,
   so that a dry run and an inspection of the same store agree.
4. **Workbench scope.** A graphical workbench is outside this repository and
   starts once the calls above are usable.
