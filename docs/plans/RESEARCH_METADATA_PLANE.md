# RESEARCH: The Lode Metadata Plane

**Status:** Research notes, 2026-11-27. Nothing here is an approved format or a
plan of record. Measurements come from a rehearsal with a private downstream
consumer (private) that runs Quarry over Lode; reference mechanisms come from
primary specifications (section 7), with inferences marked.

## 1. Question and principle

**Question.** How far can an object-native storage engine exploit the
hierarchical namespace of its underlying storage to achieve efficient,
durable and transactional access without duplicating that hierarchy inside a
separate database?

**Principle to keep.** The object layout expresses Lode's data model: data
stays intelligible without Lode, physical organization follows access
patterns, recovery and inspection work with ordinary storage tools, no
catalog service is mandatory, and the persisted form outlives any one
implementation.

**Constraints.** Backend independence; self-describing data; immutable
committed snapshots; explicit retention; no mandatory external database; no
storage semantics duplicated in consumers.

## 2. Evidence

An S3-compatible bucket (Cloudflare R2) holding a dataset of 44,917 keys / 14,834 snapshots / 119 runs, Quarry
prune dry run selecting 32 runs. Quarry v0.20.2: no report after 13 h 50 min.
v0.20.3: 989 s. quarry#492 on Lode main (#440, #442): 815 s, identical
decisions.

| Cost (815 s run) | Requests | Time | Cause in today's protocol |
|---|---|---|---|
| Cold inventory | 45 LIST pages + 14,834 GET | ~60 s | the committed set is only discoverable as every manifest key, loaded |
| Per-run refresh | 31 LIST of 14,833 keys | 265 s | an unchanged `latest` proves nothing: a pending commit can publish under it, and expiry does not move it |
| Tombstone reads | 685 | 103 s | expiry state sits beside, not inside, the committed history |
| Head reads | 321 | 49 s | observed call pattern of this dry run (a live run must reread `latest` per candidate; a dry run may keep its first read) |
| Run-subtree LISTs | 63 | 11 s | legacy commits; note the cost, 0.18 s each against 8.5 s for a dataset-wide refresh |

What the rehearsal ruled out, and why:
- `latest` as a change detector (pending window; expiry does not move it).
- Walking the parent chain (tombstones record no parent).
- Batching live deletion across runs (an unpublished proposal: every moved
  observation weakened a safety guarantee over five review rounds).
- Inferring tombstone absence from a listing (an empty listing is not an
  absent tombstone on the filesystem store).
- Reconstructing discovery in consumers (Quarry and the private consumer each
  needed separate fixes for the same invariant, and Quarry's legacy refresh still
  carries two fail-safe edges that only Lode can close).

## 3. What the reference systems do

| System | Discovery, cold / incremental | Commit point | Removal | Per-commit metadata | External service |
|---|---|---|---|---|---|
| Iceberg | derived cold path: pointer + table metadata, which lists the valid snapshots / re-read pointer | CAS of the metadata pointer (catalog); filesystem rename scheme deprecated as unsafe | `DELETED` entries; expiry is a commit | whole metadata file + manifest list; manifests reused | catalog for a safe swap |
| Delta Lake (filesystem-managed tables) | derived: `_last_checkpoint` + checkpoint + LIST/GET the log tail / LIST from version v | create-only `<v>.json` (put-if-absent); catalog-managed tables ratify through a catalog | `remove` actions retire logical files from a version's state | one small JSON; periodic checkpoints, log compaction | none with put-if-absent |
| lakeFS | KV ref, then metarange, then ranges | KV compare-and-set on the branch | key absent in the new commit; GC | changed content-addressed ranges + metarange | lakeFS server + KV database |
| Icechunk | derived: 1 GET of `repo`, which lists all snapshots and refs / re-GET `repo` | conditional update of `repo`, the only mutable object | expiry rewrites `repo`; ordered GC phases | whole `repo` (backed up first) + snapshot + transaction log + changed manifests | none |
| Hudi | timeline + metadata table's files partition (counts unconfirmed) | atomic PUT per timeline state; a lock is needed for concurrent writer processes, including one ingestion writer with a separately running table service (storage-based lock available); a lone writer running table services inline needs none | active-state flags; clean and rollback | merge-on-read log appends; compaction | none or optional |
| Zarr v3 | derived, on a listable store: `list_dir` of a group's prefix, then each child's `zarr.json` / the same | none: the core specification defines no transactions, atomic multi-key commits, or versions | `erase_prefix` of a node's prefix; no versioned retirement that keeps an earlier state | one `zarr.json` per node; chunks keyed under the node's prefix (`c/1/0`) | none |

Zarr is the closest in principle: its hierarchy is its key layout, and a
reader discovers a group's children by listing the group's prefix on a store
that supports listing; on one that does not, the names come from elsewhere
(Zarr v3 core, §Storage operations). It defines no commit, so it gives no guarantee
across keys and says nothing about concurrent writers. Icechunk adds
transactions and versions to Zarr data through a separate level of metadata,
FlatBuffers records reached from `repo`, which ordinary storage tools cannot
read the way they read a Lode manifest.

Request counts in this table are derived for a cold cache from each
specification's structures, not measured. Two inventories differ here: the
objects in one commit, and the set of commits. Lode already has the first, as
every manifest lists its snapshot's objects directly (CONTRACT_CORE §Manifest);
lakeFS's metarange and Iceberg's manifest list make that inventory reachable
through one further level of records (ranges, manifests). The common thread,
as an inference: the systems that discover snapshots cheaply also keep the
second, in one of three forms: a record reachable from the object a commit
updates (Iceberg's metadata file, Icechunk's `repo`); a walk from a branch
tip through each commit's first parent (lakeFS); or a directory of densely
numbered log files, listed or probed from a known version, whose retained
entries are the history, since cleanup removes old entries without touching
newer ones (Delta `_delta_log`, PROTOCOL.md §Delta Log Entries). Lode
has no inventory of snapshots: its commit point is the canonical manifest
(CONTRACT_CORE §Head Pointer), and no record lists the committed snapshots,
so only enumeration finds them.

## 4. Directions for Lode, layered by access pattern

The three directions are separate decisions, not a sequence. Physical
topology serves localized access; transactional metadata is added only for a
guarantee that topology cannot give. If 4.1 makes subtree discovery cheap
and correct, the measured workloads may no longer need frequent
dataset-wide discovery, and 4.2 is justified only by a consumer that does.

### 4.1 Index-first publication: make the natural path authoritative

Scope: subtree reads (a run, a partition), the access pattern every consumer
so far uses. Keeps the layout as the index.

- Order today: data, pointer CAS, canonical manifest (commit point),
  partition manifests. Proposed: data, partition entries, pointer CAS,
  canonical manifest (still the commit point).
- Visibility: a partition entry is visible only when its canonical manifest
  exists. Pending and abandoned commits stay invisible; every committed
  snapshot has a complete index by construction.
- Subtree read: LIST the subtree, then confirm the canonical manifest of each
  ID the reader has not confirmed as committed. A listed ID can gain its
  canonical manifest without the listing changing, and an interrupted expiry
  can leave an entry whose canonical manifest is gone, so a cached
  confirmation needs invalidating in both directions; how is unresolved.
  Hypothesis, to be measured with those rechecks included: a per-run refresh
  well under the 8.5 s of a dataset-wide one.
- Expiry already deletes the canonical manifest first, so an entry without
  one is invisible after expiry as before commit.

Reversing the write order alone is not enough. To resolve:
- Older readers before commit: a pre-canonical legacy Hive snapshot is
  committed when any of its partition manifests exists (CONTRACT_CORE §Head
  Pointer), so a released reader would take a staged partition entry for a
  committed legacy snapshot. Staged entries need a form that released readers
  ignore, which makes this a distinguishable publication protocol, not a
  reordering.
- Older readers after commit: a released reader then ignores the new entries
  too, and its partition-filtered listing may omit the new snapshots
  (CONTRACT_READ_API allows omission for an incomplete partition index). The
  compatibility decision is open: also publish old-form partition manifests
  after commit, or limit the new form to readers that understand it.
- Legacy discriminator: telling a legacy partition-only snapshot from a
  staged entry needs a field that no recorded format is yet known to provide.
- Lost CAS and interrupted publication, two cases with different recovery:
  a writer that loses the pointer CAS leaves staged entries for an attempt
  the pointer never named, which are orphans once the winner publishes; a
  writer that wins the CAS and stops before its canonical manifest leaves a
  pending head (CONTRACT_CORE §Head Pointer), whose protections apply. An
  indeterminate CAS outcome needs the contract's ancestry check. Readers
  see neither, but retry and Sweep must tell them apart.
- Commit test: a partition lookup confirms commitment per entry, without
  falling back to dataset-wide enumeration.
- Existing incomplete indexes: Sweep reindexes only snapshots older than its
  positive `Grace`, so a completeness marker needs that age condition or a
  separate verification pass.
- Fencing older writers that publish in the old order, and Sweep's changed
  definition of an uncommitted orphan.

Not solved: dataset-wide discovery.

### 4.2 Membership log: make dataset-wide discovery cheap

Scope: cold inventory and "what changed anywhere". Closest reference: Delta
Lake's filesystem-managed tables, which need no service when the store has
put-if-absent. Every Lode store offers that primitive as `Put`. Below the
multipart threshold it never overwrites an existing object (CONTRACT_STORAGE
§Put Upload Paths: `If-None-Match: "*"`, `os.Link`, a mutex); above it, on a
backend that ignores the completion precondition, the protection is
best-effort, so log entries stay single-part. A log claim
uses `Put`, not `CompareAndSwap` with `expected == ""`: on every store,
including S3, that CAS also replaces an existing empty object.

- A commit claims log entry N+1 with a create-only write after its manifest
  exists; the entry is the commit point and the membership record at once.
- Expiry appends a retire entry before it deletes anything. Retiring a
  snapshot is the proposed Lode adaptation; Delta's `remove` retires a logical
  file from a version's state, not a version.
- Refresh: read entries N+1, N+2, ... until absent (a dense sequence needs no
  LIST), or LIST from N if the store gains a start-after listing.
- Checkpoints fold membership at generation G with a hint file (Delta
  `_last_checkpoint`). A Delta checkpoint records table state at one version,
  not which earlier versions remain; a membership checkpoint is a proposed
  Lode adaptation. Hypothesis: cold discovery reads the checkpoint plus the
  K entries after it, instead of 14,834 manifests; a membership-only
  checkpoint gives no manifest contents, so a consumer that needs them, as the
  prune does, still loads those. To be compared on the same inventory result,
  with the checkpoint lag and contents stated.
- Layout fit: the log is a readable directory of small JSON records beside
  the data, which keeps the principle better than a single rewritten root
  (Iceberg, Icechunk), at the cost of a sequence that serializes writers as
  `latest` does today.
- An authoritative log takes part in the commit protocol itself; a log
  appended after publication would recreate the gap between commit and
  membership that this note describes.
- Relation to 4.1: if both exist, a writer can persist its canonical manifest
  and stop before claiming its log entry, so the subtree reader and the log
  disagree on what is committed. Either they are alternatives, or enabling the
  log moves the commit point to the log entry and changes the subtree
  reader's commit test, and with it the cost to benchmark. Unresolved.

### 4.3 Scale-out structures, only where a measured workload needs them

Per-partition summary rows (Iceberg manifest-list partition summaries) in
place of per-partition manifest copies; content-addressed, reusable index
blocks (lakeFS ranges, Iceberg manifest reuse) so a large membership index
is rewritten in proportion to the change. These move toward an engine-owned
structure and are the point at which the question in section 1 is tested
hardest.

### Borrowable regardless of direction

- Back up any mutable root before overwriting it (Icechunk `overwritten/`
  with an inverted timestamp, so a LIST returns newest first).
- Ordered GC with skip-on-failure and age-based safety margins (Icechunk GC
  phases; Delta's protection window for in-progress sidecars), matching
  Lode's tombstone-then-delete expiry.

## 5. Open questions

1. Fencing older writers: can the dataset settings mechanism
   (`ErrSettingsMismatch`) stop every released writer from publishing in the
   old order or without a log entry, and how far back?
2. Bootstrap: building the first checkpoint or the completeness marker from
   one full listing, while writes continue.
3. Contention: a single log sequence serializes writers like `latest`; does
   DESIGN_SCOPED_CONCURRENCY survive, or does it need per-scope sequences?
4. Write amplification: one extra object per commit and per retirement, plus
   checkpoints; reclamation of old entries.
5. Recovery: telling an orphaned manifest or partition entry from a pending
   commit without the pointer.
6. Volumes: whether Volume commits share the design. Both Datasets and
   Volumes map identities to immutable objects and need versioning, atomic
   publication, lookup and reclamation, but a unified engine is the furthest
   step from the layout principle and is not proposed here.

## 6. Benchmark to settle it

Scales of 15,000, 100,000 and 1,000,000 snapshots. Workloads: cold discovery,
warm refresh, targeted subtree reads, sustained writes, expiry, interrupted
commits, concurrent writers. Report read and write amplification and
contention, not elapsed time alone. Baseline: the 815 s run in section 2.

## 7. Sources

- Apache Iceberg table specification, `format/spec.md`:
  <https://iceberg.apache.org/spec/>
- Delta Lake transaction protocol, `PROTOCOL.md`:
  <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- lakeFS internals: <https://docs.lakefs.io/concepts/internals/>
- Icechunk specification v2:
  <https://icechunk.io/en/stable/reference/spec-v2/>
- Zarr v3 core specification, §Stored representation and §Storage
  operations: <https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html>
- Apache Hudi metadata table, timeline, and concurrency control:
  <https://hudi.apache.org/docs/metadata>,
  <https://hudi.apache.org/docs/timeline>,
  <https://hudi.apache.org/docs/concurrency_control>. The Hudi 1.0 technical
  specification could not be read, so its request counts and its
  metadata-table commit protocol are unconfirmed here.
