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
| Head reads | 321 | 49 s | per-candidate head protection (contract) |
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
| Iceberg | pointer + 1 metadata GET lists all snapshots / re-read pointer | CAS of the metadata pointer (catalog); filesystem rename scheme deprecated as unsafe | `DELETED` entries; expiry is a commit | whole metadata file + manifest list; manifests reused | catalog for a safe swap |
| Delta Lake | `_last_checkpoint` + checkpoint + LIST/GET the log tail / LIST from version v | create-only `<v>.json` (put-if-absent) | `remove` tombstones until expiry | one small JSON; periodic checkpoints, log compaction | none with put-if-absent |
| lakeFS | KV ref, then metarange, then ranges | KV compare-and-set on the branch | key absent in the new commit; GC | changed content-addressed ranges + metarange | lakeFS server + KV database |
| Icechunk | 1 GET of `repo` lists all snapshots and refs / re-GET `repo` | conditional update of `repo`, the only mutable object | expiry rewrites `repo`; ordered GC phases | whole `repo` (backed up first) + snapshot + transaction log + changed manifests | none |
| Hudi | timeline + metadata table's files partition (counts unconfirmed) | atomic PUT per timeline state, plus a lock (storage-based lock available) | active-state flags; clean and rollback | merge-on-read log appends; compaction | none or optional |

The common thread: in every system that discovers cheaply, the commit point
and the membership record are the same object or sequence. Lode separates
them: the commit point is `latest`, membership is the set of manifest keys.

## 4. Directions for Lode, layered by access pattern

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
  ID new since the last read. Hypothesis: per-run refresh about 0.2 s instead
  of 8.5 s on the measured workload.
- Expiry already deletes the canonical manifest first, so an entry without
  one is invisible after expiry as before commit.

To resolve: legacy (pre-1.2.0) snapshots committed by partition manifests
alone (exempt by recorded format); existing incomplete indexes (one Sweep
reindex, then a dataset marker that the index is complete from that point);
fencing older writers that publish in the old order; Sweep's changed
definition of an uncommitted orphan. Not solved: dataset-wide discovery.

### 4.2 Membership log: make dataset-wide discovery cheap

Scope: cold inventory and "what changed anywhere". Closest reference: Delta
Lake, which needs no service when the store has put-if-absent, as every Lode
store does (`CompareAndSwap` with `expected == ""`).

- A commit claims log entry N+1 with a create-only write after its manifest
  exists; the entry is the commit point and the membership record at once.
- Expiry appends a retire entry before it deletes anything (Delta `remove`).
- Refresh: read entries N+1, N+2, ... until absent (a dense sequence needs no
  LIST), or LIST from N if the store gains a start-after listing.
- Checkpoints fold membership at generation G with a hint file (Delta
  `_last_checkpoint`); cold discovery becomes a few GETs instead of 14,834.
- Layout fit: the log is a readable directory of small JSON records beside
  the data, which keeps the principle better than a single rewritten root
  (Iceberg, Icechunk), at the cost of a sequence that serializes writers as
  `latest` does today.

### 4.3 Scale-out structures, only if 4.1 and 4.2 measure short

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
- Apache Hudi metadata table, timeline, and concurrency control:
  <https://hudi.apache.org/docs/metadata>,
  <https://hudi.apache.org/docs/timeline>,
  <https://hudi.apache.org/docs/concurrency_control>. The Hudi 1.0 technical
  specification could not be read, so its request counts and its
  metadata-table commit protocol are unconfirmed here.
