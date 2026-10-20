# DESIGN: Scoped Concurrency for Non-Overlapping Writers

**Status:** Proposal
**Issue:** #133
**Prerequisite:** CAS optimistic concurrency (#135)
**Target:** Pre-v1.0

---

## Problem Statement

Lode's latest pointer is dataset-scoped. When multiple writers target
non-overlapping partitions within the same dataset, they contend on a
single pointer — even though their data paths never intersect.

With CAS (PR #135), this contention produces `ErrSnapshotConflict` and
the caller must retry. The retry is cheap (data files already persisted;
cost is one manifest write + one pointer swap), but the ceremony
shouldn't exist for provably non-overlapping work.

Without scoped concurrency, consumers must either:
1. Fragment one logical dataset into N datasets (one per writer), then
   merge downstream.
2. Serialize writes externally (defeating concurrency).
3. Accept CAS retry overhead for non-overlapping work.

None of these is a satisfying answer for a v1.0 library.

---

## Design Goals

1. Non-overlapping concurrent writes to the same dataset should succeed
   without retry.
2. Overlapping concurrent writes must still be detected and surfaced.
3. The solution must not couple the commit model to a specific layout
   or partitioning scheme.
4. `Latest()` semantics must remain clear and useful for all dataset
   shapes (partitioned and unpartitioned).
5. Linear snapshot history must be preserved (no DAGs, no multi-head).
6. The solution must compose with existing CAS, not replace it.

### Non-goals

- Distributed coordination (consensus, lock management).
- Multi-head snapshot history or partition-level branching.
- Query planning or partition pruning (reader-side concern).

---

## Constraint: Don't Overindex on Hive

The dogfooding evidence comes from a Hive-partitioned pipeline with
concurrent crawlers writing to non-overlapping partition keys. This is
one valid pattern, but Lode aims to be a general-purpose persistence
library. The design must serve:

| Pattern | Partitioned? | Concurrent writers? | Example |
|---------|:------------:|:-------------------:|---------|
| Hive multi-process | Yes | Yes, non-overlapping partitions | Crawler pipeline |
| Unpartitioned multi-process | No | Yes, appending to same stream | Multi-source ingestion |
| Partitioned single-writer | Yes | No | Batch ETL |
| Unpartitioned single-writer | No | No | Simple archiver |

A design that only helps row 1 is too narrow for v1.0. A good design
should help row 1 without harming rows 2-4, and should be honest about
which patterns benefit. Row 2 (unpartitioned concurrent writers) is a
genuinely conflicting pattern — writers to the same path space SHOULD
receive `ErrSnapshotConflict`. Eliminating that conflict would require
append-log semantics, which is a different problem.

---

## Options

### Option A: Per-Partition Pointer Isolation

**Idea:** Each partition gets its own latest pointer and snapshot chain.
Writers to different partitions never interact.

**API shape:**
```go
// Per-partition latest
snap, err := ds.Latest(ctx, lode.WithPartition("category", "alpha"))

// Global latest requires scanning all partition heads
snap, err := ds.Latest(ctx) // scans all partitions, returns most recent
```

**Strengths:**
- Zero contention for non-overlapping partitions.
- Conceptually simple: one pointer per partition.

**Weaknesses:**
- **Couples the commit model to partitioning.** Unpartitioned datasets
  (row 2 in the table above) get no benefit.
- **Breaks the linear history invariant.** Per-partition chains are
  independent lineages. `CONTRACT_CORE.md` says "snapshot history is
  strictly linear (single head)." This becomes per-partition linear,
  which is a model change.
- **Global `Latest()` becomes ambiguous.** Which partition's head?
  Most recent across all? This requires scanning all partition pointers
  on every call — reintroducing the O(N) scan that v0.7.3 eliminated.
- **Manifest format changes.** Snapshots need partition scope metadata.
  Parent chains are per-partition, so a snapshot's parent is not the
  previous global commit but the previous commit to the same partition.
- **Reader complexity.** `DatasetReader` must merge across partition
  heads to present a unified view. This is read-time coordination that
  Lode explicitly avoids.

**Verdict:** Solves the dogfood case but is too narrow and too invasive.
The model change radiates into read path, manifest format, and
contract invariants.

---

### Option B: Conflict-Aware CAS (Automatic Rebase)

**Idea:** Keep the single global pointer and linear history. Make the
CAS conflict check smarter: instead of "did the pointer change?",
check "did the intervening commit(s) touch data paths that overlap
with mine?" If no overlap, automatically rebase (re-parent) and
retry the pointer write internally.

**Mechanism:**

```
Writer A: reads Latest() → S0, writes partition alpha
Writer B: reads Latest() → S0, writes partition beta

Timeline:
1. A commits: CAS(S0 → S1), manifest says files in alpha/  ✓
2. B commits: CAS(S0 → S2) fails — pointer is now S1
3. B reads S1's manifest, sees files only in alpha/
4. B's files are only in beta/ — no overlap
5. B re-parents to S1, retries: CAS(S1 → S2)              ✓
6. History: S0 → S1 → S2 (linear, no fork)
```

The same timeline as a sequence:

```mermaid
sequenceDiagram
    participant A as Writer A (alpha)
    participant P as latest pointer
    participant B as Writer B (beta)
    A->>P: read head: S0
    B->>P: read head: S0
    A->>P: CAS S0 → S1
    Note over A,P: S1 committed (files in alpha/)
    B->>P: CAS S0 → S2
    P-->>B: conflict: head is S1
    B->>B: read S1's manifest: alpha/ only, no overlap with beta/
    B->>P: re-parent to S1, CAS S1 → S2
    Note over P: history S0 → S1 → S2, linear
```

Steps 3-5 happen inside `Write()` / `StreamWrite()`, invisible to the
caller. The caller never sees `ErrSnapshotConflict` for non-overlapping
work.

**API shape:** No change. `Write()`, `StreamWrite()`, `Latest()` all
keep their current signatures. The improvement is purely internal.

```go
// Unchanged — non-overlapping writes succeed without retry
snapA, _ := dsA.Write(ctx, alphaRecords, metadata)  // succeeds
snapB, _ := dsB.Write(ctx, betaRecords, metadata)   // succeeds (auto-rebased)
```

**Overlap detection:**

Overlap is determined by comparing file paths in the conflicting
manifest against file paths in the current commit. Two commits
overlap if any file paths share a common partition prefix.

For unpartitioned datasets (noop partitioner), all writes share the
same path space. Overlap is always true → behavior is identical to
current CAS (caller gets `ErrSnapshotConflict`). This is correct:
unpartitioned concurrent writes genuinely conflict.

For Hive-partitioned datasets, file paths encode partition keys:
```
partitions/source=x/category=alpha/segments/.../data
partitions/source=x/category=beta/segments/.../data
```

Prefix comparison (up to the partition boundary in the path) determines
overlap. The layout already knows where partition keys end and segment
paths begin.

**Rebase mechanics:**

On CAS failure (pointer is at S_head, writer expected S_parent):
1. Walk the snapshot chain from S_head back to S_parent, reading
   each intervening manifest (O(chain_length) store Gets).
2. For each intervening snapshot, extract partition prefixes from
   its file paths.
3. If ANY intervening snapshot's prefixes overlap with the current
   commit's prefixes → return `ErrSnapshotConflict` to the caller.
4. If ALL are disjoint: re-parent to S_head, retry CAS.
5. Limit automatic rebase to 3 attempts to prevent livelock under
   high contention.

**Why full chain check is required:**

Checking only the immediate head is insufficient and creates a
false-negative vulnerability:

```
S0 → S1 (alpha) → S2 (beta)

Writer C (alpha) read Latest() → S0.
Writer A commits S1 to alpha.
Writer B commits S2 to beta (rebased past S1).
Writer C tries CAS(S0 → S3), fails — head is S2.
If C only checks S2 (beta): no overlap → rebases. WRONG.
S1 (alpha) is in the chain and overlaps with C's alpha write.
```

The full chain walk catches this: C reads S2 (beta, disjoint) then
S1 (alpha, overlapping) → returns `ErrSnapshotConflict`. Correct.

Chain length is bounded by the number of commits since the writer
last read `Latest()` — in practice, this equals the number of
concurrent writers (single-digit in typical usage).

**Strengths:**
- **No API change.** Current callers get faster without changing code.
- **Preserves linear history.** Single global pointer, single chain.
  `CONTRACT_CORE.md` invariant holds.
- **Correct for all dataset shapes.** Partitioned datasets get automatic
  non-overlapping resolution. Unpartitioned datasets correctly fall back
  to caller-side CAS (all paths overlap → genuinely conflicting).
  Unpartitioned writers get no concurrency improvement, which is honest:
  those writes genuinely conflict.
- **Composable with CAS.** This is a refinement of CAS, not a
  replacement. CAS is the mechanism; conflict-awareness is the policy.
- **No manifest format change.** The information needed (file paths)
  is already in the manifest.
- **No reader-side impact.** `Latest()`, `Snapshots()`, `Read()` all
  work identically.

**Weaknesses:**
- **Chain walk on conflict.** Each rebase attempt reads all manifests
  between the original parent and the current head. For N concurrent
  writers, the chain length is at most N-1. Cost is O(N) store Gets
  per rebase attempt, bounded by the rebase limit (3).
- **Rebase limit adds a failure mode.** If contention exceeds 3
  concurrent conflicting commits, the caller sees `ErrSnapshotConflict`
  even for non-overlapping work. This is a safety valve, not a
  correctness issue, but it needs documentation.
- **Overlap detection is path-based.** If two writers happen to use
  file paths that share a prefix for reasons unrelated to partitioning,
  they'll be treated as overlapping. This is conservative (false
  positive, not false negative), which is safe.
- **Unpartitioned datasets get no benefit.** All paths overlap → every
  conflict requires caller retry. But this is correct behavior, not a
  limitation — unpartitioned concurrent writes do genuinely conflict.

**Verdict:** General, preserves invariants, no API change, no manifest
change. Cost is bounded by the number of concurrent writers.

---

### Option C: Write Lanes (Explicit Scoping)

**Idea:** Introduce a general "write lane" concept. Writers declare
a lane at construction or write time. Lanes have independent pointers.
Partitions can be lanes, but so can arbitrary string keys.

**API shape:**
```go
// Explicit lane scoping
ds, _ := lode.NewDataset("data", factory, lode.WithWriteLane("alpha"))
```

**Strengths:**
- General: any scoping dimension, not just partitions.
- Explicit: the caller declares intent.

**Weaknesses:**
- **New API surface.** `WithWriteLane` is a new option, new concept,
  new documentation. AGENTS.md warns against expanding API surface.
- **Per-lane pointers = per-lane history.** Same multi-head problem as
  Option A but with arbitrary keys instead of partition keys.
- **Reader-side merge.** Same as Option A — readers must merge lanes.
- **Caller burden.** The caller must know their lane at construction
  time. If lanes map to partitions, the caller is duplicating
  information already encoded in the layout.
- **Interaction with CAS is unclear.** CAS operates on the pointer.
  Per-lane pointers mean per-lane CAS. Global Latest() must scan.

**Verdict:** More general than A but has the same structural problems
(multi-head, reader merge, manifest changes) plus new API surface.
Generality without clear benefit over B.

---

## Comparison

| Property | A: Per-Partition | B: Conflict-Aware CAS | C: Write Lanes |
|----------|:----------------:|:---------------------:|:--------------:|
| API change | Yes | **No** | Yes |
| Manifest change | Yes | **No** | Yes |
| Linear history preserved | No | **Yes** | No |
| Unpartitioned benefit | No | **No** (correct) | Possible |
| Partitioned benefit | Yes | **Yes** | Yes |
| Reader-side impact | Yes | **No** | Yes |
| Contract changes | Major | **Minimal** | Major |
| Implementation scope | Large | **Medium** | Large |

---

## Recommendation: Option B (Conflict-Aware CAS)

Option B is the strongest candidate for v1.0, pending resolution of
the open questions below (particularly overlap granularity and
chain-walk cost bounds).

**Why Option B:**

1. **No API change** — existing callers improve without code changes.
2. **No model change** — linear history, single head, single pointer.
   All existing contracts hold.
3. **Honest scope** — helps partitioned concurrent writes (the
   validated use case). Does not claim to help unpartitioned concurrent
   writes, which genuinely conflict.
4. **Incremental** — builds directly on CAS (#135). The mechanism is
   a refinement, not a redesign.
5. **Reversible** — if a future use case demands per-partition heads
   (Option A/C), it can be added later without undoing Option B.

**Cost model:** Up to N-1 internal rebases for N concurrent
non-overlapping writers. Each rebase walks the chain (O(chain_length)
store Gets, where chain_length ≤ N-1). At the dogfood consumer's
scale (N = single-digit concurrent processes), this is negligible
compared to the current CAS retry overhead (which requires the caller
to re-read Latest(), rebuild state, and re-issue the entire Write()).

**What this does NOT solve:** Unpartitioned concurrent writes. These
genuinely conflict and `ErrSnapshotConflict` is the correct response.
Append-log semantics for unpartitioned concurrency is a separate
design problem that should not gate this work.

---

## Contract Impact

### CONTRACT_CORE.md
No change. Linear history invariant preserved.

### CONTRACT_WRITE_API.md
Additions:
- §Concurrency: document automatic rebase for non-overlapping commits.
- §Complexity Bounds: O(chain_length) Gets per rebase attempt
  (chain walk), bounded by 3 attempts. Total worst case: 3 × (N-1)
  Gets for N concurrent writers.
- §Error Semantics: `ErrSnapshotConflict` now means "overlapping
  concurrent commit" rather than "any concurrent commit." This is a
  semantic refinement, not a new error.

### CONTRACT_STORAGE.md
No change. CAS mechanism is unchanged.

### CONTRACT_ERRORS.md
Refinement: `ErrSnapshotConflict` description updated to specify
overlapping writes only.

### PUBLIC_API.md
- §Concurrency: document non-overlapping write behavior.
- §Usage Gotchas: update concurrent write note.

### Manifest
No change. File paths already encode partition information.

---

## Implementation Sketch

### Phase 1: Overlap Detection

Add a function that extracts partition prefixes from a manifest's
file paths, using the layout's knowledge of partition boundary depth:

```go
// partitionPrefixes returns the set of partition path prefixes
// from a manifest's file list. For unpartitioned layouts, returns
// a single empty prefix (all files overlap).
func (d *dataset) partitionPrefixes(m Manifest) []string
```

### Phase 2: Rebase Logic

On `ErrSnapshotConflict` inside `Write` / `StreamWrite` /
`StreamWriteRecords`:

```
loop (up to 3 attempts):
    1. Read current head from pointer
    2. Walk chain from head back to original parent:
       a. For each intervening snapshot, read manifest (1 Get each)
       b. Extract partition prefixes from file paths
       c. If ANY overlap with current commit's prefixes:
            return ErrSnapshotConflict
    3. All intervening commits are disjoint:
       a. Re-parent current manifest to head
       b. Retry pointer CAS
    4. If CAS fails again (new concurrent commit): loop
```

Chain walk cost: O(chain_length) Gets per attempt, where
chain_length = number of commits since original parent.
Total cost bounded by: 3 attempts × chain_length Gets.

### Phase 3: Volume Equivalent

Volume commits include a block list with explicit offsets. Overlap
detection for Volume is: do any committed block ranges intersect?
This is analogous to partition prefix comparison for Dataset.

---

## Resolved Questions

1. **Transitive overlap: full chain check is required.** Checking
   only the immediate conflicting head creates a false-negative
   vulnerability (see "Why full chain check is required" above).
   The rebase algorithm must walk the chain from current head back
   to the original parent and check overlap against ALL intervening
   commits.

2. **Rebase limit: fixed internal constant, not configurable.**
   A fixed limit of 3 is sufficient for typical concurrent writer
   counts. Adding a configuration option would expand API surface
   without clear benefit — AGENTS.md treats added configuration as
   suspicious by default. If 3 proves insufficient in practice,
   it can be raised as an internal constant without API change.

3. **Unpartitioned concurrency: out of scope.** Unpartitioned
   concurrent writes genuinely conflict. `ErrSnapshotConflict` is
   the correct response. Append-log semantics is a separate design
   problem that should not gate this work or v1.0.

## Open Questions

1. **Rebase cost observability.** Should rebases be logged, metriced,
   or silently absorbed? A counter on the returned snapshot (e.g.,
   `snapshot.RebaseCount`) would let callers detect contention without
   requiring logs. Alternatively, silent absorption is simpler and
   avoids adding to the public type surface.

2. **Overlap granularity.** Partition prefix is coarse. Two writers to
   the same partition but different segments within that partition
   would be flagged as overlapping. Is finer-grained detection needed?
   (Probably not for v1.0 — conservative overlap is safe and avoids
   the complexity of segment-level comparison.)

---

## References

- Issue #133 — partition-scoped pointer proposal
- PR #135 — CAS optimistic concurrency implementation
- `docs/contracts/CONTRACT_CORE.md` — linear history invariant
- `docs/contracts/CONTRACT_WRITE_API.md` — concurrency model
- `docs/contracts/CONTRACT_STORAGE.md` — ConditionalWriter capability
- `docs/V1_READINESS.md` — v1.0 release criteria
