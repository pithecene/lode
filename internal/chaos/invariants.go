package chaos

import (
	"context"
	"fmt"

	"github.com/pithecene/lode/lode"
)

// verifier holds the evidence a finished Dataset run left: the persisted
// history, a clean handle to read it through, and what the workload
// observed. Each invariant is one of its methods.
type verifier struct {
	ds        lode.Dataset
	snapshots map[lode.DatasetSnapshotID]*lode.DatasetSnapshot
	chain     *chain[lode.DatasetSnapshotID]
	settle    lode.DatasetSnapshotID // the settling commit
	head      lode.DatasetSnapshotID // what Latest returns
	acked     map[lode.DatasetSnapshotID]Commit
}

func newVerifier(ctx context.Context, ds lode.Dataset, settle lode.DatasetSnapshotID, ledger *Ledger) (*verifier, error) {
	snaps, err := ds.Snapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("chaos: list snapshots: %w", err)
	}
	head, err := ds.Latest(ctx)
	if err != nil {
		return nil, fmt.Errorf("chaos: resolve the head: %w", err)
	}
	v := &verifier{
		ds:        ds,
		snapshots: make(map[lode.DatasetSnapshotID]*lode.DatasetSnapshot, len(snaps)),
		chain:     newChain[lode.DatasetSnapshotID](),
		settle:    settle,
		head:      head.ID,
		acked:     make(map[lode.DatasetSnapshotID]Commit),
	}
	for _, s := range snaps {
		v.snapshots[s.ID] = s
		v.chain.add(s.ID, s.Manifest.ParentSnapshotID)
	}
	for _, c := range ledger.Commits() {
		v.acked[c.ID] = c
	}
	return v, nil
}

// check is one invariant over the final history, run by a verifier of type
// V; it returns the evidence of every violation.
type check[V any] struct {
	name string
	run  func(v V, ctx context.Context) []string
}

// runChecks runs every check over v and records each violation.
func runChecks[V any](ctx context.Context, v V, checks []check[V], ledger *Ledger) {
	for _, c := range checks {
		for _, detail := range c.run(v, ctx) {
			ledger.violate(c.name, "%s", detail)
		}
	}
}

// datasetChecks are the invariants the contracts promise for a dataset that
// concurrent writers committed to through a store that supports CAS
// (CONTRACT_WRITE_API §Concurrency, CONTRACT_CORE §Head Pointer and §History
// & Immutability).
var datasetChecks = []check[*verifier]{
	{"acknowledged-commits-persist", (*verifier).acknowledgedPersist},
	{"history-shape", (*verifier).historyShape},
	{"parents-explained", (*verifier).parentsExplained},
	{"contents-match-manifests", (*verifier).contentsMatch},
}

// verify settles the head with one clean commit, so that the pointer names
// a committed snapshot, and then runs every check, recording each failure.
func verify(ctx context.Context, cfg Config, store lode.Store, ledger *Ledger) int {
	ds, err := openDataset(cfg, store)
	if err != nil {
		ledger.violate("verify", "open: %v", err)
		return 0
	}
	settle, err := ds.Write(ctx, Commit{Writer: -1, Rows: 1, Day: days[0]}.records(), lode.Metadata{"op": "settle"})
	if err != nil {
		ledger.violate("verify", "settling commit: %v", err)
		return 0
	}
	v, err := newVerifier(ctx, ds, settle.ID, ledger)
	if err != nil {
		ledger.violate("verify", "%v", err)
		return 0
	}
	runChecks(ctx, v, datasetChecks, ledger)
	return v.chain.size()
}

// acknowledgedPersist requires every acknowledged commit to have a
// manifest.
func (v *verifier) acknowledgedPersist(context.Context) []string {
	var lost []string
	for id, c := range v.acked {
		if !v.chain.has(id) {
			lost = append(lost, fmt.Sprintf("acknowledged %s (writer %d seq %d) has no manifest", id, c.Writer, c.Seq))
		}
	}
	return lost
}

// historyShape requires Latest to return the settling commit, no fork, no
// more chain ends than missing parents account for, and, with no missing
// parent, a head that reaches every snapshot.
func (v *verifier) historyShape(context.Context) []string {
	return append(settled(v.head, v.settle), v.chain.shape(v.head)...)
}

// settled requires the head to be the settling commit, when there is one:
// once it is acknowledged, no other commit is pending.
func settled[ID ~string](head, settle ID) []string {
	if settle == "" || head == settle {
		return nil
	}
	return []string{fmt.Sprintf("Latest returns %s after the settling commit %s", head, settle)}
}

// parentsExplained accepts a parent without a manifest only as an abandoned
// commit, which no writer was told had committed.
func (v *verifier) parentsExplained(context.Context) []string {
	var bad []string
	for _, parent := range v.chain.missingParents() {
		if _, acked := v.acked[parent]; acked {
			bad = append(bad, fmt.Sprintf("acknowledged %s is a parent but has no manifest", parent))
		}
	}
	return bad
}

// contentsMatch reads every snapshot: its rows must match its manifest's
// row count and, for an acknowledged commit, be exactly the writer's
// records.
func (v *verifier) contentsMatch(ctx context.Context) []string {
	var bad []string
	for _, id := range v.chain.ids() {
		rows, err := v.ds.Read(ctx, id)
		switch c, acked := v.acked[id]; {
		case err != nil:
			bad = append(bad, fmt.Sprintf("%s: Read: %v", id, err))
		case int64(len(rows)) != v.snapshots[id].Manifest.RowCount:
			bad = append(bad, fmt.Sprintf("%s: %d rows, manifest records %d", id, len(rows), v.snapshots[id].Manifest.RowCount))
		case acked && !c.matches(rows):
			bad = append(bad, fmt.Sprintf("%s: the rows read are not the %d that writer %d committed as seq %d", id, c.Rows, c.Writer, c.Seq))
		}
	}
	return bad
}
