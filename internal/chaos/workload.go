package chaos

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/pithecene/lode/lode"
)

const datasetID lode.DatasetID = "chaos"

// layout is a dataset layout a run can use: the constructor option that
// selects it and the writes it supports.
type layout struct {
	option func() lode.Option
	writes []writeOp
}

// layouts are the dataset layouts a run can use. StreamWriteRecords does
// not partition, so the Hive layout uses Write alone.
var layouts = map[string]layout{
	"default": {option: func() lode.Option { return lode.WithLayout(lode.NewDefaultLayout()) }, writes: []writeOp{writeBatch, writeStream}},
	"hive":    {option: func() lode.Option { return lode.WithHiveLayout("day") }, writes: []writeOp{writeBatch}},
}

// LayoutNames lists the layouts a run can use in a stable order.
func LayoutNames() []string { return slices.Sorted(maps.Keys(layouts)) }

// writeOp is one kind of write a writer can issue: it writes rows and
// returns the snapshot the library acknowledged.
type writeOp func(ctx context.Context, ds lode.Dataset, rows []any) (*lode.DatasetSnapshot, error)

func writeBatch(ctx context.Context, ds lode.Dataset, rows []any) (*lode.DatasetSnapshot, error) {
	return ds.Write(ctx, rows, lode.Metadata{"op": "write"})
}

func writeStream(ctx context.Context, ds lode.Dataset, rows []any) (*lode.DatasetSnapshot, error) {
	return ds.StreamWriteRecords(ctx, &sliceIterator{rows: rows}, lode.Metadata{"op": "stream"})
}

// openDataset opens a handle on the run's dataset over store. Every
// goroutine opens its own, as a separate process would, with its own
// in-memory head.
func openDataset(cfg Config, store lode.Store) (lode.Dataset, error) {
	if err := requireKnown("layout", cfg.Layout, LayoutNames()); err != nil {
		return nil, err
	}
	l := layouts[cfg.Layout]
	factory := func() (lode.Store, error) { return store, nil }
	return lode.NewDataset(datasetID, factory,
		lode.WithCodec(lode.NewJSONLCodec()),
		l.option(),
		lode.WithRetryCount(cfg.Retries),
	)
}

// writer commits batches of 1 to 5 records until the deadline, recording
// every acknowledged commit. The deadline is checked between commits, never
// by cancelling one: a commit cancelled between its pointer update and its
// manifest write is abandoned, which the contract permits but which would
// leave the head to the largest-ID fallback (CONTRACT_CORE §Head Pointer).
// A conflict that outlasts the retries is expected under contention; any
// other error is a violation.
func writer(ctx context.Context, cfg Config, store lode.Store, index int, until time.Time, ledger *Ledger) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(index)+1)) //nolint:gosec // reproducible test randomness, not security
	writes := layouts[cfg.Layout].writes
	ds, err := openDataset(cfg, store)
	if err != nil {
		ledger.violate("open", "writer %d: %v", index, err)
		return
	}
	for seq := 0; ctx.Err() == nil && time.Now().Before(until); seq++ {
		c := newCommit(rng, index, seq)
		snap, err := writes[rng.IntN(len(writes))](ctx, ds, c.records())
		switch {
		case err == nil:
			c.ID = snap.ID
			ledger.commit(c)
		case errors.Is(err, lode.ErrSnapshotConflict):
			ledger.conflict()
		default:
			ledger.violate("write", "writer %d seq %d: %v", index, seq, err)
		}
	}
}

// reader repeatedly reads the head and a random acknowledged snapshot until
// ctx ends. A head must be readable in full, and an acknowledged snapshot
// must hold exactly the rows its writer committed.
func reader(ctx context.Context, cfg Config, store lode.Store, index int, ledger *Ledger) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(1000+index))) //nolint:gosec // reproducible test randomness, not security
	ds, err := openDataset(cfg, store)
	if err != nil {
		ledger.violate("open", "reader %d: %v", index, err)
		return
	}
	r := read{ctx: ctx, ds: ds, ledger: ledger}
	for ctx.Err() == nil {
		if r.head() != nil {
			continue
		}
		if commits := ledger.Commits(); len(commits) > 0 {
			r.commit(commits[rng.IntN(len(commits))])
		}
	}
}

// read is one reader's pass over a handle.
type read struct {
	ctx    context.Context
	ds     lode.Dataset
	ledger *Ledger
}

// ended reports whether the run ended, so that a failure says nothing about
// the library.
func (r read) ended() bool { return r.ctx.Err() != nil }

// head reads the head in full. It returns the error, so that the caller
// moves on to an acknowledged snapshot only after the head read.
func (r read) head() error {
	head, err := r.ds.Latest(r.ctx)
	if errors.Is(err, lode.ErrNoSnapshots) || r.ended() {
		return err
	}
	if err != nil {
		r.ledger.violate("read-head", "Latest: %v", err)
		return err
	}
	rows, err := r.ds.Read(r.ctx, head.ID)
	if r.ended() {
		return err
	}
	r.ledger.read()
	if err != nil || int64(len(rows)) != head.Manifest.RowCount {
		r.ledger.violate("read-head", "head %s: %d rows, manifest records %d (%v)", head.ID, len(rows), head.Manifest.RowCount, err)
	}
	return err
}

// commit reads an acknowledged snapshot, which must hold exactly its
// writer's records.
func (r read) commit(c Commit) {
	rows, err := r.ds.Read(r.ctx, c.ID)
	if r.ended() {
		return
	}
	r.ledger.read()
	if err != nil || !c.matches(rows) {
		r.ledger.violate("read-commit", "acknowledged %s (writer %d seq %d): %d rows, not the %d its writer committed (%v)", c.ID, c.Writer, c.Seq, len(rows), c.Rows, err)
	}
}

// sliceIterator feeds rows to StreamWriteRecords; the slice backs the
// iterator.
type sliceIterator struct {
	rows []any
	i    int
}

func (it *sliceIterator) Next() bool {
	it.i++
	return it.i <= len(it.rows)
}

func (it *sliceIterator) Record() any { return it.rows[it.i-1] }
func (it *sliceIterator) Err() error  { return nil }
