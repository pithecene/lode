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

// openHandle opens one handle on the run's dataset for a goroutine, as a
// separate process would, with its own in-memory head and, when faults are
// enabled, its own faulty store.
func openHandle(cfg Config, base lode.Store, rng *rand.Rand) (lode.Dataset, error) {
	return openDataset(cfg, cfg.storeFor(base, rng))
}

// openDataset opens a handle on the run's dataset over store.
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
	var ds lode.Dataset
	for seq := 0; ctx.Err() == nil && time.Now().Before(until); seq++ {
		if ds == nil {
			var err error
			if ds, err = openHandle(cfg, store, rng); err != nil {
				ledger.violate("open", "writer %d: %v", index, err)
				return
			}
		}
		c := newCommit(rng, index, seq)
		snap, err := writes[rng.IntN(len(writes))](ctx, ds, c.records())
		switch {
		case err == nil:
			c.ID = snap.ID
			ledger.commit(c)
		case errors.Is(err, lode.ErrSnapshotConflict):
			ledger.conflict()
		case injected(err):
			ledger.fault()
			if errors.Is(err, errKilled) {
				ds = nil // the process died; the next iteration restarts it
			}
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
	var ds lode.Dataset
	for ctx.Err() == nil {
		if ds == nil {
			var err error
			if ds, err = openHandle(cfg, store, rng); err != nil {
				ledger.violate("open", "reader %d: %v", index, err)
				return
			}
		}
		r := read{ctx: ctx, ds: ds, retention: cfg.Retention, ledger: ledger}
		err := r.head()
		if c, ok := ledger.randomCommit(rng); err == nil && ok {
			err = r.commit(c)
		}
		if errors.Is(err, errKilled) {
			ds = nil
		}
	}
}

// read is one reader's pass over a handle.
type read struct {
	ctx       context.Context
	ds        lode.Dataset
	retention bool
	ledger    *Ledger
}

// skip reports whether a failure says nothing about the library: the run
// ended, or the harness injected the failure.
func (r read) skip(err error) bool {
	switch {
	case r.ctx.Err() != nil:
		return true
	case injected(err):
		r.ledger.fault()
		return true
	}
	return false
}

// expired reports whether a read of id failed because retention expired
// the snapshot meanwhile, which a reader holding its ID may observe
// (CONTRACT_RETENTION §Order of Operations); the end of the run confirms
// the expiry.
func (r read) expired(id lode.DatasetSnapshotID, err error) bool {
	if !r.retention || !expiredRead(err) {
		return false
	}
	r.ledger.failedRead(id)
	return true
}

// uninformative reports whether a read of id failed for a reason that says
// nothing about the library.
func (r read) uninformative(id lode.DatasetSnapshotID, err error) bool {
	return r.skip(err) || r.expired(id, err)
}

// head reads the head in full. It returns the error so the caller can
// restart a killed process.
func (r read) head() error {
	head, err := r.ds.Latest(r.ctx)
	if errors.Is(err, lode.ErrNoSnapshots) || r.skip(err) {
		return err
	}
	if err != nil {
		r.ledger.violate("read-head", "Latest: %v", err)
		return err
	}
	rows, err := r.ds.Read(r.ctx, head.ID)
	if r.uninformative(head.ID, err) {
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
func (r read) commit(c Commit) error {
	rows, err := r.ds.Read(r.ctx, c.ID)
	if r.uninformative(c.ID, err) {
		return err
	}
	r.ledger.read()
	if err != nil || !c.matches(rows) {
		r.ledger.violate("read-commit", "acknowledged %s (writer %d seq %d): %d rows, not the %d its writer committed (%v)", c.ID, c.Writer, c.Seq, len(rows), c.Rows, err)
	}
	return err
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
