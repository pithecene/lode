package chaos

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"slices"

	"github.com/pithecene/lode/lode"
)

const datasetID lode.DatasetID = "chaos"

// layout is a dataset layout a run can use: the layout and the writes it
// supports.
type layout struct {
	layout func() lode.Layout
	writes []writeOp
}

// layouts are the dataset layouts a run can use. StreamWrite does not
// partition, so the Hive layout uses Write alone.
var layouts = map[string]layout{
	"default": {layout: lode.Unpartitioned, writes: []writeOp{writeBatch, writeStream}},
	"hive":    {layout: func() lode.Layout { return lode.Hive("day") }, writes: []writeOp{writeBatch}},
}

// LayoutNames lists the layouts a run can use in a stable order.
func LayoutNames() []string { return slices.Sorted(maps.Keys(layouts)) }

// writeOp is one kind of write a writer can issue: it writes rows and
// returns the snapshot the library acknowledged.
type writeOp func(ctx context.Context, ds lode.Records[any], rows []any) (*lode.DatasetSnapshot, error)

func writeBatch(ctx context.Context, ds lode.Records[any], rows []any) (*lode.DatasetSnapshot, error) {
	return ds.Write(ctx, rows, lode.D{"op": "write"})
}

func writeStream(ctx context.Context, ds lode.Records[any], rows []any) (*lode.DatasetSnapshot, error) {
	return ds.StreamWrite(ctx, each(rows), lode.D{"op": "stream"})
}

// openDataset opens a handle on the run's dataset over store. Opening reads
// the head, so an injected fault can fail it.
func (r *run) openDataset(ctx context.Context, store lode.RangeStore) (lode.Records[any], error) {
	if err := requireKnown("layout", r.cfg.Layout, LayoutNames()); err != nil {
		return nil, err
	}
	l := layouts[r.cfg.Layout]
	return lode.Open(lode.Use(store)).Records(ctx, datasetID, lode.JSONL[any](),
		lode.WithLayout(l.layout()),
		lode.WithRetry(r.cfg.Retries),
	)
}

// datasetHandle is the handle an actor holds on the run's dataset, as a
// separate process would: its own in-memory head and, when faults are
// enabled, its own faulty store, whose faults rng decides.
func (r *run) datasetHandle(who string, rng *rand.Rand) handle[lode.Records[any]] {
	return handle[lode.Records[any]]{
		who:    who,
		ledger: r.ledger,
		open: func(ctx context.Context) (lode.Records[any], error) {
			return r.openDataset(ctx, r.storeFor(rng))
		},
	}
}

// step is what an actor does after acquiring its handle.
type step int

const (
	// proceed: the handle is held.
	proceed step = iota
	// skip: no handle this step, because the run ended or the harness
	// injected the failure of the open.
	skip
	// halt: the open failed otherwise, a violation that ends the actor.
	halt
)

// handle is an actor's handle on the run's dataset or volume, held as one
// process would hold it: opened on first use, and dropped when an injected
// kill ends the process, so that the next step opens it again.
type handle[H any] struct {
	who    string
	ledger *Ledger
	open   func(ctx context.Context) (H, error)
	h      H
	held   bool
}

// acquire returns the handle, opening it when none is held.
func (h *handle[H]) acquire(ctx context.Context) (H, step) {
	if h.held {
		return h.h, proceed
	}
	opened, err := h.open(ctx)
	switch {
	case err == nil:
		h.h, h.held = opened, true
		return h.h, proceed
	case ctx.Err() != nil:
	case injected(err):
		h.ledger.fault()
	default:
		h.ledger.violate("open", "%s: %v", h.who, err)
		return h.h, halt
	}
	return h.h, skip
}

// drop discards the handle when err is an injected kill: the process died,
// and the next step restarts it.
func (h *handle[H]) drop(err error) {
	if errors.Is(err, errKilled) {
		var zero H
		h.h, h.held = zero, false
	}
}

// datasetCommitters are the Dataset workload's committers: the writers and,
// when the run exercises retention, the janitor.
func (r *run) datasetCommitters() []actor {
	var actors []actor
	for i := range r.cfg.Writers {
		actors = append(actors, r.newWriter(i))
	}
	if r.cfg.Retention {
		actors = append(actors, r.newJanitor())
	}
	return actors
}

// writer commits batches of 1 to 5 records until the deadline, recording
// every acknowledged commit. A conflict that outlasts the retries is
// expected under contention; any other error is a violation.
type writer struct {
	*run
	index  int
	rng    *rand.Rand
	writes []writeOp
	ds     handle[lode.Records[any]]
}

func (r *run) newWriter(index int) *writer {
	rng := r.random(uint64(index) + 1)
	return &writer{
		run:    r,
		index:  index,
		rng:    rng,
		writes: layouts[r.cfg.Layout].writes,
		ds:     r.datasetHandle(fmt.Sprintf("writer %d", index), rng),
	}
}

func (w *writer) act(ctx context.Context) {
	for seq := 0; w.live(ctx); seq++ {
		ds, next := w.ds.acquire(ctx)
		if next == halt {
			return
		}
		if next == skip {
			continue
		}
		w.commit(ctx, ds, seq)
	}
}

// commit writes the batch numbered seq with a randomly chosen write and
// records its outcome.
func (w *writer) commit(ctx context.Context, ds lode.Records[any], seq int) {
	c := newCommit(w.rng, w.index, seq)
	snap, err := w.writes[w.rng.IntN(len(w.writes))](ctx, ds, c.records())
	switch {
	case err == nil:
		c.ID = snap.ID
		w.ledger.commit(c)
	case errors.Is(err, lode.ErrSnapshotConflict):
		w.ledger.conflict()
	case injected(err):
		w.ledger.fault()
		w.ds.drop(err)
	default:
		w.ledger.violate("write", "writer %d seq %d: %v", w.index, seq, err)
	}
}

// readers are the Dataset workload's observers.
func (r *run) readers() []actor {
	var actors []actor
	for i := range r.cfg.Readers {
		rng := r.random(uint64(1000 + i))
		actors = append(actors, &reader{run: r, rng: rng, ds: r.datasetHandle(fmt.Sprintf("reader %d", i), rng)})
	}
	return actors
}

// reader repeatedly reads the head and a random acknowledged snapshot until
// the deadline. A head must be readable in full, and an acknowledged
// snapshot must hold exactly the rows its writer committed.
type reader struct {
	*run
	rng *rand.Rand
	ds  handle[lode.Records[any]]
}

func (rd *reader) act(ctx context.Context) {
	for ctx.Err() == nil {
		ds, next := rd.ds.acquire(ctx)
		if next == halt {
			return
		}
		if next == skip {
			continue
		}
		err := rd.head(ctx, ds)
		if c, ok := rd.ledger.randomCommit(rd.rng); err == nil && ok {
			err = rd.commit(ctx, ds, c)
		}
		rd.ds.drop(err)
	}
}

// ignorable reports whether a failure says nothing about the library: the run
// ended, or the harness injected the failure.
func (rd *reader) ignorable(ctx context.Context, err error) bool {
	switch {
	case ctx.Err() != nil:
		return true
	case injected(err):
		rd.ledger.fault()
		return true
	}
	return false
}

// expired reports whether a read of id failed because retention expired
// the snapshot meanwhile, which a reader holding its ID may observe
// (CONTRACT_RETENTION §Order of Operations); the end of the run confirms
// the expiry.
func (rd *reader) expired(id lode.DatasetSnapshotID, err error) bool {
	if !rd.cfg.Retention || !expiredRead(err) {
		return false
	}
	rd.ledger.failedRead(id)
	return true
}

// uninformative reports whether a read of id failed for a reason that says
// nothing about the library.
func (rd *reader) uninformative(ctx context.Context, id lode.DatasetSnapshotID, err error) bool {
	return rd.ignorable(ctx, err) || rd.expired(id, err)
}

// head reads the head in full. It returns the error so that a killed
// process is restarted.
func (rd *reader) head(ctx context.Context, ds lode.Records[any]) error {
	head, err := ds.Latest(ctx)
	if errors.Is(err, lode.ErrNoSnapshots) || rd.ignorable(ctx, err) {
		return err
	}
	if err != nil {
		rd.ledger.violate("read-head", "Latest: %v", err)
		return err
	}
	rows, err := ds.Read(ctx, head.ID)
	if rd.uninformative(ctx, head.ID, err) {
		return err
	}
	rd.ledger.read()
	if err != nil || int64(len(rows)) != head.RowCount {
		rd.ledger.violate("read-head", "head %s: %d rows, manifest records %d (%v)", head.ID, len(rows), head.RowCount, err)
	}
	return err
}

// commit reads an acknowledged snapshot, which must hold exactly its
// writer's records.
func (rd *reader) commit(ctx context.Context, ds lode.Records[any], c Commit) error {
	rows, err := ds.Read(ctx, c.ID)
	if rd.uninformative(ctx, c.ID, err) {
		return err
	}
	rd.ledger.read()
	if err != nil || !c.matches(rows) {
		rd.ledger.violate("read-commit", "acknowledged %s (writer %d seq %d): %d rows, not the %d its writer committed (%v)", c.ID, c.Writer, c.Seq, len(rows), c.Rows, err)
	}
	return err
}

// each yields rows to StreamWrite, without an error.
func each(rows []any) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}
