package chaos

import (
	"bytes"
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

// chaosDataset is the common surface of *lode.Records[lode.D] and *lode.Blobs
// used by the harness actors and verifiers.
type chaosDataset interface {
	Latest(ctx context.Context) (*lode.Snapshot, error)
	Snapshot(ctx context.Context, id lode.SnapshotID) (*lode.Snapshot, error)
	Snapshots(ctx context.Context) ([]*lode.Snapshot, error)
	ListPartitions(ctx context.Context, opts lode.PartitionListOptions) ([]lode.PartitionPath, error)
	Manifests(ctx context.Context, partition lode.PartitionPath, opts lode.ManifestListOptions) iter.Seq2[lode.LoadedManifest, error]
	Expire(ctx context.Context, req lode.ExpireRequest) (*lode.ExpireReport, error)
	Sweep(ctx context.Context, req lode.SweepRequest) (*lode.SweepReport, error)

	writeCommit(ctx context.Context, rng *rand.Rand, c Commit) (*lode.Snapshot, error)
	writeSettle(ctx context.Context) (*lode.Snapshot, error)
	readUnits(ctx context.Context, id lode.SnapshotID) (int64, error)
	checkCommit(ctx context.Context, c Commit) (bool, error)
	checkAnchored(ctx context.Context, store lode.ObjectStore, c Commit) (bool, error)
	readWithStore(ctx context.Context, store lode.ObjectStore, id lode.SnapshotID) error
}

type recordsHandle struct {
	*lode.Records[lode.D]
	codec   lode.Codec[lode.D]
	streams bool
}

func (h *recordsHandle) writeCommit(ctx context.Context, rng *rand.Rand, c Commit) (*lode.Snapshot, error) {
	if h.streams && rng.IntN(2) == 0 {
		return h.StreamWrite(ctx, each(c.records()), lode.D{"op": "stream"})
	}
	return h.Write(ctx, c.records(), lode.D{"op": "write"})
}

func (h *recordsHandle) writeSettle(ctx context.Context) (*lode.Snapshot, error) {
	settle := Commit{Writer: -1, Rows: 1, Days: []string{days[0]}}
	return h.Write(ctx, settle.records(), lode.D{"op": "settle"})
}

func (h *recordsHandle) readUnits(ctx context.Context, id lode.SnapshotID) (int64, error) {
	rows, err := h.Read(ctx, id)
	return int64(len(rows)), err
}

type anchoredReader[P any] interface {
	Snapshot(ctx context.Context, id lode.SnapshotID) (*lode.Snapshot, error)
	Read(ctx context.Context, id lode.SnapshotID) (P, error)
}

func verifyAnchoredView[V anchoredReader[P], P any](ctx context.Context, id lode.SnapshotID, open func() (V, error), match func(P) bool) (bool, error) {
	view, err := open()
	if err != nil {
		return false, err
	}
	if _, err := view.Snapshot(ctx, id); err != nil {
		return false, err
	}
	payload, err := view.Read(ctx, id)
	if err != nil {
		return false, err
	}
	return match(payload), nil
}

func (h *recordsHandle) checkCommit(ctx context.Context, c Commit) (bool, error) {
	rows, err := h.Read(ctx, c.ID)
	if err != nil {
		return false, err
	}
	return c.matches(rows), nil
}

func (h *recordsHandle) checkAnchored(ctx context.Context, store lode.ObjectStore, c Commit) (bool, error) {
	return verifyAnchoredView(ctx, c.ID, func() (*lode.RecordView[lode.D], error) {
		return lode.OpenReadOnly(lode.Use(store)).Records(ctx, datasetID, h.codec, lode.WithAnchor(c.ID))
	}, c.matches)
}

func (h *recordsHandle) readWithStore(ctx context.Context, store lode.ObjectStore, id lode.SnapshotID) error {
	_, err := verifyAnchoredView(ctx, id, func() (*lode.RecordView[lode.D], error) {
		return lode.OpenReadOnly(lode.Use(store)).Records(ctx, datasetID, h.codec, lode.WithAnchor(id))
	}, func([]lode.D) bool { return true })
	return err
}

type blobsHandle struct {
	*lode.Blobs
}

func (h *blobsHandle) writeCommit(ctx context.Context, rng *rand.Rand, c Commit) (*lode.Snapshot, error) {
	data := c.blobBytes()
	if rng.IntN(2) == 0 {
		return h.StreamWrite(ctx, bytes.NewReader(data), lode.D{"op": "stream-blob"})
	}
	return h.Write(ctx, data, lode.D{"op": "write-blob"})
}

func (h *blobsHandle) writeSettle(ctx context.Context) (*lode.Snapshot, error) {
	settle := Commit{Writer: -1, Seq: -1, Rows: 1, Blob: true}
	return h.StreamWrite(ctx, bytes.NewReader(settle.blobBytes()), lode.D{"op": "settle"})
}

func (h *blobsHandle) checkCommit(ctx context.Context, c Commit) (bool, error) {
	data, err := h.Read(ctx, c.ID)
	return err == nil && c.matchesBlob(data), err
}

func (h *blobsHandle) readUnits(ctx context.Context, id lode.SnapshotID) (int64, error) {
	_, err := h.Read(ctx, id)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (h *blobsHandle) checkAnchored(ctx context.Context, store lode.ObjectStore, c Commit) (bool, error) {
	return verifyAnchoredView(ctx, c.ID, func() (*lode.BlobView, error) {
		return lode.OpenReadOnly(lode.Use(store)).Blobs(ctx, datasetID, lode.WithAnchor(c.ID))
	}, c.matchesBlob)
}

func (h *blobsHandle) readWithStore(ctx context.Context, store lode.ObjectStore, id lode.SnapshotID) error {
	_, err := verifyAnchoredView(ctx, id, func() (*lode.BlobView, error) {
		return lode.OpenReadOnly(lode.Use(store)).Blobs(ctx, datasetID, lode.WithAnchor(id))
	}, func([]byte) bool { return true })
	return err
}

// layoutSpec configures one dataset workload a run can execute.
type layoutSpec struct {
	blob        bool
	partitioned bool
	open        func(ctx context.Context, root *lode.Root, opts ...lode.DatasetOption) (chaosDataset, error)
}

func hiveByDay() lode.Layout { return lode.Hive("day") }

func parquetCommitCodec() lode.Codec[lode.D] { return lode.Parquet[lode.D](commitSchema) }

func recordsLayout(newCodec func() lode.Codec[lode.D], newLayout func() lode.Layout, streams, partitioned bool) layoutSpec {
	return layoutSpec{
		partitioned: partitioned,
		open: func(ctx context.Context, root *lode.Root, opts ...lode.DatasetOption) (chaosDataset, error) {
			codec := newCodec()
			allOpts := append([]lode.DatasetOption{lode.WithLayout(newLayout())}, opts...)
			ds, err := root.Records(ctx, datasetID, codec, allOpts...)
			if err != nil {
				return nil, err
			}
			return &recordsHandle{Records: ds, codec: codec, streams: streams}, nil
		},
	}
}

// layouts are the dataset workloads a run can use.
var layouts = map[string]layoutSpec{
	"default":      recordsLayout(lode.JSONL[lode.D], lode.Unpartitioned, true, false),
	"hive":         recordsLayout(lode.JSONL[lode.D], hiveByDay, false, true),
	"parquet":      recordsLayout(parquetCommitCodec, lode.Unpartitioned, false, false),
	"parquet-hive": recordsLayout(parquetCommitCodec, hiveByDay, false, true),
	"blobs": {
		blob: true,
		open: func(ctx context.Context, root *lode.Root, opts ...lode.DatasetOption) (chaosDataset, error) {
			b, err := root.Blobs(ctx, datasetID, opts...)
			if err != nil {
				return nil, err
			}
			return &blobsHandle{Blobs: b}, nil
		},
	},
}

// LayoutNames lists the layouts a run can use in a stable order.
func LayoutNames() []string { return slices.Sorted(maps.Keys(layouts)) }

// openDataset opens a handle on the run's dataset over store. Opening reads
// the head, so an injected fault can fail it.
func (r *run) openDataset(ctx context.Context, store lode.RangeStore) (chaosDataset, error) {
	if err := requireKnown("layout", r.cfg.Layout, LayoutNames()); err != nil {
		return nil, err
	}
	spec := layouts[r.cfg.Layout]
	return spec.open(ctx, lode.Open(lode.Use(store)),
		lode.WithChecksum(lode.CRC32C()),
		lode.WithRetry(r.cfg.Retries),
	)
}

// datasetHandle is the handle an actor holds on the run's dataset, as a
// separate process would: its own in-memory head and, when faults are
// enabled, its own faulty store, whose faults rng decides.
func (r *run) datasetHandle(who string, rng *rand.Rand) handle[chaosDataset] {
	return handle[chaosDataset]{
		who:    who,
		ledger: r.ledger,
		open: func(ctx context.Context) (chaosDataset, error) {
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

// writer commits batches of records or blobs until the deadline, recording
// every acknowledged commit. A conflict that outlasts the retries is
// expected under contention; any other error is a violation.
type writer struct {
	*run
	index int
	rng   *rand.Rand
	blob  bool
	ds    handle[chaosDataset]
}

func (r *run) newWriter(index int) *writer {
	rng := r.random(uint64(index) + 1)
	return &writer{
		run:   r,
		index: index,
		rng:   rng,
		blob:  layouts[r.cfg.Layout].blob,
		ds:    r.datasetHandle(fmt.Sprintf("writer %d", index), rng),
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

// commit writes the batch numbered seq and records its outcome.
func (w *writer) commit(ctx context.Context, ds chaosDataset, seq int) {
	c := newCommit(w.rng, w.index, seq, w.blob)
	snap, err := ds.writeCommit(ctx, w.rng, c)
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
		actors = append(actors, r.newReader(i))
	}
	return actors
}

// reader repeatedly reads the head and a random acknowledged snapshot (via
// either its held handle or a fresh ReadRoot view with WithAnchor) until the
// deadline.
type reader struct {
	*run
	rng   *rand.Rand
	store lode.RangeStore
	ds    handle[chaosDataset]
}

func (r *run) newReader(index int) *reader {
	rng := r.random(uint64(1000 + index))
	rd := &reader{run: r, rng: rng}
	rd.ds = handle[chaosDataset]{
		who:    fmt.Sprintf("reader %d", index),
		ledger: r.ledger,
		open: func(ctx context.Context) (chaosDataset, error) {
			rd.store = r.storeFor(rng)
			return r.openDataset(ctx, rd.store)
		},
	}
	return rd
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
func (rd *reader) expired(id lode.SnapshotID, err error) bool {
	if !rd.cfg.Retention || !expiredRead(err) {
		return false
	}
	rd.ledger.failedRead(id)
	return true
}

// uninformative reports whether a read of id failed for a reason that says
// nothing about the library.
func (rd *reader) uninformative(ctx context.Context, id lode.SnapshotID, err error) bool {
	return rd.ignorable(ctx, err) || rd.expired(id, err)
}

// head reads the head in full. It returns the error so that a killed
// process is restarted.
func (rd *reader) head(ctx context.Context, ds chaosDataset) error {
	head, err := ds.Latest(ctx)
	if errors.Is(err, lode.ErrNoSnapshots) || rd.ignorable(ctx, err) {
		return err
	}
	if err != nil {
		rd.ledger.violate("read-head", "Latest: %v", err)
		return err
	}
	units, err := ds.readUnits(ctx, head.ID)
	if rd.uninformative(ctx, head.ID, err) {
		return err
	}
	rd.ledger.read()
	if err != nil || units != head.RowCount {
		rd.ledger.violate("read-head", "head %s: %d units, manifest records %d (%v)", head.ID, units, head.RowCount, err)
	}
	return err
}

// commit reads an acknowledged snapshot (alternating between the held handle
// and an anchored ReadRoot view), which must hold exactly what its writer
// committed.
func (rd *reader) commit(ctx context.Context, ds chaosDataset, c Commit) error {
	var ok bool
	var err error
	if rd.rng.IntN(2) == 0 {
		ok, err = ds.checkAnchored(ctx, rd.store, c)
	} else {
		ok, err = ds.checkCommit(ctx, c)
	}
	if rd.uninformative(ctx, c.ID, err) {
		return err
	}
	rd.ledger.read()
	if err != nil || !ok {
		rd.ledger.violate("read-commit", "acknowledged %s (writer %d seq %d) did not match (%v)", c.ID, c.Writer, c.Seq, err)
	}
	return err
}

// each yields rows to StreamWrite, without an error.
func each(rows []lode.D) iter.Seq2[lode.D, error] {
	return func(yield func(lode.D, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}
