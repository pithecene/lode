package lode_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/lode"
)

type gatedStore struct {
	lode.RangeStore
	mu     sync.Mutex
	onPut  func(ctx context.Context, key string, r io.Reader) error
	onDel  func(ctx context.Context, key string) error
	putBlk func(ctx context.Context) error
}

func (g *gatedStore) setOnPut(fn func(ctx context.Context, key string, r io.Reader) error) {
	g.mu.Lock()
	g.onPut = fn
	g.mu.Unlock()
}

func (g *gatedStore) setOnDelete(fn func(ctx context.Context, key string) error) {
	g.mu.Lock()
	g.onDel = fn
	g.mu.Unlock()
}

func (g *gatedStore) Put(ctx context.Context, key string, r io.Reader) error {
	g.mu.Lock()
	hook := g.onPut
	putBlk := g.putBlk
	g.mu.Unlock()
	if hook != nil {
		return hook(ctx, key, r)
	}
	if putBlk != nil && strings.Contains(key, "/data/") {
		return putBlk(ctx)
	}
	return g.RangeStore.Put(ctx, key, r)
}

func (g *gatedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return g.RangeStore.Get(ctx, key)
}

func (g *gatedStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	hook := g.onDel
	g.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key); err != nil {
			return err
		}
	}
	return g.RangeStore.Delete(ctx, key)
}

type gatedHarness struct {
	mem  lode.RangeStore
	gs   *gatedStore
	root *lode.Root
	ds   *lode.Records[lode.D]
}

func openGatedHarness(t *testing.T, opts ...lode.DatasetOption) gatedHarness {
	t.Helper()
	mem := lode.NewMemory()
	gs := &gatedStore{RangeStore: mem}
	root := lode.Open(lode.Use(gs))
	ds, err := root.Records(t.Context(), "events", lode.JSONL[lode.D](), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return gatedHarness{mem: mem, gs: gs, root: root, ds: ds}
}

func pauseFirstPut(t *testing.T, gs *gatedStore, match func(string) bool, afterPut func() error) (<-chan struct{}, func()) {
	t.Helper()
	ent := make(chan struct{})
	rel := make(chan struct{})
	release := sync.OnceFunc(func() { close(rel) })
	t.Cleanup(release)
	var once sync.Once
	gs.setOnPut(func(ctx context.Context, key string, r io.Reader) error {
		var paused bool
		if match(key) {
			once.Do(func() {
				paused = true
				close(ent)
				<-rel
			})
		}
		if err := gs.RangeStore.Put(ctx, key, r); err != nil {
			return err
		}
		if paused && afterPut != nil {
			return afterPut()
		}
		return nil
	})
	return ent, release
}

func mustWriteDoc(t *testing.T, ds *lode.Records[lode.D], doc lode.D) *lode.Snapshot {
	t.Helper()
	snap, err := ds.Write(t.Context(), lode.R(doc), nil)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func assertDatasetFencedOnly(t *testing.T, store lode.Store) {
	t.Helper()
	keys, err := store.List(t.Context(), "datasets/events/")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"datasets/events/latest"}) {
		t.Errorf("remaining keys = %v, want only datasets/events/latest", keys)
	}
}

func finishRemovalAndAwaitWrite(t *testing.T, h gatedHarness, release func(), writeErrCh <-chan error) error {
	t.Helper()
	if err := h.root.RemoveDataset(t.Context(), "events"); err != nil {
		t.Fatalf("RemoveDataset = %v, want nil", err)
	}
	release()
	err := <-writeErrCh
	assertDatasetFencedOnly(t, h.mem)
	return err
}

func startPausedCanonicalWriteAndFence(t *testing.T, cancelAfterPut bool, putErr error) (gatedHarness, func(), <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	h := openGatedHarness(t)
	entered, release := pauseFirstPut(t, h.gs, func(key string) bool {
		return strings.HasPrefix(key, "datasets/events/snapshots/") && strings.HasSuffix(key, "/manifest.json")
	}, func() error {
		if cancelAfterPut {
			cancel()
		}
		return putErr
	})

	writeErrCh := make(chan error, 1)
	go func() {
		_, werr := h.ds.Write(ctx, lode.R(lode.D{"k": "v"}), nil)
		writeErrCh <- werr
	}()

	<-entered
	if err := h.root.RemoveDataset(t.Context(), "events"); !errors.Is(err, lode.ErrHeadUncommitted) {
		t.Fatalf("first RemoveDataset = %v, want ErrHeadUncommitted", err)
	}
	return h, release, writeErrCh
}

func TestAudit_AdvancePostPublicationFence_DatasetAndHive(t *testing.T) {
	t.Parallel()

	t.Run("unpartitioned dataset write paused before canonical manifest", func(t *testing.T) {
		t.Parallel()
		h, release, writeErrCh := startPausedCanonicalWriteAndFence(t, false, nil)
		if err := finishRemovalAndAwaitWrite(t, h, release, writeErrCh); !errors.Is(err, lode.ErrRemoved) {
			t.Fatalf("Write after removal = %v, want ErrRemoved", err)
		}
	})

	t.Run("dataset manifest persisted before Put failed", func(t *testing.T) {
		t.Parallel()
		putErr := errors.New("manifest Put response lost")
		h, release, writeErrCh := startPausedCanonicalWriteAndFence(t, false, putErr)
		err := finishRemovalAndAwaitWrite(t, h, release, writeErrCh)
		assertRemovedPublicationError(t, err, putErr)
	})

	t.Run("caller canceled after publication", func(t *testing.T) {
		t.Parallel()
		h, release, writeErrCh := startPausedCanonicalWriteAndFence(t, true, nil)
		if err := finishRemovalAndAwaitWrite(t, h, release, writeErrCh); !errors.Is(err, lode.ErrRemoved) {
			t.Fatalf("Write after removal and cancellation = %v, want ErrRemoved", err)
		}
	})

	t.Run("hive dataset write paused before partition manifest", func(t *testing.T) {
		t.Parallel()
		h := openGatedHarness(t, lode.WithLayout(lode.Hive("run_id")))
		putErr := errors.New("partition manifest Put response lost")
		entered, release := pauseFirstPut(t, h.gs, func(key string) bool {
			return strings.Contains(key, "/partitions/run_id=a/segments/") && strings.HasSuffix(key, "/manifest.json")
		}, func() error { return putErr })

		writeErrCh := make(chan error, 1)
		go func() {
			_, werr := h.ds.Write(t.Context(), lode.R(lode.D{"run_id": "a", "n": 1}), nil)
			writeErrCh <- werr
		}()

		<-entered
		err := finishRemovalAndAwaitWrite(t, h, release, writeErrCh)
		assertRemovedPublicationError(t, err, putErr)
	})

	t.Run("sweep reindex cleans up late partition manifest after removal", func(t *testing.T) {
		t.Parallel()
		h := openGatedHarness(t, lode.WithLayout(lode.Hive("run_id")))
		snap := mustWriteDoc(t, h.ds, lode.D{"run_id": "a", "n": 1})
		partManifest := "datasets/events/partitions/run_id=a/segments/" + string(snap.ID) + "/manifest.json"
		if err := h.mem.Delete(t.Context(), partManifest); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		putErr := errors.New("repaired manifest Put response lost")
		entered, release := pauseFirstPut(t, h.gs, func(key string) bool { return key == partManifest }, func() error {
			cancel()
			return putErr
		})

		type sweepRes struct {
			report *lode.SweepReport
			err    error
		}
		sweepCh := make(chan sweepRes, 1)
		go func() {
			rep, serr := h.ds.Sweep(ctx, lode.SweepRequest{Grace: time.Nanosecond})
			sweepCh <- sweepRes{report: rep, err: serr}
		}()

		<-entered
		if err := h.root.RemoveDataset(t.Context(), "events"); err != nil {
			t.Fatalf("RemoveDataset = %v, want nil", err)
		}
		release()

		res := <-sweepCh
		assertDatasetFencedOnly(t, h.mem)
		if !errors.Is(res.err, lode.ErrRemoved) || !errors.Is(res.err, putErr) {
			t.Fatalf("Sweep after removal = %v, want ErrRemoved and %v", res.err, putErr)
		}
		if len(res.report.Reindexed) != 0 {
			t.Fatalf("Reindexed = %v, want empty", res.report.Reindexed)
		}
	})
}

func assertRemovedPublicationError(t *testing.T, err, putErr error) {
	t.Helper()
	for _, want := range []error{lode.ErrRemoved, history.ErrManifestUncertain, putErr} {
		if !errors.Is(err, want) {
			t.Errorf("Write after removal = %v, want it to match %v", err, want)
		}
	}
}

type headerStreamCodec struct {
	headerBytes int
	initErr     error
}

func (c headerStreamCodec) Name() string     { return "header-jsonl" }
func (c headerStreamCodec) Describe() string { return "" }
func (c headerStreamCodec) Encode(w io.Writer, rs []lode.D) error {
	return lode.JSONL[lode.D]().Encode(w, rs)
}

func (c headerStreamCodec) Decode(r io.Reader) ([]lode.D, error) {
	if c.headerBytes > 0 {
		if _, err := io.CopyN(io.Discard, r, int64(c.headerBytes)); err != nil {
			return nil, err
		}
	}
	return lode.JSONL[lode.D]().Decode(r)
}

func (c headerStreamCodec) NewStreamEncoder(w io.Writer) (lode.StreamEncoder[lode.D], error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	if c.headerBytes > 0 {
		hdr := bytes.Repeat([]byte("H"), c.headerBytes)
		if _, err := w.Write(hdr); err != nil {
			return nil, err
		}
	}
	return lode.JSONL[lode.D]().(lode.StreamingCodec[lode.D]).NewStreamEncoder(w)
}

type headerCompressor struct {
	headerBytes int
}

func (c headerCompressor) Name() string      { return "hdrcomp" }
func (c headerCompressor) Extension() string { return ".hc" }
func (c headerCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	if _, err := w.Write(bytes.Repeat([]byte("C"), c.headerBytes)); err != nil {
		return nil, err
	}
	return nopWriteCloser{Writer: w}, nil
}

func (c headerCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	if _, err := io.CopyN(io.Discard, r, int64(c.headerBytes)); err != nil {
		return nil, err
	}
	return io.NopCloser(r), nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func TestAudit_StreamWrite_PipeInitNoDeadlock(t *testing.T) {
	t.Parallel()

	t.Run("header-writing NewStreamEncoder succeeds without deadlock", func(t *testing.T) {
		t.Parallel()
		for _, comp := range []lode.Compressor{lode.Uncompressed(), lode.Gzip()} {
			ctx := t.Context()
			var codec lode.Codec[lode.D] = headerStreamCodec{headerBytes: 256 * 1024}
			ds, err := lode.Open(lode.Memory()).Records(ctx, "events", codec, lode.WithCompressor(comp))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			var snap *lode.Snapshot
			go func() {
				var werr error
				snap, werr = ds.StreamWrite(ctx, func(yield func(lode.D, error) bool) {
					yield(lode.D{"id": "1"}, nil)
				}, nil)
				done <- werr
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("StreamWrite(%s) = %v", comp.Name(), err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("StreamWrite(%s) deadlocked during header write", comp.Name())
			}
			got, err := ds.Read(ctx, snap.ID)
			if err != nil || len(got) != 1 || got[0]["id"] != "1" {
				t.Fatalf("Read(%s) = %v, %v", comp.Name(), got, err)
			}
		}
	})

	t.Run("failing NewStreamEncoder with Gzip does not deadlock on Close", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		errInit := errors.New("encoder init boom")
		var codec lode.Codec[lode.D] = headerStreamCodec{initErr: errInit}
		ds, err := lode.Open(lode.Memory()).Records(ctx, "events", codec, lode.WithCompressor(lode.Gzip()))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, werr := ds.StreamWrite(ctx, func(yield func(lode.D, error) bool) {
				yield(lode.D{"id": "1"}, nil)
			}, nil)
			done <- werr
		}()
		select {
		case err := <-done:
			if !errors.Is(err, errInit) {
				t.Fatalf("StreamWrite = %v, want %v", err, errInit)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("StreamWrite deadlocked closing Gzip after NewStreamEncoder failure")
		}
	})

	t.Run("header-writing Compress and canceled stalled Put unblock cleanly", func(t *testing.T) {
		t.Parallel()
		mem := lode.NewMemory()
		stalled := make(chan struct{})
		gs := &gatedStore{
			RangeStore: mem,
			putBlk: func(ctx context.Context) error {
				close(stalled)
				<-ctx.Done()
				return ctx.Err()
			},
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var codec lode.Codec[lode.D] = headerStreamCodec{headerBytes: 128 * 1024}
		ds, err := lode.Open(lode.Use(gs)).Records(t.Context(), "events", codec, lode.WithCompressor(headerCompressor{headerBytes: 128 * 1024}))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, werr := ds.StreamWrite(ctx, func(yield func(lode.D, error) bool) {
				yield(lode.D{"id": "1"}, nil)
			}, nil)
			done <- werr
		}()
		<-stalled
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("StreamWrite = %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("StreamWrite deadlocked when stalled Put context was canceled")
		}
	})
}

func TestAudit_InterruptedExpiry_ConsistentAcrossReadAndListingAPIs(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	h := openGatedHarness(t, lode.WithLayout(lode.Hive("run_id")))

	legacySnap := mustWriteDoc(t, h.ds, lode.D{"run_id": "legacy", "v": 1})
	expiredSnap := mustWriteDoc(t, h.ds, lode.D{"run_id": "gone", "v": 2})
	expiringSnap := mustWriteDoc(t, h.ds, lode.D{"run_id": "expiring", "v": 3})
	liveHead := mustWriteDoc(t, h.ds, lode.D{"run_id": "live", "v": 4})

	if err := h.mem.Delete(ctx, "datasets/events/segments/"+string(legacySnap.ID)+"/manifest.json"); err != nil {
		t.Fatal(err)
	}

	errStopPartDelete := errors.New("stop before partition manifest delete")
	h.gs.setOnDelete(func(_ context.Context, key string) error {
		if strings.Contains(key, "partitions/run_id=gone/segments/") && strings.HasSuffix(key, "/manifest.json") {
			return errStopPartDelete
		}
		return nil
	})
	if _, err := h.ds.Expire(ctx, lode.ExpireRequest{IDs: []lode.SnapshotID{expiredSnap.ID}}); !errors.Is(err, errStopPartDelete) {
		t.Fatalf("Expire = %v, want %v", err, errStopPartDelete)
	}
	h.gs.setOnDelete(nil)

	tombPayload, err := json.Marshal(map[string]any{
		"snapshot_id":         expiringSnap.ID,
		"requested_at":        time.Now().UTC(),
		"manifest_created_at": expiringSnap.CreatedAt,
		"objects":             expiringSnap.Objects,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.mem.Put(ctx, "datasets/events/expired/"+string(expiringSnap.ID), bytes.NewReader(tombPayload)); err != nil {
		t.Fatal(err)
	}

	view, err := lode.OpenReadOnly(lode.Use(h.gs)).Records(ctx, "events", lode.JSONL[lode.D](), lode.WithLayout(lode.Hive("run_id")))
	if err != nil {
		t.Fatal(err)
	}

	assertExpirySurfaceParity(t, "writer", expirySurface{
		snap:      h.ds.Snapshot,
		snapshots: h.ds.Snapshots,
		parts:     h.ds.ListPartitions,
		manifests: h.ds.Manifests,
	}, legacySnap.ID, expiredSnap.ID, expiringSnap.ID, liveHead.ID)

	assertExpirySurfaceParity(t, "view", expirySurface{
		snap:      view.Snapshot,
		snapshots: view.Snapshots,
		parts:     view.ListPartitions,
		manifests: view.Manifests,
	}, legacySnap.ID, expiredSnap.ID, expiringSnap.ID, liveHead.ID)
}

type expirySurface struct {
	snap      func(context.Context, lode.SnapshotID) (*lode.Snapshot, error)
	snapshots func(context.Context) ([]*lode.Snapshot, error)
	parts     func(context.Context, lode.PartitionListOptions) ([]lode.PartitionPath, error)
	manifests func(context.Context, lode.PartitionPath, lode.ManifestListOptions) iter.Seq2[lode.LoadedManifest, error]
}

func assertExpirySurfaceParity(
	t *testing.T,
	name string,
	r expirySurface,
	legacyID, expiredID, expiringID, liveID lode.SnapshotID,
) {
	t.Helper()
	ctx := t.Context()
	if _, err := r.snap(ctx, expiredID); !errors.Is(err, lode.ErrSnapshotExpired) {
		t.Fatalf("%s Snapshot(expired) = %v, want ErrSnapshotExpired", name, err)
	}
	if _, err := r.snap(ctx, legacyID); err != nil {
		t.Fatalf("%s Snapshot(legacy) = %v, want nil", name, err)
	}
	if _, err := r.snap(ctx, expiringID); err != nil {
		t.Fatalf("%s Snapshot(expiring) = %v, want nil", name, err)
	}

	snaps, err := r.snapshots(ctx)
	if err != nil {
		t.Fatalf("%s Snapshots = %v", name, err)
	}
	var snapIDs []lode.SnapshotID
	for _, s := range snaps {
		snapIDs = append(snapIDs, s.ID)
	}
	slices.Sort(snapIDs)
	want := []lode.SnapshotID{legacyID, expiringID, liveID}
	slices.Sort(want)
	if !slices.Equal(snapIDs, want) {
		t.Fatalf("%s Snapshots = %v, want %v", name, snapIDs, want)
	}

	parts, err := r.parts(ctx, lode.PartitionListOptions{})
	if err != nil {
		t.Fatalf("%s ListPartitions = %v", name, err)
	}
	var partStrs []string
	for _, p := range parts {
		partStrs = append(partStrs, p.String())
	}
	if want := []string{"run_id=expiring", "run_id=legacy", "run_id=live"}; !slices.Equal(partStrs, want) {
		t.Fatalf("%s ListPartitions = %v, want %v", name, partStrs, want)
	}

	goneParts, err := r.parts(ctx, lode.PartitionListOptions{Prefix: "run_id=gone"})
	if err != nil || len(goneParts) != 0 {
		t.Fatalf("%s ListPartitions(Prefix=run_id=gone) = %v, %v; want empty", name, goneParts, err)
	}

	var gotManIDs []lode.SnapshotID
	for m, err := range r.manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{}) {
		if err != nil {
			t.Fatalf("%s Manifests(all): %v", name, err)
		}
		gotManIDs = append(gotManIDs, m.Ref.ID)
	}
	if want := []lode.SnapshotID{expiringID, legacyID, liveID}; !slices.Equal(gotManIDs, want) {
		t.Fatalf("%s Manifests(all) = %v, want %v", name, gotManIDs, want)
	}

	gonePart, err := lode.ParsePartitionPath("run_id=gone")
	if err != nil {
		t.Fatal(err)
	}
	var goneErr error
	for _, err := range r.manifests(ctx, gonePart, lode.ManifestListOptions{}) {
		if err != nil {
			goneErr = err
			break
		}
	}
	if !errors.Is(goneErr, lode.ErrNotFound) {
		t.Fatalf("%s Manifests(run_id=gone) = %v, want ErrNotFound", name, goneErr)
	}
}

func TestAudit_WithRetryMaxInt_DoesNotOverflow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := lode.Open(lode.Memory())

	recs, err := root.Records(ctx, "events", lode.JSONL[lode.D](), lode.WithRetry(math.MaxInt))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := recs.Write(ctx, lode.R(lode.D{"k": "v"}), nil)
	if err != nil || snap == nil || snap.Manifest == nil {
		t.Fatalf("Records.Write(WithRetry(MaxInt)) = %v, %v; want committed snapshot", snap, err)
	}
	got, err := recs.Read(ctx, snap.ID)
	if err != nil || len(got) != 1 {
		t.Fatalf("Records.Read = %v, %v", got, err)
	}

	blobs, err := root.Blobs(ctx, "files", lode.WithRetry(math.MaxInt))
	if err != nil {
		t.Fatal(err)
	}
	bsnap, err := blobs.Write(ctx, []byte("ok"), nil)
	if err != nil || bsnap == nil || bsnap.Manifest == nil {
		t.Fatalf("Blobs.Write(WithRetry(MaxInt)) = %v, %v; want committed snapshot", bsnap, err)
	}
}

func TestAudit_ReviewRegressions_PR395(t *testing.T) {
	t.Parallel()

	t.Run("late manifest cleanup delete failure joins with ErrRemoved", func(t *testing.T) {
		t.Parallel()
		putErr := errors.New("manifest Put response lost")
		h, release, writeErrCh := startPausedCanonicalWriteAndFence(t, false, putErr)
		if err := h.root.RemoveDataset(t.Context(), "events"); err != nil {
			t.Fatalf("second RemoveDataset = %v, want nil", err)
		}

		errDelBoom := errors.New("cleanup delete boom")
		h.gs.setOnDelete(func(_ context.Context, key string) error {
			if strings.HasSuffix(key, "/manifest.json") {
				return errDelBoom
			}
			return nil
		})
		release()

		err := <-writeErrCh
		assertRemovedPublicationError(t, err, putErr)
		if !errors.Is(err, errDelBoom) {
			t.Errorf("Write after removal = %v, want it to match %v", err, errDelBoom)
		}
	})

	t.Run("Snapshots skips tombstone Get when canonical manifest is listed", func(t *testing.T) {
		t.Parallel()
		var tombGets int
		var mu sync.Mutex
		mem := lode.NewMemory()
		spy := &getSpyStore{
			RangeStore: mem,
			onGet: func(key string) {
				if strings.Contains(key, "/expired/") {
					mu.Lock()
					tombGets++
					mu.Unlock()
				}
			},
		}
		ds, err := lode.Open(lode.Use(spy)).Records(t.Context(), "events", lode.JSONL[lode.D](), lode.WithLayout(lode.Hive("run_id")))
		if err != nil {
			t.Fatal(err)
		}
		mustWriteDoc(t, ds, lode.D{"run_id": "a", "v": 1})
		mustWriteDoc(t, ds, lode.D{"run_id": "b", "v": 2})

		snaps, err := ds.Snapshots(t.Context())
		if err != nil || len(snaps) != 2 {
			t.Fatalf("Snapshots = %v, %v; want 2", snaps, err)
		}
		mu.Lock()
		got := tombGets
		mu.Unlock()
		if got != 0 {
			t.Fatalf("Snapshots issued %d tombstone Gets on canonical snapshots, want 0", got)
		}
	})
}

type getSpyStore struct {
	lode.RangeStore
	onGet func(string)
}

func (s *getSpyStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.onGet != nil {
		s.onGet(key)
	}
	return s.RangeStore.Get(ctx, key)
}
