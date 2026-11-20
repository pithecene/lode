package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// TestSingleProcess_NilStore checks that nil and typed-nil stores return nil,
// so that Root/Volume constructors reject them with ErrInvalidArgument rather
// than panicking later.
func TestSingleProcess_NilStore(t *testing.T) {
	if got := SingleProcess(nil); got != nil {
		t.Errorf("SingleProcess(nil) = %v, want nil", got)
	}
	var typedNilNonCAS *pausedDelete
	if got := SingleProcess(typedNilNonCAS); got != nil {
		t.Errorf("SingleProcess(typedNilNonCAS) = %v, want nil", got)
	}
	var typedNilCAS *memoryStore
	if got := SingleProcess(typedNilCAS); got != nil {
		t.Errorf("SingleProcess(typedNilCAS) = %v, want nil", got)
	}
	if _, err := Open(Use(SingleProcess(typedNilNonCAS))).Records(t.Context(), "ds", JSONL[D]()); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Open(SingleProcess(typedNilNonCAS)) = %v, want ErrInvalidArgument", err)
	}
}

// pausedDelete deletes targetKey and then waits until release is closed,
// holding a replacement between its Delete and its Put. It exposes ObjectStore,
// RangeReader, and PrefixLister without ConditionalWriter so SingleProcess
// wraps it with singleProcessFullStore.
type pausedDelete struct {
	ObjectStore
	RangeReader
	PrefixLister
	targetKey        string
	deleted, release chan struct{}
}

func newPausedDelete() *pausedDelete {
	mem := NewMemory()
	return &pausedDelete{
		ObjectStore:  mem,
		RangeReader:  mem,
		PrefixLister: mem.(PrefixLister),
		targetKey:    "ptr/latest",
		deleted:      make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (p *pausedDelete) Delete(ctx context.Context, path string) error {
	if err := p.ObjectStore.Delete(ctx, path); err != nil {
		return err
	}
	if path == p.targetKey {
		close(p.deleted)
		<-p.release
	}
	return nil
}

// ReaderAt returns a stateless key-addressed ReaderAt (mirroring S3/HTTP range
// stores) so a ReadAt executed during the Delete/Put gap would observe
// ErrNotFound if SingleProcess only locked ReaderAt construction.
func (p *pausedDelete) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	if exists, err := p.Exists(ctx, path); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrNotFound
	}
	return keyReaderAt{ctx: ctx, ranges: p.RangeReader, path: path}, nil
}

type keyReaderAt struct {
	ctx    context.Context
	ranges RangeReader
	path   string
}

func (k keyReaderAt) ReadAt(buf []byte, off int64) (int, error) {
	data, err := k.ranges.ReadRange(k.ctx, k.path, off, int64(len(buf)))
	if err != nil {
		return 0, err
	}
	copy(buf, data)
	if len(data) < len(buf) {
		return len(data), io.EOF
	}
	return len(data), nil
}

// replacementReads are the read methods of the returned store, each
// reporting what it observes at ptr/latest.
var replacementReads = []struct {
	name string
	read func(ctx context.Context, s Store) string
}{
	{"Get", func(ctx context.Context, s Store) string {
		rc, err := s.Get(ctx, "ptr/latest")
		if err != nil {
			return err.Error()
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		return string(data)
	}},
	{"Exists", func(ctx context.Context, s Store) string {
		ok, err := s.Exists(ctx, "ptr/latest")
		return fmt.Sprint(ok, err)
	}},
	{"List", func(ctx context.Context, s Store) string {
		paths, err := s.List(ctx, "ptr/")
		return fmt.Sprintf("%s %v", strings.Join(paths, ","), err)
	}},
	{"ListPrefixes", func(ctx context.Context, s Store) string {
		pl, ok := s.(PrefixLister)
		if !ok {
			return "the store does not list prefixes"
		}
		prefixes, err := pl.ListPrefixes(ctx, "")
		return fmt.Sprintf("%s %v", strings.Join(prefixes, ","), err)
	}},
	{"ReadRange", func(ctx context.Context, s Store) string {
		ranges, ok := s.(RangeReader)
		if !ok {
			return "the store does not read ranges"
		}
		data, err := ranges.ReadRange(ctx, "ptr/latest", 0, 6)
		if err != nil {
			return err.Error()
		}
		return string(data)
	}},
	{"ReaderAt", func(ctx context.Context, s Store) string {
		ranges, ok := s.(RangeReader)
		if !ok {
			return "the store does not read ranges"
		}
		ra, err := ranges.ReaderAt(ctx, "ptr/latest")
		if err != nil {
			return err.Error()
		}
		buf := make([]byte, 6)
		if _, err := ra.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			return err.Error()
		}
		return string(buf)
	}},
}

// TestSingleProcess_ReadsWaitForReplacement checks that a read through the
// returned store never observes a path as absent between the Delete and the
// Put of a replacement: it waits, and then observes the new content.
func TestSingleProcess_ReadsWaitForReplacement(t *testing.T) {
	want := map[string]string{
		"Get": "snap-2", "Exists": "true <nil>", "List": "ptr/latest <nil>",
		"ListPrefixes": "ptr/ <nil>", "ReadRange": "snap-2", "ReaderAt": "snap-2",
	}
	for _, r := range replacementReads {
		t.Run(r.name, func(t *testing.T) {
			ctx := t.Context()
			inner := newPausedDelete()
			store := SingleProcess(inner)
			if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
				t.Fatal(err)
			}

			replaced := make(chan error, 1)
			go func() { replaced <- store.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2") }()
			<-inner.deleted
			observed := make(chan string, 1)
			go func() { observed <- r.read(ctx, store) }()

			// A read that returns while the path is deleted observed the gap.
			select {
			case got := <-observed:
				close(inner.release)
				t.Fatalf("%s returned during the replacement and observed %q", r.name, got)
			case <-time.After(50 * time.Millisecond):
			}
			close(inner.release)
			if err := <-replaced; err != nil {
				t.Fatal(err)
			}
			if got := <-observed; got != want[r.name] {
				t.Errorf("%s observed %q after the replacement, want %q", r.name, got, want[r.name])
			}
		})
	}
}

// TestSingleProcess_PreOpenedReaderAtWaitsForReplacement verifies that a
// ReaderAt opened BEFORE CompareAndSwap begins still acquires the read lock on
// each ReadAt call rather than observing ErrNotFound during the Delete/Put gap.
func TestSingleProcess_PreOpenedReaderAtWaitsForReplacement(t *testing.T) {
	ctx := t.Context()
	inner := newPausedDelete()
	store := SingleProcess(inner).(RangeStore)
	if err := store.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
		t.Fatal(err)
	}

	ra, err := store.ReaderAt(ctx, "ptr/latest")
	if err != nil {
		t.Fatal(err)
	}

	replaced := make(chan error, 1)
	go func() { replaced <- store.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2") }()
	<-inner.deleted

	readDone := make(chan string, 1)
	go func() {
		buf := make([]byte, 6)
		if _, err := ra.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			readDone <- "err:" + err.Error()
			return
		}
		readDone <- string(buf)
	}()

	select {
	case got := <-readDone:
		close(inner.release)
		t.Fatalf("pre-opened ReaderAt.ReadAt returned during replacement gap: %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(inner.release)
	if err := <-replaced; err != nil {
		t.Fatal(err)
	}
	if got := <-readDone; got != "snap-2" {
		t.Fatalf("pre-opened ReaderAt.ReadAt = %q, want snap-2", got)
	}
}

// failingPointerStore injects Put failures on a target key so we can test both
// single-shot replacement failure (where immediate restore succeeds) and
// sustained Put outage / simulated crash (where only the .lode-cas journal
// survives on the underlying ObjectStore).
type failingPointerStore struct {
	ObjectStore
	RangeReader
	PrefixLister
	mu          sync.Mutex
	failKey     string
	failNextPut int // number of Put calls on failKey to fail (-1 = fail all)
}

func newFailingPointerStore(failKey string) *failingPointerStore {
	mem := NewMemory()
	return &failingPointerStore{
		ObjectStore:  testutil.WithoutCAS(mem),
		RangeReader:  mem,
		PrefixLister: mem.(PrefixLister),
		failKey:      failKey,
	}
}

func (f *failingPointerStore) setFailPuts(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNextPut = n
}

func (f *failingPointerStore) Put(ctx context.Context, path string, r io.Reader) error {
	f.mu.Lock()
	shouldFail := path == f.failKey && f.failNextPut != 0
	if shouldFail && f.failNextPut > 0 {
		f.failNextPut--
	}
	f.mu.Unlock()
	if shouldFail {
		return errors.New("injected replacement put failure")
	}
	return f.ObjectStore.Put(ctx, path, r)
}

// TestSingleProcess_FailedReplacementPreservesCommittedHead reproduces the P1
// history-fork scenario: Writer A starts first (older snapshot ID), Writer B
// starts later and commits first (newer snapshot ID), A retries and commits on
// top of B (so A is the head despite A.ID < B.ID), and a subsequent pointer
// replacement fails after deleting latest. Neither a single-shot failure nor a
// sustained Put outage + fresh SingleProcess wrapper may lose A or fork on B.
func TestSingleProcess_FailedReplacementPreservesCommittedHead(t *testing.T) {
	for _, sustainedOutage := range []bool{false, true} {
		name := "single-shot put failure"
		if sustainedOutage {
			name = "sustained put outage across process restart"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			raw := newFailingPointerStore("datasets/events/latest")
			shared := SingleProcess(raw).(RangeStore)

			// Pause Writer A right before its pointer CAS so Writer B generates
			// a newer SnapshotID and commits first.
			writerAEntered := make(chan struct{})
			releaseWriterA := make(chan struct{})
			gated := &writerAGateStore{
				RangeStore: shared,
				entered:    writerAEntered,
				release:    releaseWriterA,
			}

			dsA, err := Open(Use[RangeStore](gated)).Records(ctx, "events", JSONL[D](), WithRetry(2))
			if err != nil {
				t.Fatal(err)
			}
			dsB, err := Open(Use(shared)).Records(ctx, "events", JSONL[D]())
			if err != nil {
				t.Fatal(err)
			}

			type writeRes struct {
				snap *Snapshot
				err  error
			}
			aDone := make(chan writeRes, 1)
			go func() {
				s, werr := dsA.Write(ctx, R(D{"writer": "A"}), nil)
				aDone <- writeRes{s, werr}
			}()

			<-writerAEntered
			// Ensure Writer B gets a strictly larger SnapshotID timestamp.
			time.Sleep(2 * time.Millisecond)
			snapB, err := dsB.Write(ctx, R(D{"writer": "B"}), nil)
			if err != nil {
				close(releaseWriterA)
				t.Fatalf("Writer B commit: %v", err)
			}
			close(releaseWriterA)

			resA := <-aDone
			if resA.err != nil {
				t.Fatalf("Writer A commit: %v", resA.err)
			}
			snapA := resA.snap
			if snapA.ID >= snapB.ID {
				t.Fatalf("expected snapA.ID (%s) < snapB.ID (%s) to exercise scan inversion", snapA.ID, snapB.ID)
			}
			if snapA.ParentSnapshotID != snapB.ID {
				t.Fatalf("snapA parent = %q, want snapB %q", snapA.ParentSnapshotID, snapB.ID)
			}

			// Writer C attempts to advance latest, failing on Put("datasets/events/latest").
			if sustainedOutage {
				raw.setFailPuts(-1) // fail both replacement Put and immediate restore Put
			} else {
				raw.setFailPuts(1) // fail replacement Put; immediate restore Put succeeds
			}
			dsC, err := Open(Use(shared)).Records(ctx, "events", JSONL[D]())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := dsC.Write(ctx, R(D{"writer": "C"}), nil); err == nil {
				t.Fatal("expected Writer C commit to fail when pointer Put fails")
			}

			// Simulate a fresh handle (and under sustainedOutage, a brand-new
			// SingleProcess instance after process restart once storage recovers).
			checkStore := shared
			if sustainedOutage {
				checkStore = SingleProcess(raw).(RangeStore)
			}
			freshView, err := OpenReadOnly(Use(checkStore)).Records(ctx, "events", JSONL[D]())
			if err != nil {
				t.Fatal(err)
			}
			head, err := freshView.Latest(ctx)
			if err != nil {
				t.Fatalf("fresh Latest after failed replacement: %v", err)
			}
			if head.ID != snapA.ID {
				t.Fatalf("fresh Latest selected %s (writer B=%s), want committed head A=%s", head.ID, snapB.ID, snapA.ID)
			}

			// Clear storage fault and verify the next commit parents on A, not B.
			raw.setFailPuts(0)
			dsD, err := Open(Use(checkStore)).Records(ctx, "events", JSONL[D]())
			if err != nil {
				t.Fatal(err)
			}
			snapD, err := dsD.Write(ctx, R(D{"writer": "D"}), nil)
			if err != nil {
				t.Fatalf("Writer D commit after recovery: %v", err)
			}
			if snapD.ParentSnapshotID != snapA.ID {
				t.Fatalf("history forked: Writer D parented on %q, want %q", snapD.ParentSnapshotID, snapA.ID)
			}

			// Verify journal object is cleaned up and invisible to List.
			keys, err := checkStore.List(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				if strings.HasSuffix(k, casJournalSuffix) {
					t.Errorf("leaked CAS journal key in List: %q", k)
				}
			}
		})
	}
}

type writerAGateStore struct {
	RangeStore
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (g *writerAGateStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.RangeStore.CompareAndSwap(ctx, path, expected, replacement)
}

func TestSingleProcess_RejectsReservedJournalKey(t *testing.T) {
	store := SingleProcess(testutil.WithoutCAS(NewMemory())).(RangeStore)
	assertKeyOpsRejectInvalidPath(t, store, "ptr/latest"+casJournalSuffix)
}
