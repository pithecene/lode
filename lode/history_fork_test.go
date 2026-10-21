package lode

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pithecene-io/lode/internal/testutil"
)

// Regression tests for lode#208: a production history forked once while every
// writer used CAS on the latest pointer. Each test drives one mechanism
// deterministically and asserts the history stays linear.
//
// Background: snapshot IDs are taken when a write starts, so a head can have a
// smaller ID than its parent, and the scan fallback (largest ID) then returns
// the parent. Production forked when a valid head's existence check failed
// transiently: the writer treated the pointer as stale, scanned to the older
// snapshot, and its CAS still matched the pointer.

// assertLinear fails if any snapshot has more than one child, or if a
// snapshot is not reachable from the head by following parents.
func assertLinear(t *testing.T, store Store) {
	t.Helper()
	ds, err := NewDataset("ds", NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	all, err := ds.Snapshots(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	children := map[DatasetSnapshotID][]DatasetSnapshotID{}
	parent := map[DatasetSnapshotID]DatasetSnapshotID{}
	for _, s := range all {
		parent[s.ID] = s.Manifest.ParentSnapshotID
		if p := s.Manifest.ParentSnapshotID; p != "" {
			children[p] = append(children[p], s.ID)
		}
	}
	for p, cs := range children {
		if len(cs) > 1 {
			t.Errorf("history forked: snapshot %s has children %v", p, cs)
		}
	}
	head, err := ds.Latest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	reached := 0
	for id := head.ID; id != ""; id = parent[id] {
		reached++
	}
	if reached != len(all) {
		t.Errorf("head %s reaches %d of %d snapshots: the head was rewound or a branch was abandoned", head.ID, reached, len(all))
	}
}

func writeBlob(t *testing.T, ds Dataset) *DatasetSnapshot {
	t.Helper()
	snap, err := ds.Write(t.Context(), []any{[]byte("x")}, nil)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	return snap
}

// pauseAt holds writer A inside a Put whose path matches, until released.
// reached closes when A is held.
type pauseAt struct {
	reached, release chan struct{}
	once             sync.Once
}

func holdPut(fs *testutil.FaultStore, match func(path string) bool) *pauseAt {
	p := &pauseAt{reached: make(chan struct{}), release: make(chan struct{})}
	fs.SetBeforePut(func(path string) {
		if !match(path) {
			return
		}
		p.once.Do(func() {
			close(p.reached)
			<-p.release
		})
	})
	return p
}

func isManifest(path string) bool { return strings.HasSuffix(path, "manifest.json") }

// startWrite runs ds.Write in the background; the returned function waits for
// it and returns its snapshot.
func startWrite(t *testing.T, ds Dataset) func() *DatasetSnapshot {
	t.Helper()
	var snap *DatasetSnapshot
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		snap, err = ds.Write(context.Background(), []any{[]byte("a")}, nil) //nolint:usetesting // outlives the calling goroutine's assertions
	}()
	return func() *DatasetSnapshot {
		<-done
		if err != nil {
			t.Fatalf("background write: %v", err)
		}
		return snap
	}
}

// A reader calls Latest while a commit's pointer names a manifest not yet
// written. Latest used to scan and "self-heal" the pointer back to the old
// head, rewinding it past the commit; it must never write the pointer.
func TestHistoryFork_LatestInsidePointerManifestWindow(t *testing.T) {
	inner := NewMemory()
	fsA := testutil.NewFaultStore(inner)
	dsA, err := NewDataset("ds", newFaultStoreFactory(fsA))
	if err != nil {
		t.Fatal(err)
	}
	writeBlob(t, dsA) // P

	hold := holdPut(fsA, isManifest)
	waitA := startWrite(t, dsA) // C1
	<-hold.reached

	reader, err := NewDataset("ds", NewMemoryFactoryFrom(inner))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Latest(t.Context()); err != nil {
		t.Fatalf("reader Latest: %v", err)
	}

	close(hold.release)
	waitA()

	// A later writer on a fresh handle must parent on C1.
	dsC, err := NewDataset("ds", NewMemoryFactoryFrom(inner))
	if err != nil {
		t.Fatal(err)
	}
	writeBlob(t, dsC)
	assertLinear(t, inner)
}

var errTransient = errors.New("transient store failure")

// flakyStore fails Exists, and the first Get of one snapshot's manifest, as a
// transient HEAD or GET error would. It keeps the inner store's
// CompareAndSwap, so writers on it still commit with CAS.
type flakyStore struct {
	Store
	failExists bool
	failGetID  DatasetSnapshotID
	failedGet  bool
}

func (f *flakyStore) Exists(ctx context.Context, path string) (bool, error) {
	if f.failExists {
		return false, errTransient
	}
	return f.Store.Exists(ctx, path)
}

func (f *flakyStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if f.failGetID != "" && !f.failedGet && isManifest(path) && strings.Contains(path, "/"+string(f.failGetID)+"/") {
		f.failedGet = true
		return nil, errTransient
	}
	return f.Store.Get(ctx, path)
}

func (f *flakyStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	return f.Store.(ConditionalWriter).CompareAndSwap(ctx, path, expected, replacement)
}

// commitInvertedPair commits P and then C1 as P's child with C1's ID smaller
// than P's (C1's write started first and committed second), the shape seen
// in production, and returns the head C1. A scan by largest ID returns P.
func commitInvertedPair(t *testing.T, inner Store) *DatasetSnapshot {
	t.Helper()
	fsA := testutil.NewFaultStore(inner)
	dsA, err := NewDataset("ds", newFaultStoreFactory(fsA))
	if err != nil {
		t.Fatal(err)
	}
	// A takes its ID, then is held while staging its data file.
	hold := holdPut(fsA, func(p string) bool { return !isManifest(p) && !strings.HasSuffix(p, "/latest") })
	waitA := startWrite(t, dsA)
	<-hold.reached

	dsP, err := NewDataset("ds", NewMemoryFactoryFrom(inner))
	if err != nil {
		t.Fatal(err)
	}
	p := writeBlob(t, dsP)

	close(hold.release)
	c1 := waitA()
	if c1.Manifest.ParentSnapshotID != p.ID || c1.ID >= p.ID {
		t.Fatalf("setup: want C1 (%s) < P (%s) and parented on P, got parent %s", c1.ID, p.ID, c1.Manifest.ParentSnapshotID)
	}
	return c1
}

// The production fork: a writer whose existence check on the valid head
// fails must fail visibly, not treat the pointer as stale and scan to P.
func TestHistoryFork_IDOrderInvertedAndExistsFails(t *testing.T) {
	inner := NewMemory()
	commitInvertedPair(t, inner)

	dsB, err := NewDataset("ds", NewMemoryFactoryFrom(&flakyStore{Store: inner, failExists: true}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dsB.Write(t.Context(), []any{[]byte("b")}, nil); !errors.Is(err, errTransient) {
		t.Fatalf("writer B: got %v, want the existence-check error", err)
	}
	assertLinear(t, inner)
}

// A retrying writer refreshes the head with Latest. A transient GET of the
// head's manifest during that refresh must fail the write — not scan to P,
// rewind the pointer to P, and let the retry parent on P.
func TestHistoryFork_RetryRefreshGetFails(t *testing.T) {
	inner := NewMemory()
	flaky := &flakyStore{Store: inner}
	dsW, err := NewDataset("ds", NewMemoryFactoryFrom(flaky), WithRetryCount(2), WithRetryBaseDelay(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	writeBlob(t, dsW) // W remembers this head, so its next CAS conflicts.

	c1 := commitInvertedPair(t, inner)
	flaky.failGetID = c1.ID

	if _, err := dsW.Write(t.Context(), []any{[]byte("w")}, nil); !errors.Is(err, errTransient) {
		t.Fatalf("writer W: got %v, want the refresh's GET error", err)
	}
	if !flaky.failedGet {
		t.Fatal("setup: the refresh never read the head's manifest")
	}
	assertLinear(t, inner)
}
