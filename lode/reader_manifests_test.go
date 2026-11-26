package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-openapi/testify/v2/assert"

	"github.com/pithecene/lode/internal/testutil"
)

// Tests of Manifests: it yields manifests in (Partition, ID) order across every
// window and partition, matches Snapshot(id) for each yielded manifest, and
// loads each manifest once on demand as the caller consumes it.

// manifestLister is the minimal surface needed by collect.
type manifestLister interface {
	Manifests(ctx context.Context, partition PartitionPath, opts ManifestListOptions) iter.Seq2[LoadedManifest, error]
}

// manifestFixture is a dataset to list, built in store.
type manifestFixture struct {
	name  string
	build func(t *testing.T, store Store) *Records[D]
}

// writeEach writes each record as its own snapshot.
func writeEach(t *testing.T, ds *Records[D], records ...D) []SnapshotID {
	t.Helper()
	ids := make([]SnapshotID, 0, len(records))
	for _, r := range records {
		snap, err := ds.Write(t.Context(), R(r), nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snap.ID)
	}
	return ids
}

func openHive(t *testing.T, store Store) *Records[D] {
	t.Helper()
	ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

var manifestFixtures = []manifestFixture{
	{"hive, one snapshot per partition and one across two", func(t *testing.T, store Store) *Records[D] {
		ds := openHive(t, store)
		writeEach(t, ds, D{"day": "c"}, D{"day": "a"}, D{"day": "b"})
		if _, err := ds.Write(t.Context(), R(D{"day": "d"}, D{"day": "a"}), nil); err != nil {
			t.Fatal(err)
		}
		return ds
	}},
	{"default layout", func(t *testing.T, store Store) *Records[D] {
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		writeEach(t, ds, D{"id": 1}, D{"id": 2}, D{"id": 3})
		return ds
	}},
	{"hive, a snapshot whose partition copies are missing", func(t *testing.T, store Store) *Records[D] {
		ds := openHive(t, store)
		ids := writeEach(t, ds, D{"day": "a"}, D{"day": "b"})
		if err := store.Delete(t.Context(), "datasets/test-ds/partitions/day=b/segments/"+string(ids[1])+"/manifest.json"); err != nil {
			t.Fatal(err)
		}
		return ds
	}},
	{"hive, a snapshot with no objects", func(t *testing.T, store Store) *Records[D] {
		ds := openHive(t, store)
		writeEach(t, ds, D{"day": "a"})
		if _, err := ds.Write(t.Context(), nil, nil); err != nil {
			t.Fatal(err)
		}
		return ds
	}},
}

// collect drains Manifests, failing on any error.
func collect(t *testing.T, r manifestLister, partition PartitionPath, opts ManifestListOptions) []LoadedManifest {
	t.Helper()
	var out []LoadedManifest
	for loaded, err := range r.Manifests(t.Context(), partition, opts) {
		if err != nil {
			t.Fatalf("Manifests: %v", err)
		}
		out = append(out, loaded)
	}
	return out
}

// refsOf extracts the ManifestRef slice from a LoadedManifest slice.
func refsOf(ms []LoadedManifest) []ManifestRef {
	refs := make([]ManifestRef, len(ms))
	for i, m := range ms {
		refs[i] = m.Ref
	}
	return refs
}

// listingWindows are the listing options each fixture is listed under: whole, by
// page, and from a cursor.
func listingWindows(refs []ManifestRef) []ManifestListOptions {
	opts := []ManifestListOptions{{}, {Limit: 1}, {Limit: 2}, {Limit: len(refs)}}
	for _, ref := range refs {
		opts = append(opts, ManifestListOptions{StartAfter: ref}, ManifestListOptions{StartAfter: ref, Limit: 1})
	}
	// Each window again with loads running ahead of the caller, which
	// changes nothing that the listing yields.
	for _, o := range slices.Clone(opts) {
		for _, prefetch := range []int{1, 2, 8} {
			o.Prefetch = prefetch
			opts = append(opts, o)
		}
	}
	return opts
}

// partitionsOf are the partitions each fixture is listed within: the whole
// dataset and each partition its listing names.
func partitionsOf(refs []ManifestRef) []PartitionPath {
	parts := []PartitionPath{{}}
	for _, ref := range refs {
		if ref.Partition.path != "" && !slices.Contains(parts, ref.Partition) {
			parts = append(parts, ref.Partition)
		}
	}
	return parts
}

func verifyManifestWindow(t *testing.T, r *Records[D], partition PartitionPath, full []LoadedManifest, opts ManifestListOptions) {
	t.Helper()
	want := cursorWindow(full, func(m LoadedManifest) bool { return manifestRefLess(opts.StartAfter, m.Ref) }, opts.Limit)
	got := collect(t, r, partition, opts)
	if len(got) != len(want) {
		t.Fatalf("partition %q %+v: Manifests yielded %d, want %d", partition, opts, len(got), len(want))
	}
	for i, loaded := range got {
		if loaded.Ref != want[i].Ref {
			t.Fatalf("partition %q %+v: ref %d = %v, want %v", partition, opts, i, loaded.Ref, want[i].Ref)
		}
		if !reflect.DeepEqual(loaded.Manifest, want[i].Manifest) {
			t.Fatalf("partition %q %+v: manifest %d differs from full sequence", partition, opts, i)
		}
		snap, err := r.Snapshot(t.Context(), loaded.Ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(loaded.Manifest, snap.Manifest) {
			t.Fatalf("partition %q: manifest of %v differs from Snapshot(%s)", partition, loaded.Ref, loaded.Ref.ID)
		}
	}
}

// Manifests yields ordered references in every window and partition, each
// carrying the exact manifest Snapshot(id) loads.
func TestManifests_EquivalentToListThenGet(t *testing.T) {
	t.Parallel()
	for _, f := range manifestFixtures {
		t.Run(f.name, func(t *testing.T) {
			r := f.build(t, NewMemory())
			allRefs := refsOf(collect(t, r, PartitionPath{}, ManifestListOptions{}))
			for _, partition := range partitionsOf(allRefs) {
				full := collect(t, r, partition, ManifestListOptions{})
				for i := 1; i < len(full); i++ {
					if !manifestRefLess(full[i-1].Ref, full[i].Ref) {
						t.Fatalf("partition %q not strictly ordered: %v before %v", partition, full[i-1].Ref, full[i].Ref)
					}
				}
				for _, opts := range listingWindows(allRefs) {
					verifyManifestWindow(t, r, partition, full, opts)
				}
			}
		})
	}
}

// manifestGets counts the manifest reads among a store's Get calls.
func manifestGets(fs *testutil.FaultStore) int {
	n := 0
	for _, p := range fs.GetCalls() {
		if strings.HasSuffix(p, "manifest.json") {
			n++
		}
	}
	return n
}

// Reading a partition through Manifests loads each manifest once.
func TestManifests_LoadsEachManifestOnce(t *testing.T) {
	t.Parallel()
	const snapshots = 5
	fs := testutil.NewFaultStore(NewMemory())
	ds := openHive(t, fs)
	for range snapshots {
		writeEach(t, ds, D{"day": "a"})
	}
	day := partitionPath(t, "day=a")

	fs.Reset()
	if got := len(collect(t, ds, day, ManifestListOptions{})); got != snapshots {
		t.Fatalf("yielded %d manifests, want %d", got, snapshots)
	}
	if got := manifestGets(fs); got != snapshots {
		t.Errorf("Manifests read %d manifests, want %d", got, snapshots)
	}
}

// Manifests loads a manifest only when the caller asks for it, so a caller
// that stops early loads no more.
func TestManifests_LoadsAsConsumed(t *testing.T) {
	t.Parallel()
	fs := testutil.NewFaultStore(NewMemory())
	ds := openHive(t, fs)
	writeEach(t, ds, D{"day": "a"}, D{"day": "a"}, D{"day": "a"})

	fs.Reset()
	for _, err := range ds.Manifests(t.Context(), partitionPath(t, "day=a"), ManifestListOptions{}) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if got := manifestGets(fs); got != 1 {
		t.Errorf("a caller that took one manifest caused %d manifest reads, want 1", got)
	}
}

// A dataset with no manifest yields ErrNotFound and nothing else.
func TestManifests_NoManifest(t *testing.T) {
	t.Parallel()
	ds := openHive(t, NewMemory())
	var errs []error
	for loaded, err := range ds.Manifests(t.Context(), PartitionPath{}, ManifestListOptions{}) {
		if loaded.Manifest != nil {
			t.Fatalf("yielded %v from an empty dataset", loaded.Ref)
		}
		errs = append(errs, err)
	}
	if len(errs) != 1 || !errors.Is(errs[0], ErrNotFound) {
		t.Fatalf("errors %v, want one ErrNotFound", errs)
	}
}

// An invalid manifest is yielded as an error, and nothing follows it.
func TestManifests_StopsAtAnInvalidManifest(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	ds := openHive(t, store)
	ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"})
	// Snapshot IDs order the listing, and two writes can share a clock tick,
	// so the first listed is the smallest ID, not the first written.
	slices.Sort(ids)
	bad := "datasets/test-ds/partitions/day=a/segments/" + string(ids[0]) + "/manifest.json"
	if err := store.Delete(t.Context(), bad); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), bad, strings.NewReader("{")); err != nil {
		t.Fatal(err)
	}
	var yielded, errs int
	for _, err := range ds.Manifests(t.Context(), partitionPath(t, "day=a"), ManifestListOptions{}) {
		if err != nil {
			errs++
			continue
		}
		yielded++
	}
	if errs != 1 || yielded != 0 {
		t.Fatalf("yielded %d manifests and %d errors, want 0 and 1", yielded, errs)
	}
}

// A snapshot removed after it is yielded surfaces when its data is read, as
// ErrNotFound. One removed before it is loaded is skipped.
func TestManifests_RemovalBetweenListingAndUse(t *testing.T) {
	t.Parallel()
	t.Run("after it is yielded", func(t *testing.T) {
		store := NewMemory()
		ds := openHive(t, store)
		writeEach(t, ds, D{"day": "a"})
		got := collect(t, ds, partitionPath(t, "day=a"), ManifestListOptions{})
		if len(got) != 1 || len(got[0].Manifest.Objects) == 0 {
			t.Fatalf("yielded %v", got)
		}
		obj := got[0].Manifest.Objects[0]
		if err := store.Delete(t.Context(), obj.Path); err != nil {
			t.Fatal(err)
		}
		_, err := ds.OpenObject(t.Context(), obj)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("OpenObject of a removed snapshot's data: %v, want ErrNotFound", err)
		}
	})

	t.Run("before it is loaded", func(t *testing.T) {
		store := NewMemory()
		ds := openHive(t, store)
		ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"})
		day := partitionPath(t, "day=a")
		slices.Sort(ids)
		var seen []SnapshotID
		for loaded, err := range ds.Manifests(t.Context(), day, ManifestListOptions{}) {
			if err != nil {
				t.Fatal(err)
			}
			seen = append(seen, loaded.Ref.ID)
			// The second snapshot goes once the first is yielded, after the
			// listing that found it.
			if len(seen) == 1 {
				if err := store.Delete(t.Context(), "datasets/test-ds/partitions/day=a/segments/"+string(ids[1])+"/manifest.json"); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !slices.Equal(seen, ids[:1]) {
			t.Fatalf("yielded %v, want only %v", seen, ids[:1])
		}
	})
}

// Manifests loads only what it yields, so an invalid manifest outside the
// requested window does not fail the window, while widening the window to
// include it yields the validation error.
func TestManifests_ValidatesOnlyTheWindow(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	ds := openHive(t, store)
	ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"})
	slices.Sort(ids)
	bad := "datasets/test-ds/partitions/day=a/segments/" + string(ids[1]) + "/manifest.json"
	if err := store.Delete(t.Context(), bad); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), bad, strings.NewReader("{")); err != nil {
		t.Fatal(err)
	}
	day := partitionPath(t, "day=a")

	got := collect(t, ds, day, ManifestListOptions{Limit: 1})
	if len(got) != 1 || got[0].Ref.ID != ids[0] {
		t.Fatalf("Manifests(Limit: 1) yielded %v, want only %s", got, ids[0])
	}

	var errs int
	for _, err := range ds.Manifests(t.Context(), day, ManifestListOptions{Limit: 2}) {
		if err != nil {
			errs++
		}
	}
	if errs != 1 {
		t.Fatalf("Manifests(Limit: 2) yielded %d errors, want 1", errs)
	}
}

// prefetchFixture writes n snapshots into day=a and returns their IDs in
// listing order.
func prefetchFixture(t *testing.T, store Store, n int) []SnapshotID {
	t.Helper()
	ds := openHive(t, store)
	ids := make([]SnapshotID, 0, n)
	for range n {
		ids = append(ids, writeEach(t, ds, D{"day": "a"})...)
	}
	slices.Sort(ids)
	return ids
}

func manifestKey(id SnapshotID) string {
	return "datasets/test-ds/partitions/day=a/segments/" + string(id) + "/manifest.json"
}

// fetchedIDs is the snapshots whose manifests a store's Get calls named, in
// sorted order.
func fetchedIDs(fs *testutil.FaultStore) []SnapshotID {
	var out []SnapshotID
	for _, p := range fs.GetCalls() {
		if _, rest, ok := strings.Cut(p, "/segments/"); ok && strings.HasSuffix(p, "/manifest.json") {
			out = append(out, SnapshotID(strings.TrimSuffix(rest, "/manifest.json")))
		}
	}
	slices.Sort(out)
	return out
}

// Loads that run ahead never read a manifest beyond Limit.
func TestManifests_PrefetchStaysWithinTheLimit(t *testing.T) {
	t.Parallel()
	fs := testutil.NewFaultStore(NewMemory())
	ids := prefetchFixture(t, fs, 6)
	fs.Reset()
	ds := openHive(t, fs)
	fs.Reset()
	if got := refsOf(collect(t, ds, partitionPath(t, "day=a"), ManifestListOptions{Limit: 2, Prefetch: 4})); len(got) != 2 {
		t.Fatalf("yielded %v, want 2 manifests", got)
	}
	if got := fetchedIDs(fs); !slices.Equal(got, ids[:2]) {
		t.Errorf("Limit 2 with Prefetch 4 read the manifests of %v, want %v", got, ids[:2])
	}
}

// A skipped manifest inside a cursor's page frees its slot for the next one,
// and no load reaches at or before the cursor or past the page's last
// manifest.
func TestManifests_PrefetchRefillsASkipWithinTheWindow(t *testing.T) {
	t.Parallel()
	fs := testutil.NewFaultStore(NewMemory())
	ids := prefetchFixture(t, fs, 6)
	ds := openHive(t, fs)
	fs.Reset()
	fs.SetGetError(ErrNotFound, "segments/"+string(ids[2])+"/manifest.json")
	day := partitionPath(t, "day=a")
	got := refsOf(collect(t, ds, day, ManifestListOptions{StartAfter: ManifestRef{ID: ids[0], Partition: day}, Limit: 2, Prefetch: 2}))
	if len(got) != 2 || got[0].ID != ids[1] || got[1].ID != ids[3] {
		t.Fatalf("yielded %v, want %s and %s", got, ids[1], ids[3])
	}
	if fetched := fetchedIDs(fs); !slices.Equal(fetched, ids[1:4]) {
		t.Errorf("read the manifests of %v, want %v", fetched, ids[1:4])
	}
}

// holdStore holds the Get of one manifest until its context ends, and reports
// when that Get has returned.
type holdStore struct {
	Store
	held     string
	returned chan struct{}
}

func (h *holdStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if path != h.held {
		return h.Store.Get(ctx, path)
	}
	defer close(h.returned)
	<-ctx.Done()
	return nil, ctx.Err()
}

// A caller that stops, or a load that fails, ends the loads still under way
// before the iterator returns.
func TestManifests_PrefetchEndsEveryLoadBeforeReturning(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		fail  bool
		steps []string
	}{
		{"the caller stops", false, []string{"manifest"}},
		{"an earlier load fails", true, []string{"error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := NewMemory()
			ids := prefetchFixture(t, mem, 3)
			if tc.fail {
				if err := mem.Delete(t.Context(), manifestKey(ids[0])); err != nil {
					t.Fatal(err)
				}
				if err := mem.Put(t.Context(), manifestKey(ids[0]), strings.NewReader("{")); err != nil {
					t.Fatal(err)
				}
			}
			held := &holdStore{Store: mem, held: manifestKey(ids[2]), returned: make(chan struct{})}
			view, err := Open(Use(held)).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
			if err != nil {
				t.Fatal(err)
			}
			var steps []string
			for _, err := range view.Manifests(t.Context(), partitionPath(t, "day=a"), ManifestListOptions{Prefetch: 3}) {
				if err != nil {
					steps = append(steps, "error")
					continue
				}
				steps = append(steps, "manifest")
				if !tc.fail {
					break
				}
			}
			if !slices.Equal(steps, tc.steps) {
				t.Fatalf("steps %v, want %v", steps, tc.steps)
			}
			select {
			case <-held.returned:
			default:
				t.Fatal("a load was still under way when the iterator returned")
			}
		})
	}
}

// gateStore counts the manifest Gets that have started and holds each until
// release closes.
type gateStore struct {
	Store
	entered atomic.Int32
	release chan struct{}
}

func (g *gateStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if g.release != nil && strings.Contains(path, "/partitions/") && strings.HasSuffix(path, "/manifest.json") {
		g.entered.Add(1)
		<-g.release
	}
	return g.Store.Get(ctx, path)
}

// Prefetch loads that many manifests at once, and never more: with every load
// held, exactly that many have started once all other work has settled.
func TestManifests_PrefetchLoadsConcurrently(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem := NewMemory()
		prefetchFixture(t, mem, 6)
		gate := &gateStore{Store: mem}
		view, err := Open(Use(gate)).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}
		gate.release = make(chan struct{})
		day := partitionPath(t, "day=a")
		assert.NoGoRoutineLeak(t, func() {
			yielded := make(chan int, 1)
			var wg sync.WaitGroup
			defer wg.Wait()
			wg.Go(func() {
				n := 0
				for _, err := range view.Manifests(t.Context(), day, ManifestListOptions{Prefetch: 3}) {
					if err != nil {
						t.Error(err)
					}
					n++
				}
				yielded <- n
			})
			synctest.Wait()
			if got := gate.entered.Load(); got != 3 {
				t.Errorf("%d manifest loads started at once, want 3", got)
			}
			close(gate.release)
			if n := <-yielded; n != 6 {
				t.Errorf("yielded %d manifests, want 6", n)
			}
		})
	})
}

// With loads running ahead, an invalid manifest is yielded once, in its place,
// as ErrManifestInvalid, and nothing follows it.
func TestManifests_PrefetchStopsAtAnInvalidManifest(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	ids := prefetchFixture(t, store, 3)
	rc, err := store.Get(t.Context(), manifestKey(ids[1]))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	delete(m, "schema_name")
	invalid, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(t.Context(), manifestKey(ids[1])); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), manifestKey(ids[1]), bytes.NewReader(invalid)); err != nil {
		t.Fatal(err)
	}
	ds := openHive(t, store)
	var steps []string
	for loaded, err := range ds.Manifests(t.Context(), partitionPath(t, "day=a"), ManifestListOptions{Prefetch: 3}) {
		switch {
		case err == nil:
			steps = append(steps, string(loaded.Ref.ID))
		case errors.Is(err, ErrManifestInvalid) && loaded == (LoadedManifest{}):
			steps = append(steps, "invalid")
		default:
			steps = append(steps, "other: "+err.Error())
		}
	}
	if want := []string{string(ids[0]), "invalid"}; !slices.Equal(steps, want) {
		t.Fatalf("yielded %v, want %v", steps, want)
	}
}

// With loads running ahead, a manifest that went away after the listing is
// skipped, as a lazy listing skips it.
func TestManifests_PrefetchSkipsARemovedManifest(t *testing.T) {
	t.Parallel()
	fs := testutil.NewFaultStore(NewMemory())
	ids := prefetchFixture(t, fs, 3)
	ds := openHive(t, fs)
	fs.SetGetError(ErrNotFound, "segments/"+string(ids[1])+"/manifest.json")
	got := refsOf(collect(t, ds, partitionPath(t, "day=a"), ManifestListOptions{Prefetch: 3}))
	if len(got) != 2 || got[0].ID != ids[0] || got[1].ID != ids[2] {
		t.Fatalf("yielded %v, want %s and %s", got, ids[0], ids[2])
	}
}

// A caller that cancels its context receives the context's error at its next
// advance: in place of a manifest already loaded, and after the window's last
// manifest.
func TestManifests_PrefetchHonoursCancellation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		limit int
	}{
		{"a manifest already loaded", 2},
		{"after the window's last manifest", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := NewMemory()
				ids := prefetchFixture(t, store, 2)
				ds := openHive(t, store)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var steps []string
				for loaded, err := range ds.Manifests(ctx, partitionPath(t, "day=a"), ManifestListOptions{Limit: tc.limit, Prefetch: 2}) {
					if err != nil {
						if errors.Is(err, context.Canceled) {
							steps = append(steps, "canceled")
						} else {
							steps = append(steps, "other: "+err.Error())
						}
						continue
					}
					steps = append(steps, string(loaded.Ref.ID))
					// Every load the window allows has finished before the
					// caller cancels.
					synctest.Wait()
					cancel()
				}
				if want := []string{string(ids[0]), "canceled"}; !slices.Equal(steps, want) {
					t.Fatalf("yielded %v, want %v", steps, want)
				}
			})
		})
	}
}

// recheckStore changes the answer to the second Exists of path, the worker's
// recheck after the listing's own check: it removes path first, or fails.
type recheckStore struct {
	Store
	path   string
	fail   bool
	mu     sync.Mutex
	checks int
}

func (s *recheckStore) Exists(ctx context.Context, path string) (bool, error) {
	if path == s.path {
		s.mu.Lock()
		s.checks++
		second := s.checks == 2
		s.mu.Unlock()
		if second && s.fail {
			return false, errProbe
		}
		if second {
			if err := s.Delete(ctx, path); err != nil {
				return false, err
			}
		}
	}
	return s.Store.Exists(ctx, path)
}

var errProbe = errors.New("probe failure")

// A snapshot whose expiry completes after the listing leaves a stale partition
// copy, which a prefetched load skips without using a slot of Limit, while a
// storage error from the same check is yielded in its place.
func TestManifests_PrefetchRechecksATombstonedSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		fail bool
		want func(ids []SnapshotID) []string
	}{
		{"expiry completed", false, func(ids []SnapshotID) []string { return []string{string(ids[0]), string(ids[2])} }},
		{"the check fails", true, func(ids []SnapshotID) []string { return []string{string(ids[0]), "probe failure"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := NewMemory()
			ds := openHive(t, mem)
			var snaps []*Snapshot
			for range 3 {
				snap, err := ds.Write(t.Context(), R(D{"day": "a"}), nil)
				if err != nil {
					t.Fatal(err)
				}
				snaps = append(snaps, snap)
			}
			slices.SortFunc(snaps, func(a, b *Snapshot) int { return strings.Compare(string(a.ID), string(b.ID)) })
			ids := []SnapshotID{snaps[0].ID, snaps[1].ID, snaps[2].ID}
			expiring := snaps[1]
			tomb, err := json.Marshal(map[string]any{
				"snapshot_id":         expiring.ID,
				"requested_at":        time.Now().UTC(),
				"manifest_created_at": expiring.CreatedAt,
				"objects":             expiring.Objects,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := mem.Put(t.Context(), "datasets/test-ds/expired/"+string(expiring.ID), bytes.NewReader(tomb)); err != nil {
				t.Fatal(err)
			}
			canonical := "datasets/test-ds/segments/" + string(expiring.ID) + "/manifest.json"
			view, err := Open(Use(&recheckStore{Store: mem, path: canonical, fail: tc.fail})).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
			if err != nil {
				t.Fatal(err)
			}
			var steps []string
			for loaded, err := range view.Manifests(t.Context(), partitionPath(t, "day=a"), ManifestListOptions{Limit: 2, Prefetch: 3}) {
				if err != nil {
					if errors.Is(err, errProbe) {
						steps = append(steps, "probe failure")
					} else {
						steps = append(steps, "other: "+err.Error())
					}
					continue
				}
				steps = append(steps, string(loaded.Ref.ID))
			}
			if want := tc.want(ids); !slices.Equal(steps, want) {
				t.Fatalf("yielded %v, want %v", steps, want)
			}
		})
	}
}
