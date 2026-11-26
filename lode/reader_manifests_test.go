package lode

import (
	"context"
	"errors"
	"io"
	"iter"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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

// Loads that run ahead never read a manifest beyond Limit, and a caller that
// stops early finds every load finished when the iterator returns.
func TestManifests_PrefetchStaysWithinTheWindow(t *testing.T) {
	t.Parallel()
	fs := testutil.NewFaultStore(NewMemory())
	ds := openHive(t, fs)
	for range 6 {
		writeEach(t, ds, D{"day": "a"})
	}
	day := partitionPath(t, "day=a")

	fs.Reset()
	if got := len(collect(t, ds, day, ManifestListOptions{Limit: 2, Prefetch: 4})); got != 2 {
		t.Fatalf("yielded %d manifests, want 2", got)
	}
	if got := manifestGets(fs); got != 2 {
		t.Errorf("Limit 2 with Prefetch 4 read %d manifests, want 2", got)
	}

	fs.Reset()
	for _, err := range ds.Manifests(t.Context(), day, ManifestListOptions{Prefetch: 3}) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	stopped := manifestGets(fs)
	if stopped > 3 {
		t.Errorf("a caller that took one manifest with Prefetch 3 caused %d manifest reads, want at most 3", stopped)
	}
	time.Sleep(10 * time.Millisecond)
	if got := manifestGets(fs); got != stopped {
		t.Errorf("manifest reads went on after the iterator returned: %d, then %d", stopped, got)
	}
}

// gatedStore holds each manifest Get until width of them are in flight at
// once, or until the wait runs out.
type gatedStore struct {
	Store
	width int
	mu    sync.Mutex
	inFly int
	most  int
	open  chan struct{}
}

func (g *gatedStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if !strings.HasSuffix(path, "/manifest.json") || strings.Contains(path, "/latest") {
		return g.Store.Get(ctx, path)
	}
	g.mu.Lock()
	g.inFly++
	g.most = max(g.most, g.inFly)
	if g.inFly == g.width {
		close(g.open)
	}
	g.mu.Unlock()
	select {
	case <-g.open:
	case <-time.After(2 * time.Second):
	}
	g.mu.Lock()
	g.inFly--
	g.mu.Unlock()
	return g.Store.Get(ctx, path)
}

// Prefetch loads that many manifests at once.
func TestManifests_PrefetchLoadsConcurrently(t *testing.T) {
	t.Parallel()
	mem := NewMemory()
	ds := openHive(t, mem)
	for range 6 {
		writeEach(t, ds, D{"day": "a"})
	}
	gated := &gatedStore{Store: mem, width: 3, open: make(chan struct{})}
	view, err := Open(Use(gated)).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if got := len(collect(t, view, partitionPath(t, "day=a"), ManifestListOptions{Prefetch: 3})); got != 6 {
		t.Fatalf("yielded %d manifests, want 6", got)
	}
	if gated.most < 3 || time.Since(start) > time.Second {
		t.Errorf("at most %d manifest loads ran at once in %v, want 3 at once", gated.most, time.Since(start))
	}
}

// With loads running ahead, an invalid manifest is still yielded once as an
// error, and nothing follows it.
func TestManifests_PrefetchStopsAtAnInvalidManifest(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	ds := openHive(t, store)
	ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"}, D{"day": "a"})
	slices.Sort(ids)
	bad := "datasets/test-ds/partitions/day=a/segments/" + string(ids[1]) + "/manifest.json"
	if err := store.Delete(t.Context(), bad); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), bad, strings.NewReader("{")); err != nil {
		t.Fatal(err)
	}
	var seen []SnapshotID
	var errs int
	for loaded, err := range ds.Manifests(t.Context(), partitionPath(t, "day=a"), ManifestListOptions{Prefetch: 3}) {
		if err != nil {
			errs++
			continue
		}
		seen = append(seen, loaded.Ref.ID)
	}
	if errs != 1 || !slices.Equal(seen, ids[:1]) {
		t.Fatalf("yielded %v and %d errors, want %v and 1", seen, errs, ids[:1])
	}
}

// With loads running ahead, a manifest that went away after the listing is
// skipped, as a lazy listing skips it.
func TestManifests_PrefetchSkipsARemovedManifest(t *testing.T) {
	t.Parallel()
	fs := testutil.NewFaultStore(NewMemory())
	ds := openHive(t, fs)
	ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"}, D{"day": "a"})
	slices.Sort(ids)
	fs.SetGetError(ErrNotFound, "segments/"+string(ids[1])+"/manifest.json")
	got := refsOf(collect(t, ds, partitionPath(t, "day=a"), ManifestListOptions{Prefetch: 3}))
	if len(got) != 2 || got[0].ID != ids[0] || got[1].ID != ids[2] {
		t.Fatalf("yielded %v, want %s and %s", got, ids[0], ids[2])
	}
}
