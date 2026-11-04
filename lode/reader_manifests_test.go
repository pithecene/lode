package lode

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// Tests of SnapshotReader.Manifests: it lists what ListManifests lists, in
// the same order and window, with the manifests GetManifest would load, and
// it loads each manifest once, as the caller consumes it.

// manifestFixture is a dataset to list, built in store.
type manifestFixture struct {
	name  string
	build func(t *testing.T, store Store) SnapshotReader
}

// writeEach writes each record as its own snapshot.
func writeEach(t *testing.T, ds Records[D], records ...D) []DatasetSnapshotID {
	t.Helper()
	ids := make([]DatasetSnapshotID, 0, len(records))
	for _, r := range records {
		snap, err := ds.Write(t.Context(), R(r), nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snap.ID)
	}
	return ids
}

func openHive(t *testing.T, store Store) Records[D] {
	t.Helper()
	ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

var manifestFixtures = []manifestFixture{
	{"hive, one snapshot per partition and one across two", func(t *testing.T, store Store) SnapshotReader {
		ds := openHive(t, store)
		writeEach(t, ds, D{"day": "c"}, D{"day": "a"}, D{"day": "b"})
		if _, err := ds.Write(t.Context(), R(D{"day": "d"}, D{"day": "a"}), nil); err != nil {
			t.Fatal(err)
		}
		return ds
	}},
	{"default layout", func(t *testing.T, store Store) SnapshotReader {
		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		writeEach(t, ds, D{"id": 1}, D{"id": 2}, D{"id": 3})
		return ds
	}},
	{"hive, a snapshot whose partition copies are missing", func(t *testing.T, store Store) SnapshotReader {
		ds := openHive(t, store)
		ids := writeEach(t, ds, D{"day": "a"}, D{"day": "b"})
		if err := store.Delete(t.Context(), "datasets/test-ds/partitions/day=b/segments/"+string(ids[1])+"/manifest.json"); err != nil {
			t.Fatal(err)
		}
		return ds
	}},
	{"hive, a snapshot with no files", func(t *testing.T, store Store) SnapshotReader {
		ds := openHive(t, store)
		writeEach(t, ds, D{"day": "a"})
		if _, err := ds.Write(t.Context(), nil, nil); err != nil {
			t.Fatal(err)
		}
		return ds
	}},
}

// collect drains Manifests, failing on any error.
func collect(t *testing.T, r SnapshotReader, partition PartitionPath, opts ManifestListOptions) []LoadedManifest {
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

// listingWindows are the listing options each fixture is listed under: whole, by
// page, and from a cursor.
func listingWindows(refs []ManifestRef) []ManifestListOptions {
	opts := []ManifestListOptions{{}, {Limit: 1}, {Limit: 2}, {Limit: len(refs)}}
	for _, ref := range refs {
		opts = append(opts, ManifestListOptions{StartAfter: ref}, ManifestListOptions{StartAfter: ref, Limit: 1})
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

// Manifests yields exactly the references ListManifests returns, in every
// window and partition, each with the manifest GetManifest loads for it.
func TestManifests_EquivalentToListThenGet(t *testing.T) {
	for _, f := range manifestFixtures {
		t.Run(f.name, func(t *testing.T) {
			r := f.build(t, NewMemory())
			all, err := r.ListManifests(t.Context(), PartitionPath{}, ManifestListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, partition := range partitionsOf(all) {
				for _, opts := range listingWindows(all) {
					want, err := r.ListManifests(t.Context(), partition, opts)
					if err != nil {
						t.Fatal(err)
					}
					got := collect(t, r, partition, opts)
					if len(got) != len(want) {
						t.Fatalf("partition %q %+v: Manifests yielded %d, ListManifests %d", partition, opts, len(got), len(want))
					}
					for i, loaded := range got {
						if loaded.Ref != want[i] {
							t.Fatalf("partition %q %+v: ref %d = %v, want %v", partition, opts, i, loaded.Ref, want[i])
						}
						m, err := r.GetManifest(t.Context(), loaded.Ref)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(loaded.Manifest, m) {
							t.Fatalf("partition %q: manifest of %v differs from GetManifest's", partition, loaded.Ref)
						}
					}
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

// Reading a partition through Manifests loads each manifest once, where
// ListManifests then GetManifest loads it twice.
func TestManifests_LoadsEachManifestOnce(t *testing.T) {
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

	fs.Reset()
	refs, err := ds.ListManifests(t.Context(), day, ManifestListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if _, err := ds.GetManifest(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
	}
	if got := manifestGets(fs); got != 2*snapshots {
		t.Errorf("ListManifests then GetManifest read %d manifests, want %d", got, 2*snapshots)
	}
}

// Manifests loads a manifest only when the caller asks for it, so a caller
// that stops early loads no more.
func TestManifests_LoadsAsConsumed(t *testing.T) {
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
	store := NewMemory()
	ds := openHive(t, store)
	ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"})
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

// The manifests are the listing's: a snapshot removed after it is yielded
// surfaces when its data is read, as ErrNotFound, where ListManifests then
// GetManifest would surface it at GetManifest. One removed before it is
// loaded is skipped, as ListManifests skips it.
func TestManifests_RemovalBetweenListingAndUse(t *testing.T) {
	t.Run("after it is yielded", func(t *testing.T) {
		store := NewMemory()
		ds := openHive(t, store)
		writeEach(t, ds, D{"day": "a"})
		got := collect(t, ds, partitionPath(t, "day=a"), ManifestListOptions{})
		if len(got) != 1 || len(got[0].Manifest.Files) == 0 {
			t.Fatalf("yielded %v", got)
		}
		file := got[0].Manifest.Files[0].Path
		if err := store.Delete(t.Context(), file); err != nil {
			t.Fatal(err)
		}
		_, err := ds.OpenObject(t.Context(), ObjectRef{Manifest: got[0].Ref, Path: file})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("OpenObject of a removed snapshot's data: %v, want ErrNotFound", err)
		}
	})

	t.Run("before it is loaded", func(t *testing.T) {
		store := NewMemory()
		ds := openHive(t, store)
		ids := writeEach(t, ds, D{"day": "a"}, D{"day": "a"})
		day := partitionPath(t, "day=a")
		var seen []DatasetSnapshotID
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

// ListManifests validates every listed manifest before it applies the
// window, so an invalid manifest outside the window fails it. Manifests
// loads only what it yields, so the same window succeeds.
func TestManifests_ValidatesOnlyTheWindow(t *testing.T) {
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
	first := ManifestListOptions{Limit: 1}

	if _, err := ds.ListManifests(t.Context(), day, first); err == nil {
		t.Fatal("ListManifests succeeded with an invalid manifest outside its window")
	}
	got := collect(t, ds, day, first)
	if len(got) != 1 || got[0].Ref.ID != ids[0] {
		t.Fatalf("Manifests yielded %v, want only %s", got, ids[0])
	}
}
