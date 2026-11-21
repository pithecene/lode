package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// testReader reads a root's datasets by dataset ID: each call opens a view of
// the dataset, whose layout comes from its head manifest. The listing methods
// do not depend on a codec, so the view is opened without one; ReadRoot.Records
// and ReadRoot.Blobs open views the same way and check the codec besides.
type testReader struct {
	root *ReadRoot
}

func newTestReader[S ObjectStore](f Factory[S], opts ...RootOption) (*testReader, error) {
	return &testReader{root: OpenReadOnly(f, opts...)}, nil
}

// view opens the view of dataset id.
func (r *testReader) view(ctx context.Context, id DatasetID) (*dataset, error) {
	if r.root.err != nil {
		return nil, r.root.err
	}
	if err := validateDatasetID(id); err != nil {
		return nil, err
	}
	head, err := resolveAnchor(ctx, r.root.store, id, viewCandidates(r.root.store, r.root.flat, id, nil), headAnchor{})
	if err != nil {
		return nil, err
	}
	if err := validateManifest(head.Manifest); err != nil {
		return nil, err
	}
	return newDataset(id, r.root.store, newPathLayout(r.root.flat, head.partitioning())), nil
}

func (r *testReader) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	return r.root.ListDatasets(ctx, opts)
}

func (r *testReader) ListPartitions(ctx context.Context, id DatasetID, opts PartitionListOptions) ([]PartitionPath, error) {
	v, err := r.view(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.ListPartitions(ctx, opts)
}

func (r *testReader) ListManifests(ctx context.Context, id DatasetID, partition string, opts ManifestListOptions) ([]ManifestRef, error) {
	path, err := ParsePartitionPath(partition)
	if err != nil {
		return nil, err
	}
	v, err := r.view(ctx, id)
	if err != nil {
		return nil, err
	}
	var refs []ManifestRef
	for loaded, err := range v.Manifests(ctx, path, opts) {
		if err != nil {
			return nil, err
		}
		refs = append(refs, loaded.Ref)
	}
	return refs, nil
}

// manifestErr opens a view of dataset test-ds over store and loads ref's
// snapshot, returning the first error: a view validates its head's manifest
// when it opens, and Snapshot validates every manifest it loads. The manifests
// these tests write record no codec, so the view reads blobs.
func manifestErr(t *testing.T, store Store, ref ManifestRef) error {
	t.Helper()
	view, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds")
	if err != nil {
		return err
	}
	_, err = view.Snapshot(t.Context(), ref.ID)
	return err
}

// -----------------------------------------------------------------------------
// G2: Manifest validation tests
// -----------------------------------------------------------------------------

func TestView_GetManifest_InvalidManifest(t *testing.T) {
	t.Parallel()
	mutCases := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"missing schema name", func(m *Manifest) { m.SchemaName = "" }},
		{"missing format version", func(m *Manifest) { m.FormatVersion = "" }},
		{"zero created at", func(m *Manifest) { m.CreatedAt = time.Time{} }},
		{"nil objects", func(m *Manifest) { m.Objects = nil }},
		{"negative row count", func(m *Manifest) { m.RowCount = -1 }},
		{"missing compressor", func(m *Manifest) { m.Compressor = "" }},
		{"missing partitioner", func(m *Manifest) { m.Partitioner = "" }},
		{"empty object path", func(m *Manifest) { m.Objects = []ObjectRef{{Path: "", SizeBytes: 100}}; m.RowCount = 1 }},
		{"negative object size", func(m *Manifest) { m.Objects = []ObjectRef{{Path: "data/file.bin", SizeBytes: -100}}; m.RowCount = 1 }},
	}
	for _, tc := range mutCases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			m := validTestManifest()
			tc.mutate(m)
			writeManifest(t.Context(), t, store, m)
			if err := manifestErr(t, store, ManifestRef{ID: "snap-1"}); !errors.Is(err, ErrManifestInvalid) {
				t.Errorf("expected ErrManifestInvalid, got: %v", err)
			}
		})
	}

	t.Run("nil metadata", func(t *testing.T) {
		store := NewMemory()
		data, err := json.Marshal(validTestManifest())
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.Replace(data, []byte(`"metadata":{}`), []byte(`"metadata":null`), 1)
		if err := store.Put(t.Context(), "datasets/test-ds/snapshots/snap-1/manifest.json", bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if err := manifestErr(t, store, ManifestRef{ID: "snap-1"}); !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing dataset ID", func(t *testing.T) {
		store := NewMemory()
		m := validTestManifest()
		m.DatasetID = ""
		data, _ := json.Marshal(m)
		_ = store.Put(t.Context(), "datasets/test-ds/snapshots/snap-1/manifest.json", bytes.NewReader(data))
		if err := manifestErr(t, store, ManifestRef{ID: "snap-1"}); !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing snapshot ID", func(t *testing.T) {
		store := NewMemory()
		m := validTestManifest()
		m.SnapshotID = ""
		writeManifestAt(t, store, "datasets/test-ds/snapshots/snap-1/manifest.json", m)
		valid := validTestManifest()
		valid.SnapshotID = "snap-0"
		writeManifest(t.Context(), t, store, valid)
		if err := manifestErr(t, store, ManifestRef{ID: "snap-1"}); !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})
}

func TestView_ListManifests(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, part string }{
		{"invalid manifest with partition filter returns error", "some-partition"},
		{"invalid manifest without partition filter returns error", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			writeManifest(t.Context(), t, store, &Manifest{DatasetID: "test-ds", SnapshotID: "snap-1"})
			reader, err := newTestReader(Use(store))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reader.ListManifests(t.Context(), "test-ds", tc.part, ManifestListOptions{}); !errors.Is(err, ErrManifestInvalid) {
				t.Errorf("expected ErrManifestInvalid, got: %v", err)
			}
		})
	}

	t.Run("hive layout skips canonical manifest", func(t *testing.T) {
		store := NewMemory()

		ds, err := Open(Use(store)).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
		if err != nil {
			t.Fatal(err)
		}

		records := R(
			D{"id": 1, "day": "2024-01-15"},
			D{"id": 2, "day": "2024-01-16"},
		)
		if _, err := ds.Write(t.Context(), records, nil); err != nil {
			t.Fatal(err)
		}

		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}

		refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
		if err != nil {
			t.Fatal(err)
		}

		for _, ref := range refs {
			if ref.Partition == (PartitionPath{}) {
				t.Errorf("ManifestRef %s has empty Partition; canonical manifest should be excluded from listing", ref.ID)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// G3: ListDatasets discovery tests
// -----------------------------------------------------------------------------

func TestView_ListDatasets(t *testing.T) {
	t.Parallel()
	assertNoDatasets := func(t *testing.T, store RangeStore) {
		t.Helper()
		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}
		datasets, err := reader.ListDatasets(t.Context(), DatasetListOptions{})
		if err != nil {
			t.Fatalf("ListDatasets = %v, want nil", err)
		}
		if datasets == nil || len(datasets) != 0 {
			t.Errorf("expected non-nil empty slice, got: %#v", datasets)
		}
	}

	t.Run("uncommitted data without manifests returns empty list", func(t *testing.T) {
		store := NewMemory()
		if err := store.Put(t.Context(), "datasets/test-ds/snapshots/snap-1/data/file.txt", bytes.NewReader([]byte("data"))); err != nil {
			t.Fatal(err)
		}
		assertNoDatasets(t, store)
	})

	t.Run("empty storage", func(t *testing.T) {
		assertNoDatasets(t, NewMemory())
	})

	t.Run("with valid manifest", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()
		writeManifest(ctx, t, store, validTestManifest())

		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}

		datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(datasets) != 1 || datasets[0] != "test-ds" {
			t.Errorf("expected [test-ds], got: %v", datasets)
		}
	})

	t.Run("flat layout returns err datasets not modeled", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		reader, err := newTestReader(Use(store), WithFlatPaths())
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.ListDatasets(ctx, DatasetListOptions{})
		if !errors.Is(err, ErrDatasetsNotModeled) {
			t.Errorf("expected ErrDatasetsNotModeled, got: %v", err)
		}
	})

	t.Run("FS store empty storage returns empty list", func(t *testing.T) {
		ctx := t.Context()

		tmpDir, err := os.MkdirTemp("", "lode-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		reader, err := newTestReader(FS(tmpDir))
		if err != nil {
			t.Fatal(err)
		}

		datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
		if err != nil {
			t.Fatalf("expected no error for empty FS storage, got: %v", err)
		}
		if datasets == nil || len(datasets) != 0 {
			t.Errorf("expected non-nil empty list, got: %#v", datasets)
		}
	})
}

// prefixRangeObjectStore exposes ObjectStore, RangeReader, and PrefixLister
// without ConditionalWriter, so SingleProcess returns singleProcessFullStore.
type prefixRangeObjectStore struct {
	ObjectStore
	RangeReader
	PrefixLister
}

// prefixOnlyObjectStore exposes ObjectStore and PrefixLister without
// RangeReader or ConditionalWriter, so SingleProcess returns
// singleProcessPrefixStore and OpenReadOnly returns readOnlyPrefixStore.
type prefixOnlyObjectStore struct {
	ObjectStore
	PrefixLister
}

func assertDatasetList(t *testing.T, root *Root, ro *ReadRoot, opts DatasetListOptions, want []DatasetID) {
	t.Helper()
	gotW, err := root.ListDatasets(t.Context(), opts)
	if err != nil {
		t.Fatalf("Root.ListDatasets: %v", err)
	}
	gotR, err := ro.ListDatasets(t.Context(), opts)
	if err != nil {
		t.Fatalf("ReadRoot.ListDatasets: %v", err)
	}
	if gotW == nil || gotR == nil {
		t.Fatalf("ListDatasets returned nil slice: write=%#v read=%#v", gotW, gotR)
	}
	if !slices.Equal(gotW, want) || !slices.Equal(gotR, want) {
		t.Fatalf("ListDatasets = write:%v read:%v, want %v", gotW, gotR, want)
	}
}

func verifyListDatasetsStore(t *testing.T, store Store) {
	t.Helper()
	ctx := t.Context()
	root := Open(Use(store))
	ro := OpenReadOnly(Use(store))

	assertDatasetList(t, root, ro, DatasetListOptions{}, []DatasetID{})

	if err := store.Put(ctx, "datasets/uncommitted/snapshots/s1/data/data", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "datasets/orphan/latest", strings.NewReader("1-0123456789abcdef")); err != nil {
		t.Fatal(err)
	}
	if _, err := recordsOn(t, store, "ds-alpha").Write(ctx, R(D{"n": 1}), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := recordsOn(t, store, "ds-beta", hive("day")).Write(ctx, R(D{"day": "2024-01-01"}), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := recordsOn(t, store, "ds-legacy").Write(ctx, R(D{"n": 2}), nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "datasets/ds-legacy/latest"); err != nil {
		t.Fatal(err)
	}

	assertDatasetList(t, root, ro, DatasetListOptions{}, []DatasetID{"ds-alpha", "ds-beta", "ds-legacy"})

	if err := root.RemoveDataset(ctx, "ds-beta"); err != nil {
		t.Fatalf("RemoveDataset: %v", err)
	}
	assertDatasetList(t, root, ro, DatasetListOptions{}, []DatasetID{"ds-alpha", "ds-legacy"})
	assertDatasetList(t, root, ro, DatasetListOptions{StartAfter: "ds-alpha", Limit: 1}, []DatasetID{"ds-legacy"})
}

func TestListDatasets_PrefixListerAndFallback(t *testing.T) {
	t.Parallel()
	t.Run("memory", func(t *testing.T) {
		verifyListDatasetsStore(t, NewMemory())
	})
	t.Run("fs", func(t *testing.T) {
		fs, err := NewFS(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		verifyListDatasetsStore(t, fs)
	})
	t.Run("prefixed memory", func(t *testing.T) {
		s, err := Prefixed(Memory(), "env/prod")()
		if err != nil {
			t.Fatal(err)
		}
		verifyListDatasetsStore(t, s)
	})
	t.Run("prefixed fs", func(t *testing.T) {
		s, err := Prefixed(FS(t.TempDir()), "env/prod")()
		if err != nil {
			t.Fatal(err)
		}
		verifyListDatasetsStore(t, s)
	})
	t.Run("single-process full", func(t *testing.T) {
		mem := NewMemory()
		verifyListDatasetsStore(t, SingleProcess(prefixRangeObjectStore{mem, mem, mem.(PrefixLister)}))
	})
	t.Run("single-process prefix-only", func(t *testing.T) {
		mem := NewMemory()
		raw := prefixOnlyObjectStore{mem, mem.(PrefixLister)}
		verifyListDatasetsStore(t, SingleProcess(raw))
		got, err := OpenReadOnly(Use[ObjectStore](raw)).ListDatasets(t.Context(), DatasetListOptions{})
		if err != nil || !slices.Equal(got, []DatasetID{"ds-alpha", "ds-legacy"}) {
			t.Fatalf("ReadRoot over prefix-only ObjectStore = %v, %v; want [ds-alpha ds-legacy]", got, err)
		}
	})
	t.Run("fallback without PrefixLister", func(t *testing.T) {
		verifyListDatasetsStore(t, testutil.NewFaultStore(NewMemory()))
	})
}

// -----------------------------------------------------------------------------
// G5: Complexity verification tests
// -----------------------------------------------------------------------------

// TestView_ListPartitions_SingleManifestLoad verifies that ListPartitions
// derives Hive partition paths directly from listed partition-manifest keys
// (0 Gets when indexed), and loads canonical manifests only for unindexed
// snapshots (1 Get per unindexed snapshot).
func TestView_ListPartitions_SingleManifestLoad(t *testing.T) {
	t.Parallel()
	inner := NewMemory()
	fs := testutil.NewFaultStore(inner)
	factory := Use(fs)

	ds, err := Open(factory).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ds.Write(t.Context(), R(D{"id": 1, "day": "2024-01-15"}), nil); err != nil {
		t.Fatal(err)
	}
	snap2, err := ds.Write(t.Context(), R(D{"id": 2, "day": "2024-01-16"}), nil)
	if err != nil {
		t.Fatal(err)
	}

	view, err := OpenReadOnly(factory).Records(t.Context(), "test-ds", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}

	fs.Reset()
	partitions, err := view.ListPartitions(t.Context(), PartitionListOptions{})
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	want := []string{"day=2024-01-15", "day=2024-01-16"}
	if !slices.Equal(refPaths(partitions), want) {
		t.Fatalf("ListPartitions = %v, want %v", refPaths(partitions), want)
	}
	if gets := manifestGets(fs); gets != 0 {
		t.Errorf("indexed ListPartitions issued %d manifest Gets, want 0", gets)
	}

	// Delete the second snapshot's partition index copy; ListPartitions falls
	// back to loading only that one snapshot's canonical manifest.
	if err := inner.Delete(t.Context(), "datasets/test-ds/partitions/day=2024-01-16/segments/"+string(snap2.ID)+"/manifest.json"); err != nil {
		t.Fatal(err)
	}
	fs.Reset()
	partitions, err = view.ListPartitions(t.Context(), PartitionListOptions{})
	if err != nil {
		t.Fatalf("fallback ListPartitions failed: %v", err)
	}
	if !slices.Equal(refPaths(partitions), want) {
		t.Fatalf("fallback ListPartitions = %v, want %v", refPaths(partitions), want)
	}
	if got := manifestGets(fs); got != 1 {
		t.Errorf("fallback ListPartitions issued %d manifest Gets, want 1", got)
	}
}

// -----------------------------------------------------------------------------
// Path & Foreign-Object Validation
// -----------------------------------------------------------------------------

func TestValidateManifest_PathsNameEveryDirectory(t *testing.T) {
	t.Parallel()
	complete := defaultLegacyPaths(false)
	flat := defaultLegacyPaths(true)
	flatWithDatasets := flat
	flatWithDatasets.Datasets = "datasets"
	flatWithSnapshots := flat
	flatWithSnapshots.Snapshots = "snapshots"

	cases := map[string]struct {
		manifest *Manifest
		valid    bool
	}{
		"1.2.0 default":                 {&Manifest{FormatVersion: "1.2.0"}, true},
		"1.2.0 flat":                    {&Manifest{FormatVersion: "1.2.0", Flat: true}, true},
		"1.1.0 default paths":           {&Manifest{FormatVersion: "1.1.0", legacyPaths: &complete}, true},
		"1.1.0 flat paths":              {&Manifest{FormatVersion: "1.1.0", legacyPaths: &flat, Flat: true}, true},
		"1.1.0 missing paths":           {&Manifest{FormatVersion: "1.1.0"}, false},
		"1.1.0 empty paths":             {&Manifest{FormatVersion: "1.1.0", legacyPaths: &legacyManifestPaths{}}, false},
		"1.1.0 flat with datasets dir":  {&Manifest{FormatVersion: "1.1.0", legacyPaths: &flatWithDatasets, Flat: true}, false},
		"1.1.0 flat with snapshots dir": {&Manifest{FormatVersion: "1.1.0", legacyPaths: &flatWithSnapshots, Flat: true}, false},
	}
	for name, c := range cases {
		err := checkLegacyPaths(c.manifest)
		if c.valid != (err == nil) {
			t.Errorf("%s: got %v, want valid=%v", name, err, c.valid)
		}
	}
}

func TestRead_RejectsMisplacedManifestAndForeignDataPaths(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := NewMemory()
	root := Open(Use(store))

	dsB, err := root.Records(ctx, "secret-b", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}
	snapB, err := dsB.Write(ctx, R(D{"secret": "classified"}), nil)
	if err != nil {
		t.Fatal(err)
	}

	dsA, err := root.Records(ctx, "public-a", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}
	olderA, err := dsA.Write(ctx, R(D{"v": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dsA.Write(ctx, R(D{"v": 2}), nil); err != nil {
		t.Fatal(err)
	}

	// Tamper olderA's manifest to point its object at secret-b's object.
	tampered := *olderA.Manifest
	tampered.Objects = []ObjectRef{{Path: snapB.Objects[0].Path, SizeBytes: snapB.Objects[0].SizeBytes}}
	writeManifestAt(t, store, "datasets/public-a/snapshots/"+string(olderA.ID)+"/manifest.json", &tampered)

	if _, err := dsA.Read(ctx, olderA.ID); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("Records.Read(foreign path) = %v, want ErrManifestInvalid", err)
	}

	// Tamper olderA's DatasetID and verify Snapshot and Snapshots reject it.
	wrongDS := *olderA.Manifest
	wrongDS.DatasetID = "secret-b"
	writeManifestAt(t, store, "datasets/public-a/snapshots/"+string(olderA.ID)+"/manifest.json", &wrongDS)

	if _, err := dsA.Snapshot(ctx, olderA.ID); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("Snapshot(wrong DatasetID) = %v, want ErrManifestInvalid", err)
	}
	if _, err := dsA.Snapshots(ctx); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("Snapshots(wrong DatasetID) = %v, want ErrManifestInvalid", err)
	}

	// Typed-nil map and struct pointer metadata coalesce to {} like untyped nil.
	var nilMap D
	var nilStruct *struct{ Tag string }
	s1, err := dsB.Write(ctx, R(D{"x": 1}), nilMap)
	if err != nil || string(s1.Metadata.JSON()) != "{}" {
		t.Errorf("Write(typed-nil map) = %v, %s; want nil, {}", err, s1.Metadata.JSON())
	}
	s2, err := dsB.Write(ctx, R(D{"x": 2}), nilStruct)
	if err != nil || string(s2.Metadata.JSON()) != "{}" {
		t.Errorf("Write(typed-nil struct ptr) = %v, %s; want nil, {}", err, s2.Metadata.JSON())
	}
}
