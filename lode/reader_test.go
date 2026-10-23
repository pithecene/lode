package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// testReader reads a root's datasets as v0's DatasetReader did, by dataset
// ID: each call opens a view of the dataset, whose layout comes from its
// head manifest. The listing methods do not depend on a codec, so the view
// is opened without one; ReadRoot.Records and ReadRoot.Blobs open views the
// same way and check the codec besides.
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
	head, scheme, err := resolveViewHead(ctx, r.root.store, r.root.names, id)
	if err != nil {
		return nil, err
	}
	if err := validateManifest(head); err != nil {
		return nil, err
	}
	return newDataset(id, r.root.store, newPathLayout(r.root.names, scheme)), nil
}

func (r *testReader) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	return r.root.ListDatasets(ctx, opts)
}

func (r *testReader) ListPartitions(ctx context.Context, id DatasetID, opts PartitionListOptions) ([]PartitionRef, error) {
	v, err := r.view(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.ListPartitions(ctx, opts)
}

func (r *testReader) ListManifests(ctx context.Context, id DatasetID, partition string, opts ManifestListOptions) ([]ManifestRef, error) {
	v, err := r.view(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.ListManifests(ctx, partition, opts)
}

func (r *testReader) GetManifest(ctx context.Context, id DatasetID, ref ManifestRef) (*Manifest, error) {
	v, err := r.view(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.GetManifest(ctx, ref)
}

func (r *testReader) OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error) {
	if r.root.err != nil {
		return nil, r.root.err
	}
	return r.root.store.Get(ctx, obj.Path)
}

func (r *testReader) ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error) {
	if r.root.err != nil {
		return nil, r.root.err
	}
	return r.root.store.ReaderAt(ctx, obj.Path)
}

// manifestErr opens a view of dataset test-ds over store and loads ref's
// manifest, returning the first error: a view validates its head's manifest
// when it opens, and GetManifest validates every manifest it loads. The
// manifests these tests write record no codec, so the view reads blobs.
func manifestErr(t *testing.T, store Store, ref ManifestRef) error {
	t.Helper()
	view, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds")
	if err != nil {
		return err
	}
	_, err = view.GetManifest(t.Context(), ref)
	return err
}

// -----------------------------------------------------------------------------
// G2: Manifest validation tests
// -----------------------------------------------------------------------------

func TestView_GetManifest_InvalidManifest(t *testing.T) {
	t.Run("missing schema name", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			// SchemaName missing
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing format version", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName: "lode-manifest",
			// FormatVersion missing
			DatasetID:   "test-ds",
			SnapshotID:  "snap-1",
			CreatedAt:   time.Now().UTC(),
			Metadata:    emptyMetadata(),
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("nil metadata", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		// Metadata always encodes as an object, so the absent field is
		// written by hand: a manifest whose metadata is null is invalid.
		data, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.Replace(data, []byte(`"metadata":{}`), []byte(`"metadata":null`), 1)
		if err := store.Put(ctx, "datasets/test-ds/snapshots/snap-1/manifest.json", bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}

		err = manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing dataset ID", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			// DatasetID missing
			SnapshotID:  "snap-1",
			CreatedAt:   time.Now().UTC(),
			Metadata:    emptyMetadata(),
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		// Manually write with a path that includes dataset ID
		data, _ := json.Marshal(manifest)
		_ = store.Put(ctx, "datasets/test-ds/snapshots/snap-1/manifest.json", bytes.NewReader(data))

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing snapshot ID", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			// SnapshotID missing
			CreatedAt:   time.Now().UTC(),
			Metadata:    emptyMetadata(),
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)
		// A valid sibling is the head, so the view opens; the manifest
		// without an ID is reached by its empty reference.
		valid := validTestManifest()
		valid.SnapshotID = "snap-0"
		writeManifest(ctx, t, store, valid)

		// Use empty ManifestRef since manifest has no ID
		err := manifestErr(t, store, ManifestRef{ID: ""})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("zero created at", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			// CreatedAt zero value (not set)
			Metadata:    emptyMetadata(),
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("nil files", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         nil, // nil not allowed (use empty slice)
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("negative row count", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         []FileRef{},
			RowCount:      -1, // negative not allowed
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing compressor", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         []FileRef{},
			RowCount:      0,
			// Compressor missing
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing partitioner", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			// Partitioner missing
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("empty file path", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files: []FileRef{
				{Path: "", SizeBytes: 100}, // empty path not allowed
			},
			RowCount:    1,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("negative file size", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files: []FileRef{
				{Path: "data/file.bin", SizeBytes: -100}, // negative size not allowed
			},
			RowCount:    1,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		err := manifestErr(t, store, ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})
}

func TestView_ListManifests(t *testing.T) {
	t.Run("invalid manifest with partition filter returns error", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		// Write an invalid manifest (missing required fields)
		manifest := &Manifest{
			// SchemaName missing - invalid
			DatasetID:  "test-ds",
			SnapshotID: "snap-1",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.ListManifests(ctx, "test-ds", "some-partition", ManifestListOptions{})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("invalid manifest without partition filter returns error", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		// Write an invalid manifest (missing required fields)
		manifest := &Manifest{
			// SchemaName missing - invalid
			DatasetID:  "test-ds",
			SnapshotID: "snap-1",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}

		// Per CONTRACT_READ_API.md, invalid manifests must return an error
		// even when no partition filter is specified
		_, err = reader.ListManifests(ctx, "test-ds", "", ManifestListOptions{})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})

	// TestView_ListManifests_HiveLayout_SkipsCanonicalManifest verifies
	// that ListManifests excludes the canonical manifest written for O(1) Snapshot
	// lookups. Without this, ManifestRef.Partition would be nondeterministic
	// (empty vs populated) depending on store listing order.
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

		// Every ref must have a non-empty Partition (canonical manifest excluded).
		for _, ref := range refs {
			if ref.Partition == "" {
				t.Errorf("ManifestRef %s has empty Partition; canonical manifest should be excluded from listing", ref.ID)
			}
		}
	})
}

// -----------------------------------------------------------------------------
// G3: ErrNoManifests test
// -----------------------------------------------------------------------------

func TestView_ListDatasets(t *testing.T) {
	t.Run("err no manifests", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		// Write data files but no manifest
		err := store.Put(ctx, "datasets/test-ds/snapshots/snap-1/data/file.txt", bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatal(err)
		}

		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.ListDatasets(ctx, DatasetListOptions{})
		if !errors.Is(err, ErrNoManifests) {
			t.Errorf("expected ErrNoManifests, got: %v", err)
		}
	})

	t.Run("empty storage", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		reader, err := newTestReader(Use(store))
		if err != nil {
			t.Fatal(err)
		}

		datasets, err := reader.ListDatasets(ctx, DatasetListOptions{})
		if err != nil {
			t.Fatalf("expected no error for empty storage, got: %v", err)
		}
		if len(datasets) != 0 {
			t.Errorf("expected empty list, got: %v", datasets)
		}
	})

	t.Run("with valid manifest", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			SchemaName:    "lode-manifest",
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      emptyMetadata(),
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

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
		if len(datasets) != 0 {
			t.Errorf("expected empty list, got: %v", datasets)
		}
	})
}

// -----------------------------------------------------------------------------
// G4: Layout-specific tests
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// G5: Complexity verification tests
// -----------------------------------------------------------------------------

// TestView_ListPartitions_SingleManifestLoad verifies that ListPartitions
// loads each manifest exactly once (M Gets), not twice (2M Gets via ListManifests + GetManifest).
func TestView_ListPartitions_SingleManifestLoad(t *testing.T) {
	inner := NewMemory()
	fs := testutil.NewFaultStore(inner)
	factory := Use(fs)

	// Write two snapshots with hive-partitioned data.
	ds, err := Open(factory).Records(t.Context(), "test-ds", JSONL[D](), hive("day"))
	if err != nil {
		t.Fatal(err)
	}

	records1 := R(D{"id": 1, "day": "2024-01-15"})
	if _, err := ds.Write(t.Context(), records1, nil); err != nil {
		t.Fatal(err)
	}

	records2 := R(D{"id": 2, "day": "2024-01-16"})
	if _, err := ds.Write(t.Context(), records2, nil); err != nil {
		t.Fatal(err)
	}

	// Opening the view reads the head's manifest; the count covers the
	// listing alone.
	view, err := OpenReadOnly(factory).Records(t.Context(), "test-ds", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}

	// Reset call tracking before the operation under test.
	fs.Reset()

	partitions, err := view.ListPartitions(t.Context(), PartitionListOptions{})
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	if len(partitions) == 0 {
		t.Fatal("expected at least one partition")
	}

	// Count manifest Get calls (paths ending in manifest.json).
	getCalls := fs.GetCalls()
	manifestGets := 0
	for _, p := range getCalls {
		if strings.HasSuffix(p, "manifest.json") {
			manifestGets++
		}
	}

	// With single-pass, we expect exactly 2 Gets (one per snapshot manifest),
	// not 4 (which the old ListManifests + GetManifest path would produce).
	// There are 2 snapshots × 2 partition manifests each = 4 partition manifests,
	// but deduplicated by snapshot ID = 2 manifests loaded.
	if manifestGets != 2 {
		t.Errorf("expected exactly 2 manifest Gets (single-pass), got %d", manifestGets)
	}
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

func TestValidateManifest_PathsNameEveryDirectory(t *testing.T) {
	complete := defaultPaths(false)
	flat := defaultPaths(true)
	incomplete := complete
	incomplete.Staged = ""
	flatWithSnapshots := flat
	flatWithSnapshots.Snapshots = "snapshots"
	withEquals := complete
	withEquals.Data = "k=v"
	duplicate := complete
	duplicate.Data = complete.Expired
	pointerName := complete
	pointerName.Staged = "latest"
	cases := map[string]struct {
		paths ManifestPaths
		valid bool
	}{
		"default names":                   {complete, true},
		"flat names":                      {flat, true},
		"a missing name":                  {incomplete, false},
		"flat with a snapshots directory": {flatWithSnapshots, false},
		"only a manifest name":            {ManifestPaths{Manifest: "manifest.json"}, false},
		"a name with =":                   {withEquals, false},
		"a repeated name":                 {duplicate, false},
		"the pointer's name":              {pointerName, false},
	}
	for name, c := range cases {
		err := missingPaths(&Manifest{FormatVersion: manifestFormatVersion, Paths: c.paths})
		if c.valid != (err == nil) {
			t.Errorf("%s: got %v, want valid=%v", name, err, c.valid)
		}
	}
}
