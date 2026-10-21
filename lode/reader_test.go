package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// G2: Manifest validation tests
// -----------------------------------------------------------------------------

func TestDatasetReader_GetManifest_InvalidManifest(t *testing.T) {
	t.Run("missing schema name", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		manifest := &Manifest{
			// SchemaName missing
			FormatVersion: "1.0.0",
			DatasetID:     "test-ds",
			SnapshotID:    "snap-1",
			CreatedAt:     time.Now().UTC(),
			Metadata:      Metadata{},
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:    Metadata{},
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      nil, // nil not allowed
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:    Metadata{},
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		// Manually write with a path that includes dataset ID
		data, _ := json.Marshal(manifest)
		_ = store.Put(ctx, "datasets/test-ds/snapshots/snap-1/manifest.json", bytes.NewReader(data))

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:    Metadata{},
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		// Use empty ManifestRef since manifest has no ID
		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: ""})
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
			Metadata:    Metadata{},
			Files:       []FileRef{},
			RowCount:    0,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      Metadata{},
			Files:         nil, // nil not allowed (use empty slice)
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      Metadata{},
			Files:         []FileRef{},
			RowCount:      -1, // negative not allowed
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      Metadata{},
			Files:         []FileRef{},
			RowCount:      0,
			// Compressor missing
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      Metadata{},
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			// Partitioner missing
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      Metadata{},
			Files: []FileRef{
				{Path: "", SizeBytes: 100}, // empty path not allowed
			},
			RowCount:    1,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
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
			Metadata:      Metadata{},
			Files: []FileRef{
				{Path: "data/file.bin", SizeBytes: -100}, // negative size not allowed
			},
			RowCount:    1,
			Compressor:  "noop",
			Partitioner: "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatal(err)
		}

		_, err = reader.GetManifest(ctx, "test-ds", ManifestRef{ID: "snap-1"})
		if !errors.Is(err, ErrManifestInvalid) {
			t.Errorf("expected ErrManifestInvalid, got: %v", err)
		}
	})
}

func TestDatasetReader_ListManifests(t *testing.T) {
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

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
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

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
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

	// TestDatasetReader_ListManifests_HiveLayout_SkipsCanonicalManifest verifies
	// that ListManifests excludes the canonical manifest written for O(1) Snapshot
	// lookups. Without this, ManifestRef.Partition would be nondeterministic
	// (empty vs populated) depending on store listing order.
	t.Run("hive layout skips canonical manifest", func(t *testing.T) {
		store := NewMemory()

		ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store),
			WithCodec(NewJSONLCodec()),
			WithHiveLayout("day"))
		if err != nil {
			t.Fatal(err)
		}

		records := R(
			D{"id": 1, "day": "2024-01-15"},
			D{"id": 2, "day": "2024-01-16"},
		)
		if _, err := ds.Write(t.Context(), records, Metadata{}); err != nil {
			t.Fatal(err)
		}

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithHiveLayout("day"))
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

func TestDatasetReader_ListDatasets(t *testing.T) {
	t.Run("err no manifests", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()

		// Write data files but no manifest
		err := store.Put(ctx, "datasets/test-ds/snapshots/snap-1/data/file.txt", bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatal(err)
		}

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
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

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
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
			Metadata:      Metadata{},
			Files:         []FileRef{},
			RowCount:      0,
			Compressor:    "noop",
			Partitioner:   "noop",
		}
		writeManifest(ctx, t, store, manifest)

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
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

		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithLayout(NewFlatLayout()))
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

		reader, err := NewDatasetReader(NewFSFactory(tmpDir))
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

// TestDatasetReader_ListPartitions_SingleManifestLoad verifies that ListPartitions
// loads each manifest exactly once (M Gets), not twice (2M Gets via ListManifests + GetManifest).
func TestDatasetReader_ListPartitions_SingleManifestLoad(t *testing.T) {
	inner := NewMemory()
	fs := testutil.NewFaultStore(inner)
	factory := newFaultStoreFactory(fs)

	// Write two snapshots with hive-partitioned data.
	ds, err := NewDataset("test-ds", factory,
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	records1 := R(D{"id": 1, "day": "2024-01-15"})
	if _, err := ds.Write(t.Context(), records1, Metadata{}); err != nil {
		t.Fatal(err)
	}

	records2 := R(D{"id": 2, "day": "2024-01-16"})
	if _, err := ds.Write(t.Context(), records2, Metadata{}); err != nil {
		t.Fatal(err)
	}

	reader, err := NewDatasetReader(factory, WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	// Reset call tracking before the operation under test.
	fs.Reset()

	partitions, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{})
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

// NewMemoryFactoryFrom creates a StoreFactory that returns an existing store.
func NewMemoryFactoryFrom(store Store) StoreFactory {
	return func() (Store, error) {
		return store, nil
	}
}

func writeManifest(ctx context.Context, t *testing.T, store Store, m *Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	path := "datasets/" + string(m.DatasetID) + "/snapshots/" + string(m.SnapshotID) + "/manifest.json"
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}
