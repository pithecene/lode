package lode

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// -----------------------------------------------------------------------------
// DatasetReader.OpenObject and ReaderAt coverage
// -----------------------------------------------------------------------------

func TestDatasetReader_Objects(t *testing.T) {
	t.Run("open object", func(t *testing.T) {
		store := NewMemory()
		payload := []byte("hello-open-object")

		// Write a blob via Dataset
		ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatalf("NewDataset: %v", err)
		}
		snap, err := ds.Write(t.Context(), []any{payload}, Metadata{})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}

		// Create a DatasetReader and discover the manifest
		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatalf("NewDatasetReader: %v", err)
		}

		refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
		if err != nil {
			t.Fatalf("ListManifests: %v", err)
		}
		if len(refs) != 1 {
			t.Fatalf("expected 1 manifest ref, got %d", len(refs))
		}

		manifest, err := reader.GetManifest(t.Context(), "test-ds", refs[0])
		if err != nil {
			t.Fatalf("GetManifest: %v", err)
		}
		if len(manifest.Files) != 1 {
			t.Fatalf("expected 1 file ref, got %d", len(manifest.Files))
		}

		obj := ObjectRef{
			Dataset:  "test-ds",
			Manifest: ManifestRef{ID: snap.ID},
			Path:     manifest.Files[0].Path,
		}

		rc, err := reader.OpenObject(t.Context(), obj)
		if err != nil {
			t.Fatalf("OpenObject: %v", err)
		}
		defer closer(rc)()

		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("content mismatch: got %q, want %q", got, payload)
		}
	})

	t.Run("open object not found", func(t *testing.T) {
		reader, err := NewDatasetReader(NewMemoryFactory())
		if err != nil {
			t.Fatalf("NewDatasetReader: %v", err)
		}

		obj := ObjectRef{
			Dataset:  "no-such-ds",
			Manifest: ManifestRef{ID: "no-such-snap"},
			Path:     "no/such/path",
		}

		_, err = reader.OpenObject(t.Context(), obj)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("reader at", func(t *testing.T) {
		store := NewMemory()
		payload := []byte("abcdefghijklmnop")

		// Write a blob via Dataset
		ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatalf("NewDataset: %v", err)
		}
		snap, err := ds.Write(t.Context(), []any{payload}, Metadata{})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}

		// Create a DatasetReader and discover the file path
		reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
		if err != nil {
			t.Fatalf("NewDatasetReader: %v", err)
		}

		refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
		if err != nil {
			t.Fatalf("ListManifests: %v", err)
		}

		manifest, err := reader.GetManifest(t.Context(), "test-ds", refs[0])
		if err != nil {
			t.Fatalf("GetManifest: %v", err)
		}

		obj := ObjectRef{
			Dataset:  "test-ds",
			Manifest: ManifestRef{ID: snap.ID},
			Path:     manifest.Files[0].Path,
		}

		ra, err := reader.ReaderAt(t.Context(), obj)
		if err != nil {
			t.Fatalf("ReaderAt: %v", err)
		}

		// Read a range from the middle: offset 4, length 4 -> "efgh"
		buf := make([]byte, 4)
		n, err := ra.ReadAt(buf, 4)
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		if n != 4 {
			t.Fatalf("expected 4 bytes, got %d", n)
		}
		if string(buf) != "efgh" {
			t.Errorf("range read mismatch: got %q, want %q", buf, "efgh")
		}
	})
}
