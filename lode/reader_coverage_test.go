package lode

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// -----------------------------------------------------------------------------
// View.OpenObject and ReaderAt coverage
// -----------------------------------------------------------------------------

// blobView writes payload as one blob of dataset "test-ds" over store and
// opens a view of it, returning the view and the blob's object reference,
// discovered through the view's listing.
func blobViewOf(t *testing.T, store Store, payload []byte) (BlobView, ObjectRef) {
	t.Helper()
	snap, err := blobsOn(t, store, "test-ds").Write(t.Context(), payload, nil)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	view, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "test-ds")
	if err != nil {
		t.Fatalf("Blobs view: %v", err)
	}
	refs, err := view.ListManifests(t.Context(), "", ManifestListOptions{})
	if err != nil {
		t.Fatalf("ListManifests: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 manifest ref, got %d", len(refs))
	}
	manifest, err := view.GetManifest(t.Context(), refs[0])
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if len(manifest.Files) != 1 {
		t.Fatalf("expected 1 file ref, got %d", len(manifest.Files))
	}
	return view, ObjectRef{Manifest: ManifestRef{ID: snap.ID}, Path: manifest.Files[0].Path}
}

func TestView_Objects(t *testing.T) {
	t.Run("open object", func(t *testing.T) {
		payload := []byte("hello-open-object")
		view, obj := blobViewOf(t, NewMemory(), payload)

		rc, err := view.OpenObject(t.Context(), obj)
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
		view, _ := blobViewOf(t, NewMemory(), []byte("x"))
		obj := ObjectRef{Manifest: ManifestRef{ID: "no-such-snap"}, Path: "no/such/path"}
		if _, err := view.OpenObject(t.Context(), obj); !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("reader at", func(t *testing.T) {
		view, obj := blobViewOf(t, NewMemory(), []byte("abcdefghijklmnop"))

		ra, err := view.ReaderAt(t.Context(), obj)
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
