package lode

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
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
	refs, err := view.ListManifests(t.Context(), PartitionPath{}, ManifestListOptions{})
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
		view, obj := blobViewOf(t, NewMemory(), []byte("x"))
		missing := idAt(time.Now().Add(time.Hour))
		obj = ObjectRef{Manifest: ManifestRef{ID: missing}, Path: strings.Replace(obj.Path, string(obj.Manifest.ID), string(missing), 1)}
		if _, err := view.OpenObject(t.Context(), obj); !errors.Is(err, ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("objects outside the dataset are refused", func(t *testing.T) {
		store := NewMemory()
		other, err := blobsOn(t, store, "other").Write(t.Context(), []byte("secret"), nil)
		if err != nil {
			t.Fatal(err)
		}
		view, obj := blobViewOf(t, store, []byte("x"))
		refs := map[string]ObjectRef{
			"another dataset":        {Path: other.Files[0].Path},
			"a manifest":             {Path: strings.Replace(obj.Path, "data/", "", 1)},
			"another snapshot's ref": {Manifest: ManifestRef{ID: other.ID}, Path: obj.Path},
			"an arbitrary key":       {Path: "no/such/path"},
		}
		for name, ref := range refs {
			if _, err := view.OpenObject(t.Context(), ref); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("OpenObject %s: got %v, want ErrInvalidArgument", name, err)
			}
			if _, err := view.ReaderAt(t.Context(), ref); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("ReaderAt %s: got %v, want ErrInvalidArgument", name, err)
			}
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

// noRanges hides a store's range reads, as a backend without them would.
type noRanges struct{ Store }

// objectsOnly hides a store's CompareAndSwap and range reads.
type objectsOnly struct{ ObjectStore }

func TestRangeReader_IsOptional(t *testing.T) {
	t.Run("ReaderAt without range reads", func(t *testing.T) {
		store := noRanges{NewMemory()}
		view, obj := blobViewOf(t, store, []byte("abc"))
		if _, err := view.ReaderAt(t.Context(), obj); !errors.Is(err, ErrRangeReadNotSupported) {
			t.Errorf("view ReaderAt = %v, want ErrRangeReadNotSupported", err)
		}
		if _, err := blobsOn(t, store, "test-ds").ReaderAt(t.Context(), obj); !errors.Is(err, ErrRangeReadNotSupported) {
			t.Errorf("writer ReaderAt = %v, want ErrRangeReadNotSupported", err)
		}
	})

	t.Run("wrappers read ranges only when the store does", func(t *testing.T) {
		stores := map[string]struct {
			store ObjectStore
			want  bool
		}{
			"SingleProcess with range reads":    {SingleProcess(testutil.WithoutCAS(NewMemory())), true},
			"SingleProcess without range reads": {SingleProcess(objectsOnly{NewMemory()}), false},
			"read-only with range reads":        {newReadOnlyStore(NewMemory()), true},
			"read-only without range reads":     {newReadOnlyStore(noRanges{NewMemory()}), false},
		}
		for name, c := range stores {
			if _, got := c.store.(RangeReader); got != c.want {
				t.Errorf("%s: implements RangeReader = %v, want %v", name, got, c.want)
			}
		}
	})
}
