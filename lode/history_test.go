package lode

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/history"
)

// Unit tests for history where the Dataset/Volume surfaces do not reach a
// branch directly.
//
// Known equivalent mutants, left alive: scan's `entries[i].id > latest.id`
// → `>=` (snapshot IDs are unique within a history, so the greatest is found
// either way); manifests' `p < cur` → `<=` or `>=` (two manifest copies of
// one snapshot are never the same path and hold the same bytes, so whichever
// copy is kept loads the same manifest).

// failingStore fails every read so that history's error paths are reachable
// without racing another writer.
type failingStore struct {
	Store
	err error
}

func (f failingStore) Get(context.Context, string) (io.ReadCloser, error) { return nil, f.err }
func (f failingStore) List(context.Context, string) ([]string, error)     { return nil, f.err }

// defaultLayout is the unpartitioned layout under the default names.
func defaultLayout() *pathLayout { return newPathLayout(defaultPaths(false), noopScheme) }

func datasetHistory(store Store) history.History[DatasetSnapshotID, Manifest] {
	return history.New(store, newDatasetKind("ds", defaultLayout(), store), defaultLayout().latestPointerPath("ds"), "manifest", historyErrors)
}

func TestHistory_Refresh(t *testing.T) {
	// Refresh after a conflict must tolerate an empty history (nothing to
	// re-parent on) but surface a store that cannot be read.
	t.Run("empty is not an error", func(t *testing.T) {
		h := datasetHistory(NewMemory())
		if err := h.Refresh(t.Context()); err != nil {
			t.Fatalf("Refresh on empty history: %v", err)
		}
	})

	t.Run("store error surfaces", func(t *testing.T) {
		boom := errors.New("store down")
		h := datasetHistory(failingStore{Store: NewMemory(), err: boom})
		err := h.Refresh(t.Context())
		if !errors.Is(err, boom) {
			t.Fatalf("Refresh: got %v, want the store error", err)
		}
	})
}

// manifestPutFailStore fails every Put of a manifest so Advance's wording can
// be observed for each kind.
type manifestPutFailStore struct {
	Store
	err error
}

func (f manifestPutFailStore) Put(ctx context.Context, path string, r io.Reader) error {
	if strings.HasSuffix(path, "manifest.json") {
		return f.err
	}
	return f.Store.Put(ctx, path, r)
}

// The shared protocol must keep each kind's wording: a Volume manifest
// failure says "volume manifest", a Dataset one says "manifest".
func TestHistory_Advance_ErrorNamesTheKind(t *testing.T) {
	boom := errors.New("disk full")
	cases := []struct {
		name, noun, want string
	}{
		{"dataset", "manifest", "lode: failed to write manifest: disk full"},
		{"volume", "volume manifest", "lode: failed to write volume manifest: disk full"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := manifestPutFailStore{Store: NewMemory(), err: boom}
			h := history.New(store, newDatasetKind("ds", defaultLayout(), store), defaultLayout().latestPointerPath("ds"), c.noun, historyErrors)
			err := h.Advance(t.Context(), "", "s1", &Manifest{}, []string{"datasets/ds/snapshots/s1/manifest.json"})
			if err == nil || err.Error() != c.want {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}
}
