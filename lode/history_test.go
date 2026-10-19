package lode

import (
	"context"
	"errors"
	"io"
	"testing"
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

func datasetHistory(store Store) history[DatasetSnapshotID, Manifest] {
	return newHistory(store, datasetKind{id: "ds", layout: NewDefaultLayout()})
}

// Refresh after a conflict must tolerate an empty history (nothing to
// re-parent on) but surface a store that cannot be read.
func TestHistory_Refresh_EmptyIsNotAnError(t *testing.T) {
	h := datasetHistory(NewMemory())
	if err := h.Refresh(t.Context()); err != nil {
		t.Fatalf("Refresh on empty history: %v", err)
	}
}

func TestHistory_Refresh_StoreErrorSurfaces(t *testing.T) {
	boom := errors.New("store down")
	h := datasetHistory(failingStore{Store: NewMemory(), err: boom})
	err := h.Refresh(t.Context())
	if !errors.Is(err, boom) {
		t.Fatalf("Refresh: got %v, want the store error", err)
	}
}
