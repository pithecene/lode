package lode

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// layoutFixture is a dataset of one snapshot, written with a layout, over a
// store that records the keys each call reaches.
type layoutFixture struct {
	store *testutil.KeyRecorder
	snap  *DatasetSnapshot
}

func newLayoutFixture(t *testing.T, opts ...DatasetOption) layoutFixture {
	t.Helper()
	store := testutil.NewKeyRecorder(NewMemory())
	snap, err := recordsWith(t, store, "ds", jsonl, opts...).Write(t.Context(), R(D{"k": "a", "j": "b"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	return layoutFixture{store: store, snap: snap}
}

// open opens a view of the fixture's dataset with opts and returns the keys
// that opening reached.
func (f layoutFixture) open(t *testing.T, opts ...ViewOption) (RecordView[D], []string, error) {
	t.Helper()
	var view RecordView[D]
	var err error
	keys := f.store.Recorded(func() {
		view, err = OpenReadOnly(Use(f.store)).Records(t.Context(), "ds", jsonl, opts...)
	})
	return view, keys, err
}

// A view that states its layout opens a dataset written with that layout,
// looks for manifests only in that layout's manifest directory, and reads
// the snapshot.
func TestWithLayout_ViewReadsOnlyItsManifestDirectory(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		written []DatasetOption
		layout  Layout
		other   string // the manifest directory the view must not reach
	}{
		{name: "unpartitioned", layout: Unpartitioned(), other: "/segments/"},
		{name: "hive", written: []DatasetOption{hive("k")}, layout: Hive("k"), other: "/snapshots/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newLayoutFixture(t, c.written...)
			view, keys, err := f.open(t, WithLayout(c.layout))
			if err != nil {
				t.Fatal(err)
			}
			if i := slices.IndexFunc(keys, func(k string) bool { return strings.Contains(k, c.other) }); i >= 0 {
				t.Errorf("opening reached %s, want no read outside the layout's manifest directory", keys[i])
			}
			if len(keys) != 2 {
				t.Errorf("opening reached %v, want the pointer and the head's manifest", keys)
			}
			recs, err := view.Read(t.Context(), f.snap.ID)
			if err != nil || len(recs) != 1 {
				t.Fatalf("Read = %v, %v", recs, err)
			}
		})
	}
}

// A view whose layout differs from the one the opened snapshot records fails
// to open with ErrSettingsMismatch, as a writer's settings check does.
func TestWithLayout_ViewRejectsAnotherLayout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		written []DatasetOption
		layout  Layout
	}{
		{name: "hive keys in another order", written: []DatasetOption{hive("k", "j")}, layout: Hive("j", "k")},
		{name: "other hive keys", written: []DatasetOption{hive("k")}, layout: Hive("j")},
		{name: "unpartitioned view of a hive dataset", written: []DatasetOption{hive("k")}, layout: Unpartitioned()},
		{name: "hive view of an unpartitioned dataset", layout: Hive("k")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newLayoutFixture(t, c.written...)
			if _, _, err := f.open(t, WithLayout(c.layout)); !errors.Is(err, ErrSettingsMismatch) {
				t.Fatalf("got %v, want ErrSettingsMismatch", err)
			}
		})
	}
}

// Against a manifest of format version 1.0.0, which records no partition
// keys, a Hive view verifies its keys against the partition path of the
// snapshot it opens at, as a writer does. A pinned view verifies them
// against the named snapshot.
func TestWithLayout_ViewVerifiesLegacyKeys(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		layout Layout
		pinned bool
		err    error
	}{
		{name: "the dataset's keys", layout: Hive("k", "j")},
		{name: "the keys in another order", layout: Hive("j", "k"), err: ErrSettingsMismatch},
		{name: "a pinned view with the dataset's keys", layout: Hive("k", "j"), pinned: true},
		{name: "a pinned view with other keys", layout: Hive("k"), pinned: true, err: ErrSettingsMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newLayoutFixture(t, hive("k", "j"))
			legacyHive(t, f.store, f.snap)
			opts := []ViewOption{WithLayout(c.layout)}
			if c.pinned {
				opts = append(opts, WithPinnedSnapshot(f.snap.ID))
			}
			_, _, err := f.open(t, opts...)
			if !errors.Is(err, c.err) || (c.err == nil) != (err == nil) {
				t.Fatalf("got %v, want %v", err, c.err)
			}
		})
	}
}

// A view rejects a nil layout and a Hive layout without keys before any
// store call.
func TestWithLayout_ViewRejectsAnInvalidLayout(t *testing.T) {
	t.Parallel()
	for name, layout := range map[string]Layout{"nil": nil, "hive without keys": Hive()} {
		t.Run(name, func(t *testing.T) {
			f := newLayoutFixture(t)
			_, keys, err := f.open(t, WithLayout(layout))
			if !errors.Is(err, ErrInvalidArgument) || len(keys) != 0 {
				t.Fatalf("got %v after %v, want ErrInvalidArgument before any store call", err, keys)
			}
		})
	}
}

// A view that expects a layout and finds no manifest of the head under it
// rejects a head persisted under another layout, rather than scan its own
// layout and open an older snapshot. A head without any manifest belongs to
// a pending commit, and the view opens the newest snapshot as before.
func TestWithLayout_ViewRejectsAHeadUnderAnotherLayout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		move func(t *testing.T, store Store, canonical string) // what becomes of the head's manifest
		err  error
	}{
		{name: "under the segments directory", move: func(t *testing.T, store Store, canonical string) {
			t.Helper()
			moveObject(t, store, canonical, strings.Replace(canonical, "/snapshots/", "/segments/", 1))
			if err := store.Delete(t.Context(), canonical); err != nil {
				t.Fatal(err)
			}
		}, err: ErrSettingsMismatch},
		{name: "nowhere, as for a pending commit", move: func(t *testing.T, store Store, canonical string) {
			t.Helper()
			if err := store.Delete(t.Context(), canonical); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newLayoutFixture(t)
			head, err := recordsWith(t, f.store, "ds", jsonl).Write(t.Context(), R(D{"k": "c"}), nil)
			if err != nil {
				t.Fatal(err)
			}
			c.move(t, f.store, "datasets/ds/snapshots/"+string(head.ID)+"/manifest.json")
			view, _, err := f.open(t, WithLayout(Unpartitioned()))
			if !errors.Is(err, c.err) || (c.err == nil) != (err == nil) {
				t.Fatalf("got %v, want %v", err, c.err)
			}
			if c.err != nil {
				return
			}
			if latest, err := view.Latest(t.Context()); err != nil || latest.ID != f.snap.ID {
				t.Fatalf("Latest = %v, %v; want the older %s", latest, err, f.snap.ID)
			}
		})
	}
}
