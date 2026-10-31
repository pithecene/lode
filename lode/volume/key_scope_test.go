package volume

import (
	"bytes"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// keyScopeFixture holds the volume "a" and the volume "b", whose snapshot the
// escaping names target.
type keyScopeFixture struct {
	store *testutil.KeyRecorder
	a     Volume
	other *Snapshot
}

func newKeyScopeFixture(t *testing.T) keyScopeFixture {
	t.Helper()
	ctx := t.Context()
	store := testutil.NewKeyRecorder(lode.NewMemory())
	factory := memoryFactoryFrom(store)
	commit := func(id ID) (Volume, *Snapshot) {
		vol, err := New(id, factory, 4)
		if err != nil {
			t.Fatal(err)
		}
		blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte(string(id)+"xyz")))
		if err != nil {
			t.Fatal(err)
		}
		snap, err := vol.Commit(ctx, []BlockRef{blk}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return vol, snap
	}
	a, _ := commit("a")
	_, other := commit("b")
	return keyScopeFixture{store: store, a: a, other: other}
}

// escapingSnapshotIDs are snapshot IDs that are not one path segment; the
// first names volume b's snapshot from volume a's directory.
func (f keyScopeFixture) escapingSnapshotIDs() []SnapshotID {
	return []SnapshotID{"../../b/snapshots/" + f.other.ID, "..", ".", "", "x/y", `x\y`, ".lode-tmp-x"}
}

var escapingIDs = []ID{"../datasets/events", "..", ".", "", "a/data", `a\b`, ".lode-tmp-a"}

// Every entry point that builds a key from a caller's name stays within its
// own volume's directory (CONTRACT_STORAGE §Names in Keys). The list of cases
// must match the entry points the package declares.
func TestKeyScope_EveryEntryPoint(t *testing.T) {
	t.Parallel()
	cases := map[string]func(t *testing.T, f keyScopeFixture){
		"New": func(t *testing.T, f keyScopeFixture) {
			t.Helper()
			factory := memoryFactoryFrom(f.store)
			for _, id := range escapingIDs {
				var err error
				keys := f.store.Recorded(func() { _, err = New(id, factory, 4) })
				if !errors.Is(err, lode.ErrInvalidArgument) {
					t.Errorf("New(%q) = %v, want ErrInvalidArgument", id, err)
				}
				if len(keys) > 0 {
					t.Errorf("New(%q) reached %v before rejecting the ID", id, keys)
				}
			}
		},
		"Remove": func(t *testing.T, f keyScopeFixture) {
			t.Helper()
			factory := memoryFactoryFrom(f.store)
			for _, id := range escapingIDs {
				var err error
				keys := f.store.Recorded(func() { err = Remove(t.Context(), id, factory) })
				if !errors.Is(err, lode.ErrInvalidArgument) {
					t.Errorf("Remove(%q) = %v, want ErrInvalidArgument", id, err)
				}
				if len(keys) > 0 {
					t.Errorf("Remove(%q) reached %v before rejecting the ID", id, keys)
				}
			}
			b, err := New("b", factory, 4)
			if err != nil {
				t.Fatal(err)
			}
			if latest, err := b.Latest(t.Context()); err != nil || latest.ID != f.other.ID {
				t.Errorf("volume b after the refused removals: %v, %v", latest, err)
			}
		},
		"Reader.Snapshot": readFinds(func(f keyScopeFixture, id SnapshotID) error {
			_, err := f.a.Snapshot(t.Context(), id)
			return err
		}),
		"Reader.ReadAt": readFinds(func(f keyScopeFixture, id SnapshotID) error {
			_, err := f.a.ReadAt(t.Context(), id, 0, 4)
			return err
		}),
	}

	declared, err := testutil.EntryPoints(".", "ID", "SnapshotID")
	if err != nil {
		t.Fatal(err)
	}
	if tested := slices.Sorted(maps.Keys(cases)); !slices.Equal(tested, declared) {
		t.Fatalf("the cases %v do not match the entry points %v that take a volume or snapshot name", tested, declared)
	}
	for name, check := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			check(t, newKeyScopeFixture(t))
		})
	}
}

// readFinds checks that a read by a snapshot ID that is not one path segment
// returns ErrNotFound, since no snapshot has such an ID, and reaches no key
// outside volume a's directory.
func readFinds(read func(f keyScopeFixture, id SnapshotID) error) func(t *testing.T, f keyScopeFixture) {
	return func(t *testing.T, f keyScopeFixture) {
		t.Helper()
		for _, id := range f.escapingSnapshotIDs() {
			var err error
			keys := f.store.Recorded(func() { err = read(f, id) })
			if !errors.Is(err, lode.ErrNotFound) {
				t.Errorf("read(%q) = %v, want ErrNotFound", id, err)
			}
			for _, k := range keys {
				if !strings.HasPrefix(k, "volumes/a/") {
					t.Errorf("a read with %q reached %q, outside volumes/a/", id, k)
				}
			}
		}
	}
}
