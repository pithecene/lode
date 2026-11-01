package volume

import (
	"bytes"
	"errors"
	"maps"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// snapshotOf reads every object of a store, so a test can tell that a
// call wrote nothing.
func snapshotOf(t *testing.T, store lode.Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, k := range listed(t, store, "") {
		rc, err := store.Get(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		var b bytes.Buffer
		_, _ = b.ReadFrom(rc)
		_ = rc.Close()
		out[k] = b.String()
	}
	return out
}

// Head names the head pointer's snapshot, committed or pending, without
// the volume's length, and writes nothing.
func TestHead(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup writes the volume and returns the snapshot Head must name.
		setup   func(t *testing.T, fs *testutil.FaultStore) SnapshotID
		wantErr error
	}{
		{"a volume never written", func(*testing.T, *testutil.FaultStore) SnapshotID { return "" }, lode.ErrNoSnapshots},
		{"a volume with only a staged block", func(t *testing.T, fs *testutil.FaultStore) SnapshotID {
			stageBlock(t, newTestVolume(t, "v", newFaultStoreFactory(fs), 10), 0, []byte("A"))
			return ""
		}, lode.ErrNoSnapshots},
		{"a committed head", func(t *testing.T, fs *testutil.FaultStore) SnapshotID {
			vol := newTestVolume(t, "v", newFaultStoreFactory(fs), 10)
			commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, []byte("A"))}, Metadata{})
			return commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 5, []byte("B"))}, Metadata{}).ID
		}, nil},
		{"a pending commit", func(t *testing.T, fs *testutil.FaultStore) SnapshotID {
			vol := newTestVolume(t, "v", newFaultStoreFactory(fs), 10)
			commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, []byte("A"))}, Metadata{})
			blk := stageBlock(t, vol, 5, []byte("B"))
			fs.SetPutError(testutil.ErrInjectedPut, "manifest.json")
			defer fs.SetPutError(nil)
			if _, err := vol.Commit(t.Context(), []BlockRef{blk}, Metadata{}); err == nil {
				t.Fatal("commit with a failing manifest write succeeded")
			}
			return SnapshotID(pointerContent(t, fs, "v"))
		}, nil},
		{"a removed volume", func(t *testing.T, fs *testutil.FaultStore) SnapshotID {
			vol := newTestVolume(t, "v", newFaultStoreFactory(fs), 10)
			commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, []byte("A"))}, Metadata{})
			if err := Remove(t.Context(), "v", newFaultStoreFactory(fs)); err != nil {
				t.Fatal(err)
			}
			return ""
		}, ErrRemoved},
		{"an absent pointer beside a manifest", func(t *testing.T, fs *testutil.FaultStore) SnapshotID {
			vol := newTestVolume(t, "v", newFaultStoreFactory(fs), 10)
			commitBlocks(t, vol, []BlockRef{stageBlock(t, vol, 0, []byte("A"))}, Metadata{})
			if err := fs.Delete(t.Context(), volumeLatestPointerPath("v")); err != nil {
				t.Fatal(err)
			}
			return ""
		}, lode.ErrHeadUncommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := testutil.NewFaultStore(lode.NewMemory())
			want := tc.setup(t, fs)
			before := snapshotOf(t, fs)

			got, err := Head(t.Context(), "v", newFaultStoreFactory(fs))
			if !errors.Is(err, tc.wantErr) || got != want {
				t.Fatalf("Head = %q, %v; want %q, %v", got, err, want, tc.wantErr)
			}
			if !maps.Equal(snapshotOf(t, fs), before) {
				t.Fatal("Head wrote to the store")
			}
		})
	}
}

// A pointer that holds no snapshot ID names no head, and Head says so
// rather than returning its content.
func TestHead_RefusesACorruptPointer(t *testing.T) {
	mem := lode.NewMemory()
	if err := mem.Put(t.Context(), volumeLatestPointerPath("v"), bytes.NewReader([]byte("../x"))); err != nil {
		t.Fatal(err)
	}
	got, err := Head(t.Context(), "v", memoryFactoryFrom(mem))
	if err == nil || got != "" {
		t.Fatalf("Head = %q, %v; want an error", got, err)
	}
}

// Head checks its arguments before it creates the store.
func TestHead_InvalidArguments(t *testing.T) {
	created := false
	factory := func() (lode.RangeStore, error) { created = true; return lode.NewMemory(), nil }
	for _, tc := range []struct {
		name    string
		id      ID
		factory lode.Factory[lode.RangeStore]
	}{
		{"nil factory", "v", nil},
		{"an ID that is no path segment", "a/b", factory},
		{"an empty ID", "", factory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Head(t.Context(), tc.id, tc.factory); !errors.Is(err, lode.ErrInvalidArgument) {
				t.Fatalf("Head = %v, want lode.ErrInvalidArgument", err)
			}
		})
	}
	if created {
		t.Fatal("Head created the store for an invalid argument")
	}
}
