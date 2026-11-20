package lode

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// anchorFixture holds a dataset of two snapshots, older and head, over
// a store that records the keys each call reaches.
type anchorFixture struct {
	store         *testutil.KeyRecorder
	writer        *Records[D]
	older, latest *Snapshot
}

func newAnchorFixture(t *testing.T, opts ...DatasetOption) anchorFixture {
	t.Helper()
	ctx := t.Context()
	store := testutil.NewKeyRecorder(NewMemory())
	writer, err := Open(Use(store)).Records(ctx, "ds", JSONL[D](), opts...)
	if err != nil {
		t.Fatal(err)
	}
	older, err := writer.Write(ctx, R(D{"k": "a", "n": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := writer.Write(ctx, R(D{"k": "b", "n": 2}), nil)
	if err != nil {
		t.Fatal(err)
	}
	return anchorFixture{store: store, writer: writer, older: older, latest: latest}
}

// open opens a view of the fixture's dataset with opts and returns the keys
// that opening reached.
func (f anchorFixture) open(t *testing.T, opts ...ViewOption) (*RecordView[D], []string, error) {
	t.Helper()
	var view *RecordView[D]
	var err error
	keys := f.store.Recorded(func() {
		view, err = OpenReadOnly(Use(f.store)).Records(t.Context(), "ds", JSONL[D](), opts...)
	})
	return view, keys, err
}

// A view opened with WithAnchor reads that snapshot's canonical manifest once,
// checks the removal fence, skips the head manifest, takes the layout from it, and
// retains the immutable anchor manifest so Snapshot(id) and Read(id) issue
// zero additional manifest Gets. Other reads behave as on a view of the head.
func TestWithAnchor_OpensAtTheNamedSnapshot(t *testing.T) {
	t.Parallel()
	layouts := map[string][]DatasetOption{
		"unpartitioned": nil,
		"hive":          {WithLayout(Hive("k"))},
	}
	for name, opts := range layouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newAnchorFixture(t, opts...)
			view, keys, err := f.open(t, WithAnchor(f.older.ID))
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				if strings.Contains(k, string(f.latest.ID)) {
					t.Errorf("opening at %s reached %s", f.older.ID, k)
				}
			}
			var snap *Snapshot
			reads := f.store.Recorded(func() { snap, err = view.Snapshot(t.Context(), f.older.ID) })
			if err != nil || snap.ID != f.older.ID {
				t.Fatalf("Snapshot(%s) = %v, %v", f.older.ID, snap, err)
			}
			if len(reads) != 2 || !strings.HasSuffix(reads[0], "/latest") || !strings.HasSuffix(reads[1], "/manifest.json") {
				t.Errorf("Snapshot of the anchored snapshot reached %v, want the fence and a canonical existence check", reads)
			}
			snap.Objects[0].SizeBytes = -1
			again, err := view.Snapshot(t.Context(), f.older.ID)
			if err != nil || again.Objects[0].SizeBytes < 0 {
				t.Fatalf("caller mutation affected cached snapshot: %v, %v", again, err)
			}
			var recs []D
			dataReads := f.store.Recorded(func() { recs, err = view.Read(t.Context(), f.older.ID) })
			if err != nil || len(recs) != 1 || recs[0]["n"] != float64(1) {
				t.Fatalf("Read(%s) = %v, %v", f.older.ID, recs, err)
			}
			if len(dataReads) != 3 || !slices.Equal(dataReads[:2], reads) || dataReads[2] != snap.Objects[0].Path {
				t.Errorf("Read of the anchored snapshot reached %v, want lifecycle checks followed by the data object", dataReads)
			}
			head, err := view.Latest(t.Context())
			if err != nil || head.ID != f.latest.ID {
				t.Fatalf("Latest = %v, %v; want the head %s", head, err, f.latest.ID)
			}
			if other, err := view.Snapshot(t.Context(), f.latest.ID); err != nil || other.ID != f.latest.ID {
				t.Fatalf("Snapshot(%s) = %v, %v", f.latest.ID, other, err)
			}
		})
	}
}

// Opening at a snapshot and reading it fits within the request budget of opening
// at the head and then reading the snapshot.
func TestWithAnchor_HiveLookupRequestBudget(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t, WithLayout(Hive("k")))
	cost := func(opts ...ViewOption) int {
		view, keys, err := f.open(t, opts...)
		if err != nil {
			t.Fatal(err)
		}
		reads := f.store.Recorded(func() { _, err = view.Snapshot(t.Context(), f.older.ID) })
		if err != nil {
			t.Fatal(err)
		}
		return len(keys) + len(reads)
	}
	head, at := cost(), cost(WithAnchor(f.older.ID))
	if at > head {
		t.Errorf("open at the snapshot and read it: %d requests; open at the head and read it: %d; want at most the head lookup cost", at, head)
	}
	t.Logf("requests: at the snapshot %d, at the head %d", at, head)
}

// Opening at a snapshot that does not exist, or that retention expired,
// fails as Snapshot does; an expired snapshot is not resurrected by the
// partition manifests that an interrupted expiry leaves.
func TestWithAnchor_ReportsAnAbsentOrExpiredSnapshot(t *testing.T) {
	t.Parallel()
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		f := newAnchorFixture(t)
		absent := SnapshotID(strings.Replace(string(f.older.ID), "-", "0-", 1))
		if _, _, err := f.open(t, WithAnchor(absent)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("open at an absent snapshot: %v, want ErrNotFound", err)
		}
	})
	t.Run("expired", func(t *testing.T) {
		t.Parallel()
		f := newAnchorFixture(t, WithLayout(Hive("k")))
		view, _, err := f.open(t, WithAnchor(f.older.ID))
		if err != nil {
			t.Fatal(err)
		}
		residual := f.partitionManifests(t, f.older.ID)
		if _, err := f.writer.Expire(t.Context(), ExpireRequest{IDs: []SnapshotID{f.older.ID}}); err != nil {
			t.Fatal(err)
		}
		f.restore(t, residual)
		if _, err := view.Snapshot(t.Context(), f.older.ID); !errors.Is(err, ErrSnapshotExpired) {
			t.Fatalf("previously opened view returned %v, want ErrSnapshotExpired", err)
		}
		if _, _, err := f.open(t, WithAnchor(f.older.ID)); !errors.Is(err, ErrSnapshotExpired) {
			t.Fatalf("open at an expired snapshot: %v, want ErrSnapshotExpired", err)
		}
	})
}

// A dataset written before canonical manifests has partition manifests
// only, and opening at one of its snapshots finds the snapshot by scan.
func TestWithAnchor_FindsASnapshotWithoutACanonicalManifest(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t, WithLayout(Hive("k")))
	keys, err := f.store.List(t.Context(), "datasets/ds/")
	if err != nil {
		t.Fatal(err)
	}
	canonical := slices.IndexFunc(keys, func(k string) bool {
		return strings.HasPrefix(k, "datasets/ds/segments/"+string(f.older.ID)+"/")
	})
	if canonical < 0 {
		t.Fatalf("no canonical manifest of %s among %v", f.older.ID, keys)
	}
	if err := f.store.Delete(t.Context(), keys[canonical]); err != nil {
		t.Fatal(err)
	}
	view, _, err := f.open(t, WithAnchor(f.older.ID))
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := view.Snapshot(t.Context(), f.older.ID); err != nil || snap.ID != f.older.ID {
		t.Fatalf("Snapshot(%s) = %v, %v", f.older.ID, snap, err)
	}
}

// The view's codec and the root's path topology are checked against the
// named snapshot, as they are against the head.
func TestWithAnchor_ChecksTheNamedSnapshotsSettings(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := NewMemory()
	blobs, err := Open(Use(store)).Blobs(ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := blobs.Write(ctx, []byte("blob"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(Use(store)).Records(ctx, "ds", JSONL[D](), WithAnchor(snap.ID)); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("records view at a blob snapshot: %v, want ErrSettingsMismatch", err)
	}
	if _, err := OpenReadOnly(Use(store), WithFlatPaths()).Blobs(ctx, "ds", WithAnchor(snap.ID)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("flat view over default-path dataset: %v, want ErrNotFound", err)
	}
	view, err := OpenReadOnly(Use(store)).Blobs(ctx, "ds", WithAnchor(snap.ID))
	if err != nil {
		t.Fatal(err)
	}
	if b, err := view.Read(ctx, snap.ID); err != nil || string(b) != "blob" {
		t.Fatalf("Read = %q, %v", b, err)
	}
}

// An invalid manifest of the named snapshot fails the open with
// ErrManifestInvalid, and the error names that snapshot, as an invalid head
// fails a view of the head.
func TestWithAnchor_RejectsAnInvalidManifest(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	p := "datasets/ds/snapshots/" + string(f.older.ID) + "/manifest.json"
	m := readManifest(t, f.store, p)
	m.Compressor = ""
	writeManifestAt(t, f.store, p, m)
	_, _, err := f.open(t, WithAnchor(f.older.ID))
	if !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("open at an invalid manifest: %v, want ErrManifestInvalid", err)
	}
	if want := "snapshot " + string(f.older.ID) + " of ds"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not name %q", err, want)
	}
}

// partitionManifests returns the partition manifests of snapshot id with
// their bytes, so a test can put them back after a complete expiry.
func (f anchorFixture) partitionManifests(t *testing.T, id SnapshotID) map[string][]byte {
	t.Helper()
	keys, err := f.store.List(t.Context(), "datasets/ds/partitions/")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, k := range keys {
		if strings.Contains(k, string(id)) && strings.HasSuffix(k, "manifest.json") {
			rc, err := f.store.Get(t.Context(), k)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close() // read-only stream
			if err != nil {
				t.Fatal(err)
			}
			out[k] = b
		}
	}
	if len(out) == 0 {
		t.Fatalf("no partition manifest of %s", id)
	}
	return out
}

// restore puts objects back, as an interrupted expiry leaves them.
func (f anchorFixture) restore(t *testing.T, objects map[string][]byte) {
	t.Helper()
	for k, b := range objects {
		if err := f.store.Put(t.Context(), k, strings.NewReader(string(b))); err != nil {
			t.Fatal(err)
		}
	}
}
