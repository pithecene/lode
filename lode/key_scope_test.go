package lode

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// keyScopeFixture holds the record dataset "a", the blob dataset "blobs",
// and the record dataset "b", whose snapshot the escaping names target.
type keyScopeFixture struct {
	store   *testutil.KeyRecorder
	records Records[D]
	blobs   Blobs
	other   *DatasetSnapshot
}

func newKeyScopeFixture(t *testing.T) keyScopeFixture {
	t.Helper()
	ctx := t.Context()
	store := testutil.NewKeyRecorder(NewMemory())
	root := Open(Use(store))
	records, err := root.Records(ctx, "a", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := records.Write(ctx, R(D{"in": "a"}), nil); err != nil {
		t.Fatal(err)
	}
	blobs, err := root.Blobs(ctx, "blobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blobs.Write(ctx, []byte("blob"), nil); err != nil {
		t.Fatal(err)
	}
	other, err := root.Records(ctx, "b", JSONL[D]())
	if err != nil {
		t.Fatal(err)
	}
	snap, err := other.Write(ctx, R(D{"in": "b"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	return keyScopeFixture{store: store, records: records, blobs: blobs, other: snap}
}

// escapingSnapshotIDs are snapshot IDs that are not one path segment; the
// first names dataset b's snapshot from dataset a's directory.
func (f keyScopeFixture) escapingSnapshotIDs() []DatasetSnapshotID {
	return []DatasetSnapshotID{
		"../../b/snapshots/" + f.other.ID, "..", ".", "", "x/y", `x\y`, ".lode-tmp-x",
	}
}

var escapingDatasetIDs = []DatasetID{"../b", "..", ".", "", "a/b", `a\b`, ".lode-tmp-a"}

// Every entry point that builds a key from a caller's name stays within its
// own dataset's directory (CONTRACT_STORAGE §Names in Keys). Each case calls
// one entry point with names that are not one path segment; the list of
// cases must match the entry points the package declares.
func TestKeyScope_EveryEntryPoint(t *testing.T) {
	t.Parallel()
	cases := map[string]func(t *testing.T, f keyScopeFixture){
		"Root.Records": openRejects(func(f keyScopeFixture, id DatasetID) error {
			_, err := Open(Use(f.store)).Records(t.Context(), id, JSONL[D]())
			return err
		}),
		"Root.Blobs": openRejects(func(f keyScopeFixture, id DatasetID) error {
			_, err := Open(Use(f.store)).Blobs(t.Context(), id)
			return err
		}),
		"ReadRoot.Records": openRejects(func(f keyScopeFixture, id DatasetID) error {
			_, err := OpenReadOnly(Use(f.store)).Records(t.Context(), id, JSONL[D]())
			return err
		}),
		"ReadRoot.Blobs": openRejects(func(f keyScopeFixture, id DatasetID) error {
			_, err := OpenReadOnly(Use(f.store)).Blobs(t.Context(), id)
			return err
		}),
		"SnapshotReader.Snapshot": readFinds("datasets/a/", func(f keyScopeFixture, id DatasetSnapshotID) error {
			_, err := f.records.Snapshot(t.Context(), id)
			return err
		}),
		"SnapshotReader.GetManifest": readFinds("datasets/a/", func(f keyScopeFixture, id DatasetSnapshotID) error {
			_, err := f.records.GetManifest(t.Context(), ManifestRef{ID: id})
			return err
		}),
		"RecordView.Read": readFinds("datasets/a/", func(f keyScopeFixture, id DatasetSnapshotID) error {
			_, err := f.records.Read(t.Context(), id)
			return err
		}),
		"BlobView.Read": readFinds("datasets/blobs/", func(f keyScopeFixture, id DatasetSnapshotID) error {
			_, err := f.blobs.Read(t.Context(), id)
			return err
		}),
		"Retention.Expire": func(t *testing.T, f keyScopeFixture) {
			t.Helper()
			for _, id := range f.escapingSnapshotIDs() {
				var report *ExpireReport
				var err error
				keys := f.store.Recorded(func() {
					report, err = f.records.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{id}})
				})
				requireWithin(t, keys, "datasets/a/", string(id))
				if err == nil && len(report.Expired) > 0 {
					t.Errorf("Expire(%q) expired %v", id, report.Expired)
				}
			}
		},
	}

	declared, err := testutil.EntryPoints(".", "DatasetID", "DatasetSnapshotID", "ManifestRef", "ExpireRequest")
	if err != nil {
		t.Fatal(err)
	}
	if tested := slices.Sorted(maps.Keys(cases)); !slices.Equal(tested, declared) {
		t.Fatalf("the cases %v do not match the entry points %v that take a dataset or snapshot name", tested, declared)
	}
	for name, check := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newKeyScopeFixture(t)
			check(t, f)
			if _, err := f.records.Snapshot(t.Context(), f.other.ID); err == nil {
				t.Errorf("dataset a's handle reached dataset b's snapshot %s by its own ID", f.other.ID)
			}
			if b, err := Open(Use(f.store)).Records(t.Context(), "b", JSONL[D]()); err != nil {
				t.Fatal(err)
			} else if _, err := b.Snapshot(t.Context(), f.other.ID); err != nil {
				t.Errorf("dataset b's snapshot is gone: %v", err)
			}
		})
	}
}

// openRejects checks that opening a handle with a dataset ID that is not one
// path segment returns ErrInvalidArgument before any store call.
func openRejects(open func(f keyScopeFixture, id DatasetID) error) func(t *testing.T, f keyScopeFixture) {
	return func(t *testing.T, f keyScopeFixture) {
		t.Helper()
		for _, id := range escapingDatasetIDs {
			var err error
			keys := f.store.Recorded(func() { err = open(f, id) })
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("open(%q) = %v, want ErrInvalidArgument", id, err)
			}
			if len(keys) > 0 {
				t.Errorf("open(%q) reached %v before rejecting the ID", id, keys)
			}
		}
	}
}

// readFinds checks that a read by a snapshot ID that is not one path segment
// returns ErrNotFound, since no snapshot has such an ID, and reaches no key
// outside dir.
func readFinds(dir string, read func(f keyScopeFixture, id DatasetSnapshotID) error) func(t *testing.T, f keyScopeFixture) {
	return func(t *testing.T, f keyScopeFixture) {
		t.Helper()
		for _, id := range f.escapingSnapshotIDs() {
			var err error
			keys := f.store.Recorded(func() { err = read(f, id) })
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("read(%q) = %v, want ErrNotFound", id, err)
			}
			requireWithin(t, keys, dir, string(id))
		}
	}
}

func requireWithin(t *testing.T, keys []string, dir, name string) {
	t.Helper()
	for _, k := range keys {
		if !strings.HasPrefix(k, dir) {
			t.Errorf("a call with %q reached %q, outside %s", name, k, dir)
		}
	}
}

// A writer names each data file by its role and the compressor's extension,
// so an extension that is not part of one path segment is rejected when the
// handle opens (CONTRACT_STORAGE §Names in Keys).
func TestKeyScope_CompressorExtension(t *testing.T) {
	t.Parallel()
	for _, ext := range []string{"/../../../../b/escaped", "/x", `\x`} {
		store := testutil.NewKeyRecorder(NewMemory())
		var err error
		keys := store.Recorded(func() {
			_, err = Open(Use(store)).Records(t.Context(), "a", JSONL[D](), WithCompressor(&extensionCompressor{ext: ext}))
		})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("WithCompressor(extension %q) = %v, want ErrInvalidArgument", ext, err)
		}
		if len(keys) > 0 {
			t.Errorf("WithCompressor(extension %q) reached %v before rejecting it", ext, keys)
		}
	}
}

// extensionCompressor is the no-op compressor under another name and
// extension.
type extensionCompressor struct {
	noopCompressor
	ext string
}

func (c *extensionCompressor) Name() string      { return "extension" }
func (c *extensionCompressor) Extension() string { return c.ext }
