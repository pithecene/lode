package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/errs"

	"github.com/pithecene/lode/internal/testutil"
)

// jsonl is the default codec of these tests.
var jsonl = JSONL[D]()

// openOn opens dataset "events" over store with codec and opts, failing the
// test on error.
func openOn(t *testing.T, store Store, codec Codec[D], opts ...DatasetOption) *Records[D] {
	t.Helper()
	return recordsWith(t, store, "events", codec, opts...)
}

// tryWrite opens dataset "events" over store and writes records, returning
// the first error: a writer whose settings conflict fails when it opens, or
// at its commit when the conflict arises after it opened.
func tryWrite(t *testing.T, store Store, codec Codec[D], records []D, opts ...DatasetOption) (*Snapshot, error) {
	t.Helper()
	ds, err := Open(Use(store)).Records(t.Context(), "events", codec, opts...)
	if err != nil {
		return nil, err
	}
	return ds.Write(t.Context(), records, nil)
}

// parquetOf is a Parquet codec of one string field.
func parquetOf(field string, nullable bool) Codec[D] {
	return Parquet[D](ParquetSchema{Fields: []ParquetField{{Name: field, Type: ParquetString, Nullable: nullable}}})
}

// pendFirst turns the committed first snapshot id into a pending one: its
// canonical manifest moves to the staged path, where an interrupted first
// commit leaves it.
func pendFirst(t *testing.T, store Store, id SnapshotID) {
	t.Helper()
	canonical := "datasets/events/snapshots/" + string(id) + "/manifest.json"
	moveObject(t, store, canonical, "datasets/events/staged/"+string(id)+".json")
	if err := store.Delete(t.Context(), canonical); err != nil {
		t.Fatal(err)
	}
}

// readManifest decodes the object at p as a manifest.
func readManifest(t *testing.T, store Store, p string) *Manifest {
	t.Helper()
	rc, err := store.Get(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { errs.DiscardClose(rc) }()
	var m Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		t.Fatal(err)
	}
	return &m
}

// writeManifestAt replaces the object at p with m.
func writeManifestAt(t *testing.T, store Store, p string, m *Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(t.Context(), p); err != nil { // objects are immutable
		t.Fatal(err)
	}
	if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

func TestRoot_DatasetID(t *testing.T) {
	t.Parallel()
	root := Open(Memory())
	for _, id := range []DatasetID{"", ".", "..", "a/b", "../x", `a\b`, ".lode-tmp-x"} {
		if _, err := root.Records(t.Context(), id, jsonl); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Records(%q) = %v, want ErrInvalidArgument", id, err)
		}
		if _, err := root.Blobs(t.Context(), id); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Blobs(%q) = %v, want ErrInvalidArgument", id, err)
		}
		if err := root.RemoveDataset(t.Context(), id); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("RemoveDataset(%q) = %v, want ErrInvalidArgument", id, err)
		}
	}
	for _, id := range []DatasetID{"events", "tenant=acme", "a.b"} {
		if _, err := root.Records(t.Context(), id, jsonl); err != nil {
			t.Errorf("Records(%q) = %v, want it accepted", id, err)
		}
	}
}

func TestWrite_ManifestRecordsSettings(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	codec := parquetOf("day", false)
	ds := openOn(t, store, codec, hive("day"))
	snap, err := ds.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, store, "datasets/events/segments/"+string(snap.ID)+"/manifest.json")
	if m.FormatVersion != "1.2.0" || m.CodecDescription != codec.Describe() || !slices.Equal(m.PartitionKeys, []string{"day"}) || m.Flat {
		t.Errorf("manifest = %+v, want format 1.2.0, flat=false, codec description and keys", m)
	}

	flatStore := NewMemory()
	blobs, err := Open(Use(flatStore), WithFlatPaths()).Blobs(t.Context(), "events")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := blobs.Write(t.Context(), []byte("x"), nil)
	if err != nil {
		t.Fatal(err)
	}
	flat := readManifest(t, flatStore, "events/"+string(blob.ID)+"/manifest.json")
	if !flat.Flat {
		t.Errorf("flat manifest = %+v, want Flat=true", flat)
	}
}

func TestWrite_StagesOnlyTheFirstCommit(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	lister := &listInjector{Store: store}
	ds := openOn(t, lister, jsonl)
	first, err := ds.Write(t.Context(), R(D{"n": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	staged := "datasets/events/staged/" + string(first.ID) + ".json"
	if !slices.Contains(lister.deleted, staged) {
		t.Errorf("deleted %v, want the first commit's staged manifest %s removed", lister.deleted, staged)
	}
	if exists(t, store, staged) {
		t.Error("staged manifest remains after the first commit")
	}
	lister.deleted = nil
	if _, err := ds.Write(t.Context(), R(D{"n": 2}), nil); err != nil {
		t.Fatal(err)
	}
	if len(lister.deleted) != 0 {
		t.Errorf("deleted %v, want a later commit to stage nothing", lister.deleted)
	}
}

func TestWrite_SettingsMismatch(t *testing.T) {
	t.Parallel()
	type writer struct {
		codec Codec[D]
		opts  []DatasetOption
	}
	for _, tc := range []struct {
		name          string
		first, second writer
	}{
		{"codec", writer{jsonl, nil}, writer{parquetOf("a", true), nil}},
		{"codec description", writer{parquetOf("a", true), nil}, writer{parquetOf("b", true), nil}},
		{"compressor", writer{jsonl, nil}, writer{jsonl, []DatasetOption{WithCompressor(Gzip())}}},
		{"checksum", writer{jsonl, nil}, writer{jsonl, []DatasetOption{WithChecksum(MD5())}}},
		{"partition keys", writer{jsonl, []DatasetOption{hive("a")}}, writer{jsonl, []DatasetOption{hive("a", "b")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			first := openOn(t, store, tc.first.codec, tc.first.opts...)
			second := openOn(t, store, tc.second.codec, tc.second.opts...)
			if _, err := first.Write(t.Context(), R(D{"a": "x", "b": "y"}), nil); err != nil {
				t.Fatal(err)
			}
			_, err := second.Write(t.Context(), R(D{"a": "x", "b": "y"}), nil)
			if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), tc.name+" mismatch") {
				t.Fatalf("at commit: got %v, want a %s ErrSettingsMismatch", err, tc.name)
			}
			_, err = Open(Use(store)).Records(t.Context(), "events", tc.second.codec, tc.second.opts...)
			if !errors.Is(err, ErrSettingsMismatch) {
				t.Fatalf("at open: got %v, want ErrSettingsMismatch", err)
			}
		})
	}
	t.Run("matching settings commit", func(t *testing.T) {
		store := NewMemory()
		gzip := WithCompressor(Gzip())
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), gzip); err != nil {
			t.Fatal(err)
		}
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 2}), gzip); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("the losing first commit is rejected, not retried", func(t *testing.T) {
		store := NewMemory()
		loser := openOn(t, store, jsonl, WithRetry(3))
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(Gzip())); err != nil {
			t.Fatal(err)
		}
		if _, err := loser.Write(t.Context(), R(D{"a": 2}), nil); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestWrite_SettingsOfAPendingFirstCommit(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (Store, *Snapshot) {
		t.Helper()
		store := NewMemory()
		first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
		if err != nil {
			t.Fatal(err)
		}
		return store, first
	}
	canonical := func(id SnapshotID) string {
		return "datasets/events/snapshots/" + string(id) + "/manifest.json"
	}

	t.Run("its staged copy records the settings", func(t *testing.T) {
		store, first := setup(t)
		pendFirst(t, store, first.ID)
		_, err := tryWrite(t, store, jsonl, R(D{"a": 2}), WithCompressor(Gzip()))
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want the staged copy's settings enforced", err)
		}
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 2})); err != nil {
			t.Fatalf("got %v, want a matching writer to adopt the pending commit", err)
		}
		if exists(t, store, "datasets/events/staged/"+string(first.ID)+".json") {
			t.Error("the adopted commit's staged manifest remains after its child committed")
		}
	})
	t.Run("without any manifest the writer conflicts", func(t *testing.T) {
		store, first := setup(t)
		if err := store.Delete(t.Context(), canonical(first.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 2})); !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("got %v, want ErrSnapshotConflict", err)
		}
	})
	t.Run("an abandoned later commit falls back to the newest manifest", func(t *testing.T) {
		store, _ := setup(t)
		second, err := tryWrite(t, store, jsonl, R(D{"a": 2}))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(t.Context(), canonical(second.ID)); err != nil {
			t.Fatal(err)
		}
		next, err := tryWrite(t, store, jsonl, R(D{"a": 3}))
		if err != nil || next.ParentSnapshotID != second.ID {
			t.Fatalf("got %+v/%v, want a commit adopting the abandoned %s", next, err, second.ID)
		}
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 4}), WithCompressor(Gzip())); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want the newest manifest's settings enforced", err)
		}
	})
	t.Run("an abandoned commit on a pending first commit is staged", func(t *testing.T) {
		store := NewMemory()
		first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
		if err != nil {
			t.Fatal(err)
		}
		pendFirst(t, store, first.ID)
		faulty := testutil.NewFaultStore(store)
		faulty.SetPutError(errors.New("injected: manifest write lost"), "/manifest.json")
		abandoned, err := tryWrite(t, faulty, jsonl, R(D{"a": 2}))
		if err == nil {
			t.Fatalf("got %+v, want the commit to fail after it advanced the pointer", abandoned)
		}
		recorder := testutil.NewKeyRecorder(store)
		ds := openOn(t, recorder, jsonl)
		next, err := ds.Write(t.Context(), R(D{"a": 3}), nil)
		if err != nil {
			t.Fatalf("got %v, want the abandoned commit's staged copy to record the settings", err)
		}
		if next.ParentSnapshotID == first.ID {
			t.Fatalf("parent = %s, want the abandoned commit on %s", next.ParentSnapshotID, first.ID)
		}
		if exists(t, store, "datasets/events/staged/"+string(next.ParentSnapshotID)+".json") {
			t.Error("the adopted commit's staged manifest remains after its child committed")
		}
		keys := recorder.Recorded(func() { _, err = ds.Write(t.Context(), R(D{"a": 4}), nil) })
		if err != nil {
			t.Fatal(err)
		}
		if i := slices.IndexFunc(keys, func(k string) bool { return strings.Contains(k, "/staged/") }); i >= 0 {
			t.Errorf("a commit after a completed one reached %s, want no staged copy", keys[i])
		}
	})
	t.Run("a commit removes the staged copies of its pending ancestors", func(t *testing.T) {
		store := NewMemory()
		first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
		if err != nil {
			t.Fatal(err)
		}
		pendFirst(t, store, first.ID)
		late := openOn(t, store, jsonl)
		faulty := testutil.NewFaultStore(store)
		faulty.SetPutError(errors.New("injected: manifest write lost"), "/manifest.json")
		if _, err := tryWrite(t, faulty, jsonl, R(D{"a": 2})); err == nil {
			t.Fatal("want the commit to fail after it advanced the pointer")
		}
		snap, err := late.Write(t.Context(), R(D{"a": 3}), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []SnapshotID{first.ID, snap.ParentSnapshotID} {
			if exists(t, store, "datasets/events/staged/"+string(id)+".json") {
				t.Errorf("the staged copy of %s remains after its descendant committed", id)
			}
		}
	})
	t.Run("a staged copy of an earlier attempt is kept", func(t *testing.T) {
		store := NewMemory()
		first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
		if err != nil {
			t.Fatal(err)
		}
		pendFirst(t, store, first.ID)
		faulty := testutil.NewFaultStore(store)
		var stale string
		faulty.SetBeforePut(func(p string) {
			if stale == "" && strings.Contains(p, "/staged/") {
				stale = p
				putObject(t, store, p)
			}
		})
		snap, err := tryWrite(t, faulty, jsonl, R(D{"a": 2}))
		if err != nil {
			t.Fatalf("got %v, want the earlier attempt's staged copy kept", err)
		}
		if stale != "datasets/events/staged/"+string(snap.ID)+".json" || exists(t, store, stale) {
			t.Fatalf("staged %q, want the commit's copy staged and then removed", stale)
		}
	})
}

// commitRace hides the canonical manifest at canonical from its first Get,
// as a writer sees it when that Get precedes the commit's write of the
// manifest and the next Get follows the commit's removal of its staged
// copy. Its listings are empty, as a lagging listing can be.
type commitRace struct {
	Store
	canonical string
	missed    bool
}

func (s *commitRace) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	if p == s.canonical && !s.missed {
		s.missed = true
		return nil, ErrNotFound
	}
	return s.Store.Get(ctx, p)
}

func (*commitRace) List(context.Context, string) ([]string, error) { return nil, nil }

func TestWrite_SettingsCheckRacingAFirstCommit(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	race := &commitRace{Store: store, canonical: "datasets/events/snapshots/" + string(first.ID) + "/manifest.json"}
	next, err := tryWrite(t, race, jsonl, R(D{"a": 2}))
	if err != nil {
		t.Fatalf("got %v, want the manifest found on the second read", err)
	}
	if !race.missed || next.ParentSnapshotID != first.ID {
		t.Fatalf("missed = %v, parent = %s; want a commit on %s after the race", race.missed, next.ParentSnapshotID, first.ID)
	}
}

// legacyHive rewrites every manifest copy of snap as format 1.0.0, without
// the fields added in 1.1.0+.
func legacyHive(t *testing.T, store Store, snap *Snapshot) {
	t.Helper()
	for _, k := range storeKeys(t, store) {
		if !strings.HasSuffix(k, string(snap.ID)+"/manifest.json") {
			continue
		}
		m := readManifest(t, store, k)
		m.FormatVersion, m.PartitionKeys, m.Flat, m.CodecDescription = "1.0.0", nil, false, ""
		writeManifestAt(t, store, k, m)
	}
}

func TestWrite_LegacyHiveKeys(t *testing.T) {
	t.Parallel()
	t.Run("keys are verified against the partition paths", func(t *testing.T) {
		store := NewMemory()
		snap, err := tryWrite(t, store, jsonl, R(D{"day": "d", "region": "eu"}), hive("day", "region"))
		if err != nil {
			t.Fatal(err)
		}
		legacyHive(t, store, snap)
		for _, keys := range [][]string{{"region", "day"}, {"day"}} {
			if _, err := tryWrite(t, store, jsonl, R(D{"day": "d", "region": "eu"}), hive(keys...)); !errors.Is(err, ErrSettingsMismatch) {
				t.Errorf("keys %v: got %v, want ErrSettingsMismatch", keys, err)
			}
		}
		next, err := tryWrite(t, store, jsonl, R(D{"day": "d", "region": "eu"}), hive("day", "region"))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(next.PartitionKeys, []string{"day", "region"}) {
			t.Errorf("keys = %v, want the verified keys recorded", next.PartitionKeys)
		}
	})
	t.Run("a history without files accepts any keys", func(t *testing.T) {
		store := NewMemory()
		snap, err := tryWrite(t, store, jsonl, R(), hive("day"))
		if err != nil {
			t.Fatal(err)
		}
		legacyHive(t, store, snap)
		if _, err := tryWrite(t, store, jsonl, R(D{"region": "eu"}), hive("region")); err != nil {
			t.Fatalf("got %v, want any keys accepted", err)
		}
	})
	t.Run("a chain broken before a snapshot with files is rejected", func(t *testing.T) {
		store := NewMemory()
		ds := openOn(t, store, jsonl, hive("day"))
		first, err := ds.Write(t.Context(), R(D{"day": "d"}), nil)
		if err != nil {
			t.Fatal(err)
		}
		empty, err := ds.Write(t.Context(), R(), nil)
		if err != nil {
			t.Fatal(err)
		}
		legacyHive(t, store, empty)
		for _, k := range storeKeys(t, store) {
			if strings.Contains(k, string(first.ID)) && strings.HasSuffix(k, "manifest.json") {
				if err := store.Delete(t.Context(), k); err != nil {
					t.Fatal(err)
				}
			}
		}
		_, err = tryWrite(t, store, jsonl, R(D{"day": "d"}), hive("day"))
		if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "cannot verify") {
			t.Fatalf("got %v, want an unverifiable ErrSettingsMismatch", err)
		}
	})
}

func TestCodec_Describe(t *testing.T) {
	t.Parallel()
	if d := jsonl.Describe(); d != "" {
		t.Errorf("JSONL describes %q, want none", d)
	}
	type row struct {
		A *int64 `parquet:"a"`
		B string `parquet:"b"`
	}
	r := row{B: "ok"}
	if r.A != nil || r.B != "ok" {
		t.Fatal("unexpected zero value")
	}
	schema := ParquetSchema{Fields: []ParquetField{{Name: "a", Type: ParquetInt64, Nullable: true}, {Name: "b", Type: ParquetString}}}
	c1 := Parquet[D](schema)
	c2 := Parquet[D](schema, WithParquetCompression(ParquetCompressionGzip))
	c3 := Parquet[row](schema)
	want := `{"fields":[{"name":"a","type":"int64","nullable":true},{"name":"b","type":"string","nullable":false}]}`
	if c1.Describe() != want || c2.Describe() != want || c3.Describe() != want {
		t.Errorf("descriptions %q, %q, %q; want %q", c1.Describe(), c2.Describe(), c3.Describe(), want)
	}
}

// beforePointerCAS runs hook once, before the first CompareAndSwap of the
// latest pointer that passes through it.
type beforePointerCAS struct {
	Store
	hook func()
}

func (b *beforePointerCAS) CompareAndSwap(ctx context.Context, p, expected, replacement string) error {
	if hook := b.hook; hook != nil && strings.HasSuffix(p, "/latest") {
		b.hook = nil
		hook()
	}
	return b.Store.CompareAndSwap(ctx, p, expected, replacement)
}

func TestWrite_LayoutMismatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		first, second []DatasetOption
	}{
		{"default, then Hive", nil, []DatasetOption{hive("a")}},
		{"Hive, then default", []DatasetOption{hive("a")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			if _, err := tryWrite(t, store, jsonl, R(D{"a": "x"}), tc.first...); err != nil {
				t.Fatal(err)
			}
			_, err := tryWrite(t, store, jsonl, R(D{"a": "x"}), tc.second...)
			if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "layout mismatch") {
				t.Fatalf("got %v, want a layout ErrSettingsMismatch", err)
			}
		})
	}
	t.Run("a pointerless foreign history is not an empty dataset", func(t *testing.T) {
		store := NewMemory()
		if _, err := tryWrite(t, store, jsonl, R(D{"a": "x"}), hive("a")); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		if _, err := tryWrite(t, store, jsonl, R(D{"a": "x"})); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestWrite_PartitionKeysCompareStructurally(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	if _, err := tryWrite(t, store, jsonl, R(D{"a,b": "x"}), hive("a,b")); err != nil {
		t.Fatal(err)
	}
	if _, err := tryWrite(t, store, jsonl, R(D{"a": "y", "b": "z"}), hive("a", "b")); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want keys \"a,b\" and \"a\", \"b\" told apart", err)
	}
}

func TestHive_KeysNamePartitionsUnambiguously(t *testing.T) {
	t.Parallel()
	for _, keys := range [][]string{{"a=b"}, {"a/b"}, {""}, {".."}, nil} {
		if _, err := Open(Memory()).Records(t.Context(), "events", jsonl, hive(keys...)); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Hive(%q) = %v, want ErrInvalidArgument", keys, err)
		}
	}
}

func TestWrite_LegacyCheckIsRepeatedAfterAConflict(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	old, err := tryWrite(t, store, parquetOf("a", true), R(D{"a": "x"}))
	if err != nil {
		t.Fatal(err)
	}
	legacyHive(t, store, old)
	racer := &beforePointerCAS{Store: store}
	loser := openOn(t, racer, parquetOf("b", true), WithRetry(2))
	racer.hook = func() {
		if _, err := tryWrite(t, store, parquetOf("a", true), R(D{"a": "y"})); err != nil {
			t.Error(err)
		}
	}
	if _, err := loser.Write(t.Context(), R(D{"b": "z"}), nil); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want the description the winner recorded enforced after the conflict", err)
	}
}

// unlisted fails every List, so a test can show a path performs none.
type unlisted struct{ Store }

func (unlisted) List(context.Context, string) ([]string, error) {
	return nil, errors.New("unlisted: List called")
}

func TestWrite_PendingFirstCommitIsCheckedWithoutListing(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	pendFirst(t, store, first.ID)
	if _, err := tryWrite(t, unlisted{store}, jsonl, R(D{"a": 2})); err != nil {
		t.Fatalf("got %v, want the staged copy read without a listing", err)
	}
}

func TestWrite_PointerLockIsNotAForeignObject(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "datasets", "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "datasets", "events", "latest.lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	ds, err := Open(FS(root)).Records(t.Context(), "events", jsonl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ds.Write(t.Context(), R(D{"a": 1}), nil); err != nil {
		t.Fatalf("got %v, want the pointer's lock file left by a failed first commit ignored", err)
	}
}

func TestWrite_ReturnedManifestDoesNotAliasTheLayout(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	ds := openOn(t, store, jsonl, hive("day"))
	snap, err := ds.Write(t.Context(), R(D{"day": "d"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	snap.PartitionKeys[0] = "region"
	next, err := ds.Write(t.Context(), R(D{"day": "d"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(next.PartitionKeys, []string{"day"}) {
		t.Errorf("keys = %v, want the layout's keys unaffected by a caller's edit", next.PartitionKeys)
	}
}

func TestWrite_ForeignManifestsAreAHistoryAndDataIsNot(t *testing.T) {
	t.Parallel()
	id := string(idAt(time.Now()))
	for _, key := range []string{
		"datasets/events/staged/x/manifest.json",
		"datasets/events/expired/" + id + "/manifest.json",
		"datasets/events/segments/" + id + "/manifest.json",
	} {
		store := NewMemory()
		putObject(t, store, key)
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1})); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("%s: got %v, want a manifest no layout of this writer persists treated as a foreign history", key, err)
		}
	}
	store := NewMemory()
	for _, key := range []string{
		"datasets/events/partitions/a=x/segments/" + id + "/data/data",
		"datasets/events/expired/" + id,
		"datasets/events/expired/" + id + ".lock",
	} {
		putObject(t, store, key)
	}
	if _, err := tryWrite(t, store, jsonl, R(D{"a": 1})); err != nil {
		t.Errorf("got %v, want objects that are not manifests ignored", err)
	}
}

func TestWrite_ConcurrentFirstWritersWithDifferentLayouts(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	partitioned := openOn(t, store, jsonl, hive("a"))
	racer := &beforePointerCAS{Store: store}
	plain := openOn(t, racer, jsonl)
	racer.hook = func() {
		if _, err := partitioned.Write(t.Context(), R(D{"a": "x"}), nil); err != nil {
			t.Errorf("first writer: %v", err)
		}
	}
	if _, err := plain.Write(t.Context(), R(D{"a": "y"}), nil); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want the loser classified as a mismatch with the winner", err)
	}
}

func TestWrite_LostFirstCommitIsAMismatchWithoutRetries(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	racer := &beforePointerCAS{Store: store}
	loser := openOn(t, racer, jsonl)
	racer.hook = func() {
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(Gzip())); err != nil {
			t.Errorf("winner: %v", err)
		}
	}
	if _, err := loser.Write(t.Context(), R(D{"a": 2}), nil); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want ErrSettingsMismatch rather than a retryable conflict", err)
	}

	store = NewMemory()
	racer = &beforePointerCAS{Store: store}
	loser = openOn(t, racer, jsonl)
	racer.hook = func() {
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1})); err != nil {
			t.Errorf("winner: %v", err)
		}
	}
	if _, err := loser.Write(t.Context(), R(D{"a": 2}), nil); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("got %v, want a matching loser's conflict returned as such", err)
	}
}

func TestModernManifestWithoutPathsIsInvalid(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	snap, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	p := "datasets/events/snapshots/" + string(snap.ID) + "/manifest.json"
	m := readManifest(t, store, p)
	m.FormatVersion = "1.1.0" // format 1.1.0 requires a paths object
	writeManifestAt(t, store, p, m)

	if _, err := tryWrite(t, store, jsonl, R(D{"a": 2})); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("writer: got %v, want a 1.1.0 manifest without paths rejected as invalid", err)
	}
	if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("view: got %v, want ErrManifestInvalid", err)
	}
}

func TestReadRoot_RejectsInvalidDatasetIDs(t *testing.T) {
	t.Parallel()
	root := OpenReadOnly(Memory())
	const bad = DatasetID("a/../events")
	if _, err := root.Records(t.Context(), bad, jsonl); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Records: got %v, want ErrInvalidArgument", err)
	}
	if _, err := root.Blobs(t.Context(), bad); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Blobs: got %v, want ErrInvalidArgument", err)
	}
}

func TestConfiguration_IsCopiedAtConstruction(t *testing.T) {
	t.Parallel()
	keys := []string{"day"}
	layout := Hive(keys...)
	keys[0] = "region"
	if got, _ := layout.scheme(); !slices.Equal(got.keys, []string{"day"}) {
		t.Errorf("layout keys = %v, want them unaffected by the caller's slice", got.keys)
	}

	fields := []ParquetField{{Name: "a", Type: ParquetString}}
	codec := Parquet[D](ParquetSchema{Fields: fields})
	before := codec.Describe()
	fields[0] = ParquetField{Name: "b", Type: parquetTypeMax}
	if codec.Describe() != before {
		t.Errorf("description changed to %q after the caller's schema changed", codec.Describe())
	}
}

func TestWrite_LegacyParentCycleIsUnverifiable(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	ds := openOn(t, store, jsonl, hive("day"))
	a, err := ds.Write(t.Context(), R(), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ds.Write(t.Context(), R(), nil)
	if err != nil {
		t.Fatal(err)
	}
	legacyHive(t, store, b)
	p := "datasets/events/segments/" + string(a.ID) + "/manifest.json"
	m := readManifest(t, store, p)
	m.ParentSnapshotID = b.ID
	m.FormatVersion, m.PartitionKeys, m.Flat = "1.0.0", nil, false
	writeManifestAt(t, store, p, m)

	_, err = tryWrite(t, store, jsonl, R(D{"day": "d"}), hive("day"))
	if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("got %v, want a cycle reported as unverifiable", err)
	}
}

func TestSettings_LegacyManifestsUseTheDefaultPathNames(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	snap, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	legacyHive(t, store, snap) // rewrites the manifest as format 1.0.0
	if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); err != nil {
		t.Errorf("view: got %v, want a root with the default names accepted", err)
	}
	if _, err := tryWrite(t, store, jsonl, R(D{"a": 2})); err != nil {
		t.Errorf("writer: got %v, want a root with the default names accepted", err)
	}
}

func TestWrite_DefaultNamedManifestsWithoutAPointer(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	id := string(idAt(time.Now()))
	// A manifest at a foreign custom directory without a pointer is rejected by
	// both writers and views rather than mistaken for an empty dataset.
	putObject(t, store, "datasets/events/snaps/"+id+"/manifest.json")
	if _, err := Open(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
		t.Errorf("writer: got %v, want foreign manifest history rejected", err)
	}
	if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
		t.Errorf("view: got %v, want foreign manifest history rejected", err)
	}
}

// With WithRecordedComponents, a writer takes the compressor and checksum of
// the head, published or pending, and uses the configured ones only for a
// dataset without a head.
func TestWrite_RecordedComponents(t *testing.T) {
	t.Parallel()
	configured := []DatasetOption{WithCompressor(Zstd()), WithChecksum(SHA256()), WithRecordedComponents()}
	cases := []struct {
		name           string
		first          []DatasetOption
		pend           bool
		wantCompressor string
		wantChecksum   string
	}{
		{"a dataset without a head uses the configured components", nil, false, "zstd", "sha256"},
		{"a published head keeps its components", []DatasetOption{}, false, "noop", ""},
		{"a pending first head keeps its components", []DatasetOption{}, true, "noop", ""},
		{"a head with other bundled components keeps them", []DatasetOption{WithCompressor(Gzip()), WithChecksum(CRC32C())}, false, "gzip", "crc32c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			if tc.first != nil {
				first, err := tryWrite(t, store, jsonl, R(D{"a": 1}), tc.first...)
				if err != nil {
					t.Fatal(err)
				}
				if tc.pend {
					pendFirst(t, store, first.ID)
				}
			}
			snap, err := tryWrite(t, store, jsonl, R(D{"a": 2}), configured...)
			if err != nil {
				t.Fatalf("write with the configured components: %v", err)
			}
			if got := snap.Manifest; got.Compressor != tc.wantCompressor || got.ChecksumAlgorithm != tc.wantChecksum {
				t.Fatalf("snapshot records %q and %q, want %q and %q", got.Compressor, got.ChecksumAlgorithm, tc.wantCompressor, tc.wantChecksum)
			}
			view, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl)
			if err != nil {
				t.Fatal(err)
			}
			if recs, err := view.Read(t.Context(), snap.ID); err != nil || len(recs) != 1 {
				t.Fatalf("read %d records, %v; want the snapshot's one record", len(recs), err)
			}
		})
	}
}

func TestOpen_RecordedComponentsReadsThePublishedHeadOnce(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	first, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(Gzip()), WithChecksum(SHA256()))
	if err != nil {
		t.Fatal(err)
	}
	spy := testutil.NewFaultStore(store)
	openOn(t, spy, jsonl, WithRecordedComponents())
	want := []string{"datasets/events/latest", "datasets/events/snapshots/" + string(first.ID) + "/manifest.json"}
	if got := spy.GetCalls(); !slices.Equal(got, want) {
		t.Fatalf("open Get calls = %v, want %v", got, want)
	}
}

// A head that records a custom component the writer does not configure
// cannot be adopted by name, so the writer fails to open.
func TestWrite_RecordedComponentsRejectsAnUnconfiguredCustomComponent(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(renamedCompressor{Gzip()})); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Use(store)).Records(t.Context(), "events", jsonl, WithRecordedComponents())
	if !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want ErrSettingsMismatch", err)
	}
}

func TestWrite_RecordedComponentsRequiresMatchingCustomChecksum(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	checksum := renamedChecksum{MD5()}
	records := R(D{"a": 1})
	first, err := tryWrite(t, store, jsonl, records, WithChecksum(checksum))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(Use(store)).Records(t.Context(), "events", jsonl, WithRecordedComponents())
	if !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("open without the custom checksum: got %v, want ErrSettingsMismatch", err)
	}
	next, err := tryWrite(t, store, jsonl, records, WithRecordedComponents(), WithChecksum(checksum))
	if err != nil {
		t.Fatalf("write with the matching custom checksum: %v", err)
	}
	if got := next.ChecksumAlgorithm; got != checksum.Name() {
		t.Errorf("checksum algorithm = %q, want %q", got, checksum.Name())
	}
	if got, want := next.Objects[0].Checksum, first.Objects[0].Checksum; got == "" || got != want {
		t.Errorf("checksum = %q, want the first snapshot's digest %q", got, want)
	}
}

// Two writers that open an empty dataset with different defaults each keep
// their configured components. The first commit records its settings, and
// the second fails with ErrSettingsMismatch rather than publishing bytes
// under the winner's labels.
func TestWrite_RecordedComponentsRacingFirstWriters(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := NewMemory()
	open := func(opts ...DatasetOption) *Records[D] {
		return openOn(t, store, jsonl, append(opts, WithRecordedComponents())...)
	}
	winner := open(WithCompressor(Zstd()), WithChecksum(SHA256()))
	loser := open(WithCompressor(Gzip()), WithChecksum(CRC32C()))
	won, err := winner.Write(ctx, R(D{"a": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loser.Write(ctx, R(D{"a": 2}), nil); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("second first writer: got %v, want ErrSettingsMismatch", err)
	}
	view, err := OpenReadOnly(Use(store)).Records(ctx, "events", jsonl)
	if err != nil {
		t.Fatal(err)
	}
	head, err := view.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.ID != won.ID || head.Compressor != "zstd" || head.ChecksumAlgorithm != "sha256" {
		t.Fatalf("head %s records %q and %q, want the winner %s with zstd and sha256", head.ID, head.Compressor, head.ChecksumAlgorithm, won.ID)
	}
}

// WithRecordedComponents adopts only the compressor and checksum: a writer
// whose codec or layout differs from the head's still fails to open.
func TestOpen_RecordedComponentsKeepsTheCodecAndLayoutChecks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		codec Codec[D]
		opts  []DatasetOption
	}{
		{"codec", parquetOf("a", true), nil},
		{"layout", jsonl, []DatasetOption{hive("a")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			if _, err := tryWrite(t, store, jsonl, R(D{"a": "x"})); err != nil {
				t.Fatal(err)
			}
			opts := slices.Concat(tc.opts, []DatasetOption{WithRecordedComponents()})
			if _, err := Open(Use(store)).Records(t.Context(), "events", tc.codec, opts...); !errors.Is(err, ErrSettingsMismatch) {
				t.Fatalf("open with another %s: got %v, want ErrSettingsMismatch", tc.name, err)
			}
		})
	}
}

// A head that records a custom compressor accepts a writer that configures a
// compressor of that name: the writer uses the configured instance, and the
// snapshot records the custom name and reads back.
func TestWrite_RecordedComponentsUsesAMatchingCustomCompressor(t *testing.T) {
	t.Parallel()
	store := NewMemory()
	custom := renamedCompressor{Gzip()}
	if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(custom)); err != nil {
		t.Fatal(err)
	}
	next, err := tryWrite(t, store, jsonl, R(D{"a": 2}), WithRecordedComponents(), WithCompressor(custom))
	if err != nil {
		t.Fatalf("write with the matching custom compressor: %v", err)
	}
	if next.Compressor != custom.Name() {
		t.Errorf("compressor = %q, want %q", next.Compressor, custom.Name())
	}
	view, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl, WithCompressor(custom))
	if err != nil {
		t.Fatal(err)
	}
	if recs, err := view.Read(t.Context(), next.ID); err != nil || len(recs) != 1 {
		t.Fatalf("read %d records, %v; want the snapshot's one record", len(recs), err)
	}
}

// A Blobs writer adopts the head's components too, and StreamWrite computes
// the adopted checksum: a head that records CRC32C gives a streamed object a
// CRC32C digest, which a read verifies.
func TestStreamWrite_BlobsAdoptRecordedComponents(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := NewMemory()
	first, err := Open(Use(store)).Blobs(ctx, "files", WithChecksum(CRC32C()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write(ctx, []byte("first"), nil); err != nil {
		t.Fatal(err)
	}
	next, err := Open(Use(store)).Blobs(ctx, "files", WithChecksum(SHA256()), WithRecordedComponents())
	if err != nil {
		t.Fatal(err)
	}
	snap, err := next.StreamWrite(ctx, strings.NewReader("second"), nil)
	if err != nil {
		t.Fatalf("stream write: %v", err)
	}
	if snap.ChecksumAlgorithm != "crc32c" || len(snap.Objects) != 1 || snap.Objects[0].Checksum == "" {
		t.Fatalf("snapshot records %q with objects %+v, want crc32c and a digest", snap.ChecksumAlgorithm, snap.Objects)
	}
	view, err := OpenReadOnly(Use(store)).Blobs(ctx, "files")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := view.Read(ctx, snap.ID); err != nil || string(got) != "second" {
		t.Fatalf("read %q, %v; want the streamed bytes", got, err)
	}
}
