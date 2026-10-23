package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// jsonl is the default codec of these tests.
var jsonl = JSONL[D]()

// openOn opens dataset "events" over store with codec and opts, failing the
// test on error.
func openOn(t *testing.T, store Store, codec Codec[D], opts ...DatasetOption) Records[D] {
	t.Helper()
	return recordsWith(t, store, "events", codec, opts...)
}

// tryWrite opens dataset "events" over store and writes records, returning
// the first error: a writer whose settings conflict fails when it opens, or
// at its commit when the conflict arises after it opened.
func tryWrite(t *testing.T, store Store, codec Codec[D], records []D, opts ...DatasetOption) (*DatasetSnapshot, error) {
	t.Helper()
	ds, err := Open(Use(store)).Records(t.Context(), "events", codec, opts...)
	if err != nil {
		return nil, err
	}
	return ds.Write(t.Context(), records, nil)
}

// parquetOf is a Parquet codec of one string field.
func parquetOf(field string, nullable bool) Codec[D] {
	return Parquet(ParquetSchema{Fields: []ParquetField{{Name: field, Type: ParquetString, Nullable: nullable}}})
}

// pendFirst turns the committed first snapshot id into a pending one: its
// canonical manifest moves to the staged path, where an interrupted first
// commit leaves it.
func pendFirst(t *testing.T, store Store, id DatasetSnapshotID) {
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
	defer func() { _ = rc.Close() }()
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
	root := Open(Memory())
	for _, id := range []DatasetID{"", ".", "..", "a/b", "../x", `a\b`, ".lode-tmp-x"} {
		if _, err := root.Records(t.Context(), id, jsonl); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Records(%q) = %v, want ErrInvalidArgument", id, err)
		}
		if _, err := root.Blobs(t.Context(), id); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Blobs(%q) = %v, want ErrInvalidArgument", id, err)
		}
	}
	for _, id := range []DatasetID{"events", "tenant=acme", "a.b"} {
		if _, err := root.Records(t.Context(), id, jsonl); err != nil {
			t.Errorf("Records(%q) = %v, want it accepted", id, err)
		}
	}
}

func TestWrite_ManifestRecordsSettings(t *testing.T) {
	store := NewMemory()
	codec := parquetOf("day", false)
	ds := openOn(t, store, codec, hive("day"))
	snap, err := ds.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, store, "datasets/events/segments/"+string(snap.ID)+"/manifest.json")
	if m.FormatVersion != "1.1.0" || m.CodecDescription != codec.Describe() || !slices.Equal(m.PartitionKeys, []string{"day"}) {
		t.Errorf("manifest = %+v, want format 1.1.0 with the codec description and keys", m)
	}
	want := ManifestPaths{
		Datasets: "datasets", Snapshots: "snapshots", Partitions: "partitions", Segments: "segments",
		Manifest: "manifest.json", Data: "data", Expired: "expired", Staged: "staged",
	}
	if m.Paths != want {
		t.Errorf("paths = %+v, want %+v", m.Paths, want)
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
	flat := readManifest(t, flatStore, "events/"+string(blob.ID)+"/manifest.json").Paths
	if !flat.Flat || flat.Datasets != "" || flat.Snapshots != "" || flat.Manifest != "manifest.json" {
		t.Errorf("flat paths = %+v, want no datasets or snapshots directory", flat)
	}
}

func TestWrite_StagesOnlyTheFirstCommit(t *testing.T) {
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
		{"compressor", writer{jsonl, nil}, writer{jsonl, []DatasetOption{WithCompressor(NewGzipCompressor())}}},
		{"checksum", writer{jsonl, nil}, writer{jsonl, []DatasetOption{WithChecksum(NewMD5Checksum())}}},
		{"partition keys", writer{jsonl, []DatasetOption{hive("a")}}, writer{jsonl, []DatasetOption{hive("a", "b")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The second writer opens before the first commits, so its
			// commit, not its opening, meets the first writer's settings.
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
			// A writer that opens afterward is rejected when it opens.
			_, err = Open(Use(store)).Records(t.Context(), "events", tc.second.codec, tc.second.opts...)
			if !errors.Is(err, ErrSettingsMismatch) {
				t.Fatalf("at open: got %v, want ErrSettingsMismatch", err)
			}
		})
	}
	t.Run("matching settings commit", func(t *testing.T) {
		store := NewMemory()
		gzip := WithCompressor(NewGzipCompressor())
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
		// The loser resolved an empty dataset when it opened; the winner
		// commits before the loser's first commit.
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(NewGzipCompressor())); err != nil {
			t.Fatal(err)
		}
		if _, err := loser.Write(t.Context(), R(D{"a": 2}), nil); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestWrite_SettingsOfAPendingFirstCommit(t *testing.T) {
	setup := func(t *testing.T) (Store, *DatasetSnapshot) {
		t.Helper()
		store := NewMemory()
		first, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
		if err != nil {
			t.Fatal(err)
		}
		return store, first
	}
	canonical := func(id DatasetSnapshotID) string {
		return "datasets/events/snapshots/" + string(id) + "/manifest.json"
	}

	t.Run("its staged copy records the settings", func(t *testing.T) {
		store, first := setup(t)
		pendFirst(t, store, first.ID)
		_, err := tryWrite(t, store, jsonl, R(D{"a": 2}), WithCompressor(NewGzipCompressor()))
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
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 4}), WithCompressor(NewGzipCompressor())); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want the newest manifest's settings enforced", err)
		}
	})
}

// legacyHive rewrites every manifest copy of snap as format 1.0.0, without
// the fields 1.1.0 added.
func legacyHive(t *testing.T, store Store, snap *DatasetSnapshot) {
	t.Helper()
	for _, k := range storeKeys(t, store) {
		if !strings.HasSuffix(k, string(snap.ID)+"/manifest.json") {
			continue
		}
		m := readManifest(t, store, k)
		m.FormatVersion, m.PartitionKeys, m.Paths, m.CodecDescription = "1.0.0", nil, ManifestPaths{}, ""
		writeManifestAt(t, store, k, m)
	}
}

func TestWrite_LegacyHiveKeys(t *testing.T) {
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
	if d := jsonl.Describe(); d != "" {
		t.Errorf("JSONL describes %q, want none", d)
	}
	schema := ParquetSchema{Fields: []ParquetField{{Name: "a", Type: ParquetInt64, Nullable: true}, {Name: "b", Type: ParquetString}}}
	c1 := Parquet(schema)
	c2 := Parquet(schema, WithParquetCompression(ParquetCompressionGzip))
	want := `{"fields":[{"name":"a","type":"int64","nullable":true},{"name":"b","type":"string","nullable":false}]}`
	if c1.Describe() != want || c2.Describe() != want {
		t.Errorf("descriptions %q and %q, want %q for either compression", c1.Describe(), c2.Describe(), want)
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
	store := NewMemory()
	if _, err := tryWrite(t, store, jsonl, R(D{"a,b": "x"}), hive("a,b")); err != nil {
		t.Fatal(err)
	}
	if _, err := tryWrite(t, store, jsonl, R(D{"a": "y", "b": "z"}), hive("a", "b")); !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want keys \"a,b\" and \"a\", \"b\" told apart", err)
	}
}

func TestHive_KeysNamePartitionsUnambiguously(t *testing.T) {
	for _, keys := range [][]string{{"a=b"}, {"a/b"}, {""}, {".."}, nil} {
		if _, err := Open(Memory()).Records(t.Context(), "events", jsonl, hive(keys...)); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Hive(%q) = %v, want ErrInvalidArgument", keys, err)
		}
	}
}

func TestWrite_LegacyCheckIsRepeatedAfterAConflict(t *testing.T) {
	store := NewMemory()
	old, err := tryWrite(t, store, parquetOf("a", true), R(D{"a": "x"}))
	if err != nil {
		t.Fatal(err)
	}
	legacyHive(t, store, old) // records no description
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
	id := string(idAt(time.Now()))
	for _, key := range []string{
		"datasets/events/staged/x/manifest.json",
		"datasets/events/expired/" + id + "/manifest.json",
		"datasets/events/segments/" + id + "/manifest.json",
		// A manifest under another file name is recognized by its content.
		"datasets/events/snapshots/" + id + "/_manifest.json",
	} {
		store := NewMemory()
		putManifestObject(t, store, key)
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1})); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("%s: got %v, want a manifest no layout of this writer persists treated as a foreign history", key, err)
		}
	}
	// A data object whose first record names a manifest's schema is not a
	// manifest: a map record's keys are sorted, so its fields are not a
	// manifest's leading fields in their order.
	dataStore := NewMemory()
	if err := dataStore.Put(t.Context(), "datasets/events/elsewhere/data.jsonl", strings.NewReader(`{"dataset_id":"events","format_version":"1.1.0","schema_name":"lode-manifest"}`+"\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := tryWrite(t, dataStore, jsonl, R(D{"a": 1})); err != nil {
		t.Errorf("got %v, want a data object that names the schema not treated as a history", err)
	}
	// Another writer's uncommitted data, a tombstone, and its lock file are
	// not a history.
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
	store := NewMemory()
	partitioned := openOn(t, store, jsonl, hive("a"))
	racer := &beforePointerCAS{Store: store}
	plain := openOn(t, racer, jsonl)
	// The Hive writer persists its data and commits while the other writer
	// is between staging its data and its pointer write.
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
	store := NewMemory()
	racer := &beforePointerCAS{Store: store}
	loser := openOn(t, racer, jsonl) // no retries
	racer.hook = func() {
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(NewGzipCompressor())); err != nil {
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
	store := NewMemory()
	snap, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	p := "datasets/events/snapshots/" + string(snap.ID) + "/manifest.json"
	m := readManifest(t, store, p)
	m.Paths = ManifestPaths{}
	writeManifestAt(t, store, p, m)

	if _, err := tryWrite(t, store, jsonl, R(D{"a": 2})); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("writer: got %v, want a 1.1.0 manifest without paths rejected as invalid", err)
	}
	if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("view: got %v, want ErrManifestInvalid", err)
	}
}

func TestReadRoot_RejectsInvalidDatasetIDs(t *testing.T) {
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
	keys := []string{"day"}
	layout := Hive(keys...)
	keys[0] = "region"
	if got, _ := layout.scheme(); !slices.Equal(got.keys, []string{"day"}) {
		t.Errorf("layout keys = %v, want them unaffected by the caller's slice", got.keys)
	}

	fields := []ParquetField{{Name: "a", Type: ParquetString}}
	codec := Parquet(ParquetSchema{Fields: fields})
	before := codec.Describe()
	fields[0] = ParquetField{Name: "b", Type: parquetTypeMax}
	if codec.Describe() != before {
		t.Errorf("description changed to %q after the caller's schema changed", codec.Describe())
	}
}

func TestWrite_LegacyParentCycleIsUnverifiable(t *testing.T) {
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
	// a names b as its parent, closing a cycle without files.
	p := "datasets/events/segments/" + string(a.ID) + "/manifest.json"
	m := readManifest(t, store, p)
	m.ParentSnapshotID = b.ID
	m.FormatVersion, m.PartitionKeys, m.Paths = "1.0.0", nil, ManifestPaths{}
	writeManifestAt(t, store, p, m)

	_, err = tryWrite(t, store, jsonl, R(D{"day": "d"}), hive("day"))
	if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("got %v, want a cycle reported as unverifiable", err)
	}
}

func TestSettings_LegacyManifestsUseTheDefaultPathNames(t *testing.T) {
	store := NewMemory()
	snap, err := tryWrite(t, store, jsonl, R(D{"a": 1}))
	if err != nil {
		t.Fatal(err)
	}
	legacyHive(t, store, snap) // rewrites the manifest as format 1.0.0
	if _, err := Open(Use(store), WithDataDir("d")).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
		t.Errorf("writer: got %v, want a root with other names rejected", err)
	}
	if _, err := OpenReadOnly(Use(store), WithDataDir("d")).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
		t.Errorf("view: got %v, want a root with other names rejected", err)
	}
	if _, err := tryWrite(t, store, jsonl, R(D{"a": 2})); err != nil {
		t.Errorf("got %v, want a root with the default names accepted", err)
	}
}
