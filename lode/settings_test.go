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

// openOn opens dataset "events" over store with opts.
func openOn(t *testing.T, store Store, opts ...Option) Dataset {
	t.Helper()
	ds, err := NewDataset("events", NewMemoryFactoryFrom(store), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return ds
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

func TestNewDataset_DatasetID(t *testing.T) {
	for _, id := range []DatasetID{"", ".", "..", "a/b", "../x", `a\b`, ".lode-tmp-x"} {
		if _, err := NewDataset(id, NewMemoryFactory()); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("NewDataset(%q) = %v, want ErrInvalidArgument", id, err)
		}
	}
	for _, id := range []DatasetID{"events", "tenant=acme", "a.b"} {
		if _, err := NewDataset(id, NewMemoryFactory()); err != nil {
			t.Errorf("NewDataset(%q) = %v, want it accepted", id, err)
		}
	}
}

func TestWrite_ManifestRecordsSettings(t *testing.T) {
	store := NewMemory()
	codec, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{{Name: "day", Type: ParquetString}}})
	if err != nil {
		t.Fatal(err)
	}
	ds := openOn(t, store, WithHiveLayout("day"), WithCodec(codec))
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
	blob, err := openOn(t, flatStore, WithLayout(NewFlatLayout())).Write(t.Context(), []any{[]byte("x")}, nil)
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
	ds := openOn(t, lister, WithCodec(NewJSONLCodec()))
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
	parquet := func(field string) Codec {
		c, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{{Name: field, Type: ParquetString}}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	for _, tc := range []struct {
		name          string
		first, second []Option
	}{
		{"codec", []Option{WithCodec(NewJSONLCodec())}, []Option{WithCodec(parquet("a"))}},
		{"codec description", []Option{WithCodec(parquet("a"))}, []Option{WithCodec(parquet("b"))}},
		{"compressor", []Option{WithCodec(NewJSONLCodec())}, []Option{WithCodec(NewJSONLCodec()), WithCompressor(NewGzipCompressor())}},
		{"checksum", []Option{WithCodec(NewJSONLCodec())}, []Option{WithCodec(NewJSONLCodec()), WithChecksum(NewMD5Checksum())}},
		{"partition keys", []Option{WithCodec(NewJSONLCodec()), WithHiveLayout("a")}, []Option{WithCodec(NewJSONLCodec()), WithHiveLayout("a", "b")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			first, second := openOn(t, store, tc.first...), openOn(t, store, tc.second...)
			if _, err := first.Write(t.Context(), R(D{"a": "x", "b": "y"}), nil); err != nil {
				t.Fatal(err)
			}
			_, err := second.Write(t.Context(), R(D{"a": "x", "b": "y"}), nil)
			if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), tc.name+" mismatch") {
				t.Fatalf("got %v, want a %s ErrSettingsMismatch", err, tc.name)
			}
		})
	}
	t.Run("matching settings commit", func(t *testing.T) {
		store := NewMemory()
		opts := []Option{WithCodec(NewJSONLCodec()), WithCompressor(NewGzipCompressor())}
		if _, err := openOn(t, store, opts...).Write(t.Context(), R(D{"a": 1}), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := openOn(t, store, opts...).Write(t.Context(), R(D{"a": 2}), nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("the losing first commit is rejected, not retried", func(t *testing.T) {
		store := NewMemory()
		loser := openOn(t, store, WithCodec(NewJSONLCodec()), WithRetryCount(3))
		// The loser resolves an empty dataset; the winner commits before
		// the loser's pointer write.
		winner := openOn(t, store, WithCodec(NewJSONLCodec()), WithCompressor(NewGzipCompressor()))
		if _, err := winner.Write(t.Context(), R(D{"a": 1}), nil); err != nil {
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
		first, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 1}), nil)
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
		_, err := openOn(t, store, WithCodec(NewJSONLCodec()), WithCompressor(NewGzipCompressor())).Write(t.Context(), R(D{"a": 2}), nil)
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want the staged copy's settings enforced", err)
		}
		if _, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 2}), nil); err != nil {
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
		_, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 2}), nil)
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Fatalf("got %v, want ErrSnapshotConflict", err)
		}
	})
	t.Run("an abandoned later commit falls back to the newest manifest", func(t *testing.T) {
		store, _ := setup(t)
		second, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 2}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(t.Context(), canonical(second.ID)); err != nil {
			t.Fatal(err)
		}
		next, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 3}), nil)
		if err != nil || next.Manifest.ParentSnapshotID != second.ID {
			t.Fatalf("got %+v/%v, want a commit adopting the abandoned %s", next, err, second.ID)
		}
		_, err = openOn(t, store, WithCodec(NewJSONLCodec()), WithCompressor(NewGzipCompressor())).Write(t.Context(), R(D{"a": 4}), nil)
		if !errors.Is(err, ErrSettingsMismatch) {
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
	hive := func(keys ...string) []Option {
		return []Option{WithCodec(NewJSONLCodec()), WithHiveLayout(keys...)}
	}
	t.Run("keys are verified against the partition paths", func(t *testing.T) {
		store := NewMemory()
		snap, err := openOn(t, store, hive("day", "region")...).Write(t.Context(), R(D{"day": "d", "region": "eu"}), nil)
		if err != nil {
			t.Fatal(err)
		}
		legacyHive(t, store, snap)
		for _, keys := range [][]string{{"region", "day"}, {"day"}} {
			_, err := openOn(t, store, hive(keys...)...).Write(t.Context(), R(D{"day": "d", "region": "eu"}), nil)
			if !errors.Is(err, ErrSettingsMismatch) {
				t.Errorf("keys %v: got %v, want ErrSettingsMismatch", keys, err)
			}
		}
		next, err := openOn(t, store, hive("day", "region")...).Write(t.Context(), R(D{"day": "d", "region": "eu"}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(next.Manifest.PartitionKeys, []string{"day", "region"}) {
			t.Errorf("keys = %v, want the verified keys recorded", next.Manifest.PartitionKeys)
		}
	})
	t.Run("a history without files accepts any keys", func(t *testing.T) {
		store := NewMemory()
		snap, err := openOn(t, store, hive("day")...).Write(t.Context(), R(), nil)
		if err != nil {
			t.Fatal(err)
		}
		legacyHive(t, store, snap)
		if _, err := openOn(t, store, hive("region")...).Write(t.Context(), R(D{"region": "eu"}), nil); err != nil {
			t.Fatalf("got %v, want any keys accepted", err)
		}
	})
	t.Run("a chain broken before a snapshot with files is rejected", func(t *testing.T) {
		store := NewMemory()
		ds := openOn(t, store, hive("day")...)
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
		_, err = openOn(t, store, hive("day")...).Write(t.Context(), R(D{"day": "d"}), nil)
		if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "cannot verify") {
			t.Fatalf("got %v, want an unverifiable ErrSettingsMismatch", err)
		}
	})
}

func TestCodec_Describe(t *testing.T) {
	if d := NewJSONLCodec().Describe(); d != "" {
		t.Errorf("JSONL describes %q, want none", d)
	}
	schema := ParquetSchema{Fields: []ParquetField{{Name: "a", Type: ParquetInt64, Nullable: true}, {Name: "b", Type: ParquetString}}}
	c1, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := NewParquetCodec(schema, WithParquetCompression(ParquetCompressionGzip))
	if err != nil {
		t.Fatal(err)
	}
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
	jsonl := WithCodec(NewJSONLCodec())
	for _, tc := range []struct {
		name          string
		first, second []Option
	}{
		{"default, then Hive", []Option{jsonl}, []Option{jsonl, WithHiveLayout("a")}},
		{"Hive, then default", []Option{jsonl, WithHiveLayout("a")}, []Option{jsonl}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			if _, err := openOn(t, store, tc.first...).Write(t.Context(), R(D{"a": "x"}), nil); err != nil {
				t.Fatal(err)
			}
			_, err := openOn(t, store, tc.second...).Write(t.Context(), R(D{"a": "x"}), nil)
			if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "layout mismatch") {
				t.Fatalf("got %v, want a layout ErrSettingsMismatch", err)
			}
		})
	}
	t.Run("a pointerless foreign history is not an empty dataset", func(t *testing.T) {
		store := NewMemory()
		if _, err := openOn(t, store, jsonl, WithHiveLayout("a")).Write(t.Context(), R(D{"a": "x"}), nil); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(t.Context(), "datasets/events/latest"); err != nil {
			t.Fatal(err)
		}
		if _, err := openOn(t, store, jsonl).Write(t.Context(), R(D{"a": "x"}), nil); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestWrite_PartitionKeysCompareStructurally(t *testing.T) {
	store := NewMemory()
	jsonl := WithCodec(NewJSONLCodec())
	if _, err := openOn(t, store, jsonl, WithHiveLayout("a,b")).Write(t.Context(), R(D{"a,b": "x"}), nil); err != nil {
		t.Fatal(err)
	}
	_, err := openOn(t, store, jsonl, WithHiveLayout("a", "b")).Write(t.Context(), R(D{"a": "y", "b": "z"}), nil)
	if !errors.Is(err, ErrSettingsMismatch) {
		t.Fatalf("got %v, want keys \"a,b\" and \"a\", \"b\" told apart", err)
	}
}

func TestNewHiveLayout_KeysNamePartitionsUnambiguously(t *testing.T) {
	for _, key := range []string{"a=b", "a/b", "", ".."} {
		if _, err := NewHiveLayout(key); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("NewHiveLayout(%q) = %v, want ErrInvalidArgument", key, err)
		}
	}
}

func TestWrite_LegacyCheckIsRepeatedAfterAConflict(t *testing.T) {
	parquet := func(field string) Codec {
		c, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{{Name: field, Type: ParquetString, Nullable: true}}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	store := NewMemory()
	old, err := openOn(t, store, WithCodec(parquet("a"))).Write(t.Context(), R(D{"a": "x"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	legacyHive(t, store, old) // records no description
	racer := &beforePointerCAS{Store: store}
	loser := openOn(t, racer, WithCodec(parquet("b")), WithRetryCount(2))
	racer.hook = func() {
		if _, err := openOn(t, store, WithCodec(parquet("a"))).Write(t.Context(), R(D{"a": "y"}), nil); err != nil {
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
	first, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}
	pendFirst(t, store, first.ID)
	if _, err := openOn(t, unlisted{store}, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 2}), nil); err != nil {
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
	ds, err := NewDataset("events", NewFSFactory(root), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ds.Write(t.Context(), R(D{"a": 1}), nil); err != nil {
		t.Fatalf("got %v, want the pointer's lock file left by a failed first commit ignored", err)
	}
}

func TestWrite_ReturnedManifestDoesNotAliasTheLayout(t *testing.T) {
	store := NewMemory()
	ds := openOn(t, store, WithCodec(NewJSONLCodec()), WithHiveLayout("day"))
	snap, err := ds.Write(t.Context(), R(D{"day": "d"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	snap.Manifest.PartitionKeys[0] = "region"
	next, err := ds.Write(t.Context(), R(D{"day": "d"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(next.Manifest.PartitionKeys, []string{"day"}) {
		t.Errorf("keys = %v, want the layout's keys unaffected by a caller's edit", next.Manifest.PartitionKeys)
	}
}

func TestWrite_UnknownStagedObjectIsForeign(t *testing.T) {
	for _, key := range []string{
		"datasets/events/staged/not-a-manifest",
		"datasets/events/expired/not-a-snapshot",
		"datasets/events/expired/" + string(idAt(time.Now())) + "/nested",
	} {
		store := NewMemory()
		putObject(t, store, key)
		_, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 1}), nil)
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("%s: got %v, want an object no layout persists treated as foreign", key, err)
		}
	}
	store := NewMemory()
	id := string(idAt(time.Now()))
	putObject(t, store, "datasets/events/expired/"+id)
	putObject(t, store, "datasets/events/expired/"+id+".lock")
	if _, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 1}), nil); err != nil {
		t.Errorf("got %v, want a tombstone and its lock file accepted", err)
	}
}

func TestConfiguration_IsCopiedAtConstruction(t *testing.T) {
	keys := []string{"day"}
	layout, err := NewHiveLayout(keys...)
	if err != nil {
		t.Fatal(err)
	}
	keys[0] = "region"
	if got := layout.partitioner().keys(); !slices.Equal(got, []string{"day"}) {
		t.Errorf("layout keys = %v, want them unaffected by the caller's slice", got)
	}

	fields := []ParquetField{{Name: "a", Type: ParquetString}}
	codec, err := NewParquetCodec(ParquetSchema{Fields: fields})
	if err != nil {
		t.Fatal(err)
	}
	before := codec.Describe()
	fields[0] = ParquetField{Name: "b", Type: parquetTypeMax}
	if codec.Describe() != before {
		t.Errorf("description changed to %q after the caller's schema changed", codec.Describe())
	}
}

func TestWrite_LegacyParentCycleIsUnverifiable(t *testing.T) {
	store := NewMemory()
	ds := openOn(t, store, WithCodec(NewJSONLCodec()), WithHiveLayout("day"))
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

	_, err = openOn(t, store, WithCodec(NewJSONLCodec()), WithHiveLayout("day")).Write(t.Context(), R(D{"day": "d"}), nil)
	if !errors.Is(err, ErrSettingsMismatch) || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("got %v, want a cycle reported as unverifiable", err)
	}
}
