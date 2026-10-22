package lode

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
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
		moveObject(t, store, canonical(first.ID), "datasets/events/staged/"+string(first.ID)+".json")
		_, err := openOn(t, store, WithCodec(NewJSONLCodec()), WithCompressor(NewGzipCompressor())).Write(t.Context(), R(D{"a": 2}), nil)
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want the staged copy's settings enforced", err)
		}
		if _, err := openOn(t, store, WithCodec(NewJSONLCodec())).Write(t.Context(), R(D{"a": 2}), nil); err != nil {
			t.Fatalf("got %v, want a matching writer to adopt the pending commit", err)
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
