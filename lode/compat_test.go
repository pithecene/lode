package lode

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// v011Fixture holds datasets and a volume that Lode v0.11.0 wrote through its
// public API, with the default names, before the D11 surface change. It was
// generated once and is never rewritten: it is the data an earlier release
// left behind, which this release must read, extend, and maintain
// (CONTRACT_LAYOUT §Path Names, CHANGELOG §Upgrade Notes).
const v011Fixture = "testdata/v0.11.0"

// copyFixture copies a fixture into a temporary directory, so that a test
// may write to it without changing the fixture.
func copyFixture(t *testing.T, fixture string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS(fixture)); err != nil {
		t.Fatal(err)
	}
	return dir
}

// fieldsOf returns one field of every record, formatted and sorted.
func fieldsOf(records []D, field string) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, fmt.Sprint(r[field]))
	}
	slices.Sort(out)
	return out
}

// readAll reads every snapshot of a record dataset, oldest first.
func readAll(t *testing.T, ds *Records[D]) []D {
	t.Helper()
	snaps, err := ds.Snapshots(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var all []D
	for _, s := range snaps {
		rows, err := ds.Read(t.Context(), s.ID)
		if err != nil {
			t.Fatalf("Read %s: %v", s.ID, err)
		}
		all = append(all, rows...)
	}
	return all
}

// legacyManifest is a manifest of a writer before format 1.2.0, as its JSON fields.
func legacyManifest(id, parent string, fields map[string]any) string {
	m := map[string]any{
		"schema_name": "lode-manifest", "format_version": "1.0.0", "dataset_id": "d", "snapshot_id": id,
		"created_at": "2026-02-07T00:00:00Z", "metadata": map[string]any{}, "files": nil, "row_count": 0,
		"codec": "jsonl", "compressor": "noop", "partitioner": "noop",
	}
	if parent != "" {
		m["parent_snapshot_id"] = parent
	}
	for k, v := range fields {
		if m[k] = v; v == absent {
			delete(m, k)
		}
	}
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// absent as a field's value removes the field.
const absent = "\x00absent"

// Writers before format 1.2.0 recorded a write of no records with a null list.
func TestCompat_LegacyEmptySnapshot(t *testing.T) {
	t.Parallel()
	const first, empty = "1770422400000000000", "1770422400000000001"
	paths := map[string]any{"partitions": "partitions", "segments": "segments", "manifest": "manifest.json", "data": "data", "expired": "expired", "staged": "staged", "datasets": "datasets", "snapshots": "snapshots"}
	for _, tc := range []struct {
		name    string
		fields  map[string]any
		invalid bool
	}{
		{"null list", nil, false},
		{"empty list", map[string]any{"files": []any{}}, false},
		{"absent list", map[string]any{"files": absent}, false},
		{"format 1.1.0", map[string]any{"format_version": "1.1.0", "paths": paths}, false},
		{"format 1.1.0 without paths", map[string]any{"format_version": "1.1.0"}, true},
		{"rows", map[string]any{"row_count": 1}, true},
		{"rows and an absent list", map[string]any{"row_count": 1, "files": absent}, true},
		{"absent row count", map[string]any{"row_count": absent}, true},
		{"null row count", map[string]any{"row_count": nil}, true},
		{"blob", map[string]any{"codec": absent}, true},
		{"format 1.2.0", map[string]any{"format_version": "1.2.0"}, true},
	} {
		for _, atHead := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, at the head %t", tc.name, atHead), func(t *testing.T) {
				ctx, store := t.Context(), NewMemory()
				head, want := empty, 2
				putLegacy(t, store, "snapshots/"+first+"/data/data", "{\"n\":1}\n")
				putLegacy(t, store, "snapshots/"+first+"/manifest.json", legacyManifest(first, "", map[string]any{
					"files": []any{map[string]any{"path": "datasets/d/snapshots/" + first + "/data/data", "size_bytes": 8}}, "row_count": 1,
				}))
				putLegacy(t, store, "snapshots/"+empty+"/manifest.json", legacyManifest(empty, first, tc.fields))
				if !atHead {
					head, want = "1770422400000000002", 3
					putLegacy(t, store, "snapshots/"+head+"/manifest.json", legacyManifest(head, empty, map[string]any{"files": []any{}}))
				}
				putLegacy(t, store, "latest", head)
				view, err := OpenReadOnly(Use(store)).Records(ctx, "d", JSONL[D]())
				var snaps []*Snapshot
				if err == nil {
					snaps, err = view.Snapshots(ctx)
				}
				if tc.invalid {
					if !errors.Is(err, ErrManifestInvalid) {
						t.Fatalf("Snapshots = %d, %v; want ErrManifestInvalid", len(snaps), err)
					}
					return
				}
				if err != nil || len(snaps) != want {
					t.Fatalf("Snapshots = %d, %v; want %d", len(snaps), err, want)
				}
				for id, count := range map[SnapshotID]int{first: 1, empty: 0} {
					if rows, err := view.Read(ctx, id); err != nil || len(rows) != count {
						t.Errorf("Read %s = %v, %v; want %d rows", id, rows, err, count)
					}
				}
				ds, err := Open(Use(store)).Records(ctx, "d", JSONL[D]())
				if err != nil {
					t.Fatal(err)
				}
				snap, err := ds.Write(ctx, R(D{"n": 2}), nil)
				if err != nil || snap.ParentSnapshotID != SnapshotID(head) {
					t.Fatalf("Write = %+v, %v; want parent %s", snap, err, head)
				}
				report, err := ds.Expire(ctx, ExpireRequest{IDs: []SnapshotID{empty}})
				if err != nil || !slices.Equal(report.Expired, []SnapshotID{empty}) {
					t.Errorf("Expire = %+v, %v; want %s expired", report, err, empty)
				}
				if _, err := ds.Snapshot(ctx, empty); !errors.Is(err, ErrSnapshotExpired) {
					t.Errorf("Snapshot = %v, want ErrSnapshotExpired", err)
				}
			})
		}
	}
}

// putLegacy stores data at path of dataset d.
func putLegacy(t *testing.T, store Store, path, data string) {
	t.Helper()
	if err := store.Put(t.Context(), "datasets/d/"+path, strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// Sweep settles an interrupted expiry of a legacy empty snapshot by its tombstone,
// and a tombstone without an inventory authorizes nothing.
func TestCompat_LegacyEmptySnapshot_Sweep(t *testing.T) {
	t.Parallel()
	const empty, head = "1770422400000000001", "1770422400000000002"
	for files, settles := range map[string]bool{"[]": true, "null": false} {
		t.Run("files "+files, func(t *testing.T) {
			ctx, store := t.Context(), NewMemory()
			putLegacy(t, store, "snapshots/"+empty+"/manifest.json", legacyManifest(empty, "", nil))
			putLegacy(t, store, "snapshots/"+head+"/manifest.json", legacyManifest(head, empty, map[string]any{"files": []any{}}))
			putLegacy(t, store, "latest", head)
			putLegacy(t, store, "expired/"+empty, fmt.Sprintf(`{"snapshot_id": %q, "requested_at": "2026-02-08T00:00:00Z", "manifest_created_at": "2026-02-07T00:00:00Z", "files": %s}`, empty, files))
			ds, err := Open(Use(store)).Records(ctx, "d", JSONL[D]())
			if err != nil {
				t.Fatal(err)
			}
			report, err := ds.Sweep(ctx, SweepRequest{Grace: time.Hour})
			if err != nil || slices.Equal(report.Settled, []SnapshotID{empty}) != settles || (len(report.Unparseable) == 0) != settles {
				t.Errorf("Sweep = %+v, %v; want settled = %t", report, err, settles)
			}
			if kept, err := store.Exists(ctx, "datasets/d/snapshots/"+empty+"/manifest.json"); err != nil || kept == settles {
				t.Errorf("manifest kept = %t, %v; want %t", kept, err, !settles)
			}
		})
	}
}

func TestCompat_v0_11_0_HiveRecords(t *testing.T) {
	t.Parallel()
	ds, err := Open(FS(copyFixture(t, v011Fixture))).Records(t.Context(), "events", JSONL[D](), hive("day"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("reads every snapshot", func(t *testing.T) {
		if got, want := fieldsOf(readAll(t, ds), "id"), []string{"1", "2", "3", "4", "5"}; !slices.Equal(got, want) {
			t.Errorf("ids = %v, want %v", got, want)
		}
	})

	t.Run("lists partitions and manifests", func(t *testing.T) {
		parts, err := ds.ListPartitions(t.Context(), PartitionListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := refPaths(parts), []string{"day=2024-01-15", "day=2024-01-16", "day=2024-01-17"}; !slices.Equal(got, want) {
			t.Errorf("partitions = %v, want %v", got, want)
		}
		loaded := collect(t, ds, PartitionPath{}, ManifestListOptions{})
		if len(loaded) != 2 {
			t.Errorf("Manifests = %v; want both snapshots", loaded)
		}
	})

	t.Run("appends to the history", func(t *testing.T) {
		head, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		snap, err := ds.Write(t.Context(), R(D{"id": 6, "day": "2024-01-17", "event": "logout"}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if snap.ParentSnapshotID != head.ID {
			t.Errorf("parent = %s, want the v0.11.0 head %s", snap.ParentSnapshotID, head.ID)
		}
		if got := fieldsOf(readAll(t, ds), "id"); len(got) != 6 {
			t.Errorf("ids after the write = %v, want six", got)
		}
	})

	t.Run("expires and sweeps", func(t *testing.T) {
		snaps, err := ds.Snapshots(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		oldest := snaps[0].ID
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []SnapshotID{oldest}})
		if err != nil || !slices.Equal(report.Expired, []SnapshotID{oldest}) {
			t.Fatalf("Expire = %+v, %v; want %s expired", report, err, oldest)
		}
		if _, err := ds.Snapshot(t.Context(), oldest); !errors.Is(err, ErrSnapshotExpired) {
			t.Errorf("Snapshot(%s) = %v, want ErrSnapshotExpired", oldest, err)
		}
		swept, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Nanosecond})
		if err != nil {
			t.Fatal(err)
		}
		if swept.Objects != 0 || len(swept.Reindexed) != 0 || len(swept.Unparseable) != 0 {
			t.Errorf("Sweep = %+v; want nothing left to reclaim, reindex, or reject", swept)
		}
	})
}

func TestCompat_v0_11_0_CompressedRecords(t *testing.T) {
	t.Parallel()
	dir := copyFixture(t, v011Fixture)

	t.Run("a view takes the compressor from the manifests", func(t *testing.T) {
		view, err := OpenReadOnly(FS(dir)).Records(t.Context(), "logs", JSONL[D]())
		if err != nil {
			t.Fatal(err)
		}
		head, err := view.Latest(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		rows, err := view.Read(t.Context(), head.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got := fieldsOf(rows, "line"); !slices.Equal(got, []string{"stopped"}) {
			t.Errorf("head rows = %v, want [stopped]", got)
		}
	})

	t.Run("a writer with the recorded compressor appends", func(t *testing.T) {
		ds, err := Open(FS(dir)).Records(t.Context(), "logs", JSONL[D](), WithCompressor(Gzip()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ds.Write(t.Context(), R(D{"line": "restarted"}), nil); err != nil {
			t.Fatal(err)
		}
		want := []string{"ready", "restarted", "started", "stopped"}
		if got := fieldsOf(readAll(t, ds), "line"); !slices.Equal(got, want) {
			t.Errorf("lines = %v, want %v", got, want)
		}
	})

	t.Run("a writer without it is rejected", func(t *testing.T) {
		_, err := Open(FS(dir)).Records(t.Context(), "logs", JSONL[D]())
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("open without the compressor = %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestCompat_v0_11_0_Blob(t *testing.T) {
	t.Parallel()
	blobs, err := Open(FS(copyFixture(t, v011Fixture))).Blobs(t.Context(), "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	head, err := blobs.Latest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data, err := blobs.Read(t.Context(), head.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "model weights v1" {
		t.Errorf("blob = %q, want the bytes v0.11.0 wrote", data)
	}
}

func TestManifestLegacyFieldsAreVersionScoped(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"1.0.0", "1.1.0", "1.2.0"} {
		t.Run(version, func(t *testing.T) {
			var m Manifest
			data := []byte(`{"format_version":"` + version + `","files":[],"paths":{"flat":true}}`)
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			legacyFields := version != "1.2.0"
			if (m.Objects != nil) != legacyFields || m.Flat != legacyFields {
				t.Errorf("objects = %v, flat = %t, legacy fields accepted = %t", m.Objects, m.Flat, legacyFields)
			}
		})
	}
}
