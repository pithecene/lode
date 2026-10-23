package lode

import (
	"errors"
	"fmt"
	"os"
	"slices"
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
func readAll(t *testing.T, ds Records[D]) []D {
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

func TestCompat_v0_11_0_HiveRecords(t *testing.T) {
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
		refs, err := ds.ListManifests(t.Context(), PartitionPath{}, ManifestListOptions{})
		if err != nil || len(refs) != 2 {
			t.Errorf("ListManifests = %v, %v; want both snapshots", refs, err)
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
		report, err := ds.Expire(t.Context(), ExpireRequest{IDs: []DatasetSnapshotID{oldest}})
		if err != nil || !slices.Equal(report.Expired, []DatasetSnapshotID{oldest}) {
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
		ds, err := Open(FS(dir)).Records(t.Context(), "logs", JSONL[D](), WithCompressor(NewGzipCompressor()))
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
