package lode

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRootPaths_Flat(t *testing.T) {
	store := NewMemory()
	root := Open(Use(store), WithFlatPaths())

	plain, err := root.Records(t.Context(), "events", jsonl)
	if err != nil {
		t.Fatal(err)
	}
	plainSnap, err := plain.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}

	partitioned, err := root.Records(t.Context(), "logs", jsonl, hive("day"))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := partitioned.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"events/" + string(plainSnap.ID) + "/data/data",
		"events/" + string(plainSnap.ID) + "/manifest.json",
		"events/latest",
		"logs/latest",
		"logs/partitions/day=d1/segments/" + string(snap.ID) + "/data/data",
		"logs/partitions/day=d1/segments/" + string(snap.ID) + "/manifest.json",
		"logs/segments/" + string(snap.ID) + "/manifest.json",
	}
	if got := storeKeys(t, store); !slices.Equal(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}

	// Format 1.2.0 persists "objects" and "flat":true, omitting 1.1.0 "paths" and "files".
	rawFlat := readObject(t, store, "events/"+string(plainSnap.ID)+"/manifest.json")
	var wireFlat map[string]any
	if err := json.Unmarshal([]byte(rawFlat), &wireFlat); err != nil {
		t.Fatal(err)
	}
	if wireFlat["format_version"] != "1.2.0" || wireFlat["flat"] != true || wireFlat["objects"] == nil || wireFlat["files"] != nil || wireFlat["paths"] != nil {
		t.Errorf("flat 1.2.0 manifest wire JSON = %s", rawFlat)
	}

	// Default root omits "flat", "paths", and "files".
	defStore := NewMemory()
	defSnap, err := recordsOn(t, defStore, "events").Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	rawDef := readObject(t, defStore, "datasets/events/snapshots/"+string(defSnap.ID)+"/manifest.json")
	var wireDef map[string]any
	if err := json.Unmarshal([]byte(rawDef), &wireDef); err != nil {
		t.Fatal(err)
	}
	if wireDef["format_version"] != "1.2.0" || wireDef["flat"] != nil || wireDef["objects"] == nil || wireDef["files"] != nil || wireDef["paths"] != nil {
		t.Errorf("default 1.2.0 manifest wire JSON = %s", rawDef)
	}

	if _, err := root.ListDatasets(t.Context(), DatasetListOptions{}); !errors.Is(err, ErrDatasetsNotModeled) {
		t.Errorf("ListDatasets = %v, want ErrDatasetsNotModeled", err)
	}
	if _, err := OpenReadOnly(Use(store), WithFlatPaths()).ListDatasets(t.Context(), DatasetListOptions{}); !errors.Is(err, ErrDatasetsNotModeled) {
		t.Errorf("ReadRoot.ListDatasets = %v, want ErrDatasetsNotModeled", err)
	}
	view, err := OpenReadOnly(Use(store), WithFlatPaths()).Records(t.Context(), "logs", jsonl)
	if err != nil {
		t.Fatal(err)
	}
	if parts, err := view.ListPartitions(t.Context(), PartitionListOptions{}); err != nil || len(parts) != 1 || parts[0].String() != "day=d1" {
		t.Errorf("ListPartitions = %v, %v; want [day=d1]", parts, err)
	}
}

func TestRootPaths_InvalidOptions(t *testing.T) {
	root := Open(Memory(), nil)
	if _, err := root.Records(t.Context(), "events", jsonl); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Records: got %v, want ErrInvalidArgument", err)
	}
	if _, err := root.ListDatasets(t.Context(), DatasetListOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("ListDatasets: got %v, want ErrInvalidArgument", err)
	}
	if err := root.RemoveDataset(t.Context(), "events"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("RemoveDataset: got %v, want ErrInvalidArgument", err)
	}
	if _, err := OpenReadOnly(Memory(), nil).Blobs(t.Context(), "events"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("ReadRoot.Blobs: got %v, want ErrInvalidArgument", err)
	}
}

func TestRootPaths_LegacyV11Compat(t *testing.T) {
	t.Run("default 1.1.0 paths and files accepted", func(t *testing.T) {
		store := NewMemory()
		snapID := idAt(time.Now().Add(-time.Hour))
		dataKey := "datasets/events/snapshots/" + string(snapID) + "/data/data"
		manifestKey := "datasets/events/snapshots/" + string(snapID) + "/manifest.json"
		if err := store.Put(t.Context(), dataKey, strings.NewReader(`{"n":1}`+"\n")); err != nil {
			t.Fatal(err)
		}
		v11JSON := `{
			"schema_name": "lode-manifest",
			"format_version": "1.1.0",
			"dataset_id": "events",
			"snapshot_id": "` + string(snapID) + `",
			"created_at": "2026-01-15T10:00:00Z",
			"metadata": {},
			"files": [{"path": "` + dataKey + `", "size_bytes": 8}],
			"row_count": 1,
			"codec": "jsonl",
			"compressor": "noop",
			"partitioner": "noop",
			"paths": {
				"datasets": "datasets",
				"snapshots": "snapshots",
				"partitions": "partitions",
				"segments": "segments",
				"manifest": "manifest.json",
				"data": "data",
				"expired": "expired",
				"staged": "staged"
			}
		}`
		if err := store.Put(t.Context(), manifestKey, strings.NewReader(v11JSON)); err != nil {
			t.Fatal(err)
		}
		setPointer(t, store, string(snapID))

		view, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl)
		if err != nil {
			t.Fatalf("ReadRoot.Records: %v", err)
		}
		loaded, err := view.Snapshot(t.Context(), snapID)
		if err != nil || len(loaded.Objects) != 1 || loaded.Objects[0].Path != dataKey {
			t.Fatalf("Snapshot = %+v, %v; want Objects decoded from 1.1.0 files", loaded, err)
		}
		rows, err := view.Read(t.Context(), snapID)
		if err != nil || len(rows) != 1 || rows[0]["n"] != float64(1) {
			t.Fatalf("Read = %v, %v", rows, err)
		}

		ds, err := Open(Use(store)).Records(t.Context(), "events", jsonl)
		if err != nil {
			t.Fatalf("Root.Records: %v", err)
		}
		next, err := ds.Write(t.Context(), R(D{"n": 2}), nil)
		if err != nil || next.FormatVersion != "1.2.0" || next.ParentSnapshotID != snapID {
			t.Fatalf("Write = %+v, %v; want 1.2.0 child on 1.1.0 parent", next, err)
		}
	})

	t.Run("flat 1.1.0 paths and files accepted", func(t *testing.T) {
		store := NewMemory()
		snapID := idAt(time.Now().Add(-time.Hour))
		dataKey := "events/" + string(snapID) + "/data/data"
		manifestKey := "events/" + string(snapID) + "/manifest.json"
		if err := store.Put(t.Context(), dataKey, strings.NewReader(`{"n":1}`+"\n")); err != nil {
			t.Fatal(err)
		}
		v11Flat := `{
			"schema_name": "lode-manifest",
			"format_version": "1.1.0",
			"dataset_id": "events",
			"snapshot_id": "` + string(snapID) + `",
			"created_at": "2026-01-15T10:00:00Z",
			"metadata": {},
			"files": [{"path": "` + dataKey + `", "size_bytes": 8}],
			"row_count": 1,
			"codec": "jsonl",
			"compressor": "noop",
			"partitioner": "noop",
			"paths": {
				"partitions": "partitions",
				"segments": "segments",
				"manifest": "manifest.json",
				"data": "data",
				"expired": "expired",
				"staged": "staged",
				"flat": true
			}
		}`
		if err := store.Put(t.Context(), manifestKey, strings.NewReader(v11Flat)); err != nil {
			t.Fatal(err)
		}
		if err := store.Put(t.Context(), "events/latest", strings.NewReader(string(snapID))); err != nil {
			t.Fatal(err)
		}

		view, err := OpenReadOnly(Use(store), WithFlatPaths()).Records(t.Context(), "events", jsonl)
		if err != nil {
			t.Fatalf("flat ReadRoot.Records: %v", err)
		}
		if rows, err := view.Read(t.Context(), snapID); err != nil || len(rows) != 1 {
			t.Fatalf("flat Read = %v, %v", rows, err)
		}
		if _, err := Open(Use(store), WithFlatPaths()).Records(t.Context(), "events", jsonl); err != nil {
			t.Fatalf("flat Root.Records: %v", err)
		}
	})

	t.Run("custom 1.1.0 directory names rejected", func(t *testing.T) {
		store := NewMemory()
		snapID := idAt(time.Now().Add(-time.Hour))
		manifestKey := "datasets/events/snapshots/" + string(snapID) + "/manifest.json"
		v11Custom := `{
			"schema_name": "lode-manifest",
			"format_version": "1.1.0",
			"dataset_id": "events",
			"snapshot_id": "` + string(snapID) + `",
			"created_at": "2026-01-15T10:00:00Z",
			"metadata": {},
			"files": [],
			"row_count": 0,
			"codec": "jsonl",
			"compressor": "noop",
			"partitioner": "noop",
			"paths": {
				"datasets": "datasets",
				"snapshots": "snapshots",
				"partitions": "partitions",
				"segments": "segments",
				"manifest": "manifest.json",
				"data": "custom_data",
				"expired": "expired",
				"staged": "staged"
			}
		}`
		if err := store.Put(t.Context(), manifestKey, strings.NewReader(v11Custom)); err != nil {
			t.Fatal(err)
		}
		setPointer(t, store, string(snapID))

		if _, err := Open(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("writer: got %v, want ErrSettingsMismatch for custom 1.1.0 dir", err)
		}
		if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("view: got %v, want ErrSettingsMismatch for custom 1.1.0 dir", err)
		}
	})

	t.Run("legacy 1.1.0 tombstone files decoded", func(t *testing.T) {
		store := NewMemory()
		ds := newDefaultRetention(t, store)
		first := writeRecord(t, ds, 1)
		writeRecord(t, ds, 2)
		v11Tomb := `{
			"snapshot_id": "` + string(first.ID) + `",
			"requested_at": "2026-01-15T10:00:00Z",
			"manifest_created_at": "` + first.CreatedAt.Format(time.RFC3339Nano) + `",
			"files": [{"path": "` + first.Objects[0].Path + `", "size_bytes": ` + "6" + `}]
		}`
		// Match exact recorded size so tombstone.describes(manifest) holds.
		var parsed tombstone
		if err := json.Unmarshal([]byte(v11Tomb), &parsed); err != nil {
			t.Fatal(err)
		}
		parsed.Objects[0].SizeBytes = first.Objects[0].SizeBytes
		raw, err := json.Marshal(struct {
			SnapshotID        SnapshotID  `json:"snapshot_id"`
			RequestedAt       time.Time   `json:"requested_at"`
			ManifestCreatedAt time.Time   `json:"manifest_created_at"`
			Files             []ObjectRef `json:"files"`
		}{parsed.SnapshotID, parsed.RequestedAt, parsed.ManifestCreatedAt, parsed.Objects})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(t.Context(), "datasets/events/expired/"+string(first.ID), strings.NewReader(string(raw))); err != nil {
			t.Fatal(err)
		}
		report, err := ds.Sweep(t.Context(), SweepRequest{Grace: time.Hour})
		if err != nil || !slices.Equal(report.Settled, []SnapshotID{first.ID}) || report.Bytes != first.Objects[0].SizeBytes {
			t.Fatalf("Sweep = %+v, %v; want 1.1.0 tombstone settled with recorded bytes", report, err)
		}
	})
}
