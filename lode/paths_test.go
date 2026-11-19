package lode

import (
	"encoding/json"
	"errors"
	"fmt"
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

func putFormat11Manifest(t *testing.T, store Store, manifestKey string, snapID SnapshotID, filesJSON, pathsJSON string, rowCount int) {
	t.Helper()
	doc := fmt.Sprintf(`{"schema_name":"lode-manifest","format_version":"1.1.0","dataset_id":"events","snapshot_id":%q,"created_at":"2026-01-15T10:00:00Z","metadata":{},"files":%s,"row_count":%d,"codec":"jsonl","compressor":"noop","partitioner":"noop","paths":%s}`,
		snapID, filesJSON, rowCount, pathsJSON)
	if err := store.Put(t.Context(), manifestKey, strings.NewReader(doc)); err != nil {
		t.Fatal(err)
	}
}

func TestRootPaths_ManifestFormat11Compat(t *testing.T) {
	for _, tc := range []struct {
		name      string
		dataFmt   string
		manFmt    string
		ptrKey    string
		pathsJSON string
		opts      []RootOption
	}{
		{
			name:      "default format_version 1.1.0 paths and files accepted",
			dataFmt:   "datasets/events/snapshots/%s/data/data",
			manFmt:    "datasets/events/snapshots/%s/manifest.json",
			ptrKey:    "datasets/events/latest",
			pathsJSON: `{"datasets":"datasets","snapshots":"snapshots","partitions":"partitions","segments":"segments","manifest":"manifest.json","data":"data","expired":"expired","staged":"staged"}`,
		},
		{
			name:      "flat format_version 1.1.0 paths and files accepted",
			dataFmt:   "events/%s/data/data",
			manFmt:    "events/%s/manifest.json",
			ptrKey:    "events/latest",
			pathsJSON: `{"partitions":"partitions","segments":"segments","manifest":"manifest.json","data":"data","expired":"expired","staged":"staged","flat":true}`,
			opts:      []RootOption{WithFlatPaths()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemory()
			snapID := idAt(time.Now().Add(-time.Hour))
			dataKey := fmt.Sprintf(tc.dataFmt, snapID)
			manifestKey := fmt.Sprintf(tc.manFmt, snapID)
			if err := store.Put(t.Context(), dataKey, strings.NewReader(`{"n":1}`+"\n")); err != nil {
				t.Fatal(err)
			}
			putFormat11Manifest(t, store, manifestKey, snapID, `[{"path":"`+dataKey+`","size_bytes":8}]`, tc.pathsJSON, 1)
			if err := store.Put(t.Context(), tc.ptrKey, strings.NewReader(string(snapID))); err != nil {
				t.Fatal(err)
			}
			view, err := OpenReadOnly(Use(store), tc.opts...).Records(t.Context(), "events", jsonl)
			if err != nil {
				t.Fatalf("ReadRoot.Records: %v", err)
			}
			if rows, err := view.Read(t.Context(), snapID); err != nil || len(rows) != 1 {
				t.Fatalf("Read = %v, %v", rows, err)
			}
			if _, err := Open(Use(store), tc.opts...).Records(t.Context(), "events", jsonl); err != nil {
				t.Fatalf("Root.Records: %v", err)
			}
		})
	}

	t.Run("custom format_version 1.1.0 directory names rejected", func(t *testing.T) {
		store := NewMemory()
		snapID := idAt(time.Now().Add(-time.Hour))
		manifestKey := "datasets/events/snapshots/" + string(snapID) + "/manifest.json"
		putFormat11Manifest(t, store, manifestKey, snapID, `[]`, `{"datasets":"datasets","snapshots":"snapshots","partitions":"partitions","segments":"segments","manifest":"manifest.json","data":"custom_data","expired":"expired","staged":"staged"}`, 0)
		setPointer(t, store, string(snapID))

		if _, err := Open(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("writer: got %v, want ErrSettingsMismatch", err)
		}
		if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Errorf("view: got %v, want ErrSettingsMismatch", err)
		}
	})
}
