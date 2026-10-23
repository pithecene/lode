package lode

import (
	"errors"
	"slices"
	"testing"
)

// customOptions are root options naming every directory and file other than
// the defaults.
func customOptions() []RootOption {
	return []RootOption{
		WithDatasetsDir("tables"), WithSnapshotsDir("snaps"), WithPartitionsDir("parts"),
		WithSegmentsDir("segs"), WithManifestFile("_m.json"), WithDataDir("d"),
		WithExpiredDir("gone"), WithStagedDir("pending"),
	}
}

func TestRootPaths_CustomNames(t *testing.T) {
	store := NewMemory()
	root := Open(Use(store), customOptions()...)

	plain, err := root.Records(t.Context(), "events", jsonl)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := plain.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	partitioned, err := root.Records(t.Context(), "logs", jsonl, hive("day"))
	if err != nil {
		t.Fatal(err)
	}
	logged, err := partitioned.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"tables/events/latest",
		"tables/events/snaps/" + string(snap.ID) + "/_m.json",
		"tables/events/snaps/" + string(snap.ID) + "/d/data",
		"tables/logs/latest",
		"tables/logs/parts/day=d1/segs/" + string(logged.ID) + "/_m.json",
		"tables/logs/parts/day=d1/segs/" + string(logged.ID) + "/d/data",
		"tables/logs/segs/" + string(logged.ID) + "/_m.json",
	}
	if got := storeKeys(t, store); !slices.Equal(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if m := readManifest(t, store, "tables/events/snaps/"+string(snap.ID)+"/_m.json"); m.Paths != customNames().convention() {
		t.Errorf("recorded paths = %+v, want the custom names", m.Paths)
	}

	t.Run("a root with the same names reads them", func(t *testing.T) {
		view, err := OpenReadOnly(Use(store), customOptions()...).Records(t.Context(), "logs", jsonl)
		if err != nil {
			t.Fatal(err)
		}
		got, err := view.Read(t.Context(), logged.ID)
		if err != nil || len(got) != 1 {
			t.Fatalf("Read = %v, %v; want the one record", got, err)
		}
		datasets, err := root.ListDatasets(t.Context(), DatasetListOptions{})
		if err != nil || !slices.Equal(datasets, []DatasetID{"events", "logs"}) {
			t.Fatalf("ListDatasets = %v, %v; want both datasets", datasets, err)
		}
	})
	t.Run("a root with another datasets directory sees another dataset", func(t *testing.T) {
		if _, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})
	t.Run("a root sharing the pointer but not the manifest name is rejected", func(t *testing.T) {
		opts := append(customOptions(), WithManifestFile("manifest.json"))
		if _, err := Open(Use(store), opts...).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("writer: got %v, want ErrSettingsMismatch", err)
		}
		if _, err := OpenReadOnly(Use(store), opts...).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("view: got %v, want ErrSettingsMismatch", err)
		}
	})
	t.Run("a root sharing the manifest but not the data name is rejected", func(t *testing.T) {
		opts := append(customOptions(), WithDataDir("data"))
		if _, err := Open(Use(store), opts...).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("writer: got %v, want ErrSettingsMismatch", err)
		}
		if _, err := OpenReadOnly(Use(store), opts...).Records(t.Context(), "events", jsonl); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("view: got %v, want ErrSettingsMismatch", err)
		}
	})
}

func TestRootPaths_InvalidNames(t *testing.T) {
	cases := map[string][]RootOption{
		"empty":                          {WithDataDir("")},
		"slash":                          {WithManifestFile("a/b")},
		"backslash":                      {WithSegmentsDir(`a\b`)},
		"dot":                            {WithStagedDir(".")},
		"dot dot":                        {WithExpiredDir("..")},
		"equals":                         {WithPartitionsDir("k=v")},
		"reserved prefix":                {WithSnapshotsDir(".lode-tmp-x")},
		"the pointer's name":             {WithDatasetsDir("latest")},
		"duplicate":                      {WithDataDir("x"), WithStagedDir("x")},
		"a default name used twice":      {WithDataDir("snapshots")},
		"flat with a datasets directory": {WithFlatPaths(), WithDatasetsDir("tables")},
		"flat with a snapshots dir":      {WithFlatPaths(), WithSnapshotsDir("snaps")},
		"nil":                            {nil},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			root := Open(Memory(), opts...)
			if _, err := root.Records(t.Context(), "events", jsonl); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("Records: got %v, want ErrInvalidArgument", err)
			}
			if _, err := root.ListDatasets(t.Context(), DatasetListOptions{}); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("ListDatasets: got %v, want ErrInvalidArgument", err)
			}
			if _, err := OpenReadOnly(Memory(), opts...).Blobs(t.Context(), "events"); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("ReadRoot.Blobs: got %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestRootPaths_Flat(t *testing.T) {
	store := NewMemory()
	root := Open(Use(store), WithFlatPaths())
	partitioned, err := root.Records(t.Context(), "logs", jsonl, hive("day"))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := partitioned.Write(t.Context(), R(D{"day": "d1"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"logs/latest",
		"logs/partitions/day=d1/segments/" + string(snap.ID) + "/data/data",
		"logs/partitions/day=d1/segments/" + string(snap.ID) + "/manifest.json",
		"logs/segments/" + string(snap.ID) + "/manifest.json",
	}
	if got := storeKeys(t, store); !slices.Equal(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
	if _, err := root.ListDatasets(t.Context(), DatasetListOptions{}); !errors.Is(err, ErrDatasetsNotModeled) {
		t.Errorf("ListDatasets = %v, want ErrDatasetsNotModeled", err)
	}
	view, err := OpenReadOnly(Use(store), WithFlatPaths()).Records(t.Context(), "logs", jsonl)
	if err != nil {
		t.Fatal(err)
	}
	if parts, err := view.ListPartitions(t.Context(), PartitionListOptions{}); err != nil || len(parts) != 1 || parts[0].Path != "day=d1" {
		t.Errorf("ListPartitions = %v, %v; want day=d1", parts, err)
	}
}
