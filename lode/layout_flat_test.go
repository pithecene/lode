package lode

import (
	"bytes"
	"testing"
)

// -----------------------------------------------------------------------------
// Flat paths lifecycle tests
// -----------------------------------------------------------------------------

// flatBlobs opens a blob dataset under a root with flat paths.
func flatBlobs(t *testing.T, id DatasetID) Blobs {
	t.Helper()
	ds, err := Open(Memory(), WithFlatPaths()).Blobs(t.Context(), id)
	if err != nil {
		t.Fatalf("Blobs: %v", err)
	}
	return ds
}

// flatLayout is the unpartitioned layout under flat names.
func flatLayout() *pathLayout {
	return newPathLayout(defaultPaths(true), noopScheme)
}

func TestFlatLayout(t *testing.T) {
	t.Run("write and read", func(t *testing.T) {
		ds := flatBlobs(t, "myds")

		payload := []byte("flat-layout-data")
		snap, err := ds.Write(t.Context(), payload, D{"tag": "flat"})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}

		if snap.ID == "" {
			t.Fatal("expected non-empty snapshot ID")
		}
		if tag := metadataOf(t, snap.Metadata)["tag"]; tag != "flat" {
			t.Errorf("expected metadata tag=flat, got %v", tag)
		}

		readBytes, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if !bytes.Equal(readBytes, payload) {
			t.Errorf("expected %q, got %q", payload, readBytes)
		}
	})

	t.Run("multiple snapshots", func(t *testing.T) {
		ds := flatBlobs(t, "multi")

		snap1, err := ds.Write(t.Context(), []byte("first"), D{"n": "1"})
		if err != nil {
			t.Fatalf("Write 1: %v", err)
		}

		snap2, err := ds.Write(t.Context(), []byte("second"), D{"n": "2"})
		if err != nil {
			t.Fatalf("Write 2: %v", err)
		}

		if snap1.ID == snap2.ID {
			t.Fatal("snapshots must have distinct IDs")
		}

		// Both snapshots should be readable
		data1, err := ds.Read(t.Context(), snap1.ID)
		if err != nil {
			t.Fatalf("Read snap1: %v", err)
		}
		if string(data1) != "first" {
			t.Errorf("snap1 data mismatch: %q", data1)
		}

		data2, err := ds.Read(t.Context(), snap2.ID)
		if err != nil {
			t.Fatalf("Read snap2: %v", err)
		}
		if string(data2) != "second" {
			t.Errorf("snap2 data mismatch: %q", data2)
		}

		// Latest should be snap2
		latest, err := ds.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if latest.ID != snap2.ID {
			t.Errorf("expected latest=%s, got %s", snap2.ID, latest.ID)
		}
	})

	t.Run("path generation", func(t *testing.T) {
		fl := flatLayout()

		dataset := DatasetID("myds")
		segment := DatasetSnapshotID("snap-abc")

		tests := []struct {
			name string
			got  string
			want string
		}{
			{
				name: "manifest path",
				got:  fl.manifestPath(dataset, segment),
				want: "myds/snap-abc/manifest.json",
			},
			{
				name: "data file path",
				got:  fl.dataFilePath(dataset, segment, "", "records.jsonl"),
				want: "myds/snap-abc/data/records.jsonl",
			},
			{
				name: "latest pointer path",
				got:  fl.latestPointerPath(dataset),
				want: "myds/latest",
			},
			{
				name: "segments prefix",
				got:  fl.snapshotsPrefix(dataset),
				want: "myds/",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.got != tt.want {
					t.Errorf("got %q, want %q", tt.got, tt.want)
				}
			})
		}

		// Verify manifest parsing
		manifestP := "myds/snap-abc/manifest.json"
		if !isManifestPath(fl, manifestP) {
			t.Errorf("expected manifest(%q) to parse", manifestP)
		}
		if got := manifestFields(fl, manifestP).dataset; got != dataset {
			t.Errorf("manifest dataset: got %q, want %q", got, dataset)
		}
		if got := manifestFields(fl, manifestP).snapshot; got != segment {
			t.Errorf("manifest snapshot: got %q, want %q", got, segment)
		}

		// Verify non-manifest path
		if isManifestPath(fl, "myds/snap-abc/data/file.jsonl") {
			t.Error("data file should not be recognized as manifest")
		}

		// Verify the unpartitioned flat layout has no partitions
		if fl.supportsPartitions() {
			t.Error("FlatLayout should not support partitions")
		}
	})
}
