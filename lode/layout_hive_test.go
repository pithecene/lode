package lode

import "testing"

// Unit tests for hiveLayout path parsing. The reader tests exercise these
// through real datasets, but only on well-formed paths at fixed depths, so
// the index arithmetic (loop starts, -1 sentinels, boundary checks) was not
// observed by any test. These pin the edges directly.
//
// Known equivalent mutants, left alive: `len(parts) < 4` → `<=` (a 4-part
// path can never be a manifest); the `-1` sentinel inversions and `< 0`
// → `<= 0` checks on partitionsIdx/segmentsIdx (indices are never 0 or 1);
// the loop upper bounds in isManifest/parseSegmentID (the isManifest guard
// proves a segments marker exists before the loop could overrun); and
// `segmentsIdx <= partitionsIdx+1` → `<` (segments directly after partitions
// yields an empty join either way).
//
// The loop starts are not equivalent: scanning from index 1 would mistake a
// dataset named "segments" or "partitions" for the marker. Those names are
// covered below as reserved-name cases.

func hiveForTest(t *testing.T) *hiveLayout {
	t.Helper()
	l, err := NewHiveLayout("day")
	if err != nil {
		t.Fatal(err)
	}
	return l.(*hiveLayout)
}

func TestHiveLayout_IsManifest(t *testing.T) {
	l := hiveForTest(t)
	cases := []struct {
		path string
		want bool
	}{
		{"datasets/ds/segments/1/manifest.json", true},
		{"datasets/ds/partitions/day=1/segments/1/manifest.json", true},
		{"datasets/ds/partitions/day=1/region=us/segments/1/manifest.json", true},
		// Wrong root, empty dataset, wrong leaf.
		{"other/ds/segments/1/manifest.json", false},
		{"datasets//segments/1/manifest.json", false},
		{"datasets/ds/segments/1/manifest.json.bak", false},
		{"datasets/ds/segments/1/data/manifest.json", false},
		// Too shallow: no room for segments/<id>/manifest.json.
		{"datasets/ds/manifest.json", false},
		{"datasets/ds/x/manifest.json", false},
		{"datasets/ds/segments/manifest.json", false},
		// Empty segment ID.
		{"datasets/ds/segments//manifest.json", false},
		// A "latest" pointer is not a manifest.
		{"datasets/ds/latest", false},
		// A dataset named "segments" with no segments directory beneath it:
		// the dataset ID must not be read as the marker.
		{"datasets/segments/1/manifest.json", false},
		{"datasets/segments/segments/1/manifest.json", true},
	}
	for _, c := range cases {
		if got := l.isManifest(c.path); got != c.want {
			t.Errorf("isManifest(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestHiveLayout_ParseIDs(t *testing.T) {
	l := hiveForTest(t)
	cases := []struct {
		path        string
		wantDataset DatasetID
		wantSegment DatasetSnapshotID
	}{
		{"datasets/ds/segments/1/manifest.json", "ds", "1"},
		{"datasets/ds/partitions/day=1/segments/7/manifest.json", "ds", "7"},
		{"datasets/ds/partitions/day=1/region=us/segments/7/manifest.json", "ds", "7"},
		// Non-manifests parse to empty IDs rather than partial values.
		{"datasets/ds/segments//manifest.json", "", ""},
		{"datasets/ds/segments/1/data/f.jsonl", "", ""},
	}
	for _, c := range cases {
		if got := l.parseDatasetID(c.path); got != c.wantDataset {
			t.Errorf("parseDatasetID(%q) = %q, want %q", c.path, got, c.wantDataset)
		}
		if got := l.parseSegmentID(c.path); got != c.wantSegment {
			t.Errorf("parseSegmentID(%q) = %q, want %q", c.path, got, c.wantSegment)
		}
	}
}

func TestHiveLayout_ParsePartitionFromManifest(t *testing.T) {
	l := hiveForTest(t)
	cases := []struct {
		path string
		want string
	}{
		{"datasets/ds/segments/1/manifest.json", ""},
		{"datasets/ds/partitions/day=1/segments/1/manifest.json", "day=1"},
		{"datasets/ds/partitions/day=1/region=us/segments/1/manifest.json", "day=1/region=us"},
		// A manifest under a non-partition subdirectory has no partition; the
		// partitions marker must actually be present, not assumed at index 2.
		{"datasets/ds/x/segments/1/manifest.json", ""},
		// partitions immediately followed by segments: empty partition path.
		{"datasets/ds/partitions/segments/1/manifest.json", ""},
		// Not a manifest at all.
		{"datasets/ds/partitions/day=1/segments/1/data/f.jsonl", ""},
		// A dataset literally named "segments" must not be mistaken for the
		// segments marker when scanning after the partitions marker.
		{"datasets/segments/partitions/day=1/segments/1/manifest.json", "day=1"},
		// Likewise a dataset named "partitions" must not be read as the
		// partitions marker, which would leak it into the partition path.
		{"datasets/partitions/partitions/day=1/segments/1/manifest.json", "day=1"},
	}
	for _, c := range cases {
		if got := l.parsePartitionFromManifest(c.path); got != c.want {
			t.Errorf("parsePartitionFromManifest(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

func TestHiveLayout_ExtractPartitionPath(t *testing.T) {
	l := hiveForTest(t)
	cases := []struct {
		path string
		want string
	}{
		{"datasets/ds/partitions/day=1/segments/1/data/f.jsonl", "day=1"},
		{"datasets/ds/partitions/day=1/region=us/segments/1/data/f.jsonl", "day=1/region=us"},
		// Canonical (unpartitioned) data path: no partitions marker anywhere.
		{"datasets/ds/segments/1/data/f.jsonl", ""},
		// partitions without a following segments marker.
		{"datasets/ds/partitions/day=1/data/f.jsonl", ""},
		// partitions immediately followed by segments.
		{"datasets/ds/partitions/segments/1/data/f.jsonl", ""},
		// Marker deeper than index 2 is still found.
		{"datasets/ds/x/partitions/day=1/segments/1/data/f.jsonl", "day=1"},
		// A dataset literally named "segments" sits before the partitions
		// marker and must not terminate the scan.
		{"datasets/segments/partitions/day=1/segments/1/data/f.jsonl", "day=1"},
		// A dataset named "partitions" sits before the real marker.
		{"datasets/partitions/partitions/day=1/segments/1/data/f.jsonl", "day=1"},
	}
	for _, c := range cases {
		if got := l.extractPartitionPath(c.path); got != c.want {
			t.Errorf("extractPartitionPath(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

func TestHiveLayout_Prefixes(t *testing.T) {
	l := hiveForTest(t)
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"segmentsPrefix", l.segmentsPrefix("ds"), "datasets/ds/"},
		{"segmentsPrefixForPartition empty", l.segmentsPrefixForPartition("ds", ""), "datasets/ds/"},
		{"segmentsPrefixForPartition", l.segmentsPrefixForPartition("ds", "day=1"), "datasets/ds/partitions/day=1/segments/"},
		{"partitionsPrefix partial token", l.partitionsPrefix("ds", "day=2026-10"), "datasets/ds/partitions/day=2026-10"},
		{"partitionsPrefix empty", l.partitionsPrefix("ds", ""), "datasets/ds/partitions/"},
		{"manifestPath", l.manifestPath("ds", "1"), "datasets/ds/segments/1/manifest.json"},
		{"manifestPathInPartition", l.manifestPathInPartition("ds", "1", "day=1"), "datasets/ds/partitions/day=1/segments/1/manifest.json"},
		{"manifestPathInPartition empty", l.manifestPathInPartition("ds", "1", ""), "datasets/ds/segments/1/manifest.json"},
		{"dataFilePath", l.dataFilePath("ds", "1", "day=1", "f.jsonl"), "datasets/ds/partitions/day=1/segments/1/data/f.jsonl"},
		{"dataFilePath canonical", l.dataFilePath("ds", "1", "", "f.jsonl"), "datasets/ds/segments/1/data/f.jsonl"},
		{"latestPointerPath", l.latestPointerPath("ds"), "datasets/ds/latest"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, c.got, c.want)
		}
	}
}
