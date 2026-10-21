package lode

import (
	"path"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/storepath"
)

// fuzzLayouts are the built-in layouts, each with the dataset directory its
// paths must stay under.
func fuzzLayouts(t testing.TB) map[string]layout {
	t.Helper()
	hive, err := NewHiveLayout("day", "region")
	if err != nil {
		t.Fatal(err)
	}
	return map[string]layout{"default": NewDefaultLayout(), "hive": hive, "flat": NewFlatLayout()}
}

// FuzzLayout_IsObjectOf checks the guard retention deletes behind: a path
// that isObjectOf accepts is a canonical store key, lies under its dataset's
// directory, contains no parent segment, and parses back to the same
// dataset, snapshot and kind.
func FuzzLayout_IsObjectOf(f *testing.F) {
	for _, seed := range []string{
		"datasets/ds/snapshots/1-0123456789abcdef/manifest.json",
		"datasets/ds/snapshots/1/data/f.jsonl",
		"datasets/ds/partitions/day=1/region=us/segments/7/data/f.jsonl",
		"datasets/ds/segments/7/manifest.json",
		"ds/7/data/f",
		"datasets/ds/snapshots/1/data/../../other/x",
		"datasets/ds/snapshots/1/data/a\\..\\b",
		"datasets//snapshots/1/manifest.json",
	} {
		f.Add(seed)
	}
	layouts := fuzzLayouts(f)
	f.Fuzz(func(t *testing.T, p string) {
		for name, l := range layouts {
			sp, ok := l.parse(p)
			if !ok || !l.isObjectOf(sp.dataset, sp.snapshot, sp.kind, p) {
				continue
			}
			if key, ok := storepath.Key(p); !ok || key != p {
				t.Fatalf("%s: isObjectOf accepted %q, which is not a canonical key", name, p)
			}
			if path.Clean(p) != p || strings.Contains("/"+p+"/", "/../") {
				t.Fatalf("%s: isObjectOf accepted %q, which has a parent segment", name, p)
			}
			if !strings.HasPrefix(p, l.snapshotsPrefix(sp.dataset)) {
				t.Fatalf("%s: isObjectOf accepted %q outside %q", name, p, l.snapshotsPrefix(sp.dataset))
			}
			again, ok := l.parse(p)
			if !ok || again != sp {
				t.Fatalf("%s: %q parses inconsistently: %+v then %+v", name, p, sp, again)
			}
		}
	})
}

// FuzzLayout_RoundTrip checks that every path a layout builds from valid
// components parses back to those components and passes isObjectOf.
func FuzzLayout_RoundTrip(f *testing.F) {
	f.Add("ds", "1792459022524750970-0123456789abcdef", "2026-10-20", "us", "data.jsonl")
	f.Add("a", "1", "x", "y", "f")
	layouts := fuzzLayouts(f)
	f.Fuzz(func(t *testing.T, dataset, snapshot, day, region, file string) {
		for _, seg := range []string{dataset, snapshot, day, region, file} {
			if !isPlainSegment(seg) {
				return
			}
		}
		for name, l := range layouts {
			partition := ""
			if l.supportsPartitions() {
				partition = "day=" + day + "/region=" + region
			}
			id, snap := DatasetID(dataset), DatasetSnapshotID(snapshot)
			for kind, p := range map[pathKind]string{
				manifestLeaf: l.manifestPathInPartition(id, snap, partition),
				dataLeaf:     l.dataFilePath(id, snap, partition, file),
			} {
				sp, ok := l.parse(p)
				if !ok || sp.dataset != id || sp.snapshot != snap || sp.kind != kind || sp.partition != partition {
					t.Fatalf("%s: %q parses to %+v, %v; want %s/%s/%q", name, p, sp, ok, dataset, snapshot, partition)
				}
				if !l.isObjectOf(id, snap, kind, p) {
					t.Fatalf("%s: isObjectOf rejects the built path %q", name, p)
				}
			}
		}
	})
}

// isPlainSegment reports whether s can stand as one path component of every
// layout: non-empty, no separator, no "=", and not a dot segment or a
// layout directory name.
func isPlainSegment(s string) bool {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/\\=") {
		return false
	}
	switch s {
	case datasetsDir, snapshotsDir, manifestFile, dataDir, partitionsDir, segmentsDir, latestFile, tombstonesDir:
		return false
	}
	key, ok := storepath.Key(s)
	return ok && key == s
}

// FuzzPartitionPrefix checks that a partition prefix accepted for listing or
// expiry resolves beneath the dataset's partitions directory: no accepted
// value can address another dataset.
func FuzzPartitionPrefix(f *testing.F) {
	for _, seed := range []string{"day=1", "day=1/", "day=1/region=us", "../other", "day=1/../../x", "/day=1", "a\\b", "", "./day"} {
		f.Add(seed)
	}
	hive, err := NewHiveLayout("day", "region")
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, prefix string) {
		if !validPartitionPrefix(prefix) {
			return
		}
		base := "datasets/ds/partitions/"
		got := hive.partitionsPrefix("ds", prefix)
		if !strings.HasPrefix(got, base) {
			t.Fatalf("prefix %q resolves to %q outside %q", prefix, got, base)
		}
		if strings.Contains("/"+got, "/../") || strings.Contains("/"+got, "/./") || strings.ContainsRune(got, '\\') {
			t.Fatalf("prefix %q resolves to %q, which has a dot segment or backslash", prefix, got)
		}
	})
}
