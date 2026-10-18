package lode

import (
	"errors"
	"testing"
)

// Listing filters and cursors (#175): Prefix, StartAfter, ordering.

// writeHiveDays writes one snapshot per day value into a Hive dataset
// partitioned by "day" and returns a reader over the same store.
func writeHiveDays(t *testing.T, days ...string) DatasetReader {
	t.Helper()
	store := NewMemory()
	ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	for i, day := range days {
		if _, err := ds.Write(t.Context(), R(D{"id": i, "day": day}), Metadata{}); err != nil {
			t.Fatalf("write %s: %v", day, err)
		}
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func partitionPaths(refs []PartitionRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Path)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDatasetReader_ListPartitions_Sorted verifies results are lexicographic
// by Path regardless of write or store order (the memory store is map-ordered).
func TestDatasetReader_ListPartitions_Sorted(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-16", "2024-02-01", "2024-01-15")

	got, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"day=2024-01-15", "day=2024-01-16", "day=2024-02-01"}
	if !equalStrings(partitionPaths(got), want) {
		t.Fatalf("partitions = %v, want %v", partitionPaths(got), want)
	}
}

// TestDatasetReader_ListPartitions_Prefix verifies a raw path prefix narrows
// results, including a partial final token (no key boundary implied).
func TestDatasetReader_ListPartitions_Prefix(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-15", "2024-01-16", "2024-02-01")

	got, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: "day=2024-01"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"day=2024-01-15", "day=2024-01-16"}
	if !equalStrings(partitionPaths(got), want) {
		t.Fatalf("partitions = %v, want %v", partitionPaths(got), want)
	}
}

// TestDatasetReader_ListPartitions_Prefix_ExcludesOtherPartitionsOfSameSnapshot
// verifies that when one snapshot spans partitions inside and outside the
// prefix, only the inside partitions are returned.
func TestDatasetReader_ListPartitions_Prefix_ExcludesOtherPartitionsOfSameSnapshot(t *testing.T) {
	store := NewMemory()
	ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	// One snapshot, two partitions.
	if _, err := ds.Write(t.Context(), R(
		D{"id": 1, "day": "2024-01-15"},
		D{"id": 2, "day": "2024-02-01"},
	), Metadata{}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	got, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: "day=2024-01"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"day=2024-01-15"}
	if !equalStrings(partitionPaths(got), want) {
		t.Fatalf("partitions = %v, want %v", partitionPaths(got), want)
	}
}

// TestDatasetReader_ListPartitions_Prefix_NoMatch_ReturnsEmpty verifies an
// empty subtree is an empty result, not ErrNotFound.
func TestDatasetReader_ListPartitions_Prefix_NoMatch_ReturnsEmpty(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-15")

	got, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: "day=2030"})
	if err != nil {
		t.Fatalf("expected nil error for empty prefix subtree, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no partitions, got %v", partitionPaths(got))
	}
}

// TestDatasetReader_ListPartitions_Prefix_DefaultLayout_Rejected verifies the
// sentinel on a layout without partitions.
func TestDatasetReader_ListPartitions_Prefix_DefaultLayout_Rejected(t *testing.T) {
	reader, err := NewDatasetReader(NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: "day=2024"})
	if !errors.Is(err, ErrPartitionPrefixNotSupported) {
		t.Fatalf("expected ErrPartitionPrefixNotSupported, got %v", err)
	}
}

// TestDatasetReader_ListPartitions_StartAfter_Pages verifies Limit + StartAfter
// walk the full sorted set exactly once, and that a cursor past the end is empty.
func TestDatasetReader_ListPartitions_StartAfter_Pages(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-15", "2024-01-16", "2024-01-17")

	var seen []string
	cursor := ""
	for range 4 { // one more iteration than pages, to hit the empty tail
		page, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Limit: 1, StartAfter: cursor})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if len(page) != 1 {
			t.Fatalf("page size = %d, want 1", len(page))
		}
		seen = append(seen, page[0].Path)
		cursor = page[0].Path
	}
	want := []string{"day=2024-01-15", "day=2024-01-16", "day=2024-01-17"}
	if !equalStrings(seen, want) {
		t.Fatalf("paged partitions = %v, want %v", seen, want)
	}
}

// TestDatasetReader_ListPartitions_StartAfter_Exclusive verifies the cursor
// itself is not returned and a cursor between keys resumes at the next key.
func TestDatasetReader_ListPartitions_StartAfter_Exclusive(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-15", "2024-01-17")

	got, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{StartAfter: "day=2024-01-15"})
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(partitionPaths(got), []string{"day=2024-01-17"}) {
		t.Fatalf("after exact cursor: %v", partitionPaths(got))
	}

	got, err = reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{StartAfter: "day=2024-01-16"})
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(partitionPaths(got), []string{"day=2024-01-17"}) {
		t.Fatalf("after between-keys cursor: %v", partitionPaths(got))
	}
}

// TestDatasetReader_ListManifests_SortedAndDeterministicPartition verifies
// (Partition, ID) ordering and that a multi-partition snapshot resolves to its
// lexicographically smallest partition regardless of store order.
func TestDatasetReader_ListManifests_SortedAndDeterministicPartition(t *testing.T) {
	store := NewMemory()
	ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot A spans two partitions; snapshot B is in one.
	snapA, err := ds.Write(t.Context(), R(
		D{"id": 1, "day": "2024-01-20"},
		D{"id": 2, "day": "2024-01-10"},
	), Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	snapB, err := ds.Write(t.Context(), R(D{"id": 3, "day": "2024-01-15"}), Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	for range 5 { // repeat: memory store iteration order varies per call
		refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := []ManifestRef{
			{ID: snapA.ID, Partition: "day=2024-01-10"},
			{ID: snapB.ID, Partition: "day=2024-01-15"},
		}
		if len(refs) != len(want) {
			t.Fatalf("refs = %v, want %v", refs, want)
		}
		for i := range want {
			if refs[i] != want[i] {
				t.Fatalf("refs[%d] = %v, want %v", i, refs[i], want[i])
			}
		}
	}
}

// TestDatasetReader_ListManifests_StartAfter_Pages verifies Limit + StartAfter
// over manifests using the previous page's last ref as the cursor.
func TestDatasetReader_ListManifests_StartAfter_Pages(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-15", "2024-01-16", "2024-01-17")

	all, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 manifests, got %d", len(all))
	}

	var paged []ManifestRef
	var cursor ManifestRef
	for range 4 {
		page, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{Limit: 2, StartAfter: cursor})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		cursor = page[len(page)-1]
	}
	if len(paged) != len(all) {
		t.Fatalf("paged %d refs, want %d", len(paged), len(all))
	}
	for i := range all {
		if paged[i] != all[i] {
			t.Fatalf("paged[%d] = %v, want %v", i, paged[i], all[i])
		}
	}
}

// TestDatasetReader_ListManifests_Limit_Boundary verifies Limit returns exactly
// Limit results when more exist, and all results when fewer exist.
func TestDatasetReader_ListManifests_Limit_Boundary(t *testing.T) {
	reader := writeHiveDays(t, "2024-01-15", "2024-01-16", "2024-01-17")

	got, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("Limit 2 returned %d", len(got))
	}
	got, err = reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("Limit 10 returned %d, want 3", len(got))
	}
}

// TestDatasetReader_ListManifests_DefaultLayout_SortedByID verifies ordering on
// a layout without partitions is by snapshot ID.
func TestDatasetReader_ListManifests_DefaultLayout_SortedByID(t *testing.T) {
	store := NewMemory()
	ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}
	var ids []DatasetSnapshotID
	for i := range 3 {
		snap, err := ds.Write(t.Context(), R(D{"id": i}), Metadata{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snap.ID)
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}
	for i := 1; i < len(refs); i++ {
		if refs[i-1].ID >= refs[i].ID {
			t.Fatalf("refs not sorted by ID: %v", refs)
		}
	}
	// Cursor on the first ref yields the remaining two.
	rest, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{StartAfter: refs[0]})
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 || rest[0] != refs[1] || rest[1] != refs[2] {
		t.Fatalf("after cursor: %v, want %v", rest, refs[1:])
	}
	_ = ids
}

// TestDatasetReader_ListPartitions_Prefix_RejectsNonCanonical verifies a
// prefix cannot escape the dataset's partitions directory. The prefix is
// concatenated under it, so "..", "." and empty segments are rejected before
// any store call.
func TestDatasetReader_ListPartitions_Prefix_RejectsNonCanonical(t *testing.T) {
	store := NewMemory()
	victim, err := NewDataset("victim", NewMemoryFactoryFrom(store),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := victim.Write(t.Context(), R(D{"id": 1, "day": "2024-01-15"}), Metadata{}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}

	for _, prefix := range []string{
		"../../victim/partitions/day=2024",
		"../x",
		"/day=2024",
		"day=2024//x",
		"./day=2024",
		"day=2024/..",
		`..\..\victim\partitions\day=2024`, // "\" is a separator for the fs store on Windows
		`day=2024\x`,
	} {
		got, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: prefix})
		if !errors.Is(err, ErrInvalidPath) {
			t.Errorf("Prefix %q: expected ErrInvalidPath, got err=%v result=%v", prefix, err, partitionPaths(got))
		}
	}

	// Canonical forms, including a trailing slash, are accepted.
	for _, prefix := range []string{"day=2024", "day=2024-01-15/", "day=2024-01-15/region=x"} {
		if _, err := reader.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: prefix}); err != nil {
			t.Errorf("Prefix %q: unexpected error %v", prefix, err)
		}
	}

	// Malformed wins over unsupported: a partitionless layout still reports
	// ErrInvalidPath for an escaping prefix, not ErrPartitionPrefixNotSupported.
	plain, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	_, err = plain.ListPartitions(t.Context(), "test-ds", PartitionListOptions{Prefix: "../x"})
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("default layout, escaping prefix: expected ErrInvalidPath, got %v", err)
	}
}

// TestDatasetReader_ListManifests_SmallestParsedPartition_NotPathOrder covers
// a snapshot spanning partitions where one name prefixes another: "day=a-b/"
// sorts before "day=a/" as a storage path ('-' < '/'), but "day=a" is the
// smaller partition and is what the contract promises.
func TestDatasetReader_ListManifests_SmallestParsedPartition_NotPathOrder(t *testing.T) {
	store := NewMemory()
	ds, err := NewDataset("test-ds", NewMemoryFactoryFrom(store),
		WithCodec(NewJSONLCodec()),
		WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := ds.Write(t.Context(), R(
		D{"id": 1, "day": "a-b"},
		D{"id": 2, "day": "a"},
	), Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store), WithHiveLayout("day"))
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		refs, err := reader.ListManifests(t.Context(), "test-ds", "", ManifestListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := ManifestRef{ID: snap.ID, Partition: "day=a"}
		if len(refs) != 1 || refs[0] != want {
			t.Fatalf("refs = %v, want [%v]", refs, want)
		}
	}
}
