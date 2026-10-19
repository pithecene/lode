package lode

import (
	"errors"
	"testing"
	"time"
)

// Boundary tests for the read API: limits that equal the result size, the
// cursor window helper in isolation, and manifest validation at zero, which
// is valid but was only ever exercised with positive or clearly negative
// values.
//
// Known equivalent mutants left alive: the `<` → `<=` boundary mutants in the
// sort.Slice comparators (reader.go ListPartitions/ListManifests) — a
// non-strict comparator sorts the same distinct keys identically.

func validTestManifest() *Manifest {
	return &Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      Metadata{},
		Files:         []FileRef{{Path: "data/f", SizeBytes: 1}},
		RowCount:      1,
		Compressor:    "noop",
		Partitioner:   "noop",
	}
}

func TestValidateManifest_ZeroIsValid_NegativeRejected(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(m *Manifest)
		wantField string // "" means valid
	}{
		{"zero row count", func(m *Manifest) { m.RowCount = 0 }, ""},
		{"zero size file", func(m *Manifest) { m.Files[0].SizeBytes = 0 }, ""},
		{"negative row count", func(m *Manifest) { m.RowCount = -1 }, "row_count"},
		{"negative size file", func(m *Manifest) { m.Files[0].SizeBytes = -1 }, "files[0].size_bytes"},
		{"empty file path", func(m *Manifest) { m.Files[0].Path = "" }, "files[0].path"},
	}
	for _, c := range cases {
		m := validTestManifest()
		c.mutate(m)
		err := validateManifest(m)
		if c.wantField == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", c.name, err)
			}
			continue
		}
		var verr *manifestValidationError
		if !errors.As(err, &verr) || verr.Field != c.wantField {
			t.Errorf("%s: got %v, want validation error on %q", c.name, err, c.wantField)
		}
	}
}

func TestCursorWindow_Bounds(t *testing.T) {
	items := []int{1, 2, 3}
	after := func(cursor int) func(int) bool { return func(v int) bool { return v > cursor } }

	cases := []struct {
		name   string
		cursor int
		limit  int
		want   []int
	}{
		{"no cursor, no limit", 0, 0, []int{1, 2, 3}},
		{"limit equals length", 0, 3, []int{1, 2, 3}},
		{"limit below length", 0, 2, []int{1, 2}},
		{"limit above length", 0, 5, []int{1, 2, 3}},
		{"cursor is an element", 1, 0, []int{2, 3}},
		{"cursor between elements", 1, 1, []int{2}},
		{"cursor at last element", 3, 0, []int{}},
		{"cursor past all", 9, 2, []int{}},
	}
	for _, c := range cases {
		got := cursorWindow(items, after(c.cursor), c.limit)
		if len(got) != len(c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: got %v, want %v", c.name, got, c.want)
				break
			}
		}
	}
}

func TestDatasetReader_ListDatasets_Limit_Boundary(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()
	for _, id := range []DatasetID{"ds-a", "ds-b", "ds-c"} {
		m := validTestManifest()
		m.DatasetID = id
		writeManifest(ctx, t, store, m)
	}
	reader, err := NewDatasetReader(NewMemoryFactoryFrom(store))
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct{ limit, want int }{{0, 3}, {3, 3}, {2, 2}, {1, 1}, {4, 3}} {
		got, err := reader.ListDatasets(ctx, DatasetListOptions{Limit: c.limit})
		if err != nil {
			t.Fatalf("Limit %d: %v", c.limit, err)
		}
		if len(got) != c.want {
			t.Errorf("Limit %d: got %d datasets %v, want %d", c.limit, len(got), got, c.want)
		}
	}
}
