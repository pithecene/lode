package volume

import (
	"bytes"
	"errors"
	"testing"

	"github.com/pithecene/lode/internal/validate"
	"github.com/pithecene/lode/lode"
)

// Exact-boundary tests for Volume range arithmetic and manifest validation.
// The existing tests use clearly-inside and clearly-outside values; these sit
// on the edges the contract names (CONTRACT_VOLUME.md).
//
// Known equivalent mutants left alive in volume.go: the `<` → `<=` boundary
// mutants in sort comparators; `sorted[i].Offset <= newBlocks[j].Offset` → `<`
// (equal offsets are overlaps and rejected downstream either way);
// `len(blocks) <= 1` → `<` (a single block skips the loop regardless);
// `blockEnd > cursor` → `>=` (equal reassigns the same value); and
// `length > math.MaxInt` → `>=` (a MaxInt read cannot be allocated to test).

func TestMergeBlocks_OneSideExhaustsFirst(t *testing.T) {
	cases := []struct {
		name     string
		sorted   []BlockRef
		unsorted []BlockRef
		want     []int64
	}{
		{
			name:     "new exhausted first",
			sorted:   []BlockRef{{Offset: 0, Length: 10}, {Offset: 20, Length: 10}, {Offset: 40, Length: 10}},
			unsorted: []BlockRef{{Offset: 10, Length: 10}},
			want:     []int64{0, 10, 20, 40},
		},
		{
			name:     "existing exhausted first",
			sorted:   []BlockRef{{Offset: 0, Length: 10}},
			unsorted: []BlockRef{{Offset: 20, Length: 10}, {Offset: 10, Length: 10}},
			want:     []int64{0, 10, 20},
		},
	}
	for _, c := range cases {
		got := mergeBlocks(c.sorted, c.unsorted)
		if len(got) != len(c.want) {
			t.Fatalf("%s: got %d blocks, want %d", c.name, len(got), len(c.want))
		}
		for i, want := range c.want {
			if got[i].Offset != want {
				t.Errorf("%s: block %d offset = %d, want %d", c.name, i, got[i].Offset, want)
			}
		}
	}
}

func TestFindCoveringBlocks_AdjacentAtCursor(t *testing.T) {
	adjacent := []BlockRef{{Offset: 0, Length: 10}, {Offset: 10, Length: 10}}
	gapped := []BlockRef{{Offset: 0, Length: 10}, {Offset: 12, Length: 10}}

	// The second block begins exactly where the first ends: contiguous.
	got, err := findCoveringBlocks(adjacent, 5, 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("adjacent: got %d blocks, err %v; want 2, nil", len(got), err)
	}
	// Request ending exactly at a block boundary needs only that block.
	got, err = findCoveringBlocks(adjacent, 5, 5)
	if err != nil || len(got) != 1 {
		t.Fatalf("to boundary: got %d blocks, err %v; want 1, nil", len(got), err)
	}
	// A two-byte gap is missing range even though both blocks are relevant.
	if _, err := findCoveringBlocks(gapped, 5, 10); !errors.Is(err, ErrRangeMissing) {
		t.Fatalf("gapped: got %v, want ErrRangeMissing", err)
	}
}

func TestVolume_ReadAt_ExactEndOfAddressSpace(t *testing.T) {
	vol := newTestVolume(t, "test-vol", lode.Memory(), 20)
	blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 20))
	snap := commitBlocks(t, vol, []BlockRef{blk}, Metadata{})

	// Last byte is readable.
	got, err := vol.ReadAt(t.Context(), snap.ID, 19, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("ReadAt(19,1): got %d bytes, err %v", len(got), err)
	}
	// One past the end is rejected up front as an address-space error; it is
	// not the ErrRangeMissing a block lookup would produce, which is the
	// structured distinction the boundary pins (the message itself is not).
	_, err = vol.ReadAt(t.Context(), snap.ID, 19, 2)
	if err == nil || errors.Is(err, ErrRangeMissing) {
		t.Fatalf("ReadAt(19,2): got %v, want a non-ErrRangeMissing error", err)
	}
	// Offset at the end with any length is likewise rejected up front.
	_, err = vol.ReadAt(t.Context(), snap.ID, 20, 1)
	if err == nil || errors.Is(err, ErrRangeMissing) {
		t.Fatalf("ReadAt(20,1): got %v, want a non-ErrRangeMissing error", err)
	}
}

func TestValidateVolumeManifest_Bounds(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(m *Manifest)
		wantField string // "" means valid
	}{
		{"total_length zero", func(m *Manifest) { m.TotalLength = 0 }, "total_length"},
		{"total_length one", func(m *Manifest) {
			m.TotalLength = 1
			m.Blocks[0].Length = 1
		}, ""},
		{"block ends exactly at total_length", func(m *Manifest) {
			m.Blocks[0].Offset = 90
			m.Blocks[0].Length = 10
		}, ""},
		{"block ends one past total_length", func(m *Manifest) {
			m.Blocks[0].Offset = 91
			m.Blocks[0].Length = 10
		}, "blocks[0]"},
		{"block length zero", func(m *Manifest) { m.Blocks[0].Length = 0 }, "blocks[0].length"},
		{"block offset zero is valid", func(m *Manifest) { m.Blocks[0].Offset = 0 }, ""},
		{"block offset negative", func(m *Manifest) { m.Blocks[0].Offset = -1 }, "blocks[0].offset"},
	}
	for _, c := range cases {
		m := validVolumeManifest("vol", 100)
		c.mutate(m)
		err := validateVolumeManifest(m)
		if c.wantField == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", c.name, err)
			}
			continue
		}
		var verr *validate.ManifestError
		if !errors.As(err, &verr) || verr.Field != c.wantField {
			t.Errorf("%s: got %v, want validation error on %q", c.name, err, c.wantField)
		}
	}
}
