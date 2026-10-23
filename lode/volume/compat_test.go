package volume

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/pithecene/lode/lode"
)

// TestCompat_v0_11_0 opens the volume that Lode v0.11.0 wrote through its
// public API (lode/testdata/v0.11.0, beside the datasets of the same
// release) and requires this release to read its blocks and commit onto it.
func TestCompat_v0_11_0(t *testing.T) {
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS("../testdata/v0.11.0")); err != nil {
		t.Fatal(err)
	}
	vol, err := New("disk", lode.FS(dir), 1024)
	if err != nil {
		t.Fatal(err)
	}
	head, err := vol.Latest(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("reads the committed blocks", func(t *testing.T) {
		for _, block := range []struct {
			offset int64
			fill   byte
		}{{0, 0xAA}, {512, 0xBB}} {
			data, err := vol.ReadAt(t.Context(), head.ID, block.offset, 256)
			if err != nil {
				t.Fatalf("ReadAt(%d): %v", block.offset, err)
			}
			if !bytes.Equal(data, bytes.Repeat([]byte{block.fill}, 256)) {
				t.Errorf("block at %d differs from what v0.11.0 wrote", block.offset)
			}
		}
		if _, err := vol.ReadAt(t.Context(), head.ID, 256, 256); !errors.Is(err, ErrRangeMissing) {
			t.Errorf("ReadAt of the gap = %v, want ErrRangeMissing", err)
		}
	})

	t.Run("commits onto the history", func(t *testing.T) {
		block, err := vol.StageWriteAt(t.Context(), 256, bytes.NewReader(bytes.Repeat([]byte{0xCC}, 256)))
		if err != nil {
			t.Fatal(err)
		}
		snap, err := vol.Commit(t.Context(), []BlockRef{block}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Manifest.ParentSnapshotID != head.ID {
			t.Errorf("parent = %s, want the v0.11.0 head %s", snap.Manifest.ParentSnapshotID, head.ID)
		}
		if _, err := vol.ReadAt(t.Context(), snap.ID, 0, 768); err != nil {
			t.Errorf("ReadAt across old and new blocks: %v", err)
		}
	})
}
