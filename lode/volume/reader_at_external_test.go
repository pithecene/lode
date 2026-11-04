package volume_test

import (
	"bytes"
	"io"
	"slices"
	"testing"

	"github.com/pithecene/lode/lode/volume"
)

// fixedSnapshotReader is a SnapshotReaderAt that a package outside volume
// defines, as a test double of a volume.Reader would.
type fixedSnapshotReader struct {
	data   *bytes.Reader
	id     volume.SnapshotID
	blocks []volume.BlockRef
}

func (f fixedSnapshotReader) ReadAt(p []byte, off int64) (int, error) { return f.data.ReadAt(p, off) }

func (f fixedSnapshotReader) ID() volume.SnapshotID { return f.id }

func (f fixedSnapshotReader) Blocks() []volume.BlockRef { return slices.Clone(f.blocks) }

// A package outside volume can implement SnapshotReaderAt, so an
// implementation of volume.Reader outside Lode can return one.
func TestSnapshotReaderAt_ImplementableOutsideThePackage(t *testing.T) {
	t.Parallel()
	var r volume.SnapshotReaderAt = fixedSnapshotReader{
		data:   bytes.NewReader([]byte("bytes")),
		id:     "s",
		blocks: []volume.BlockRef{{Offset: 0, Length: 5, Path: "b"}},
	}
	got, err := io.ReadAll(io.NewSectionReader(r, 0, 5))
	if err != nil || string(got) != "bytes" || r.ID() != "s" || len(r.Blocks()) != 1 {
		t.Fatalf("read %q, %v; id %s; blocks %v", got, err, r.ID(), r.Blocks())
	}
}
