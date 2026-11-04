package volume_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/pithecene/lode/lode/volume"
)

// fixedSnapshotReader is a SnapshotReaderAt that a package outside volume
// defines, as a test double of a volume.Reader would.
type fixedSnapshotReader struct {
	data *bytes.Reader
	snap volume.Snapshot
}

func (f fixedSnapshotReader) ReadAt(p []byte, off int64) (int, error) { return f.data.ReadAt(p, off) }

func (f fixedSnapshotReader) Snapshot() *volume.Snapshot {
	s := f.snap
	return &s
}

// A package outside volume can implement SnapshotReaderAt, so an
// implementation of volume.Reader outside Lode can return one.
func TestSnapshotReaderAt_ImplementableOutsideThePackage(t *testing.T) {
	t.Parallel()
	var r volume.SnapshotReaderAt = fixedSnapshotReader{data: bytes.NewReader([]byte("bytes")), snap: volume.Snapshot{ID: "s"}}
	got, err := io.ReadAll(io.NewSectionReader(r, 0, 5))
	if err != nil || string(got) != "bytes" || r.Snapshot().ID != "s" {
		t.Fatalf("read %q, %v; snapshot %v", got, err, r.Snapshot())
	}
}
