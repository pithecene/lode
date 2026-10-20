package volume

import (
	"bytes"
	"testing"

	"github.com/pithecene-io/lode/lode"
)

// -----------------------------------------------------------------------------
// BenchmarkVolume_ReadAt measures random-access reads across 10 committed blocks.
// -----------------------------------------------------------------------------

func BenchmarkVolume_ReadAt(b *testing.B) {
	const blockSize = 1024
	const blockCount = 10
	const totalLength = blockSize * blockCount

	vol, err := New("bench-vol", lode.NewMemoryFactory(), totalLength)
	if err != nil {
		b.Fatal(err)
	}

	// Stage and commit 10 blocks
	blocks := make([]BlockRef, blockCount)
	for i := range blocks {
		data := bytes.Repeat([]byte{byte(i)}, blockSize)
		blk, err := vol.StageWriteAt(b.Context(), int64(i*blockSize), bytes.NewReader(data))
		if err != nil {
			b.Fatalf("StageWriteAt block %d: %v", i, err)
		}
		blocks[i] = blk
	}

	snap, err := vol.Commit(b.Context(), blocks, lode.Metadata{})
	if err != nil {
		b.Fatalf("Commit: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Read 512 bytes from the middle of block 5
		_, err := vol.ReadAt(b.Context(), snap.ID, 5*blockSize+256, 512)
		if err != nil {
			b.Fatal(err)
		}
	}
}
