package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// writeVolumeManifest writes a Manifest directly to the store for
// validation testing. Mirrors writeManifest in reader_test.go.
func writeVolumeManifest(ctx context.Context, t *testing.T, store lode.Store, m *Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := volumeManifestPath(m.VolumeID, m.SnapshotID)
	if err := store.Put(ctx, p, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

// validVolumeManifest returns a minimal valid Manifest for mutation in tests.
func validVolumeManifest(volumeID ID, totalLength int64) *Manifest {
	return &Manifest{
		SchemaName:    volumeManifestSchemaName,
		FormatVersion: volumeManifestFormatVersion,
		VolumeID:      volumeID,
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		TotalLength:   totalLength,
		Blocks: []BlockRef{
			{Offset: 0, Length: 10, Path: volumeBlockPath(volumeID, 0, 10)},
		},
	}
}

// newTestVolume is a convenience helper that creates a Volume and t.Fatal's on error.
func newTestVolume(t *testing.T, id ID, factory lode.StoreFactory, totalLength int64, opts ...Option) Volume {
	t.Helper()
	vol, err := New(id, factory, totalLength, opts...)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return vol
}

// stageBlock is a convenience helper that stages data and t.Fatal's on error.
func stageBlock(t *testing.T, vol Volume, offset int64, data []byte) BlockRef {
	t.Helper()
	blk, err := vol.StageWriteAt(t.Context(), offset, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("StageWriteAt(offset=%d) failed: %v", offset, err)
	}
	return blk
}

// commitBlocks is a convenience helper that commits blocks and t.Fatal's on error.
func commitBlocks(t *testing.T, vol Volume, blocks []BlockRef, meta lode.Metadata) *Snapshot {
	t.Helper()
	snap, err := vol.Commit(t.Context(), blocks, meta)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	return snap
}

// =============================================================================
// Existing smoke tests (17 tests — kept as-is)
// =============================================================================

func TestNewVolume(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 1024)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vol == nil {
			t.Fatal("expected non-nil volume")
		}
		if vol.ID() != "test-vol" {
			t.Errorf("expected ID 'test-vol', got %q", vol.ID())
		}
	})

	t.Run("nil factory returns error", func(t *testing.T) {
		_, err := New("test-vol", nil, 1024)
		if err == nil {
			t.Fatal("expected error for nil factory")
		}
	})

	t.Run("zero total length returns error", func(t *testing.T) {
		_, err := New("test-vol", lode.NewMemoryFactory(), 0)
		if err == nil {
			t.Fatal("expected error for zero total length")
		}
	})

	t.Run("nil store returns error", func(t *testing.T) {
		factory := func() (lode.Store, error) { return nil, nil } //nolint:nilnil // intentionally testing nil store guard
		_, err := New("test-vol", factory, 1024)
		if err == nil {
			t.Fatal("expected error for nil store")
		}
	})

	t.Run("empty ID returns error", func(t *testing.T) {
		_, err := New("", lode.NewMemoryFactory(), 1024)
		if err == nil {
			t.Fatal("expected error for empty ID")
		}
	})

	t.Run("with checksum option", func(t *testing.T) {
		vol, err := New("test", lode.NewMemoryFactory(), 1024, WithChecksum(lode.NewMD5Checksum()))
		if err != nil {
			t.Fatalf("New with WithChecksum: %v", err)
		}

		block, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("hello")))
		if err != nil {
			t.Fatalf("stage: %v", err)
		}
		if block.Checksum == "" {
			t.Error("expected non-empty checksum when WithChecksum configured")
		}
	})
	// Dataset options (lode.WithLayout, lode.WithCodec, …) are a different
	// type and do not compile as a volume Option, so there is no runtime
	// rejection left to test.
}

func TestVolume_EndToEnd(t *testing.T) {
	t.Run("stage commit read at end to end", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := t.Context()
		data := []byte("hello world") // 11 bytes

		// Stage a block at offset 0.
		blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}
		if blk.Offset != 0 {
			t.Errorf("expected offset 0, got %d", blk.Offset)
		}
		if blk.Length != int64(len(data)) {
			t.Errorf("expected length %d, got %d", len(data), blk.Length)
		}

		// Commit.
		snap, err := vol.Commit(ctx, []BlockRef{blk}, lode.Metadata{"source": "test"})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		if snap.ID == "" {
			t.Fatal("expected non-empty snapshot ID")
		}
		if len(snap.Manifest.Blocks) != 1 {
			t.Fatalf("expected 1 block, got %d", len(snap.Manifest.Blocks))
		}

		// ReadAt.
		got, err := vol.ReadAt(ctx, snap.ID, 0, int64(len(data)))
		if err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("expected %q, got %q", data, got)
		}
	})

	t.Run("FS store stage commit read at", func(t *testing.T) {
		tmpDir := t.TempDir()
		vol := newTestVolume(t, "test-vol", lode.NewFSFactory(tmpDir), 100)
		ctx := t.Context()

		data := []byte("filesystem-backed-volume-data")
		blk := stageBlock(t, vol, 0, data)
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{"store": "fs"})

		got, err := vol.ReadAt(ctx, snap.ID, 0, int64(len(data)))
		if err != nil {
			t.Fatalf("ReadAt on FS store failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("expected %q, got %q", data, got)
		}
	})
}

func TestVolume_Cumulative(t *testing.T) {
	t.Run("cumulative manifest", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := t.Context()

		// First snapshot: block at [0, 50).
		data1 := bytes.Repeat([]byte("A"), 50)
		blk1, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data1))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}
		snap1, err := vol.Commit(ctx, []BlockRef{blk1}, lode.Metadata{})
		if err != nil {
			t.Fatalf("Commit 1 failed: %v", err)
		}
		if len(snap1.Manifest.Blocks) != 1 {
			t.Fatalf("expected 1 block in snap1, got %d", len(snap1.Manifest.Blocks))
		}

		// Second snapshot: block at [50, 100).
		data2 := bytes.Repeat([]byte("B"), 50)
		blk2, err := vol.StageWriteAt(ctx, 50, bytes.NewReader(data2))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}
		snap2, err := vol.Commit(ctx, []BlockRef{blk2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("Commit 2 failed: %v", err)
		}

		// Second snapshot manifest must contain BOTH blocks (cumulative).
		if len(snap2.Manifest.Blocks) != 2 {
			t.Fatalf("expected 2 blocks in snap2 (cumulative), got %d", len(snap2.Manifest.Blocks))
		}

		// Parent should reference snap1.
		if snap2.Manifest.ParentSnapshotID != snap1.ID {
			t.Errorf("expected parent %s, got %s", snap1.ID, snap2.Manifest.ParentSnapshotID)
		}
	})

	t.Run("three snapshots cumulative progression", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 90)
		ctx := t.Context()

		// Snap 1: [0, 30)
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 30))
		snap1 := commitBlocks(t, vol, []BlockRef{blk1}, lode.Metadata{})
		if len(snap1.Manifest.Blocks) != 1 {
			t.Fatalf("snap1: expected 1 block, got %d", len(snap1.Manifest.Blocks))
		}
		if snap1.Manifest.ParentSnapshotID != "" {
			t.Errorf("snap1: expected no parent, got %q", snap1.Manifest.ParentSnapshotID)
		}

		// Snap 2: [30, 60)
		blk2 := stageBlock(t, vol, 30, bytes.Repeat([]byte("B"), 30))
		snap2 := commitBlocks(t, vol, []BlockRef{blk2}, lode.Metadata{})
		if len(snap2.Manifest.Blocks) != 2 {
			t.Fatalf("snap2: expected 2 cumulative blocks, got %d", len(snap2.Manifest.Blocks))
		}
		if snap2.Manifest.ParentSnapshotID != snap1.ID {
			t.Errorf("snap2: expected parent %s, got %s", snap1.ID, snap2.Manifest.ParentSnapshotID)
		}

		// Snap 3: [60, 90)
		blk3 := stageBlock(t, vol, 60, bytes.Repeat([]byte("C"), 30))
		snap3 := commitBlocks(t, vol, []BlockRef{blk3}, lode.Metadata{})
		if len(snap3.Manifest.Blocks) != 3 {
			t.Fatalf("snap3: expected 3 cumulative blocks, got %d", len(snap3.Manifest.Blocks))
		}
		if snap3.Manifest.ParentSnapshotID != snap2.ID {
			t.Errorf("snap3: expected parent %s, got %s", snap2.ID, snap3.Manifest.ParentSnapshotID)
		}

		// Read full volume from snap3.
		got, err := vol.ReadAt(ctx, snap3.ID, 0, 90)
		if err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}
		expected := append(bytes.Repeat([]byte("A"), 30), bytes.Repeat([]byte("B"), 30)...)
		expected = append(expected, bytes.Repeat([]byte("C"), 30)...)
		if !bytes.Equal(got, expected) {
			t.Error("full volume read did not match expected data after 3 snapshots")
		}
	})

	t.Run("sparse blocks with gaps", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		ctx := t.Context()

		// Stage non-contiguous blocks: [0, 10) and [50, 60).
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		blk2 := stageBlock(t, vol, 50, bytes.Repeat([]byte("B"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk1, blk2}, lode.Metadata{})

		if len(snap.Manifest.Blocks) != 2 {
			t.Fatalf("expected 2 blocks, got %d", len(snap.Manifest.Blocks))
		}

		// Read committed ranges — both should succeed.
		got1, err := vol.ReadAt(ctx, snap.ID, 0, 10)
		if err != nil {
			t.Fatalf("ReadAt [0,10) failed: %v", err)
		}
		if !bytes.Equal(got1, bytes.Repeat([]byte("A"), 10)) {
			t.Error("block [0,10) data mismatch")
		}

		got2, err := vol.ReadAt(ctx, snap.ID, 50, 10)
		if err != nil {
			t.Fatalf("ReadAt [50,60) failed: %v", err)
		}
		if !bytes.Equal(got2, bytes.Repeat([]byte("B"), 10)) {
			t.Error("block [50,60) data mismatch")
		}

		// Gap [10, 50) should return ErrRangeMissing.
		_, err = vol.ReadAt(ctx, snap.ID, 10, 40)
		if !errors.Is(err, ErrRangeMissing) {
			t.Errorf("expected ErrRangeMissing for gap, got: %v", err)
		}
	})

	t.Run("dense packing adjacent", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		ctx := t.Context()

		// Pack the full volume: [0,25) [25,50) [50,75) [75,100).
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 25))
		blk2 := stageBlock(t, vol, 25, bytes.Repeat([]byte("B"), 25))
		blk3 := stageBlock(t, vol, 50, bytes.Repeat([]byte("C"), 25))
		blk4 := stageBlock(t, vol, 75, bytes.Repeat([]byte("D"), 25))
		snap := commitBlocks(t, vol, []BlockRef{blk1, blk2, blk3, blk4}, lode.Metadata{})

		if len(snap.Manifest.Blocks) != 4 {
			t.Fatalf("expected 4 blocks, got %d", len(snap.Manifest.Blocks))
		}

		// Full volume read.
		got, err := vol.ReadAt(ctx, snap.ID, 0, 100)
		if err != nil {
			t.Fatalf("ReadAt full failed: %v", err)
		}
		expected := bytes.Repeat([]byte("A"), 25)
		expected = append(expected, bytes.Repeat([]byte("B"), 25)...)
		expected = append(expected, bytes.Repeat([]byte("C"), 25)...)
		expected = append(expected, bytes.Repeat([]byte("D"), 25)...)
		if !bytes.Equal(got, expected) {
			t.Error("dense packing full read data mismatch")
		}
	})
}

func TestVolume_ReadAt(t *testing.T) {
	t.Run("spanning blocks", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := t.Context()

		// Stage two adjacent blocks.
		data1 := []byte("AAAAAAAAAA") // 10 bytes at offset 0
		data2 := []byte("BBBBBBBBBB") // 10 bytes at offset 10

		blk1, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data1))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}
		blk2, err := vol.StageWriteAt(ctx, 10, bytes.NewReader(data2))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}

		snap, err := vol.Commit(ctx, []BlockRef{blk1, blk2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Read spanning both blocks: [5, 15).
		got, err := vol.ReadAt(ctx, snap.ID, 5, 10)
		if err != nil {
			t.Fatalf("ReadAt failed: %v", err)
		}

		expected := append([]byte("AAAAA"), []byte("BBBBB")...)
		if !bytes.Equal(got, expected) {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("exact block boundary", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		data := []byte("0123456789")
		blk := stageBlock(t, vol, 0, data)
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		got, err := vol.ReadAt(t.Context(), snap.ID, 0, 10)
		if err != nil {
			t.Fatalf("ReadAt exact boundary failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("expected %q, got %q", data, got)
		}
	})

	t.Run("within single block", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		data := []byte("0123456789")
		blk := stageBlock(t, vol, 0, data)
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		// Read [2, 7) within [0, 10).
		got, err := vol.ReadAt(t.Context(), snap.ID, 2, 5)
		if err != nil {
			t.Fatalf("ReadAt within single block failed: %v", err)
		}
		if !bytes.Equal(got, []byte("23456")) {
			t.Errorf("expected %q, got %q", "23456", got)
		}
	})

	t.Run("full volume", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 20)
		data1 := bytes.Repeat([]byte("A"), 10)
		data2 := bytes.Repeat([]byte("B"), 10)
		blk1 := stageBlock(t, vol, 0, data1)
		blk2 := stageBlock(t, vol, 10, data2)
		snap := commitBlocks(t, vol, []BlockRef{blk1, blk2}, lode.Metadata{})

		got, err := vol.ReadAt(t.Context(), snap.ID, 0, 20)
		if err != nil {
			t.Fatalf("ReadAt full volume failed: %v", err)
		}
		expected := make([]byte, 0, 20)
		expected = append(expected, data1...)
		expected = append(expected, data2...)
		if !bytes.Equal(got, expected) {
			t.Error("full volume read data mismatch")
		}
	})

	t.Run("missing range returns err range missing", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := t.Context()

		// Stage and commit a block at [0, 10).
		blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte("0123456789")))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}
		snap, err := vol.Commit(ctx, []BlockRef{blk}, lode.Metadata{})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Try to read [5, 15) — partially uncommitted.
		_, err = vol.ReadAt(ctx, snap.ID, 5, 10)
		if !errors.Is(err, ErrRangeMissing) {
			t.Errorf("expected ErrRangeMissing, got: %v", err)
		}
	})

	t.Run("gap at start returns err range missing", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// Block at [10, 20), gap at [0, 10).
		blk := stageBlock(t, vol, 10, bytes.Repeat([]byte("A"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		_, err := vol.ReadAt(t.Context(), snap.ID, 0, 5)
		if !errors.Is(err, ErrRangeMissing) {
			t.Errorf("expected ErrRangeMissing for gap at start, got: %v", err)
		}
	})

	t.Run("gap in middle returns err range missing", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// Blocks at [0, 5) and [10, 15), gap at [5, 10).
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 5))
		blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 5))
		snap := commitBlocks(t, vol, []BlockRef{blk1, blk2}, lode.Metadata{})

		_, err := vol.ReadAt(t.Context(), snap.ID, 0, 15)
		if !errors.Is(err, ErrRangeMissing) {
			t.Errorf("expected ErrRangeMissing for gap in middle, got: %v", err)
		}
	})

	t.Run("negative offset returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		_, err := vol.ReadAt(t.Context(), snap.ID, -1, 5)
		if err == nil {
			t.Fatal("expected error for negative offset")
		}
	})

	t.Run("zero length returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		_, err := vol.ReadAt(t.Context(), snap.ID, 0, 0)
		if err == nil {
			t.Fatal("expected error for zero length")
		}
	})

	t.Run("overflow offset returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 1024)
		ctx := t.Context()

		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 1024))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		// offset + length would overflow int64.
		_, err := vol.ReadAt(ctx, snap.ID, math.MaxInt64-5, 10)
		if err == nil {
			t.Fatal("expected error for read offset that would overflow, got nil")
		}
	})
}

func TestVolume_Commit_Manifest(t *testing.T) {
	t.Run("empty metadata succeeds", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap, err := vol.Commit(t.Context(), []BlockRef{blk}, lode.Metadata{})
		if err != nil {
			t.Fatalf("expected success with empty lode.Metadata{}, got: %v", err)
		}
		if snap.ID == "" {
			t.Fatal("expected non-empty snapshot ID")
		}
	})

	t.Run("nil metadata coalesces to empty", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := t.Context()
		blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}

		snap, err := vol.Commit(ctx, []BlockRef{blk}, nil)
		if err != nil {
			t.Fatalf("expected nil metadata to succeed, got: %v", err)
		}
		if snap.Manifest.Metadata == nil {
			t.Fatal("expected non-nil metadata in manifest after nil coalescing")
		}
		if len(snap.Manifest.Metadata) != 0 {
			t.Errorf("expected empty metadata, got %v", snap.Manifest.Metadata)
		}
	})

	t.Run("with checksum algorithm persisted", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100,
			WithChecksum(lode.NewMD5Checksum()),
		)
		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		if snap.Manifest.ChecksumAlgorithm != "md5" {
			t.Errorf("expected checksum algorithm 'md5', got %q", snap.Manifest.ChecksumAlgorithm)
		}
		if blk.Checksum == "" {
			t.Error("expected non-empty block checksum")
		}
	})

	t.Run("without checksum no algorithm", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		if snap.Manifest.ChecksumAlgorithm != "" {
			t.Errorf("expected empty checksum algorithm, got %q", snap.Manifest.ChecksumAlgorithm)
		}
		if blk.Checksum != "" {
			t.Errorf("expected empty block checksum, got %q", blk.Checksum)
		}
	})

	t.Run("manifest put error no snapshot", func(t *testing.T) {
		fs := testutil.NewFaultStore(lode.NewMemory())
		vol := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 100)
		ctx := t.Context()

		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))

		// Inject error on manifest write.
		fs.SetPutError(testutil.ErrInjectedPut, "manifest.json")

		_, err := vol.Commit(ctx, []BlockRef{blk}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error when manifest write fails")
		}

		// Clear error and verify no snapshot is visible.
		fs.SetPutError(nil)
		_, err = vol.Latest(ctx)
		if !errors.Is(err, lode.ErrNoSnapshots) {
			t.Errorf("expected lode.ErrNoSnapshots after failed commit, got: %v", err)
		}
	})
}

func TestVolume_Commit_Blocks(t *testing.T) {
	t.Run("adjacent blocks valid", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// [0, 10) and [10, 20) are adjacent, not overlapping.
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))

		snap, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("expected adjacent blocks to succeed, got: %v", err)
		}
		if len(snap.Manifest.Blocks) != 2 {
			t.Errorf("expected 2 blocks, got %d", len(snap.Manifest.Blocks))
		}
	})

	// TestVolume_Commit_BlocksSortedInManifest verifies that committed blocks
	// are stored sorted by offset in the manifest, enabling O(log B) lookups.
	t.Run("blocks sorted in manifest", func(t *testing.T) {
		store := lode.NewMemory()
		vol, err := New("test-vol", func() (lode.Store, error) { return store, nil }, 100)
		if err != nil {
			t.Fatal(err)
		}

		// Stage blocks out of order.
		blk3 := stageBlock(t, vol, 20, bytes.Repeat([]byte("C"), 10))
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))

		snap := commitBlocks(t, vol, []BlockRef{blk3, blk1, blk2}, lode.Metadata{})

		// Verify blocks in manifest are sorted by offset.
		for i := 1; i < len(snap.Manifest.Blocks); i++ {
			if snap.Manifest.Blocks[i].Offset <= snap.Manifest.Blocks[i-1].Offset {
				t.Fatalf("blocks not sorted: [%d].Offset=%d <= [%d].Offset=%d",
					i, snap.Manifest.Blocks[i].Offset, i-1, snap.Manifest.Blocks[i-1].Offset)
			}
		}
	})

	t.Run("block exceeds bounds returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// Construct a BlockRef that exceeds bounds.
		bad := BlockRef{Offset: 90, Length: 20, Path: volumeBlockPath("test-vol", 90, 20)}
		_, err := vol.Commit(t.Context(), []BlockRef{bad}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for block exceeding volume bounds")
		}
	})

	t.Run("duplicate blocks returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		ctx := t.Context()

		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		// Re-commit the same block (no new blocks) → error.
		_, err := vol.Commit(ctx, []BlockRef{blk}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error when committing only duplicate blocks")
		}
	})

	t.Run("empty block path returns error", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// A BlockRef with an empty path should be rejected.
		_, err = vol.Commit(t.Context(), []BlockRef{{Offset: 0, Length: 10, Path: ""}}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for empty block path")
		}
	})

	t.Run("empty blocks returns error", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = vol.Commit(t.Context(), []BlockRef{}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for empty blocks")
		}
	})

	t.Run("negative block offset returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		bad := BlockRef{Offset: -1, Length: 10, Path: volumeBlockPath("test-vol", -1, 10)}
		_, err := vol.Commit(t.Context(), []BlockRef{bad}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for negative block offset")
		}
	})

	t.Run("overflow block ref returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 1024)

		// Craft a BlockRef where Offset + Length overflows int64.
		overflowBlock := BlockRef{
			Offset: math.MaxInt64 - 5,
			Length: 10,
			Path:   volumeBlockPath("test-vol", math.MaxInt64-5, 10),
		}

		_, err := vol.Commit(t.Context(), []BlockRef{overflowBlock}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for block ref that would overflow, got nil")
		}
	})

	t.Run("path mismatch returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// Stage data at [0, 10) but give it a wrong path.
		bad := BlockRef{Offset: 0, Length: 10, Path: "volumes/other-vol/data/0-10.bin"}
		_, err := vol.Commit(t.Context(), []BlockRef{bad}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for path mismatch")
		}
	})

	t.Run("zero block length returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		bad := BlockRef{Offset: 0, Length: 0, Path: volumeBlockPath("test-vol", 0, 0)}
		_, err := vol.Commit(t.Context(), []BlockRef{bad}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected error for zero block length")
		}
	})
}

func TestVolume_Commit_Overlaps(t *testing.T) {
	t.Run("overlapping blocks returns err overlapping blocks", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := t.Context()

		// Stage two overlapping blocks: [0, 10) and [5, 15).
		blk1, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(bytes.Repeat([]byte("A"), 10)))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}
		blk2, err := vol.StageWriteAt(ctx, 5, bytes.NewReader(bytes.Repeat([]byte("B"), 10)))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}

		_, err = vol.Commit(ctx, []BlockRef{blk1, blk2}, lode.Metadata{})
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Errorf("expected ErrOverlappingBlocks, got: %v", err)
		}
	})

	t.Run("contained block overlap", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// [0, 20) contains [5, 10).
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 20))
		blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 5))

		_, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2}, lode.Metadata{})
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Errorf("expected ErrOverlappingBlocks for contained block, got: %v", err)
		}
	})

	t.Run("same start offset overlap", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// [5, 10) and [5, 15) share the same start offset.
		blk1 := stageBlock(t, vol, 5, bytes.Repeat([]byte("A"), 5))
		blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 10))

		_, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2}, lode.Metadata{})
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Errorf("expected ErrOverlappingBlocks for same-start-offset blocks, got: %v", err)
		}
	})

	t.Run("three block overlap", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// [0, 10) + [5, 15) + [12, 20) — multiple overlaps.
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 10))
		blk3 := stageBlock(t, vol, 12, bytes.Repeat([]byte("C"), 8))

		_, err := vol.Commit(t.Context(), []BlockRef{blk1, blk2, blk3}, lode.Metadata{})
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Errorf("expected ErrOverlappingBlocks for three-block overlap, got: %v", err)
		}
	})

	t.Run("overlap with existing rejected", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)

		// Snap 1: commit [0, 10).
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		commitBlocks(t, vol, []BlockRef{blk1}, lode.Metadata{})

		// Snap 2: try to commit [5, 15) which overlaps with existing [0, 10).
		blk2 := stageBlock(t, vol, 5, bytes.Repeat([]byte("B"), 10))
		_, err := vol.Commit(t.Context(), []BlockRef{blk2}, lode.Metadata{})
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Errorf("expected ErrOverlappingBlocks for overlap with existing, got: %v", err)
		}
	})
}

func TestVolume_Snapshots(t *testing.T) {
	t.Run("latest empty volume returns err no snapshots", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = vol.Latest(t.Context())
		if !errors.Is(err, lode.ErrNoSnapshots) {
			t.Errorf("expected lode.ErrNoSnapshots, got: %v", err)
		}
	})

	t.Run("snapshot not found returns err not found", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = vol.Snapshot(t.Context(), "nonexistent")
		if !errors.Is(err, lode.ErrNotFound) {
			t.Errorf("expected lode.ErrNotFound, got: %v", err)
		}
	})

	t.Run("sorted by created at", func(t *testing.T) {
		store := lode.NewMemory()
		factory := memoryFactoryFrom(store)
		ctx := t.Context()

		// Write manifests with explicit, well-separated timestamps to avoid
		// sort.Slice non-determinism when timestamps are equal.
		t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		for i, snapID := range []SnapshotID{"snap-c", "snap-a", "snap-b"} {
			m := &Manifest{
				SchemaName:    volumeManifestSchemaName,
				FormatVersion: volumeManifestFormatVersion,
				VolumeID:      "test-vol",
				SnapshotID:    snapID,
				CreatedAt:     t0.Add(time.Duration(i) * time.Hour),
				Metadata:      lode.Metadata{},
				TotalLength:   100,
				Blocks:        []BlockRef{},
			}
			writeVolumeManifest(ctx, t, store, m)
		}

		vol := newTestVolume(t, "test-vol", factory, 100)
		snaps, err := vol.Snapshots(ctx)
		if err != nil {
			t.Fatalf("Snapshots failed: %v", err)
		}
		if len(snaps) != 3 {
			t.Fatalf("expected 3 snapshots, got %d", len(snaps))
		}

		// Verify non-decreasing CreatedAt order (the actual sorting invariant).
		for i := 1; i < len(snaps); i++ {
			if snaps[i].Manifest.CreatedAt.Before(snaps[i-1].Manifest.CreatedAt) {
				t.Errorf("snapshot[%d] (%s) has earlier timestamp than snapshot[%d] (%s)",
					i, snaps[i].Manifest.CreatedAt, i-1, snaps[i-1].Manifest.CreatedAt)
			}
		}

		// With distinct timestamps, order should be: snap-c (t0), snap-a (t0+1h), snap-b (t0+2h).
		if snaps[0].ID != "snap-c" || snaps[1].ID != "snap-a" || snaps[2].ID != "snap-b" {
			t.Errorf("expected order [snap-c, snap-a, snap-b], got [%s, %s, %s]",
				snaps[0].ID, snaps[1].ID, snaps[2].ID)
		}
	})

	t.Run("manifest round trip all fields preserved", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100,
			WithChecksum(lode.NewMD5Checksum()),
		)
		ctx := t.Context()

		// First snap for parent chain.
		blk1 := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap1 := commitBlocks(t, vol, []BlockRef{blk1}, lode.Metadata{"key": "value"})

		// Second snap.
		blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))
		snap2 := commitBlocks(t, vol, []BlockRef{blk2}, lode.Metadata{"step": "two"})

		// Load snap2 from store and verify all fields.
		loaded, err := vol.Snapshot(ctx, snap2.ID)
		if err != nil {
			t.Fatalf("Snapshot failed: %v", err)
		}

		m := loaded.Manifest
		if m.SchemaName != volumeManifestSchemaName {
			t.Errorf("schema_name: expected %q, got %q", volumeManifestSchemaName, m.SchemaName)
		}
		if m.FormatVersion != volumeManifestFormatVersion {
			t.Errorf("format_version: expected %q, got %q", volumeManifestFormatVersion, m.FormatVersion)
		}
		if m.VolumeID != "test-vol" {
			t.Errorf("volume_id: expected %q, got %q", "test-vol", m.VolumeID)
		}
		if m.SnapshotID != snap2.ID {
			t.Errorf("snapshot_id: expected %q, got %q", snap2.ID, m.SnapshotID)
		}
		if m.CreatedAt.IsZero() {
			t.Error("created_at: expected non-zero")
		}
		if m.TotalLength != 100 {
			t.Errorf("total_length: expected 100, got %d", m.TotalLength)
		}
		if len(m.Blocks) != 2 {
			t.Fatalf("blocks: expected 2 (cumulative), got %d", len(m.Blocks))
		}
		if m.ParentSnapshotID != snap1.ID {
			t.Errorf("parent_snapshot_id: expected %q, got %q", snap1.ID, m.ParentSnapshotID)
		}
		if m.ChecksumAlgorithm != "md5" {
			t.Errorf("checksum_algorithm: expected %q, got %q", "md5", m.ChecksumAlgorithm)
		}
		if m.Metadata["step"] != "two" {
			t.Errorf("metadata[step]: expected 'two', got %v", m.Metadata["step"])
		}
	})

	// TestVolume_LoadSnapshot_SortsBlocksAtLoadTime verifies that blocks in a manifest
	// written with unsorted offsets are sorted at load time by validateVolumeManifest.
	t.Run("load snapshot sorts blocks at load time", func(t *testing.T) {
		store := lode.NewMemory()
		ctx := t.Context()

		// Write a manifest with intentionally unsorted blocks.
		m := &Manifest{
			SchemaName:    volumeManifestSchemaName,
			FormatVersion: volumeManifestFormatVersion,
			VolumeID:      "test-vol",
			SnapshotID:    "snap-unsorted",
			CreatedAt:     time.Now().UTC(),
			Metadata:      lode.Metadata{},
			TotalLength:   100,
			Blocks: []BlockRef{
				{Offset: 20, Length: 10, Path: volumeBlockPath("test-vol", 20, 10)},
				{Offset: 0, Length: 10, Path: volumeBlockPath("test-vol", 0, 10)},
				{Offset: 10, Length: 10, Path: volumeBlockPath("test-vol", 10, 10)},
			},
		}
		writeVolumeManifest(ctx, t, store, m)

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		snap, err := vol.Snapshot(ctx, "snap-unsorted")
		if err != nil {
			t.Fatalf("Snapshot failed: %v", err)
		}

		// Verify blocks are sorted by offset after load.
		blocks := snap.Manifest.Blocks
		if len(blocks) != 3 {
			t.Fatalf("expected 3 blocks, got %d", len(blocks))
		}
		for i := 1; i < len(blocks); i++ {
			if blocks[i].Offset <= blocks[i-1].Offset {
				t.Fatalf("blocks not sorted at load time: [%d].Offset=%d <= [%d].Offset=%d",
					i, blocks[i].Offset, i-1, blocks[i-1].Offset)
			}
		}
	})

	t.Run("ID returns volume ID", func(t *testing.T) {
		vol, err := New("my-volume", lode.NewMemoryFactory(), 1024)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vol.ID() != "my-volume" {
			t.Errorf("expected 'my-volume', got %q", vol.ID())
		}
	})
}

func TestVolume_StageWriteAt(t *testing.T) {
	t.Run("with checksum", func(t *testing.T) {
		vol, err := New("test-vol", lode.NewMemoryFactory(), 100,
			WithChecksum(lode.NewMD5Checksum()),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		blk, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("checksum-test")))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}

		if blk.Checksum == "" {
			t.Error("expected non-empty checksum when WithChecksum configured")
		}
	})

	t.Run("negative offset returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		_, err := vol.StageWriteAt(t.Context(), -1, bytes.NewReader([]byte("data")))
		if err == nil {
			t.Fatal("expected error for negative offset")
		}
	})

	t.Run("exceeds bounds returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		// 10 bytes at offset 95 → [95, 105) exceeds totalLength=100.
		_, err := vol.StageWriteAt(t.Context(), 95, bytes.NewReader(bytes.Repeat([]byte("X"), 10)))
		if err == nil {
			t.Fatal("expected error for block exceeding volume bounds")
		}
	})

	t.Run("empty reader returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 100)
		_, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader(nil))
		if err == nil {
			t.Fatal("expected error for empty reader")
		}
	})

	t.Run("exact fit", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 10)
		// 10 bytes at offset 0 → [0, 10) exactly fills totalLength=10.
		blk, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader(bytes.Repeat([]byte("X"), 10)))
		if err != nil {
			t.Fatalf("expected success for exact fit, got: %v", err)
		}
		if blk.Offset != 0 || blk.Length != 10 {
			t.Errorf("expected offset=0 length=10, got offset=%d length=%d", blk.Offset, blk.Length)
		}
	})

	t.Run("path layout", func(t *testing.T) {
		vol := newTestVolume(t, "my-vol", lode.NewMemoryFactory(), 100)
		blk, err := vol.StageWriteAt(t.Context(), 5, bytes.NewReader(bytes.Repeat([]byte("X"), 20)))
		if err != nil {
			t.Fatalf("StageWriteAt failed: %v", err)
		}

		expected := "volumes/my-vol/data/5-20.bin"
		if blk.Path != expected {
			t.Errorf("expected path %q, got %q", expected, blk.Path)
		}
	})

	t.Run("put error returns error", func(t *testing.T) {
		fs := testutil.NewFaultStore(lode.NewMemory())
		vol := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 100)

		// Inject error on all Put calls.
		fs.SetPutError(testutil.ErrInjectedPut)

		_, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("data")))
		if err == nil {
			t.Fatal("expected error when store Put fails")
		}
	})

	t.Run("overflow offset returns error", func(t *testing.T) {
		vol := newTestVolume(t, "test-vol", lode.NewMemoryFactory(), 1024)

		// offset + length would overflow int64 if computed naively.
		_, err := vol.StageWriteAt(t.Context(), math.MaxInt64, bytes.NewReader([]byte("A")))
		if err == nil {
			t.Fatal("expected error for offset that would overflow, got nil")
		}
	})
}

// =============================================================================
// A. StageWriteAt edge cases
// =============================================================================

// =============================================================================
// B. Commit validation
// =============================================================================

// =============================================================================
// C. Multi-snapshot progression
// =============================================================================

// =============================================================================
// D. Resume pattern
// =============================================================================

func TestVolume_Resume(t *testing.T) {
	t.Run("new instance loads latest", func(t *testing.T) {
		store := lode.NewMemory()
		factory := memoryFactoryFrom(store)

		// Instance 1: create a snapshot.
		vol1 := newTestVolume(t, "test-vol", factory, 100)
		blk := stageBlock(t, vol1, 0, bytes.Repeat([]byte("A"), 10))
		snap1 := commitBlocks(t, vol1, []BlockRef{blk}, lode.Metadata{"step": "first"})

		// Instance 2: new Volume on same store should see the snapshot.
		vol2 := newTestVolume(t, "test-vol", factory, 100)
		latest, err := vol2.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest on new instance failed: %v", err)
		}
		if latest.ID != snap1.ID {
			t.Errorf("expected snapshot %s, got %s", snap1.ID, latest.ID)
		}
	})

	t.Run("continue staging", func(t *testing.T) {
		store := lode.NewMemory()
		factory := memoryFactoryFrom(store)

		// Instance 1: commit block [0, 10).
		vol1 := newTestVolume(t, "test-vol", factory, 100)
		blk1 := stageBlock(t, vol1, 0, bytes.Repeat([]byte("A"), 10))
		commitBlocks(t, vol1, []BlockRef{blk1}, lode.Metadata{})

		// Instance 2: resume, stage [10, 20), commit → cumulative manifest has 2 blocks.
		vol2 := newTestVolume(t, "test-vol", factory, 100)
		blk2 := stageBlock(t, vol2, 10, bytes.Repeat([]byte("B"), 10))
		snap2 := commitBlocks(t, vol2, []BlockRef{blk2}, lode.Metadata{})

		if len(snap2.Manifest.Blocks) != 2 {
			t.Fatalf("expected 2 cumulative blocks after resume, got %d", len(snap2.Manifest.Blocks))
		}

		// Verify both blocks readable from snap2.
		got, err := vol2.ReadAt(t.Context(), snap2.ID, 0, 20)
		if err != nil {
			t.Fatalf("ReadAt after resume failed: %v", err)
		}
		expected := append(bytes.Repeat([]byte("A"), 10), bytes.Repeat([]byte("B"), 10)...)
		if !bytes.Equal(got, expected) {
			t.Error("data mismatch after resume")
		}
	})
}

// =============================================================================
// E. ReadAt comprehensive
// =============================================================================

// =============================================================================
// F. Overlap validation deep dive
// =============================================================================

// =============================================================================
// G. Manifest validation on load
// =============================================================================

func TestVolume_Snapshot_InvalidManifest(t *testing.T) {
	t.Run("missing schema name", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		m.SchemaName = ""
		writeVolumeManifest(t.Context(), t, store, m)

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err := vol.Snapshot(t.Context(), m.SnapshotID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing volume ID", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		snapID := m.SnapshotID
		m.VolumeID = ""
		// Write to the correct path (using "test-vol" for lookup, not the empty ID).
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		p := volumeManifestPath("test-vol", snapID)
		if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err = vol.Snapshot(t.Context(), snapID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("missing snapshot ID", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		snapID := m.SnapshotID
		m.SnapshotID = ""
		// We must write at the correct path (using the original snapID for path lookup).
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		p := volumeManifestPath("test-vol", snapID)
		if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err = vol.Snapshot(t.Context(), snapID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("zero created at", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		m.CreatedAt = time.Time{}
		writeVolumeManifest(t.Context(), t, store, m)

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err := vol.Snapshot(t.Context(), m.SnapshotID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("nil metadata", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		m.Metadata = nil
		writeVolumeManifest(t.Context(), t, store, m)

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err := vol.Snapshot(t.Context(), m.SnapshotID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("nil blocks", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		m.Blocks = nil
		writeVolumeManifest(t.Context(), t, store, m)

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err := vol.Snapshot(t.Context(), m.SnapshotID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("negative total length", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 100)
		m.TotalLength = -1
		m.Blocks = []BlockRef{} // avoid block bounds failure
		writeVolumeManifest(t.Context(), t, store, m)

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err := vol.Snapshot(t.Context(), m.SnapshotID)
		if !errors.Is(err, lode.ErrManifestInvalid) {
			t.Errorf("expected lode.ErrManifestInvalid, got: %v", err)
		}
	})

	t.Run("volume ID mismatch returns error", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("other-vol", 100)
		// Write under "test-vol" path but with ID="other-vol".
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		p := volumeManifestPath("test-vol", m.SnapshotID)
		if err := store.Put(t.Context(), p, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}

		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err = vol.Snapshot(t.Context(), m.SnapshotID)
		if err == nil {
			t.Fatal("expected error for volume ID mismatch")
		}
	})

	t.Run("total length mismatch returns error", func(t *testing.T) {
		store := lode.NewMemory()
		m := validVolumeManifest("test-vol", 200) // manifest says 200
		writeVolumeManifest(t.Context(), t, store, m)

		// Volume created with totalLength=100, but manifest says 200.
		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)
		_, err := vol.Snapshot(t.Context(), m.SnapshotID)
		if err == nil {
			t.Fatal("expected error for total length mismatch")
		}
	})

	t.Run("validate manifest overflow block returns error", func(t *testing.T) {
		m := validVolumeManifest("test-vol", 1024)
		m.Blocks = []BlockRef{
			{Offset: math.MaxInt64 - 5, Length: 10, Path: "some/path.bin"},
		}

		err := validateVolumeManifest(m)
		if err == nil {
			t.Fatal("expected manifest validation error for block that would overflow, got nil")
		}
	})
}

// =============================================================================
// H. Cross-validation on load
// =============================================================================

// =============================================================================
// I. Checksum
// =============================================================================

// =============================================================================
// J. Manifest round-trip
// =============================================================================

// =============================================================================
// K. FS store
// =============================================================================

// =============================================================================
// L. Fault injection
// =============================================================================

// =============================================================================
// Integer overflow safety tests
// =============================================================================

func TestVolume_ValidateNoOverlaps(t *testing.T) {
	t.Run("overflow prev end no false positive", func(t *testing.T) {
		// Two non-overlapping blocks where prevEnd = Offset + Length would overflow.
		// Block 1 starts at offset 0; block 2 starts near MaxInt64.
		// Naive prevEnd on block 2 would wrap to a small number and falsely report overlap.
		blocks := []BlockRef{
			{Offset: 0, Length: 100, Path: "a.bin"},
			{Offset: math.MaxInt64 - 10, Length: 20, Path: "b.bin"},
		}

		// These blocks don't actually overlap (they're far apart),
		// so this should pass. The key is that it doesn't panic or give
		// wrong results due to overflow in the prevEnd calculation.
		err := validateNoOverlaps(blocks)
		if err != nil {
			t.Fatalf("expected no overlap for non-overlapping blocks, got: %v", err)
		}
	})

	t.Run("adjacent at high offset", func(t *testing.T) {
		// Adjacent blocks at high offsets where Offset + Length approaches MaxInt64.
		blocks := []BlockRef{
			{Offset: math.MaxInt64 - 20, Length: 10, Path: "a.bin"},
			{Offset: math.MaxInt64 - 10, Length: 10, Path: "b.bin"},
		}

		err := validateNoOverlaps(blocks)
		if err != nil {
			t.Fatalf("expected no overlap for adjacent blocks at high offset, got: %v", err)
		}
	})

	t.Run("overlap at high offset", func(t *testing.T) {
		// Overlapping blocks at high offsets where naive arithmetic overflows.
		blocks := []BlockRef{
			{Offset: math.MaxInt64 - 20, Length: 15, Path: "a.bin"},
			{Offset: math.MaxInt64 - 10, Length: 10, Path: "b.bin"},
		}

		err := validateNoOverlaps(blocks)
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Fatalf("expected ErrOverlappingBlocks for overlapping blocks at high offset, got: %v", err)
		}
	})

	t.Run("sorted input", func(t *testing.T) {
		// validateNoOverlaps requires sorted input (sorting now happens at load time
		// in validateVolumeManifest). Verify it accepts non-overlapping sorted blocks.
		blocks := []BlockRef{
			{Offset: 0, Length: 10, Path: "a.bin"},
			{Offset: 10, Length: 10, Path: "b.bin"},
			{Offset: 20, Length: 10, Path: "c.bin"},
		}

		err := validateNoOverlaps(blocks)
		if err != nil {
			t.Fatalf("expected no overlap for non-overlapping sorted blocks, got: %v", err)
		}
	})

	t.Run("sorted overlap", func(t *testing.T) {
		// validateNoOverlaps requires sorted input. Verify it detects overlaps
		// in sorted blocks.
		blocks := []BlockRef{
			{Offset: 0, Length: 20, Path: "a.bin"},
			{Offset: 15, Length: 10, Path: "b.bin"},
		}

		err := validateNoOverlaps(blocks)
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Fatalf("expected ErrOverlappingBlocks for sorted overlapping blocks, got: %v", err)
		}
	})
}

// =============================================================================
// Latest pointer tests (issue #118 / #119)
// =============================================================================

func TestVolume_LatestPointer(t *testing.T) {
	t.Run("read after commit", func(t *testing.T) {
		// After Commit, the pointer file must exist and Latest() must resolve
		// via pointer without calling List.
		store := lode.NewMemory()
		fs := testutil.NewFaultStore(store)
		vol := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 100)

		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		// Verify pointer file exists.
		pointerPath := "volumes/test-vol/latest"
		rc, err := store.Get(t.Context(), pointerPath)
		if err != nil {
			t.Fatalf("expected pointer file at %s, got error: %v", pointerPath, err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != string(snap.ID) {
			t.Errorf("pointer content: expected %q, got %q", snap.ID, string(data))
		}

		// Reset call tracking, verify Latest() uses pointer (no List calls).
		fs.Reset()
		latest, err := vol.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() failed: %v", err)
		}
		if latest.ID != snap.ID {
			t.Errorf("Latest() returned %s, expected %s", latest.ID, snap.ID)
		}
		if len(fs.ListCalls()) != 0 {
			t.Errorf("Latest() should not call List when pointer exists, got %d List calls",
				len(fs.ListCalls()))
		}
	})

	t.Run("multiple commits", func(t *testing.T) {
		// Pointer must always track the most recent snapshot across commits.
		store := lode.NewMemory()
		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 90)

		pointerPath := "volumes/test-vol/latest"

		for i := range 3 {
			offset := int64(i) * 30
			blk := stageBlock(t, vol, offset, bytes.Repeat([]byte("X"), 30))
			snap := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

			rc, err := store.Get(t.Context(), pointerPath)
			if err != nil {
				t.Fatalf("commit %d: pointer missing: %v", i, err)
			}
			data, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(data) != string(snap.ID) {
				t.Errorf("commit %d: pointer %q, expected %q", i, string(data), snap.ID)
			}
		}
	})

	t.Run("backward compat", func(t *testing.T) {
		// Pre-pointer volumes (manifests exist, no pointer) must fall back
		// to scan. After a commit, the pointer is created.
		store := lode.NewMemory()
		vol := newTestVolume(t, "test-vol", memoryFactoryFrom(store), 100)

		blk := stageBlock(t, vol, 0, bytes.Repeat([]byte("A"), 10))
		snap1 := commitBlocks(t, vol, []BlockRef{blk}, lode.Metadata{})

		// Delete the pointer to simulate pre-pointer state.
		_ = store.Delete(t.Context(), "volumes/test-vol/latest")

		// Latest() should still work via scan fallback.
		latest, err := vol.Latest(t.Context())
		if err != nil {
			t.Fatalf("Latest() scan fallback failed: %v", err)
		}
		if latest.ID != snap1.ID {
			t.Errorf("scan fallback returned %s, expected %s", latest.ID, snap1.ID)
		}

		// Next commit should create the pointer.
		blk2 := stageBlock(t, vol, 10, bytes.Repeat([]byte("B"), 10))
		snap2 := commitBlocks(t, vol, []BlockRef{blk2}, lode.Metadata{})

		rc, err := store.Get(t.Context(), "volumes/test-vol/latest")
		if err != nil {
			t.Fatalf("pointer should exist after commit: %v", err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != string(snap2.ID) {
			t.Errorf("pointer %q, expected %q", string(data), snap2.ID)
		}
	})
}

// TestFindCoveringBlocks_BinarySearch verifies that findCoveringBlocks correctly
// uses binary search on sorted blocks. Tests cover exact matches, partial overlaps,
// gaps, and large block counts.
func TestFindCoveringBlocks_BinarySearch(t *testing.T) {
	tests := []struct {
		name    string
		blocks  []BlockRef
		offset  int64
		length  int64
		wantN   int // expected number of covering blocks
		wantErr error
	}{
		{
			name: "exact single block",
			blocks: []BlockRef{
				{Offset: 0, Length: 10},
				{Offset: 10, Length: 10},
				{Offset: 20, Length: 10},
			},
			offset: 10, length: 10,
			wantN: 1,
		},
		{
			name: "spanning two blocks",
			blocks: []BlockRef{
				{Offset: 0, Length: 10},
				{Offset: 10, Length: 10},
				{Offset: 20, Length: 10},
			},
			offset: 5, length: 15,
			wantN: 2,
		},
		{
			name: "all blocks",
			blocks: []BlockRef{
				{Offset: 0, Length: 10},
				{Offset: 10, Length: 10},
				{Offset: 20, Length: 10},
			},
			offset: 0, length: 30,
			wantN: 3,
		},
		{
			name: "gap returns error",
			blocks: []BlockRef{
				{Offset: 0, Length: 10},
				{Offset: 20, Length: 10},
			},
			offset: 5, length: 20,
			wantErr: ErrRangeMissing,
		},
		{
			name:    "empty blocks",
			blocks:  nil,
			offset:  0,
			length:  10,
			wantErr: ErrRangeMissing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := findCoveringBlocks(tt.blocks, tt.offset, tt.length)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantN {
				t.Fatalf("expected %d blocks, got %d", tt.wantN, len(got))
			}
		})
	}
}

// BenchmarkFindCoveringBlocks benchmarks block lookup with varying block counts.
func BenchmarkFindCoveringBlocks(b *testing.B) {
	for _, blockCount := range []int{10, 100, 1000, 10000} {
		blocks := make([]BlockRef, blockCount)
		for i := range blocks {
			blocks[i] = BlockRef{
				Offset: int64(i) * 1024,
				Length: 1024,
			}
		}

		// Request a range in the middle (2 blocks).
		mid := int64(blockCount/2) * 1024
		b.Run(fmt.Sprintf("blocks=%d", blockCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := findCoveringBlocks(blocks, mid, 2048)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestMergeBlocks verifies that mergeBlocks produces a correctly sorted result
// from a pre-sorted slice and an unsorted slice of new blocks.
func TestMergeBlocks(t *testing.T) {
	tests := []struct {
		name     string
		sorted   []BlockRef
		unsorted []BlockRef
		want     []int64 // expected offsets in order
	}{
		{
			name:     "empty both",
			sorted:   nil,
			unsorted: nil,
			want:     nil,
		},
		{
			name:     "existing only",
			sorted:   []BlockRef{{Offset: 0, Length: 10}, {Offset: 10, Length: 10}},
			unsorted: nil,
			want:     []int64{0, 10},
		},
		{
			name:     "new only",
			sorted:   nil,
			unsorted: []BlockRef{{Offset: 20, Length: 10}, {Offset: 0, Length: 10}},
			want:     []int64{0, 20},
		},
		{
			name:     "interleaved",
			sorted:   []BlockRef{{Offset: 0, Length: 10}, {Offset: 20, Length: 10}},
			unsorted: []BlockRef{{Offset: 30, Length: 10}, {Offset: 10, Length: 10}},
			want:     []int64{0, 10, 20, 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeBlocks(tt.sorted, tt.unsorted)
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d blocks, got %d", len(tt.want), len(got))
			}
			for i, want := range tt.want {
				if got[i].Offset != want {
					t.Errorf("[%d] expected offset %d, got %d", i, want, got[i].Offset)
				}
			}
		})
	}
}

// TestVolume_Commit_PendingCommit verifies that a commit returns
// lode.ErrSnapshotConflict while the pointer names a snapshot that has
// neither a canonical nor a staged manifest (a commit persisted before
// manifests were staged, or a hand edit), because that snapshot's blocks
// cannot be merged, and that once the manifest is persisted the commit
// adopts it as parent.
func TestVolume_Commit_PendingCommit(t *testing.T) {
	mem := lode.NewMemory()
	open := func() Volume {
		vol, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		return vol
	}
	commit := func(vol Volume, off int64, data string) (*Snapshot, error) {
		block, err := vol.StageWriteAt(t.Context(), off, bytes.NewReader([]byte(data)))
		if err != nil {
			t.Fatal(err)
		}
		return vol.Commit(t.Context(), []BlockRef{block}, lode.Metadata{})
	}
	if _, err := commit(open(), 0, "block-1"); err != nil {
		t.Fatal(err)
	}
	pending, err := commit(open(), 100, "block-2")
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := "volumes/test-vol/snapshots/" + string(pending.ID) + "/manifest.json"
	rc, err := mem.Get(t.Context(), manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := io.ReadAll(rc)
	_ = rc.Close()
	if err := mem.Delete(t.Context(), manifestPath); err != nil {
		t.Fatal(err)
	}

	late := open()
	block3, err := late.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-3")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := late.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{}); !errors.Is(err, lode.ErrSnapshotConflict) {
		t.Fatalf("commit while another is pending = %v, want lode.ErrSnapshotConflict", err)
	}

	if err := mem.Put(t.Context(), manifestPath, bytes.NewReader(manifest)); err != nil {
		t.Fatal(err)
	}
	next, err := late.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	if next.Manifest.ParentSnapshotID != pending.ID || len(next.Manifest.Blocks) != 3 {
		t.Errorf("parent = %s with %d blocks, want %s with all 3", next.Manifest.ParentSnapshotID, len(next.Manifest.Blocks), pending.ID)
	}
}

// TestVolume_Commit_AbandonedCommit verifies that a commit whose writer
// advanced the pointer but failed to persist the canonical manifest does not
// stall the volume: the next commit, from another handle, loads the staged
// copy, adopts the abandoned snapshot as parent, and merges its blocks.
func TestVolume_Commit_AbandonedCommit(t *testing.T) {
	mem := lode.NewMemory()
	fs := testutil.NewFaultStore(mem)
	abandoning := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 100)
	commitBlocks(t, abandoning, []BlockRef{stageBlock(t, abandoning, 0, []byte("first"))}, lode.Metadata{})

	fs.SetPutError(testutil.ErrInjectedPut, "manifest.json")
	abandoned, err := abandoning.Commit(t.Context(), []BlockRef{stageBlock(t, abandoning, 10, []byte("second"))}, lode.Metadata{})
	if err == nil {
		t.Fatalf("commit with a failing manifest write = %+v, want an error", abandoned)
	}
	fs.SetPutError(nil)
	pointed, err := lodeGet(t, mem, "volumes/test-vol/latest")
	if err != nil {
		t.Fatal(err)
	}

	next := newTestVolume(t, "test-vol", memoryFactoryFrom(mem), 100)
	snap, err := next.Commit(t.Context(), []BlockRef{stageBlock(t, next, 20, []byte("third"))}, lode.Metadata{})
	if err != nil {
		t.Fatalf("commit after an abandoned commit: %v", err)
	}
	if string(snap.Manifest.ParentSnapshotID) != pointed || len(snap.Manifest.Blocks) != 3 {
		t.Errorf("parent = %s with %d blocks, want the abandoned %s with all 3", snap.Manifest.ParentSnapshotID, len(snap.Manifest.Blocks), pointed)
	}
	if got, err := next.ReadAt(t.Context(), snap.ID, 10, 6); err != nil || string(got) != "second" {
		t.Errorf("abandoned commit's block = %q, %v; want %q", got, err, "second")
	}
	if _, err := mem.Get(t.Context(), "volumes/test-vol/staged/"+string(snap.ID)+".json"); !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("staged copy of a completed commit: %v, want lode.ErrNotFound", err)
	}
}

// lodeGet reads an object as a string.
func lodeGet(t *testing.T, store lode.Store, path string) (string, error) {
	t.Helper()
	rc, err := store.Get(t.Context(), path)
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	return string(b), err
}

func TestVolume_Commit_Pointer(t *testing.T) {
	// TestVolume_Commit_StaleButExistingPointer_UsesInMemoryCache verifies that
	// when the pointer is externally modified after a successful commit, CAS
	// detects the external modification and returns lode.ErrSnapshotConflict.
	t.Run("stale but existing pointer uses in memory cache", func(t *testing.T) {
		mem := lode.NewMemory()
		vol, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}

		// Commit snap-1 and snap-2 normally.
		block1, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-1")))
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := vol.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		block2, err := vol.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-2")))
		if err != nil {
			t.Fatal(err)
		}
		_, err = vol.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		// Externally modify the pointer to point back to snap-1.
		pointerPath := "volumes/test-vol/latest"
		if err := mem.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte(snap1.ID))); err != nil {
			t.Fatal(err)
		}

		// With CAS, the commit detects external pointer modification.
		block3, err := vol.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-3")))
		if err != nil {
			t.Fatal(err)
		}
		_, err = vol.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if !errors.Is(err, lode.ErrSnapshotConflict) {
			t.Fatalf("expected lode.ErrSnapshotConflict for externally modified pointer, got: %v", err)
		}
	})

	// TestVolume_Commit_StalePointer_FallsBackToScan verifies that a stale
	// latest pointer (referencing a nonexistent snapshot) is detected by CAS
	// as external modification and returns lode.ErrSnapshotConflict.
	t.Run("stale pointer falls back to scan", func(t *testing.T) {
		mem := lode.NewMemory()
		vol, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}

		// Stage and commit first block.
		block1, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-1-data")))
		if err != nil {
			t.Fatal(err)
		}
		_, err = vol.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		// Corrupt the pointer to reference a nonexistent snapshot.
		pointerPath := "volumes/test-vol/latest"
		if err := mem.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte("nonexistent-snap-id"))); err != nil {
			t.Fatal(err)
		}

		// With CAS, the commit detects external pointer modification.
		block2, err := vol.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-2-data")))
		if err != nil {
			t.Fatal(err)
		}
		_, err = vol.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if !errors.Is(err, lode.ErrSnapshotConflict) {
			t.Fatalf("expected lode.ErrSnapshotConflict for externally modified pointer, got: %v", err)
		}
	})

	// TestVolume_Commit_PointerWriteFailure_AbortsCommit verifies that when
	// writeLatestPointer fails, the commit is aborted and no manifest is written.
	t.Run("pointer write failure aborts commit", func(t *testing.T) {
		fs := testutil.NewFaultStore(lode.NewMemory())
		factory := newFaultStoreFactory(fs)

		vol, err := New("test-vol", factory, 1024*1024)
		if err != nil {
			t.Fatal(err)
		}

		// First commit succeeds.
		block1, err := vol.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-1")))
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := vol.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		// Inject CAS error on the "latest" pointer path.
		// writeLatestPointer uses CompareAndSwap when the store implements lode.ConditionalWriter.
		fs.SetCASError(errors.New("injected: pointer write failure"), "latest")

		// Record calls before the failed commit.
		fs.Reset()

		// Second commit should fail because pointer write is required.
		block2, err := vol.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-2")))
		if err != nil {
			t.Fatal(err)
		}
		_, err = vol.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err == nil {
			t.Fatal("expected commit to fail when pointer write fails")
		}

		// Verify no manifest was written.
		putCalls := fs.PutCalls()
		for _, p := range putCalls {
			if strings.Contains(p, "manifest.json") {
				t.Errorf("manifest should not be written when pointer fails, but saw Put(%s)", p)
			}
		}

		// Clear the CAS error and commit again — should succeed with snap-1 as parent.
		fs.SetCASError(nil)

		block2b, err := vol.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-2b")))
		if err != nil {
			t.Fatal(err)
		}
		snap3, err := vol.Commit(t.Context(), []BlockRef{block2b}, lode.Metadata{})
		if err != nil {
			t.Fatalf("commit after clearing error should succeed: %v", err)
		}
		if snap3.Manifest.ParentSnapshotID != snap1.ID {
			t.Errorf("expected parent %s, got %s", snap1.ID, snap3.Manifest.ParentSnapshotID)
		}
	})
}

func TestVolume_Commit_ColdStart(t *testing.T) {
	// TestVolume_Commit_ColdStart_CorruptPointer_FallsBackToScan verifies that
	// a new Volume instance (cold start) with a corrupt pointer correctly falls
	// back to scan.
	t.Run("corrupt pointer falls back to scan", func(t *testing.T) {
		mem := lode.NewMemory()

		// Process A: commit 2 snapshots.
		volA, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		block1, err := volA.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-1")))
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := volA.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}
		// The scan below must find snap2 as the largest ID.
		testutil.AwaitClockPast(t, string(snap1.ID))

		block2, err := volA.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-2")))
		if err != nil {
			t.Fatal(err)
		}
		snap2, err := volA.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		// Corrupt the pointer to reference a nonexistent snapshot.
		pointerPath := "volumes/test-vol/latest"
		if err := mem.Delete(t.Context(), pointerPath); err != nil {
			t.Fatal(err)
		}
		if err := mem.Put(t.Context(), pointerPath, bytes.NewReader([]byte("nonexistent-snap-id"))); err != nil {
			t.Fatal(err)
		}

		// Process B: cold start with corrupt pointer.
		volB, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		block3, err := volB.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-3")))
		if err != nil {
			t.Fatal(err)
		}
		snap3, err := volB.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if err != nil {
			t.Fatalf("cold-start commit with corrupt pointer should succeed: %v", err)
		}

		// snap3 must have snap2 as parent (from scan fallback).
		if snap3.Manifest.ParentSnapshotID != snap2.ID {
			t.Errorf("expected parent %s (from scan), got %s", snap2.ID, snap3.Manifest.ParentSnapshotID)
		}
		// Cumulative blocks should include all three.
		if len(snap3.Manifest.Blocks) != 3 {
			t.Errorf("expected 3 cumulative blocks, got %d", len(snap3.Manifest.Blocks))
		}
	})

	// TestVolume_Commit_ColdStart_ReadsPointerFromStore verifies that a new
	// Volume instance (cold start with no in-memory cache) resolves the correct
	// parent by reading the persistent pointer from the store.
	t.Run("reads pointer from store", func(t *testing.T) {
		mem := lode.NewMemory()

		// Process A: commit 3 snapshots.
		volA, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		block1, err := volA.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-1")))
		if err != nil {
			t.Fatal(err)
		}
		snap1, err := volA.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		block2, err := volA.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-2")))
		if err != nil {
			t.Fatal(err)
		}
		_, err = volA.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		block3, err := volA.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-3")))
		if err != nil {
			t.Fatal(err)
		}
		snap3, err := volA.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if err != nil {
			t.Fatal(err)
		}

		// Verify pointer is on snap3 (protocol ensures this).
		rc, err := mem.Get(t.Context(), "volumes/test-vol/latest")
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(data) != string(snap3.ID) {
			t.Fatalf("pointer should track snap3, got %q", string(data))
		}

		// Process B: cold start (new Volume instance, empty in-memory cache).
		volB, err := New("test-vol", memoryFactoryFrom(mem), 1024*1024)
		if err != nil {
			t.Fatal(err)
		}
		block4, err := volB.StageWriteAt(t.Context(), 300, bytes.NewReader([]byte("block-4")))
		if err != nil {
			t.Fatal(err)
		}
		snap4, err := volB.Commit(t.Context(), []BlockRef{block4}, lode.Metadata{})
		if err != nil {
			t.Fatalf("cold-start commit should succeed: %v", err)
		}

		// snap4 must have snap3 as parent (pointer is correct).
		if snap4.Manifest.ParentSnapshotID != snap3.ID {
			t.Errorf("expected parent %s, got %s (snap1=%s)",
				snap3.ID, snap4.Manifest.ParentSnapshotID, snap1.ID)
		}
		// Cumulative blocks should include all four.
		if len(snap4.Manifest.Blocks) != 4 {
			t.Errorf("expected 4 cumulative blocks, got %d", len(snap4.Manifest.Blocks))
		}
	})
}

// BenchmarkMergeBlocks measures merge performance with varying existing block counts.
func BenchmarkMergeBlocks(b *testing.B) {
	for _, existingCount := range []int{10, 100, 1000, 10000} {
		existing := make([]BlockRef, existingCount)
		for i := range existing {
			existing[i] = BlockRef{Offset: int64(i) * 1024, Length: 1024}
		}
		// 3 new blocks at varying offsets (typical commit pattern).
		newBlocks := []BlockRef{
			{Offset: int64(existingCount+2) * 1024, Length: 1024},
			{Offset: int64(existingCount) * 1024, Length: 1024},
			{Offset: int64(existingCount+1) * 1024, Length: 1024},
		}

		b.Run(fmt.Sprintf("existing=%d/new=3", existingCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = mergeBlocks(existing, newBlocks)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// CAS integration tests — concurrent conflict detection
// -----------------------------------------------------------------------------

func TestVolume_Commit_Conflict(t *testing.T) {
	// TestVolume_Commit_ConcurrentConflict verifies that two volume instances
	// sharing the same store detect concurrent commits via lode.ErrSnapshotConflict.
	// Memory store implements lode.ConditionalWriter, enabling CAS.
	//
	// Scenario: v1 commits first. v2 then commits, which succeeds because it
	// reads the updated pointer. But then v1 commits again — its cached pointer
	// is now stale because v2 updated it, so CAS detects the conflict.
	t.Run("concurrent conflict", func(t *testing.T) {
		mem := lode.NewMemory()
		factory := memoryFactoryFrom(mem)

		v1, err := New("shared", factory, 1024)
		if err != nil {
			t.Fatalf("New v1: %v", err)
		}
		v2, err := New("shared", factory, 1024)
		if err != nil {
			t.Fatalf("New v2: %v", err)
		}

		// Both start empty. v1 commits first (CAS create: expected="" → snap1).
		block1, err := v1.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-a")))
		if err != nil {
			t.Fatalf("v1 stage: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v1 first commit: %v", err)
		}

		// v2 commits (reads pointer from store → sees snap1 → CAS update succeeds).
		block2, err := v2.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-b")))
		if err != nil {
			t.Fatalf("v2 stage: %v", err)
		}
		_, err = v2.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v2 first commit: %v", err)
		}

		// Now v1's cached pointer is "snap1" but v2 updated it to "snap2".
		// v1's next commit should detect the conflict.
		block3, err := v1.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-c")))
		if err != nil {
			t.Fatalf("v1 stage 2: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if !errors.Is(err, lode.ErrSnapshotConflict) {
			t.Fatalf("expected lode.ErrSnapshotConflict from v1 (stale cache), got: %v", err)
		}

		// Documented retry path: re-read Latest(), then re-commit on the same instance.
		_, err = v1.Latest(t.Context())
		if err != nil {
			t.Fatalf("v1 Latest() after conflict: %v", err)
		}

		// Re-stage and commit. Commit merges parent blocks internally,
		// so only the new block is passed.
		block4, err := v1.StageWriteAt(t.Context(), 300, bytes.NewReader([]byte("block-d")))
		if err != nil {
			t.Fatalf("v1 stage 3: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block4}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v1 retry after Latest() should succeed, got: %v", err)
		}
	})

	// TestVolume_Commit_WithRetryCount_OverlapOnRetry verifies that when a
	// concurrent writer creates an overlapping block, the retry terminates
	// with ErrOverlappingBlocks (non-retryable).
	t.Run("with retry count overlap on retry", func(t *testing.T) {
		mem := lode.NewMemory()
		factory := memoryFactoryFrom(mem)

		v1, err := New("shared", factory, 1024,
			WithRetryCount(3),
			WithRetryJitter(0.0),
			WithRetryBaseDelay(time.Millisecond),
		)
		if err != nil {
			t.Fatalf("New v1: %v", err)
		}
		v2, err := New("shared", factory, 1024)
		if err != nil {
			t.Fatalf("New v2: %v", err)
		}

		// v1 commits block at offset 0.
		block1, err := v1.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-a")))
		if err != nil {
			t.Fatalf("v1 stage: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v1 first commit: %v", err)
		}

		// v2 commits a non-overlapping block at offset 100, advancing the pointer.
		block2, err := v2.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-b")))
		if err != nil {
			t.Fatalf("v2 stage: %v", err)
		}
		_, err = v2.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v2 commit: %v", err)
		}

		// v1 stages a block that overlaps with v2's block at [100, 107).
		// On retry after CAS conflict, v1 refreshes state and discovers v2's
		// block. Merging [103, 105) with [100, 107) → ErrOverlappingBlocks.
		block3, err := v1.StageWriteAt(t.Context(), 103, bytes.NewReader([]byte("ab")))
		if err != nil {
			t.Fatalf("v1 stage 2: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if !errors.Is(err, ErrOverlappingBlocks) {
			t.Fatalf("expected ErrOverlappingBlocks on retry, got: %v", err)
		}
	})

	// TestVolume_Commit_WithRetryCount_SucceedsAfterConflict verifies that a
	// volume configured with WithRetryCount automatically retries on CAS
	// conflict and succeeds with re-merged blocks.
	t.Run("with retry count succeeds after conflict", func(t *testing.T) {
		mem := lode.NewMemory()
		factory := memoryFactoryFrom(mem)

		v1, err := New("shared", factory, 1024,
			WithRetryCount(3),
			WithRetryJitter(0.0),
			WithRetryBaseDelay(time.Millisecond),
		)
		if err != nil {
			t.Fatalf("New v1: %v", err)
		}
		v2, err := New("shared", factory, 1024)
		if err != nil {
			t.Fatalf("New v2: %v", err)
		}

		// v1 commits first.
		block1, err := v1.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-a")))
		if err != nil {
			t.Fatalf("v1 stage 1: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v1 first commit: %v", err)
		}

		// v2 commits at a different offset, advancing the pointer.
		block2, err := v2.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-b")))
		if err != nil {
			t.Fatalf("v2 stage: %v", err)
		}
		_, err = v2.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v2 commit: %v", err)
		}

		// v1 commits at yet another offset — stale cache triggers CAS conflict
		// on first attempt, but retry should succeed with re-merged blocks.
		block3, err := v1.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-c")))
		if err != nil {
			t.Fatalf("v1 stage 2: %v", err)
		}
		snap, err := v1.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v1 commit with retry should succeed, got: %v", err)
		}

		// Verify cumulative blocks: should contain all three blocks.
		if len(snap.Manifest.Blocks) != 3 {
			t.Errorf("expected 3 cumulative blocks, got %d", len(snap.Manifest.Blocks))
		}
	})
}

// -----------------------------------------------------------------------------
// CAS retry tests (WithRetryCount)
// -----------------------------------------------------------------------------

func TestVolume_Commit_RetryOptions(t *testing.T) {
	// TestVolume_Commit_WithRetryCount_Zero_CurrentBehavior verifies that
	// WithRetryCount(0) preserves the default behavior (no retry).
	t.Run("with retry count zero current behavior", func(t *testing.T) {
		mem := lode.NewMemory()
		factory := memoryFactoryFrom(mem)

		v1, err := New("shared", factory, 1024, WithRetryCount(0))
		if err != nil {
			t.Fatalf("New v1: %v", err)
		}
		v2, err := New("shared", factory, 1024)
		if err != nil {
			t.Fatalf("New v2: %v", err)
		}

		// v1 commits first.
		block1, err := v1.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-a")))
		if err != nil {
			t.Fatalf("v1 stage: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v1 first commit: %v", err)
		}

		// v2 commits, advancing pointer.
		block2, err := v2.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-b")))
		if err != nil {
			t.Fatalf("v2 stage: %v", err)
		}
		_, err = v2.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{})
		if err != nil {
			t.Fatalf("v2 commit: %v", err)
		}

		// v1 should fail immediately without retry.
		block3, err := v1.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-c")))
		if err != nil {
			t.Fatalf("v1 stage 2: %v", err)
		}
		_, err = v1.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{})
		if !errors.Is(err, lode.ErrSnapshotConflict) {
			t.Fatalf("expected lode.ErrSnapshotConflict with retries=0, got: %v", err)
		}
	})

	// TestVolume_Commit_WithRetryObserver_CalledPerRetry verifies the observer
	// is invoked once per Volume commit retry with 1-indexed attempt numbers.
	t.Run("with retry observer called per retry", func(t *testing.T) {
		mem := lode.NewMemory()
		factory := memoryFactoryFrom(mem)

		var attempts []int
		v1, err := New("shared", factory, 1024,
			WithRetryCount(3),
			WithRetryJitter(0.0),
			WithRetryBaseDelay(time.Millisecond),
			WithRetryObserver(func(attempt int) { attempts = append(attempts, attempt) }),
		)
		if err != nil {
			t.Fatalf("New v1: %v", err)
		}
		v2, err := New("shared", factory, 1024)
		if err != nil {
			t.Fatalf("New v2: %v", err)
		}

		block1, err := v1.StageWriteAt(t.Context(), 0, bytes.NewReader([]byte("block-a")))
		if err != nil {
			t.Fatalf("v1 stage 1: %v", err)
		}
		if _, err := v1.Commit(t.Context(), []BlockRef{block1}, lode.Metadata{}); err != nil {
			t.Fatalf("v1 first commit: %v", err)
		}
		if len(attempts) != 0 {
			t.Fatalf("expected no observer calls without conflict, got %v", attempts)
		}

		block2, err := v2.StageWriteAt(t.Context(), 100, bytes.NewReader([]byte("block-b")))
		if err != nil {
			t.Fatalf("v2 stage: %v", err)
		}
		if _, err := v2.Commit(t.Context(), []BlockRef{block2}, lode.Metadata{}); err != nil {
			t.Fatalf("v2 commit: %v", err)
		}

		block3, err := v1.StageWriteAt(t.Context(), 200, bytes.NewReader([]byte("block-c")))
		if err != nil {
			t.Fatalf("v1 stage 2: %v", err)
		}
		if _, err := v1.Commit(t.Context(), []BlockRef{block3}, lode.Metadata{}); err != nil {
			t.Fatalf("v1 commit with retry: %v", err)
		}
		if len(attempts) != 1 || attempts[0] != 1 {
			t.Fatalf("expected observer calls [1], got %v", attempts)
		}
	})

	// TestVolume_WithRetryCount_OptionValidation verifies volume option validation.
	t.Run("with retry count option validation", func(t *testing.T) {
		_, err := New("test", lode.NewMemoryFactory(), 1024, WithRetryCount(-1))
		if err == nil {
			t.Fatal("expected error for negative retry count")
		}

		_, err = New("test", lode.NewMemoryFactory(), 1024, WithRetryObserver(nil))
		if err == nil {
			t.Fatal("expected error for nil retry observer")
		}
	})
}
