// Example: Sparse Volume Ranges
//
// This example demonstrates the Volume persistence paradigm for sparse,
// range-addressable byte storage:
//   - Stage non-contiguous byte ranges and commit them as a snapshot
//   - ErrRangeMissing for reads of uncommitted gaps
//   - Incremental commits with cumulative manifests
//   - Resume pattern: new Volume instance picks up prior state
//   - Full-volume read after all gaps are filled
//
// Volume lives in package lode/volume and is experimental: outside Lode's
// v1.0 compatibility promise until it passes its own stability gate
// (docs/V1_API_DECISIONS.md D9). Dataset builds complete objects before
// committing; Volume commits truth incrementally via sparse byte ranges.
//
// Run with: go run ./examples/volume_sparse
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/pithecene-io/lode/lode"
	"github.com/pithecene-io/lode/lode/volume"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-volume-sparse-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem store factory
	storeFactory := lode.NewFSFactory(tmpDir)

	// -------------------------------------------------------------------------
	// SETUP: Create a 1 KB volume
	// -------------------------------------------------------------------------
	fmt.Println("=== SETUP ===")

	vol, err := volume.New("disk-image", storeFactory, 1024)
	if err != nil {
		return fmt.Errorf("failed to create volume: %w", err)
	}

	fmt.Printf("Volume ID: %s\n", vol.ID())
	fmt.Printf("Total length: 1024 bytes (4 x 256-byte sectors)\n\n")

	// -------------------------------------------------------------------------
	// FIRST COMMIT: Stage two non-contiguous sectors
	// -------------------------------------------------------------------------
	fmt.Println("=== FIRST COMMIT (sparse ranges) ===")

	// Sector 0: [0, 256) — "boot sector"
	blk0, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(bytes.Repeat([]byte{0xAA}, 256)))
	if err != nil {
		return fmt.Errorf("failed to stage sector 0: %w", err)
	}
	fmt.Printf("Staged sector 0: offset=%d length=%d path=%s\n", blk0.Offset, blk0.Length, blk0.Path)

	// Sector 2: [512, 768) — "file table" (skip sector 1)
	blk2, err := vol.StageWriteAt(ctx, 512, bytes.NewReader(bytes.Repeat([]byte{0xBB}, 256)))
	if err != nil {
		return fmt.Errorf("failed to stage sector 2: %w", err)
	}
	fmt.Printf("Staged sector 2: offset=%d length=%d path=%s\n", blk2.Offset, blk2.Length, blk2.Path)

	snap1, err := vol.Commit(ctx, []volume.BlockRef{blk0, blk2}, lode.Metadata{"step": "initial"})
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	fmt.Printf("\nSnapshot 1: %s\n", snap1.ID)
	fmt.Printf("  Blocks: %d (cumulative)\n", len(snap1.Manifest.Blocks))
	fmt.Printf("  Parent: %q (empty = first snapshot)\n", snap1.Manifest.ParentSnapshotID)

	// Read back committed ranges
	data0, err := vol.ReadAt(ctx, snap1.ID, 0, 256)
	if err != nil {
		return fmt.Errorf("failed to read sector 0: %w", err)
	}
	fmt.Printf("  Read sector 0: %d bytes, all 0x%02X: %v\n", len(data0), 0xAA, allEqual(data0, 0xAA))

	data2, err := vol.ReadAt(ctx, snap1.ID, 512, 256)
	if err != nil {
		return fmt.Errorf("failed to read sector 2: %w", err)
	}
	fmt.Printf("  Read sector 2: %d bytes, all 0x%02X: %v\n\n", len(data2), 0xBB, allEqual(data2, 0xBB))

	// -------------------------------------------------------------------------
	// UNCOMMITTED RANGE: Demonstrate ErrRangeMissing
	// -------------------------------------------------------------------------
	fmt.Println("=== UNCOMMITTED RANGE (ErrRangeMissing) ===")

	// Sector 1 [256, 512) was never committed — reading it must fail.
	_, err = vol.ReadAt(ctx, snap1.ID, 256, 256)
	switch {
	case errors.Is(err, volume.ErrRangeMissing):
		fmt.Printf("ReadAt [256, 512) returned ErrRangeMissing (expected)\n")
		fmt.Printf("  Sparse volumes explicitly track which ranges are committed.\n")
		fmt.Printf("  Gaps are detected, not silently zero-filled.\n\n")
	case err == nil:
		return fmt.Errorf("expected ErrRangeMissing for gap, but ReadAt succeeded")
	default:
		return fmt.Errorf("expected ErrRangeMissing for gap, got: %w", err)
	}

	// -------------------------------------------------------------------------
	// INCREMENTAL COMMIT: Fill the first gap
	// -------------------------------------------------------------------------
	fmt.Println("=== INCREMENTAL COMMIT (fill gap) ===")

	// Stage sector 1: [256, 512)
	blk1, err := vol.StageWriteAt(ctx, 256, bytes.NewReader(bytes.Repeat([]byte{0xCC}, 256)))
	if err != nil {
		return fmt.Errorf("failed to stage sector 1: %w", err)
	}

	snap2, err := vol.Commit(ctx, []volume.BlockRef{blk1}, lode.Metadata{"step": "fill-gap"})
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	fmt.Printf("Snapshot 2: %s\n", snap2.ID)
	fmt.Printf("  Blocks: %d (cumulative — includes sectors 0, 1, 2)\n", len(snap2.Manifest.Blocks))
	fmt.Printf("  Parent: %s\n", snap2.Manifest.ParentSnapshotID)

	// The previously-missing range now succeeds.
	data1, err := vol.ReadAt(ctx, snap2.ID, 256, 256)
	if err != nil {
		return fmt.Errorf("failed to read sector 1: %w", err)
	}
	fmt.Printf("  Read sector 1: %d bytes, all 0x%02X: %v\n\n", len(data1), 0xCC, allEqual(data1, 0xCC))

	// -------------------------------------------------------------------------
	// RESUME PATTERN: New Volume instance picks up where we left off
	// -------------------------------------------------------------------------
	fmt.Println("=== RESUME PATTERN (new Volume instance) ===")

	// Simulate a process restart: create a new Volume on the same store.
	vol2, err := volume.New("disk-image", storeFactory, 1024)
	if err != nil {
		return fmt.Errorf("failed to create resumed volume: %w", err)
	}

	latest, err := vol2.Latest(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest: %w", err)
	}

	fmt.Printf("Resumed volume %q from latest snapshot %s\n", vol2.ID(), latest.ID)
	fmt.Printf("  Committed blocks: %d\n", len(latest.Manifest.Blocks))

	// Stage the final sector: [768, 1024)
	blk3, err := vol2.StageWriteAt(ctx, 768, bytes.NewReader(bytes.Repeat([]byte{0xDD}, 256)))
	if err != nil {
		return fmt.Errorf("failed to stage sector 3: %w", err)
	}

	snap3, err := vol2.Commit(ctx, []volume.BlockRef{blk3}, lode.Metadata{"step": "complete"})
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	fmt.Printf("\nSnapshot 3: %s\n", snap3.ID)
	fmt.Printf("  Blocks: %d (all 4 sectors committed)\n", len(snap3.Manifest.Blocks))
	fmt.Printf("  Parent: %s\n\n", snap3.Manifest.ParentSnapshotID)

	// -------------------------------------------------------------------------
	// FULL READ: Read the entire volume
	// -------------------------------------------------------------------------
	fmt.Println("=== FULL READ ===")

	full, err := vol2.ReadAt(ctx, snap3.ID, 0, 1024)
	if err != nil {
		return fmt.Errorf("failed to read full volume: %w", err)
	}

	fmt.Printf("Read all 1024 bytes from snapshot %s\n", snap3.ID)
	fmt.Printf("  Sector 0 [0, 256):     all 0x%02X: %v\n", 0xAA, allEqual(full[0:256], 0xAA))
	fmt.Printf("  Sector 1 [256, 512):   all 0x%02X: %v\n", 0xCC, allEqual(full[256:512], 0xCC))
	fmt.Printf("  Sector 2 [512, 768):   all 0x%02X: %v\n", 0xBB, allEqual(full[512:768], 0xBB))
	fmt.Printf("  Sector 3 [768, 1024):  all 0x%02X: %v\n", 0xDD, allEqual(full[768:1024], 0xDD))

	// -------------------------------------------------------------------------
	// SUCCESS
	// -------------------------------------------------------------------------
	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Sparse volume example complete!")
	fmt.Println("\nKey points demonstrated:")
	fmt.Println("  1. Sparse ranges: commit non-contiguous blocks in any order")
	fmt.Println("  2. ErrRangeMissing: explicit gap detection for uncommitted ranges")
	fmt.Println("  3. Incremental commits: each manifest is cumulative (all blocks)")
	fmt.Println("  4. Resume pattern: new Volume instance picks up prior state via Latest()")
	fmt.Println("  5. Manifest-driven visibility: staged data invisible until Commit")

	return nil
}

// allEqual returns true if every byte in data equals b.
func allEqual(data []byte, b byte) bool {
	return bytes.Equal(data, bytes.Repeat([]byte{b}, len(data)))
}
