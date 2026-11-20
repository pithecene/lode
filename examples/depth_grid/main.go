// Example: Depth-Grid Sparse Volume (Three-Commit Adaptation)
//
// This example is the exact Volume recipe behind the Lode Volume inspector and
// block map on pithecene.sh/foundations/lode: a 32-by-32 bathymetric depth
// grid (one byte per cell in metres, stored as four 256-byte tiles) committed
// out of order across three snapshots through the experimental lode/volume
// package.
//
// Commit 1 writes Northwest (0..255) and Southwest (512..767); reading
// Northeast (256..511) returns volume.ErrRangeMissing rather than zeros.
// Commit 2 writes Northeast (256..511). Commit 3 reopens the volume from its
// latest snapshot, writes Southeast (768..1023), and reads all 1,024 bytes.
//
// Pass -dir <path> to retain the written manifests and block files on disk.
//
// Run with:
//
//	go run ./examples/depth_grid
//	go run ./examples/depth_grid -dir ./depth-grid-store
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"
)

var tileHex = [4]string{
	"060708090a0b0b0c0d0e0f1011121314060708090a0b0c0d0d0e0f1011121314070708090a0b0c0d0e0f101011121314070809090a0b0c0d0e0f1011121213140c0a090a0b0c0c0d0e0f101112131415110f0d0c0b0c0d0e0e0f101112131415161412100f0d0d0e0f10111112131415111517151412100f0f101112131314150d101417191715141210111213141516080c0f13161a1a19171514121314151609090b0e1215191c1c1a191715141516090a0b0b0d1114181b1f1d1c1a181716090a0b0c0d0e1013171a1e211f1d1c1a090a0b0c0d0e0f101216191d2022211f0a0a0b0c0d0e0f10111215181c1f23240a0b0c0c0d0e0f1011121314171b1e22",
	"1415161718191a1b1c1d1d1e1f2021221516161718191a1b1c1d1e1f1f2021221516171819191a1b1c1d1e1f2021222215161718191a1b1b1c1d1e1f2021222315161718191a1b1c1d1e1e1f2021222316171718191a1b1c1d1e1f2020212223161718191a1a1b1c1d1e1f2021222323161718191a1b1c1c1d1e1f2021222324161718191a1b1c1d1e1f1f2021222324171818191a1b1c1d1e1f2021212223241718191a1b1b1c1d1e1f2021222324241718191a1b1c1d1d1e1f2021222324251818191a1b1c1d1e1f202021222324251d1c1a1a1b1c1d1e1f2021222223242522201f1d1c1c1d1e1f2021222324252525252422201f1e1e1f20212223242526",
	"0a0b0c0d0e0f0f101112131415161a1d0a0b0c0d0e0f101111121314151617190b0b0c0d0e0f101112131414151617180b0c0d0d0e0f101112131415161617180b0c0d0e0f10101112131415161718190b0c0d0e0f10111212131415161718190c0c0d0e0f10111213141515161718190c0d0e0e0f10111213141516171718190c0d0e0f10111112131415161718191a0c0d0e0f10111213131415161718191a0d0d0e0f10111213141516161718191a0d0e0f0f10111213141516171818191a0d0e0f10111212131415161718191a1b0d0e0f10111213141415161718191a1b0e0e0f10111213141516171718191a1b0e0f1010111213141516171819191a1b",
	"212428272524222020212122232425261c2023272a2927252422222323242526191b1f2226292c2a2827252324252626191a1b1e212326292b2a2a2827252627191a1b1b1a1c1e21252a2c2c2c2a28271a1b1b191817171a1e23282e302f2d2c1a1b1a1816151414171c22282e3332301a1b19171513121114171c22292f32351a1b19171513110f13171a1e242b2e311b1c1a181614121214171b1f23262a2d1b1c1b191716151517191d20242728291b1c1c1b191818181a1c1f22252728291b1c1d1d1c1b1b1c1d1f2124262728291c1d1d1e1f1e1e1f20222426262728291c1d1e1f2020212223242526272829291c1d1e1f20212222232425262728292a",
}

const (
	gridBytes = 1024
	tileSize  = 256
)

func main() {
	dirPtr := flag.String("dir", "", "directory to retain store output (optional)")
	flag.Parse()

	if err := execDepthGrid(*dirPtr); err != nil {
		log.Fatal(err)
	}
}

func execDepthGrid(storeDir string) error {
	if storeDir != "" {
		if err := os.MkdirAll(storeDir, 0o755); err != nil {
			return err
		}
	} else {
		tmp, err := os.MkdirTemp("", "lode-depth-grid-*")
		if err != nil {
			return err
		}
		defer removeTempDir(tmp)
		storeDir = tmp
	}

	return runDepthGrid(context.Background(), lode.FS(storeDir))
}

func stageTiles(ctx context.Context, vol volume.Volume, tiles [4][]byte, indices ...int) ([]volume.BlockRef, error) {
	refs := make([]volume.BlockRef, 0, len(indices))
	for _, idx := range indices {
		blk, err := vol.StageWriteAt(ctx, int64(idx*tileSize), bytes.NewReader(tiles[idx]))
		if err != nil {
			return nil, fmt.Errorf("stage tile %d: %w", idx, err)
		}
		refs = append(refs, blk)
	}
	return refs, nil
}

func commitTiles(ctx context.Context, vol volume.Volume, tiles [4][]byte, label string, indices ...int) (*volume.Snapshot, error) {
	refs, err := stageTiles(ctx, vol, tiles, indices...)
	if err != nil {
		return nil, err
	}
	snap, err := vol.Commit(ctx, refs, lode.D{"tiles": label})
	if err != nil {
		return nil, fmt.Errorf("commit tiles %s: %w", label, err)
	}
	return snap, nil
}

func runDepthGrid(ctx context.Context, factory lode.Factory[lode.RangeStore]) error {
	var tiles [4][]byte
	for i, h := range tileHex {
		b, err := hex.DecodeString(h)
		if err != nil {
			return fmt.Errorf("decode tile %d: %w", i, err)
		}
		tiles[i] = b
	}

	vol, err := volume.New("depth-grid", factory, gridBytes)
	if err != nil {
		return fmt.Errorf("open depth-grid volume: %w", err)
	}

	snap1, err := commitTiles(ctx, vol, tiles, "nw,sw", 0, 2)
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot 1: %s (%d blocks)\n", snap1.ID, len(snap1.Manifest.Blocks))

	if _, err := vol.ReadAt(ctx, snap1.ID, tileSize, tileSize); !errors.Is(err, volume.ErrRangeMissing) {
		return fmt.Errorf("expected ErrRangeMissing on Northeast gap, got: %w", err)
	}
	fmt.Println("  ReadAt [256, 512) returned ErrRangeMissing as expected")

	snap2, err := commitTiles(ctx, vol, tiles, "ne", 1)
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot 2: %s (%d blocks)\n", snap2.ID, len(snap2.Manifest.Blocks))

	reopened, err := volume.New("depth-grid", factory, gridBytes)
	if err != nil {
		return fmt.Errorf("reopen depth-grid volume: %w", err)
	}
	snap3, err := commitTiles(ctx, reopened, tiles, "se", 3)
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot 3: %s (%d blocks)\n", snap3.ID, len(snap3.Manifest.Blocks))

	all, err := reopened.ReadAt(ctx, snap3.ID, 0, gridBytes)
	if err != nil {
		return fmt.Errorf("read full depth grid: %w", err)
	}
	fmt.Printf("Read full depth grid: %d bytes\n", len(all))
	return nil
}

// removeTempDir deletes the example's scratch directory. A failed removal
// does not change the example's outcome, so it is logged rather than returned.
func removeTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("remove temp dir %s: %v", dir, err)
	}
}
