// Example: Retention — expire runs, then sweep
//
// A crawler writes each run into one Hive-partitioned dataset. Once a run is
// imported downstream its raw snapshots are only an audit copy, so the
// caller expires them by partition, previewing first. A periodic Sweep
// finishes interrupted expiries and reclaims data of writes that never
// committed once they are older than the grace period.
//
// Policy (which runs, how old, how often) belongs to the caller; Lode
// provides the safe primitive (CONTRACT_RETENTION.md).
//
// Run with: go run ./examples/retention
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pithecene-io/lode/lode"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "lode-retention-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ds, err := lode.NewDataset(
		"crawls",
		lode.NewFSFactory(tmpDir),
		lode.WithHiveLayout("run_id", "event_type"),
		lode.WithCodec(lode.NewJSONLCodec()),
	)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	fmt.Println("=== WRITE ===")
	imported, err := writeRun(ctx, ds, "run-1")
	if err != nil {
		return err
	}
	for _, run := range []string{"run-10", "run-2"} {
		if _, err := writeRun(ctx, ds, run); err != nil {
			return err
		}
	}
	fmt.Println()

	fmt.Println("=== EXPIRE run-1 ===")
	// Whole-segment match: run_id=run-1 never selects run_id=run-10.
	req := lode.ExpireRequest{Partition: "run_id=run-1", DryRun: true}
	preview, err := ds.Expire(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to preview expiry: %w", err)
	}
	printExpiry("preview", preview)

	req.DryRun = false
	report, err := ds.Expire(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to expire: %w", err)
	}
	printExpiry("expired", report)

	_, err = ds.Snapshot(ctx, imported[0].ID)
	fmt.Printf("Snapshot(%s): expired=%v, not found=%v\n\n", imported[0].ID,
		errors.Is(err, lode.ErrSnapshotExpired), errors.Is(err, lode.ErrNotFound))

	fmt.Println("=== EXPIRE EVERYTHING OLDER THAN NOW ===")
	// The head is never expired: it is kept and reported instead.
	report, err = ds.Expire(ctx, lode.ExpireRequest{Before: time.Now()})
	if err != nil {
		return fmt.Errorf("failed to expire by age: %w", err)
	}
	printExpiry("expired", report)
	fmt.Printf("retained head: %s\n\n", report.RetainedHead)

	fmt.Println("=== SWEEP ===")
	sweep, err := ds.Sweep(ctx, lode.SweepRequest{Grace: 24 * time.Hour})
	if err != nil {
		return fmt.Errorf("failed to sweep: %w", err)
	}
	fmt.Printf("settled %d, reclaimed %d objects (%d bytes recorded, %d unsized, %d orphaned), unparseable %d\n",
		len(sweep.Settled), sweep.Objects, sweep.Bytes, sweep.Unsized, sweep.Orphans, len(sweep.Unparseable))

	fmt.Println("\n=== SUCCESS ===")
	return nil
}

// writeRun commits one snapshot per event type of a run, as a crawler
// flushing its batches would.
func writeRun(ctx context.Context, ds lode.Dataset, run string) ([]*lode.DatasetSnapshot, error) {
	var snaps []*lode.DatasetSnapshot
	for _, eventType := range []string{"page", "item"} {
		snap, err := ds.Write(ctx, lode.R(
			lode.D{"run_id": run, "event_type": eventType, "url": "https://example.com/" + run},
		), lode.Metadata{"run": run})
		if err != nil {
			return nil, fmt.Errorf("failed to write %s/%s: %w", run, eventType, err)
		}
		snaps = append(snaps, snap)
	}
	fmt.Printf("wrote %s: %d snapshots\n", run, len(snaps))
	return snaps, nil
}

func printExpiry(label string, r *lode.ExpireReport) {
	fmt.Printf("%s: %d snapshots, %d objects, %d bytes (mixed %d)\n",
		label, len(r.Expired), r.Objects, r.Bytes, len(r.Mixed))
}
