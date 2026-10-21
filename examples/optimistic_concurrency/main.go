// Example: CAS Optimistic Concurrency
//
// This example demonstrates the CAS (Compare-and-Swap) retry pattern for
// concurrent Dataset writes:
//   - Two dataset instances share the same storage root
//   - Both writers commit successfully when there is no contention
//   - A stale writer detects the conflict via ErrSnapshotConflict
//   - The retry path: call Latest() to refresh state, then re-write
//
// All built-in storage adapters (FS, Memory, S3) implement ConditionalWriter,
// which enables CAS automatically. No configuration is needed.
//
// Run with: go run ./examples/optimistic_concurrency
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/pithecene/lode/lode"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-optimistic-concurrency-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// -------------------------------------------------------------------------
	// SETUP: Two dataset instances on the same storage root
	// -------------------------------------------------------------------------
	fmt.Println("=== SETUP ===")

	// FS implements ConditionalWriter, enabling CAS automatically.
	storeFactory := lode.NewFSFactory(tmpDir)

	ds1, err := lode.NewDataset("events", storeFactory, lode.WithCodec(lode.NewJSONLCodec()))
	if err != nil {
		return fmt.Errorf("failed to create ds1: %w", err)
	}

	ds2, err := lode.NewDataset("events", storeFactory, lode.WithCodec(lode.NewJSONLCodec()))
	if err != nil {
		return fmt.Errorf("failed to create ds2: %w", err)
	}

	fmt.Printf("Created two dataset instances for %q on the same store\n\n", ds1.ID())

	// -------------------------------------------------------------------------
	// CONCURRENT WRITES: Both succeed when there is no contention
	// -------------------------------------------------------------------------
	fmt.Println("=== CONCURRENT WRITES (no contention) ===")

	snap1, err := ds1.Write(ctx, lode.R(lode.D{"writer": "ds1", "seq": 1}), lode.Metadata{"step": "first"})
	if err != nil {
		return fmt.Errorf("ds1 first write failed: %w", err)
	}
	fmt.Printf("ds1 wrote snapshot %s\n", snap1.ID)

	// ds2 reads the current pointer from storage, sees snap1, and succeeds.
	snap2, err := ds2.Write(ctx, lode.R(lode.D{"writer": "ds2", "seq": 1}), lode.Metadata{"step": "second"})
	if err != nil {
		return fmt.Errorf("ds2 first write failed: %w", err)
	}
	fmt.Printf("ds2 wrote snapshot %s (parent: %s)\n\n", snap2.ID, snap2.Manifest.ParentSnapshotID)

	// -------------------------------------------------------------------------
	// CONFLICT DETECTION: ds1's cached pointer is now stale
	// -------------------------------------------------------------------------
	fmt.Println("=== CONFLICT DETECTION ===")

	// ds1's in-memory cache still points to snap1, but ds2 updated the
	// pointer to snap2. CAS detects the mismatch.
	_, err = ds1.Write(ctx, lode.R(lode.D{"writer": "ds1", "seq": 2}), lode.Metadata{"step": "conflict"})
	if !errors.Is(err, lode.ErrSnapshotConflict) {
		return fmt.Errorf("expected ErrSnapshotConflict, got: %w", err)
	}
	fmt.Printf("ds1 detected conflict: %v\n", err)
	fmt.Printf("  (ds1 expected pointer to contain snap1, but ds2 updated it to snap2)\n\n")

	// -------------------------------------------------------------------------
	// RETRY: Refresh state via Latest(), then re-write
	// -------------------------------------------------------------------------
	fmt.Println("=== RETRY ===")

	// Latest() reads the current pointer from storage, refreshing the
	// in-memory CAS cache. This is the documented retry pattern.
	latest, err := ds1.Latest(ctx)
	if err != nil {
		return fmt.Errorf("ds1 Latest() failed: %w", err)
	}
	fmt.Printf("ds1 refreshed via Latest(): now at snapshot %s\n", latest.ID)

	snap3, err := ds1.Write(ctx, lode.R(lode.D{"writer": "ds1", "seq": 2}), lode.Metadata{"step": "retry"})
	if err != nil {
		return fmt.Errorf("ds1 retry write failed: %w", err)
	}
	fmt.Printf("ds1 retry succeeded: snapshot %s (parent: %s)\n\n", snap3.ID, snap3.Manifest.ParentSnapshotID)

	// -------------------------------------------------------------------------
	// SUCCESS
	// -------------------------------------------------------------------------
	fmt.Println("=== SUCCESS ===")
	fmt.Println("Optimistic concurrency example complete!")
	fmt.Println("\nKey points demonstrated:")
	fmt.Println("  1. CAS is automatic: FS, Memory, and S3 adapters all implement ConditionalWriter")
	fmt.Println("  2. No contention: concurrent writers succeed when they don't overlap")
	fmt.Println("  3. Conflict detection: stale writers get ErrSnapshotConflict (not corruption)")
	fmt.Println("  4. Retry pattern: call Latest() to refresh state, then re-write")
	fmt.Println("  5. No external locks needed for CAS-capable stores")

	return nil
}
