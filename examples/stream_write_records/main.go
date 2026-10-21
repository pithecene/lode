// Example: StreamWriteRecords — Streaming Record Writes
//
// This example demonstrates StreamWriteRecords for streaming large record sets:
//
//   - Pull-based iterator pattern (RecordIterator interface)
//   - Single-pass streaming through codec
//   - Manifest written only on successful completion
//   - Metadata explicitness requirement
//
// Run with: go run ./examples/stream_write_records
package main

import (
	"context"
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
	tmpDir, err := os.MkdirTemp("", "lode-stream-records-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem store factory
	storeFactory := lode.NewFSFactory(tmpDir)

	// -------------------------------------------------------------------------
	// SETUP: Create dataset with streaming-capable codec
	// -------------------------------------------------------------------------
	fmt.Println("=== SETUP ===")

	// StreamWriteRecords requires a codec that supports streaming.
	// JSONL codec supports streaming because it writes records line-by-line.
	ds, err := lode.NewDataset(
		"events",
		storeFactory,
		lode.WithCodec(lode.NewJSONLCodec()),
	)
	if err != nil {
		return fmt.Errorf("create dataset: %w", err)
	}
	fmt.Println("Created dataset with JSONL codec (streaming-capable)")
	fmt.Println()

	// -------------------------------------------------------------------------
	// STREAM WRITE: Write records through iterator
	// -------------------------------------------------------------------------
	fmt.Println("=== STREAM WRITE ===")

	// Create a record iterator
	// In real usage, this could read from a file, database cursor, or channel
	//
	// Note: []lode.D is used here (instead of lode.R) because the slice backs
	// the iterator. Per CONTRACT_EXAMPLES.md, []lode.D is appropriate when
	// records are consumed via iterator rather than passed directly to Write.
	records := []lode.D{
		{"id": 1, "event": "signup", "user": "alice"},
		{"id": 2, "event": "login", "user": "alice"},
		{"id": 3, "event": "purchase", "user": "alice", "amount": 99.99},
		{"id": 4, "event": "logout", "user": "alice"},
		{"id": 5, "event": "signup", "user": "bob"},
	}
	iter := NewSliceIterator(records)

	fmt.Printf("Streaming %d records...\n", len(records))

	// StreamWriteRecords:
	// - Pulls records from iterator one at a time
	// - Encodes through codec directly to storage
	// - Writes manifest only after all records are processed
	// - Returns error if iterator fails
	snapshot, err := ds.StreamWriteRecords(ctx, iter, lode.Metadata{
		"source":      "stream_write_records_example",
		"record_type": "user_events",
	})
	if err != nil {
		return fmt.Errorf("stream write records: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Row count: %d\n", snapshot.Manifest.RowCount)
	fmt.Printf("Codec: %s\n", snapshot.Manifest.Codec)
	fmt.Printf("Files:\n")
	for _, f := range snapshot.Manifest.Files {
		fmt.Printf("  - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// VERIFY: Read back and confirm
	// -------------------------------------------------------------------------
	fmt.Println("=== VERIFY ===")

	readRecords, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	fmt.Printf("Read back %d records:\n", len(readRecords))
	for _, r := range readRecords {
		fmt.Printf("  %v\n", r)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// KEY POINTS
	// -------------------------------------------------------------------------
	fmt.Println("=== KEY POINTS ===")
	fmt.Println("1. StreamWriteRecords requires a streaming-capable codec (e.g., JSONL)")
	fmt.Println("2. Records are pulled from iterator one at a time (memory efficient)")
	fmt.Println("3. Manifest is written only after successful completion")
	fmt.Println("4. If iterator returns error, no manifest is written (no snapshot)")
	fmt.Println("5. nil metadata is coalesced to Metadata{} (pass nil or {} for empty)")
	fmt.Println("6. Partitioning is NOT supported (single-pass streaming cannot partition)")
	fmt.Println()

	fmt.Println("=== SUCCESS ===")
	fmt.Println("StreamWriteRecords demonstration complete!")

	return nil
}

// -----------------------------------------------------------------------------
// SliceIterator — Simple iterator over a slice of records
// -----------------------------------------------------------------------------

// SliceIterator implements lode.RecordIterator for a slice of records.
// In production, you might implement this over a database cursor, file reader,
// or channel to avoid loading all records into memory.
type SliceIterator struct {
	records []lode.D
	index   int
	err     error
}

// NewSliceIterator creates an iterator over the given records.
func NewSliceIterator(records []lode.D) *SliceIterator {
	return &SliceIterator{
		records: records,
		index:   -1,
	}
}

// Next advances to the next record. Returns false when exhausted.
func (s *SliceIterator) Next() bool {
	if s.err != nil {
		return false
	}
	s.index++
	return s.index < len(s.records)
}

// Record returns the current record. Only valid after Next returns true.
func (s *SliceIterator) Record() any {
	if s.index < 0 || s.index >= len(s.records) {
		return nil
	}
	return s.records[s.index]
}

// Err returns any error encountered during iteration.
func (s *SliceIterator) Err() error {
	return s.err
}
