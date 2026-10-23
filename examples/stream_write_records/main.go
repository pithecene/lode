// Example: Records.StreamWrite — Streaming Record Writes
//
// This example demonstrates Records.StreamWrite for streaming large record
// sets:
//
//   - A range-over-func iterator (iter.Seq2[T, error]) as the record source
//   - Single-pass streaming through codec
//   - Manifest written only on successful completion
//   - Metadata explicitness requirement
//
// Run with: go run ./examples/stream_write_records
package main

import (
	"context"
	"fmt"
	"iter"
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

	// Open a root over the filesystem store.
	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// SETUP: Create dataset with streaming-capable codec
	// -------------------------------------------------------------------------
	fmt.Println("=== SETUP ===")

	// StreamWrite requires a codec that streams (a lode.StreamingCodec).
	// The JSONL codec streams because it writes records line by line.
	ds, err := root.Records(ctx, "events", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("create dataset: %w", err)
	}
	fmt.Println("Created dataset with JSONL codec (streaming-capable)")
	fmt.Println()

	// -------------------------------------------------------------------------
	// STREAM WRITE: Write records through iterator
	// -------------------------------------------------------------------------
	fmt.Println("=== STREAM WRITE ===")

	// Create a record iterator.
	// In real usage, this could read from a file, database cursor, or channel.
	//
	// Note: []lode.D is used here (instead of lode.R) because the slice backs
	// the iterator. Per CONTRACT_EXAMPLES.md, []lode.D is appropriate when
	// records are consumed through an iterator rather than passed to Write.
	records := []lode.D{
		{"id": 1, "event": "signup", "user": "alice"},
		{"id": 2, "event": "login", "user": "alice"},
		{"id": 3, "event": "purchase", "user": "alice", "amount": 99.99},
		{"id": 4, "event": "logout", "user": "alice"},
		{"id": 5, "event": "signup", "user": "bob"},
	}
	seq := each(records)

	fmt.Printf("Streaming %d records...\n", len(records))

	// StreamWrite:
	// - Ranges over the iterator, one record at a time
	// - Encodes through codec directly to storage
	// - Writes manifest only after all records are processed
	// - Returns error if the iterator yields one
	snapshot, err := ds.StreamWrite(ctx, seq, lode.D{
		"source":      "stream_write_records_example",
		"record_type": "user_events",
	})
	if err != nil {
		return fmt.Errorf("stream write records: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Row count: %d\n", snapshot.RowCount)
	fmt.Printf("Codec: %s\n", snapshot.Codec)
	fmt.Printf("Files:\n")
	for _, f := range snapshot.Files {
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
	fmt.Println("1. StreamWrite requires a streaming-capable codec (e.g., JSONL)")
	fmt.Println("2. Records are consumed from the iterator one at a time (memory efficient)")
	fmt.Println("3. Manifest is written only after successful completion")
	fmt.Println("4. If the iterator yields an error, no manifest is written (no snapshot)")
	fmt.Println("5. nil metadata persists as {}")
	fmt.Println("6. Partitioning is NOT supported (single-pass streaming cannot partition)")
	fmt.Println()

	fmt.Println("=== SUCCESS ===")
	fmt.Println("StreamWrite demonstration complete!")

	return nil
}

// -----------------------------------------------------------------------------
// each — an iterator over a slice of records
// -----------------------------------------------------------------------------

// each yields the records of a slice, without an error. In production, an
// iterator might range over a database cursor, a file reader, or a channel,
// yielding the source's error to stop the write.
func each(records []lode.D) iter.Seq2[lode.D, error] {
	return func(yield func(lode.D, error) bool) {
		for _, r := range records {
			if !yield(r, nil) {
				return
			}
		}
	}
}
