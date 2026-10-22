// Example: Parquet Codec Round-Trip
//
// This example demonstrates columnar storage with Parquet codec:
//
//   - Schema-explicit encoding with typed fields
//   - Nullable field support
//   - Internal Parquet compression (Snappy by default)
//
// Run with: go run ./examples/parquet
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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
	tmpDir, err := os.MkdirTemp("", "lode-parquet-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem store factory
	storeFactory := lode.NewFSFactory(tmpDir)

	// -------------------------------------------------------------------------
	// SCHEMA: Define Parquet schema with typed fields
	// -------------------------------------------------------------------------
	fmt.Println("=== SCHEMA ===")

	schema := lode.ParquetSchema{
		Fields: []lode.ParquetField{
			{Name: "id", Type: lode.ParquetInt64},
			{Name: "name", Type: lode.ParquetString},
			{Name: "score", Type: lode.ParquetFloat64, Nullable: true},
			{Name: "active", Type: lode.ParquetBool},
			{Name: "created_at", Type: lode.ParquetTimestamp},
		},
	}

	fmt.Println("Schema fields:")
	for _, f := range schema.Fields {
		nullable := ""
		if f.Nullable {
			nullable = " (nullable)"
		}
		fmt.Printf("  - %s: %s%s\n", f.Name, typeName(f.Type), nullable)
	}
	fmt.Println()

	// Create Parquet codec - note error return for schema validation
	codec, err := lode.NewParquetCodec(schema)
	if err != nil {
		return fmt.Errorf("failed to create parquet codec: %w", err)
	}

	// -------------------------------------------------------------------------
	// WRITE: Create dataset and write data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Create dataset with Parquet codec.
	// Use NoOpCompressor - Parquet handles compression internally (Snappy default).
	ds, err := lode.NewDataset(
		"users",
		storeFactory,
		lode.WithCodec(codec),
		lode.WithCompressor(lode.NewNoOpCompressor()),
	)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Write typed records - Parquet validates against schema
	now := time.Now().UTC().Truncate(time.Nanosecond)
	records := lode.R(
		lode.D{"id": int64(1), "name": "alice", "score": 95.5, "active": true, "created_at": now},
		lode.D{"id": int64(2), "name": "bob", "score": nil, "active": true, "created_at": now},
		lode.D{"id": int64(3), "name": "charlie", "score": 88.0, "active": false, "created_at": now},
	)

	snapshot, err := ds.Write(ctx, records, lode.Metadata{"source": "parquet-example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Row count: %d\n", snapshot.Manifest.RowCount)
	fmt.Printf("Codec: %s, Compressor: %s\n", snapshot.Manifest.Codec, snapshot.Manifest.Compressor)
	fmt.Printf("Files:\n")
	for _, f := range snapshot.Manifest.Files {
		fmt.Printf("  - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// Show file structure
	fmt.Println("File structure:")
	err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tmpDir, path)
		if rel != "." {
			if info.IsDir() {
				fmt.Printf("  %s/\n", rel)
			} else {
				fmt.Printf("  %s\n", rel)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Read data back with typed values
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	readRecords, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	fmt.Printf("Records read back (%d):\n", len(readRecords))
	for _, r := range readRecords {
		rec := r.(map[string]any)
		fmt.Printf("  id=%v name=%v score=%v active=%v created_at=%v\n",
			rec["id"], rec["name"], rec["score"], rec["active"], rec["created_at"])
	}

	// Demonstrate type preservation
	fmt.Println("\nType verification:")
	first := readRecords[0].(map[string]any)
	fmt.Printf("  id type: %T (value: %v)\n", first["id"], first["id"])
	fmt.Printf("  score type: %T (value: %v)\n", first["score"], first["score"])
	fmt.Printf("  created_at type: %T\n", first["created_at"])

	// Show nullable handling
	second := readRecords[1].(map[string]any)
	fmt.Printf("  bob's score (nullable): %v (nil=%v)\n", second["score"], second["score"] == nil)

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Parquet codec round-trip complete!")

	return nil
}

// typeName returns a human-readable name for a ParquetType.
func typeName(t lode.ParquetType) string {
	names := map[lode.ParquetType]string{
		lode.ParquetInt32:     "int32",
		lode.ParquetInt64:     "int64",
		lode.ParquetFloat32:   "float32",
		lode.ParquetFloat64:   "float64",
		lode.ParquetString:    "string",
		lode.ParquetBool:      "bool",
		lode.ParquetBytes:     "bytes",
		lode.ParquetTimestamp: "timestamp",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", t)
}
