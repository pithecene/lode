// Example: Default Layout Round-Trip
//
// This example demonstrates the write → list → read flow using the default layout:
//
//	datasets/<dataset>/snapshots/<segment>/
//	  manifest.json
//	  data/filename
//
// Run with: go run ./examples/default_layout
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

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
	tmpDir, err := os.MkdirTemp("", "lode-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem store factory
	storeFactory := lode.NewFSFactory(tmpDir)

	// -------------------------------------------------------------------------
	// WRITE: Create dataset and write data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Create dataset with JSONL codec.
	// Default bundle:
	//   - Layout: NewDefaultLayout() (flat, no partitions)
	//   - Compressor: NewNoOpCompressor()
	//   - Codec: none (we override with JSONL for structured records)
	ds, err := lode.NewDataset(
		"events",
		storeFactory,
		lode.WithCodec(lode.NewJSONLCodec()),
	)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Write some records
	records := lode.R(
		lode.D{"id": 1, "event": "login", "user": "alice"},
		lode.D{"id": 2, "event": "click", "user": "bob"},
		lode.D{"id": 3, "event": "logout", "user": "alice"},
	)

	snapshot, err := ds.Write(ctx, records, lode.Metadata{"source": "example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Files in manifest:\n")
	for _, f := range snapshot.Manifest.Files {
		fmt.Printf("  - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// Show the actual file structure
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
	// LIST: Discover datasets and manifests using DatasetReader
	// -------------------------------------------------------------------------
	fmt.Println("=== LIST ===")

	reader, err := lode.NewDatasetReader(storeFactory)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	// List all datasets
	datasets, err := reader.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	// List manifests in the dataset
	manifests, err := reader.ListManifests(ctx, "events", "", lode.ManifestListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list manifests: %w", err)
	}
	fmt.Printf("Manifests in 'events': %d manifest(s)\n", len(manifests))
	for _, ref := range manifests {
		fmt.Printf("  - %s\n", ref.ID)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Load manifest and read data back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	// Get manifest for the first snapshot
	manifest, err := reader.GetManifest(ctx, "events", manifests[0])
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}
	fmt.Printf("Manifest schema: %s v%s\n", manifest.SchemaName, manifest.FormatVersion)
	fmt.Printf("Row count: %d\n", manifest.RowCount)
	fmt.Printf("Codec: %s, Compressor: %s\n", manifest.Codec, manifest.Compressor)

	// Read data through the dataset
	readRecords, err := ds.Read(ctx, manifests[0].ID)
	if err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	fmt.Printf("\nRecords read back:\n")
	for _, r := range readRecords {
		fmt.Printf("  %v\n", r)
	}

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Default layout round-trip complete!")

	return nil
}
