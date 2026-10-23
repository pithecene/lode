// Example: Default Layout Round-Trip
//
// This example demonstrates the write → list → read flow using the default layout:
//
//	datasets/<dataset>/snapshots/<snapshot>/
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

	// Open a root over the filesystem store. Opening performs no I/O until a
	// dataset is opened, and a store error is returned there.
	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// WRITE: Create dataset and write data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Open the dataset for map records encoded as JSON Lines. The record
	// type, lode.D, comes from the codec. Defaults:
	//   - Layout: lode.Unpartitioned()
	//   - Compressor: lode.NewNoOpCompressor()
	ds, err := root.Records(ctx, "events", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Write some records
	records := lode.R(
		lode.D{"id": 1, "event": "login", "user": "alice"},
		lode.D{"id": 2, "event": "click", "user": "bob"},
		lode.D{"id": 3, "event": "logout", "user": "alice"},
	)

	snapshot, err := ds.Write(ctx, records, lode.D{"source": "example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Files in manifest:\n")
	for _, f := range snapshot.Files {
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
	// LIST: Discover datasets and manifests through a read-only view
	// -------------------------------------------------------------------------
	fmt.Println("=== LIST ===")

	// A read-only root never writes, so its store needs no CompareAndSwap.
	readRoot := lode.OpenReadOnly(lode.FS(tmpDir))

	// List all datasets
	datasets, err := readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	// A view takes the dataset's layout and compressor from its manifests.
	view, err := readRoot.Records(ctx, "events", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to open view: %w", err)
	}

	// List manifests in the dataset
	manifests, err := view.ListManifests(ctx, "", lode.ManifestListOptions{})
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
	manifest, err := view.GetManifest(ctx, manifests[0])
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}
	fmt.Printf("Manifest schema: %s v%s\n", manifest.SchemaName, manifest.FormatVersion)
	fmt.Printf("Row count: %d\n", manifest.RowCount)
	fmt.Printf("Codec: %s, Compressor: %s\n", manifest.Codec, manifest.Compressor)

	// Read data through the view: the records are []lode.D
	readRecords, err := view.Read(ctx, manifests[0].ID)
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
