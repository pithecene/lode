// Example: Default Layout Round-Trip
//
// This example demonstrates the write → list → read flow using the default layout:
//
//	datasets/<dataset>/snapshots/<snapshot>/
//	  manifest.json
//	  data/data
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

	tmpDir, err := os.MkdirTemp("", "lode-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer removeTempDir(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// WRITE: Create dataset and write data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Open the dataset for map records encoded as JSON Lines. Defaults:
	//   - Layout: lode.Unpartitioned()
	//   - Compressor: lode.Uncompressed()
	ds, err := root.Records(ctx, "events", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

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
	fmt.Printf("Objects in manifest:\n")
	for _, obj := range snapshot.Objects {
		fmt.Printf("  - %s (%d bytes)\n", obj.Path, obj.SizeBytes)
	}
	fmt.Println()

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

	readRoot := lode.OpenReadOnly(lode.FS(tmpDir))

	datasets, err := readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	view, err := readRoot.Records(ctx, "events", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to open view: %w", err)
	}

	var manifests []lode.LoadedManifest
	for m, err := range view.Manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{}) {
		if err != nil {
			return fmt.Errorf("failed to iterate manifests: %w", err)
		}
		manifests = append(manifests, m)
	}
	fmt.Printf("Manifests in 'events': %d manifest(s)\n", len(manifests))
	for _, m := range manifests {
		fmt.Printf("  - %s\n", m.Ref.ID)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Inspect manifest and read data back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	manifest := manifests[0].Manifest
	fmt.Printf("Manifest schema: %s v%s\n", manifest.SchemaName, manifest.FormatVersion)
	fmt.Printf("Row count: %d\n", manifest.RowCount)
	fmt.Printf("Codec: %s, Compressor: %s\n", manifest.Codec, manifest.Compressor)

	readRecords, err := view.Read(ctx, manifests[0].Ref.ID)
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

// removeTempDir deletes the example's scratch directory. A failed removal
// does not change the example's outcome, so it is logged rather than returned.
func removeTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("remove temp dir %s: %v", dir, err)
	}
}
