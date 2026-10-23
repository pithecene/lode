// Example: Manifest-Driven Discovery
//
// This example demonstrates that discovery is driven entirely by manifests.
//
// Note: This uses direct Store access for demonstration (writing data without a
// manifest). It is technically valid but not typical usage; NewDataset always
// writes manifests on success.
//   - Manifests are the commit signal (presence = visibility)
//   - Data files without manifests are not discovered
//   - All metadata comes from manifests, not file inspection
//
// This aligns with CONTRACT_READ_API.md: "manifest presence = commit signal"
//
// Run with: go run ./examples/manifest_driven
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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
	tmpDir, err := os.MkdirTemp("", "lode-manifest-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// For this demonstration, we need direct store access to write
	// uncommitted data, so the example holds the store and shares it with a
	// read-only root through lode.Use.
	store, err := lode.NewFS(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	readRoot := lode.OpenReadOnly(lode.Use(store))

	// -------------------------------------------------------------------------
	// Step 1: Write data files WITHOUT manifests (uncommitted)
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 1: Write uncommitted data ===")

	// Write a data file directly (simulating incomplete write)
	dataPath := "datasets/uncommitted-ds/snapshots/snap-1/data/data.jsonl"
	dataContent := []byte(`{"id":1,"status":"orphan"}`)
	if err := store.Put(ctx, dataPath, bytes.NewReader(dataContent)); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	fmt.Printf("Wrote data file: %s\n", dataPath)

	// Try to discover datasets - should get ErrNoManifests (objects exist but no manifests)
	datasets, err := readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	switch {
	case errors.Is(err, lode.ErrNoManifests):
		fmt.Printf("ListDatasets returned ErrNoManifests (expected: objects exist but no manifests)\n\n")
	case err != nil:
		return fmt.Errorf("failed to list datasets: %w", err)
	default:
		fmt.Printf("Datasets discovered: %d (unexpected: should have returned ErrNoManifests)\n\n", len(datasets))
	}

	// -------------------------------------------------------------------------
	// Step 2: Write a manifest (commit signal)
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 2: Write manifest (commit) ===")

	// Metadata is persisted as a JSON object; decode it to read it back.
	var metadata lode.Metadata
	if err := json.Unmarshal([]byte(`{"source":"manifest-driven-example"}`), &metadata); err != nil {
		return fmt.Errorf("failed to build metadata: %w", err)
	}

	// Create a proper manifest
	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "committed-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      metadata,
		Files: []lode.FileRef{
			{
				Path:      "datasets/committed-ds/snapshots/snap-1/data/events.jsonl",
				SizeBytes: 100,
			},
		},
		RowCount:    5,
		Codec:       "jsonl",
		Compressor:  "noop",
		Partitioner: "noop",
	}

	manifestData, _ := json.MarshalIndent(manifest, "", "  ")
	manifestPath := "datasets/committed-ds/snapshots/snap-1/manifest.json"
	if err := store.Put(ctx, manifestPath, bytes.NewReader(manifestData)); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}
	fmt.Printf("Wrote manifest: %s\n", manifestPath)

	// Now discover datasets - should find the committed one
	datasets, err = readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets discovered: %v (uncommitted-ds still invisible!)\n\n", datasets)

	// -------------------------------------------------------------------------
	// Step 3: Demonstrate manifest-driven metadata
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 3: Manifest-driven metadata ===")

	// Open a view of the committed dataset. Its manifest names the JSONL
	// codec, so the view decodes map records with it.
	view, err := readRoot.Records(ctx, "committed-ds", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to open view: %w", err)
	}

	// List manifests
	manifests, err := view.ListManifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list manifests: %w", err)
	}
	fmt.Printf("Manifests in 'committed-ds': %d manifest(s)\n", len(manifests))
	for _, ref := range manifests {
		fmt.Printf("  - %s\n", ref.ID)
	}

	// Get manifest metadata
	loadedManifest, err := view.GetManifest(ctx, manifests[0])
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}

	fmt.Println("\nManifest provides all metadata:")
	fmt.Printf("  Schema: %s v%s\n", loadedManifest.SchemaName, loadedManifest.FormatVersion)
	fmt.Printf("  Dataset: %s\n", loadedManifest.DatasetID)
	fmt.Printf("  Snapshot: %s\n", loadedManifest.SnapshotID)
	fmt.Printf("  Row count: %d\n", loadedManifest.RowCount)
	fmt.Printf("  Codec: %s\n", loadedManifest.Codec)
	fmt.Printf("  Compressor: %s\n", loadedManifest.Compressor)
	fmt.Printf("  Partitioner: %s\n", loadedManifest.Partitioner)
	fmt.Printf("  Files: %d\n", len(loadedManifest.Files))
	for _, f := range loadedManifest.Files {
		fmt.Printf("    - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Printf("  Metadata: %s\n", loadedManifest.Metadata.JSON())

	// -------------------------------------------------------------------------
	// Step 4: Show that data file paths come from manifest
	// -------------------------------------------------------------------------
	fmt.Println("\n=== STEP 4: Data paths from manifest ===")

	fmt.Println("Data file paths are stored IN the manifest, not discovered from storage.")
	fmt.Println("This enables:")
	fmt.Println("  - Atomic commits (manifest = single commit point)")
	fmt.Println("  - No race conditions during writes")
	fmt.Println("  - Clean rollback (delete manifest = uncommit)")
	fmt.Println("  - Portable manifests (can be copied/moved)")

	// -------------------------------------------------------------------------
	// Cleanup summary
	// -------------------------------------------------------------------------
	fmt.Println("\n=== KEY INSIGHTS ===")
	fmt.Println("1. Manifest presence is the ONLY commit signal")
	fmt.Println("2. Data files without manifests are invisible to discovery")
	fmt.Println("3. All metadata (row count, codec, files) comes from manifest")
	fmt.Println("4. Discovery never inspects data file contents")
	fmt.Println("5. This enables safe, atomic snapshot semantics")

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Manifest-driven discovery demonstration complete!")

	return nil
}
