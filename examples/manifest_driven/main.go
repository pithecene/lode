// Example: Manifest-Driven Discovery
//
// This example demonstrates that discovery is driven entirely by manifests:
//   - Manifests are the commit signal (presence = visibility)
//   - Data objects without manifests are invisible to discovery
//   - All metadata comes from manifests, not file inspection
//
// Run with: go run ./examples/manifest_driven
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pithecene/lode/lode"
)

func main() {
	if err := execExample(); err != nil {
		log.Fatal(err)
	}
}

func execExample() error {
	tmpDir, err := os.MkdirTemp("", "lode-manifest-example-*")
	if err != nil {
		return err
	}
	defer removeTempDir(tmpDir)
	return run(context.Background(), tmpDir)
}

func run(ctx context.Context, tmpDir string) error {
	fmt.Printf("Storage root: %s\n\n", tmpDir)

	store, err := lode.NewFS(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	readRoot := lode.OpenReadOnly(lode.Use(store))

	// -------------------------------------------------------------------------
	// Step 1: Write data objects WITHOUT manifests (uncommitted)
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 1: Write uncommitted data ===")

	dataPath := "datasets/uncommitted-ds/snapshots/snap-1/data/data.jsonl"
	dataContent := []byte(`{"id":1,"status":"orphan"}`)
	if err := store.Put(ctx, dataPath, bytes.NewReader(dataContent)); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	fmt.Printf("Wrote data object: %s\n", dataPath)

	// ListDatasets returns an empty slice when no committed manifests exist.
	datasets, err := readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Committed datasets discovered: %v (expected empty: uncommitted data is invisible)\n\n", datasets)

	// -------------------------------------------------------------------------
	// Step 2: Write a manifest (commit signal)
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 2: Write manifest (commit) ===")

	metadata, err := lode.NewMetadata(lode.D{"source": "manifest-driven-example"})
	if err != nil {
		return fmt.Errorf("failed to build metadata: %w", err)
	}

	manifest := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.2.0",
		DatasetID:     "committed-ds",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      metadata,
		Objects: []lode.ObjectRef{
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

	datasets, err = readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets discovered: %v (uncommitted-ds still invisible!)\n\n", datasets)

	// -------------------------------------------------------------------------
	// Step 3: Demonstrate manifest-driven metadata
	// -------------------------------------------------------------------------
	fmt.Println("=== STEP 3: Manifest-driven metadata ===")

	view, err := readRoot.Records(ctx, "committed-ds", lode.JSONL[lode.D]())
	if err != nil {
		return fmt.Errorf("failed to open view: %w", err)
	}

	var loadedManifest *lode.Manifest
	count := 0
	for m, err := range view.Manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{}) {
		if err != nil {
			return fmt.Errorf("iterate manifests: %w", err)
		}
		if loadedManifest == nil {
			loadedManifest = m.Manifest
		}
		count++
		fmt.Printf("  discovered manifest: %s\n", m.Ref.ID)
	}
	fmt.Printf("Total manifests in 'committed-ds': %d\n", count)

	// On an object store, where each manifest is one request, Prefetch lets
	// Manifests load several ahead of the loop, concurrently, in the same order.
	prefetched := 0
	for _, err := range view.Manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{Prefetch: 8}) {
		if err != nil {
			return fmt.Errorf("iterate manifests with prefetch: %w", err)
		}
		prefetched++
	}
	if prefetched != count {
		return fmt.Errorf("prefetched listing yielded %d manifests, want %d", prefetched, count)
	}
	fmt.Printf("Prefetched listing: %d manifests\n", prefetched)

	fmt.Println("\nManifest provides all metadata:")
	fmt.Printf("  Schema: %s v%s\n", loadedManifest.SchemaName, loadedManifest.FormatVersion)
	fmt.Printf("  Dataset: %s\n", loadedManifest.DatasetID)
	fmt.Printf("  Snapshot: %s\n", loadedManifest.SnapshotID)
	fmt.Printf("  Row count: %d\n", loadedManifest.RowCount)
	fmt.Printf("  Codec: %s\n", loadedManifest.Codec)
	fmt.Printf("  Compressor: %s\n", loadedManifest.Compressor)
	fmt.Printf("  Partitioner: %s\n", loadedManifest.Partitioner)
	fmt.Printf("  Objects: %d\n", len(loadedManifest.Objects))
	for _, obj := range loadedManifest.Objects {
		fmt.Printf("    - %s (%d bytes)\n", obj.Path, obj.SizeBytes)
	}
	fmt.Printf("  Metadata: %s\n", loadedManifest.Metadata.JSON())

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Manifest-driven discovery demonstration complete!")

	return nil
}

// removeTempDir deletes the example's scratch directory. A failed removal
// does not change the example's outcome, so it is logged rather than returned.
func removeTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("remove temp dir %s: %v", dir, err)
	}
}
