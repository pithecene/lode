// Example: Hive Layout Round-Trip
//
// This example demonstrates write → list → read with Hive (partition-first) layout:
//
//	datasets/<dataset>/partitions/<k=v>/segments/<segment>/
//	  manifest.json
//	  data/filename
//
// Hive layout places partitions at the dataset level, enabling efficient
// partition pruning at the storage layer.
//
// Run with: go run ./examples/hive_layout
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

// Event is a typed record. Its JSON field names are those the JSONL codec
// writes.
type Event struct {
	ID    int    `json:"id"`
	Event string `json:"event"`
	Day   string `json:"day"`
	User  string `json:"user"`
}

// Partition gives an event's partition values by key name, for the Hive
// layout's By.
func (e Event) Partition() lode.Partition {
	return lode.Partition{"day": e.Day}
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-hive-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Open a root over the filesystem store.
	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// WRITE: Create dataset with Hive layout and partitioned data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Open the dataset for Event records with a Hive layout. The record type
	// comes from the codec; Hive("day").By(Event.Partition) partitions each
	// event by its Day without reflection. The first commit records the
	// layout and its keys in the manifest, so readers need not repeat them.
	ds, err := root.Records(ctx, "events", lode.JSONL[Event](),
		lode.WithLayout(lode.Hive("day").By(Event.Partition)))
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Write records with different days (will create partitions)
	records := []Event{
		{ID: 1, Event: "login", Day: "2024-01-15", User: "alice"},
		{ID: 2, Event: "click", Day: "2024-01-15", User: "bob"},
		{ID: 3, Event: "logout", Day: "2024-01-16", User: "alice"},
		{ID: 4, Event: "login", Day: "2024-01-16", User: "charlie"},
	}

	snapshot, err := ds.Write(ctx, records, lode.D{"source": "hive-example"})
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
	fmt.Println("File structure (Hive layout):")
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
	// LIST: Discover datasets, manifests, and partitions
	// -------------------------------------------------------------------------
	fmt.Println("=== LIST ===")

	// A read-only view takes the Hive layout from the dataset's manifests.
	readRoot := lode.OpenReadOnly(lode.FS(tmpDir))
	view, err := readRoot.Records(ctx, "events", lode.JSONL[Event]())
	if err != nil {
		return fmt.Errorf("failed to open view: %w", err)
	}

	// List all datasets
	datasets, err := readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	// List manifests in the dataset
	manifests, err := view.ListManifests(ctx, "", lode.ManifestListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list manifests: %w", err)
	}
	fmt.Printf("Manifests in 'events': %d manifest(s)\n", len(manifests))
	for _, ref := range manifests {
		fmt.Printf("  - %s (partition: %s)\n", ref.ID, ref.Partition)
	}

	// List partitions
	partitions, err := view.ListPartitions(ctx, lode.PartitionListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list partitions: %w", err)
	}
	fmt.Printf("Partitions in 'events': %d partition(s)\n", len(partitions))
	for _, p := range partitions {
		fmt.Printf("  - %s\n", p.Path)
	}

	// Narrow the listing to a partition-path prefix. On a Hive layout this is
	// also a storage prefix, so only the matching subtree is listed.
	january, err := view.ListPartitions(ctx, lode.PartitionListOptions{Prefix: "day=2024-01"})
	if err != nil {
		return fmt.Errorf("failed to list partitions by prefix: %w", err)
	}
	fmt.Printf("Partitions under 'day=2024-01': %d partition(s)\n", len(january))

	// Resume after the last partition seen. Results are lexicographic by Path,
	// so the previous page's last Path is the cursor for the next page.
	page1, err := view.ListPartitions(ctx, lode.PartitionListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("failed to list first page: %w", err)
	}
	page2, err := view.ListPartitions(ctx, lode.PartitionListOptions{Limit: 1, StartAfter: page1[0].Path})
	if err != nil {
		return fmt.Errorf("failed to list second page: %w", err)
	}
	fmt.Printf("Paged: %s, then %s\n", page1[0].Path, page2[0].Path)
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
	fmt.Printf("Partitioner: %s %v\n", manifest.Partitioner, manifest.PartitionKeys)

	// Read data through the view: the records are []Event
	readRecords, err := view.Read(ctx, manifests[0].ID)
	if err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	fmt.Printf("\nRecords read back (%d total):\n", len(readRecords))
	for _, r := range readRecords {
		fmt.Printf("  %+v\n", r)
	}

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Hive layout round-trip complete!")

	return nil
}
