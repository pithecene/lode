// Example: Hive Layout Round-Trip
//
// This example demonstrates write → list → read with Hive (partition-first) layout:
//
//	datasets/<dataset>/partitions/<k=v>/segments/<segment>/
//	  manifest.json
//	  data/data
//
// Run with: go run ./examples/hive_layout
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

	tmpDir, err := os.MkdirTemp("", "lode-hive-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// WRITE: Create dataset with Hive layout and partitioned data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	ds, err := root.Records(ctx, "events", lode.JSONL[Event](),
		lode.WithLayout(lode.Hive("day").By(Event.Partition)))
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

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
	fmt.Printf("Objects in manifest:\n")
	for _, obj := range snapshot.Objects {
		fmt.Printf("  - %s (%d bytes)\n", obj.Path, obj.SizeBytes)
	}
	fmt.Println()

	fmt.Println("Stored Hive keys:")
	store, err := lode.NewFS(tmpDir)
	if err != nil {
		return err
	}
	keys, err := store.List(ctx, "")
	if err != nil {
		return err
	}
	for _, k := range keys {
		fmt.Printf("  %s\n", k)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// LIST: Discover datasets, manifests, and partitions
	// -------------------------------------------------------------------------
	fmt.Println("=== LIST ===")

	readRoot := lode.OpenReadOnly(lode.FS(tmpDir))
	view, err := readRoot.Records(ctx, "events", lode.JSONL[Event]())
	if err != nil {
		return fmt.Errorf("failed to open view: %w", err)
	}

	datasets, err := readRoot.ListDatasets(ctx, lode.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	var manifests []lode.LoadedManifest
	for m, err := range view.Manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{}) {
		if err != nil {
			return fmt.Errorf("failed to iterate manifests: %w", err)
		}
		manifests = append(manifests, m)
	}
	fmt.Printf("Manifests in 'events': %d manifest(s)\n", len(manifests))
	for _, m := range manifests {
		fmt.Printf("  - %s (partition: %s)\n", m.Ref.ID, m.Ref.Partition)
	}

	// ListPartitions extracts partition paths directly from listed manifest keys
	// without loading manifest files when the partition index is complete.
	partitions, err := view.ListPartitions(ctx, lode.PartitionListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list partitions: %w", err)
	}
	fmt.Printf("Partitions in 'events': %d partition(s)\n", len(partitions))
	for _, p := range partitions {
		fmt.Printf("  - %s\n", p)
	}

	january, err := view.ListPartitions(ctx, lode.PartitionListOptions{Prefix: "day=2024-01"})
	if err != nil {
		return fmt.Errorf("failed to list partitions by prefix: %w", err)
	}
	fmt.Printf("Partitions under 'day=2024-01': %d partition(s)\n", len(january))

	page1, err := view.ListPartitions(ctx, lode.PartitionListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("failed to list first page: %w", err)
	}
	page2, err := view.ListPartitions(ctx, lode.PartitionListOptions{Limit: 1, StartAfter: page1[0]})
	if err != nil {
		return fmt.Errorf("failed to list second page: %w", err)
	}
	fmt.Printf("Paged: %s, then %s\n\n", page1[0], page2[0])

	// -------------------------------------------------------------------------
	// READ: Inspect manifest and read data back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	manifest := manifests[0].Manifest
	fmt.Printf("Manifest schema: %s v%s\n", manifest.SchemaName, manifest.FormatVersion)
	fmt.Printf("Row count: %d\n", manifest.RowCount)
	fmt.Printf("Partitioner: %s %v\n", manifest.Partitioner, manifest.PartitionKeys)

	readRecords, err := view.Read(ctx, manifests[0].Ref.ID)
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
