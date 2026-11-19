// Example: Vessel Position Reports (Hive Layout Adaptation)
//
// This example is the exact dataset recipe behind the Lode Dataset inspector
// on pithecene.sh/foundations/lode: four vessel position reports across two
// UTC days written under a Hive layout keyed by day, then reopened and read
// back by snapshot ID through a read-only view.
//
// Pass -dir <path> to retain the written manifests and segment files on disk
// for inspection; without -dir, the example uses a temporary directory and
// removes it on exit.
//
// Run with:
//
//	go run ./examples/vessel_positions
//	go run ./examples/vessel_positions -dir ./positions-store
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/pithecene/lode/lode"
)

// PositionReport records one UTC vessel position observation.
type PositionReport struct {
	Vessel string  `json:"vessel"`
	Day    string  `json:"day"`
	Time   string  `json:"time"`
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Knots  float64 `json:"knots"`
}

// Partition returns the Hive partition key for a report.
func (r PositionReport) Partition() lode.Partition {
	return lode.Partition{"day": r.Day}
}

func main() {
	dirFlag := flag.String("dir", "", "directory to retain store output (optional)")
	flag.Parse()

	if err := run(*dirFlag); err != nil {
		log.Fatal(err)
	}
}

func prepareStoreDir(dirFlag string) (string, func(), error) {
	if dirFlag != "" {
		if err := os.MkdirAll(dirFlag, 0o755); err != nil {
			return "", nil, fmt.Errorf("create store dir: %w", err)
		}
		return dirFlag, func() {}, nil
	}
	tmpDir, err := os.MkdirTemp("", "lode-vessel-positions-*")
	if err != nil {
		return "", nil, fmt.Errorf("create temp dir: %w", err)
	}
	return tmpDir, func() { _ = os.RemoveAll(tmpDir) }, nil
}

func run(dirFlag string) error {
	ctx := context.Background()

	storeDir, cleanup, err := prepareStoreDir(dirFlag)
	if err != nil {
		return err
	}
	defer cleanup()

	root := lode.Open(lode.FS(storeDir))
	ds, err := root.Records(
		ctx,
		"positions",
		lode.JSONL[PositionReport](),
		lode.WithLayout(lode.Hive("day").By(PositionReport.Partition)),
	)
	if err != nil {
		return fmt.Errorf("open positions dataset: %w", err)
	}

	records := []PositionReport{
		{Vessel: "V-201", Day: "2026-10-14", Time: "06:12", Lat: 51.9512, Lon: 1.4108, Knots: 11.8},
		{Vessel: "V-347", Day: "2026-10-14", Time: "06:13", Lat: 51.9031, Lon: 1.6620, Knots: 9.4},
		{Vessel: "V-201", Day: "2026-10-15", Time: "06:10", Lat: 52.1124, Lon: 2.0457, Knots: 12.1},
		{Vessel: "V-512", Day: "2026-10-15", Time: "06:14", Lat: 51.8877, Lon: 1.5302, Knots: 0.2},
	}

	metadata := lode.D{"source": "ais-demo"}
	snapshot, err := ds.Write(ctx, records, metadata)
	if err != nil {
		return fmt.Errorf("write position reports: %w", err)
	}

	fmt.Printf("Snapshot ID: %s\n", snapshot.ID)
	for _, f := range snapshot.Files {
		fmt.Printf("  File: %s (%d bytes)\n", f.Path, f.SizeBytes)
	}

	readRoot := lode.OpenReadOnly(lode.FS(storeDir))
	view, err := readRoot.Records(ctx, "positions", lode.JSONL[PositionReport]())
	if err != nil {
		return fmt.Errorf("open positions view: %w", err)
	}

	got, err := view.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("read snapshot %s: %w", snapshot.ID, err)
	}
	fmt.Printf("Read back %d position reports\n", len(got))
	return nil
}
