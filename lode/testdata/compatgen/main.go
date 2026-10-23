// Command compatgen writes datasets and a volume with Lode v0.11.0, so that a
// later release can be checked against data it did not write.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: compatgen <empty output directory>")
	}
	if err := run(context.Background(), os.Args[1]); err != nil {
		log.Fatal(err)
	}
}

// write commits each batch of records as one snapshot.
func write(ctx context.Context, ds lode.Dataset, batches ...[]any) error {
	for i, records := range batches {
		if _, err := ds.Write(ctx, records, lode.Metadata{"batch": i}); err != nil {
			return err
		}
	}
	return nil
}

func run(ctx context.Context, root string) error {
	factory := lode.NewFSFactory(root)
	jsonl := lode.WithCodec(lode.NewJSONLCodec())

	events, err := lode.NewDataset("events", factory, lode.WithHiveLayout("day"), jsonl)
	if err != nil {
		return err
	}
	if err := write(ctx, events,
		lode.R(
			lode.D{"id": 1, "day": "2024-01-15", "event": "login"},
			lode.D{"id": 2, "day": "2024-01-15", "event": "click"},
			lode.D{"id": 3, "day": "2024-01-16", "event": "logout"},
		),
		lode.R(
			lode.D{"id": 4, "day": "2024-01-16", "event": "login"},
			lode.D{"id": 5, "day": "2024-01-17", "event": "click"},
		),
	); err != nil {
		return fmt.Errorf("events: %w", err)
	}

	logs, err := lode.NewDataset("logs", factory, jsonl, lode.WithCompressor(lode.NewGzipCompressor()))
	if err != nil {
		return err
	}
	if err := write(ctx, logs,
		lode.R(lode.D{"line": "started"}, lode.D{"line": "ready"}),
		lode.R(lode.D{"line": "stopped"}),
	); err != nil {
		return fmt.Errorf("logs: %w", err)
	}

	artifacts, err := lode.NewDataset("artifacts", factory)
	if err != nil {
		return err
	}
	if err := write(ctx, artifacts, []any{[]byte("model weights v1")}); err != nil {
		return fmt.Errorf("artifacts: %w", err)
	}

	vol, err := volume.New("disk", factory, 1024)
	if err != nil {
		return err
	}
	a, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(bytes.Repeat([]byte{0xAA}, 256)))
	if err != nil {
		return err
	}
	if _, err := vol.Commit(ctx, []volume.BlockRef{a}, lode.Metadata{"step": 1}); err != nil {
		return err
	}
	b, err := vol.StageWriteAt(ctx, 512, bytes.NewReader(bytes.Repeat([]byte{0xBB}, 256)))
	if err != nil {
		return err
	}
	if _, err := vol.Commit(ctx, []volume.BlockRef{b}, lode.Metadata{"step": 2}); err != nil {
		return err
	}
	return nil
}
