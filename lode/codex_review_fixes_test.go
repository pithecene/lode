package lode_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"
)

func TestParquet_DefinedKeyMapRejected(t *testing.T) {
	t.Parallel()
	type customKey string
	type customMap map[customKey]any

	schema := lode.ParquetSchema{
		Fields: []lode.ParquetField{{Name: "id", Type: lode.ParquetString}},
	}
	root := lode.Open(lode.Memory())
	_, err := root.Records(t.Context(), "events", lode.Parquet[customMap](schema))
	if !errors.Is(err, lode.ErrSchemaViolation) {
		t.Fatalf("Records with map[customKey]any = %v, want ErrSchemaViolation", err)
	}
}

func TestRoot_RemoveDataset_LegacyPartitionOnlyHiveHead(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	root := lode.Open(lode.Use(store))

	ds, err := root.Records(ctx, "legacy_hive", lode.JSONL[lode.D](), lode.WithLayout(lode.Hive("day")))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := ds.Write(ctx, lode.R(lode.D{"day": "2026-01-01", "v": 1}), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a pre-v0.7.3 Hive snapshot that persisted only its partition
	// manifest copy and has no canonical segments/<id>/manifest.json.
	canonicalKey := "datasets/legacy_hive/segments/" + string(snap.ID) + "/manifest.json"
	if err := store.Delete(ctx, canonicalKey); err != nil {
		t.Fatal(err)
	}

	if err := root.RemoveDataset(ctx, "legacy_hive"); err != nil {
		t.Fatalf("RemoveDataset on legacy partition-only Hive head = %v, want nil", err)
	}

	keys, err := store.List(ctx, "datasets/legacy_hive/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "datasets/legacy_hive/latest" {
		t.Fatalf("remaining keys = %v, want only [datasets/legacy_hive/latest]", keys)
	}
	rc, err := store.Get(ctx, "datasets/legacy_hive/latest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(rc)
	if !strings.Contains(buf.String(), "removed") {
		t.Fatalf("latest fence = %q, want removed marker", buf.String())
	}
}
