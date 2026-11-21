// Example: Parquet Codec Round-Trip (Typed Structs and Map Records)
//
// This example demonstrates columnar storage with the generic Parquet[T] codec:
//   - Schema-explicit encoding with Go structs (zero schema inference)
//   - Nullable pointer field support (*float64)
//   - Reading the same dataset back as typed structs or map records (lode.D)
//
// Run with: go run ./examples/parquet
package main

import (
	"context"
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

// User is a typed record mapped explicitly against ParquetSchema.
type User struct {
	ID        int64     `parquet:"id"`
	Name      string    `parquet:"name"`
	Score     *float64  `parquet:"score"`
	Active    bool      `parquet:"active"`
	CreatedAt time.Time `parquet:"created_at"`
}

func ptr[T any](v T) *T { return &v }

func run() error {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "lode-parquet-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer removeTempDir(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// SCHEMA: Define explicit Parquet schema
	// -------------------------------------------------------------------------
	fmt.Println("=== SCHEMA ===")

	schema := lode.ParquetSchema{
		Fields: []lode.ParquetField{
			{Name: "id", Type: lode.ParquetInt64},
			{Name: "name", Type: lode.ParquetString},
			{Name: "score", Type: lode.ParquetFloat64, Nullable: true},
			{Name: "active", Type: lode.ParquetBool},
			{Name: "created_at", Type: lode.ParquetTimestamp},
		},
	}

	for _, f := range schema.Fields {
		nullable := ""
		if f.Nullable {
			nullable = " (nullable)"
		}
		fmt.Printf("  - %s: %s%s\n", f.Name, typeName(f.Type), nullable)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// WRITE: Write typed Go struct records
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE (Typed Structs) ===")

	ds, err := root.Records(ctx, "users", lode.Parquet[User](schema))
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	now := time.Now().UTC().Truncate(time.Nanosecond)
	users := []User{
		{ID: 1, Name: "alice", Score: ptr(95.5), Active: true, CreatedAt: now},
		{ID: 2, Name: "bob", Score: nil, Active: true, CreatedAt: now},
		{ID: 3, Name: "charlie", Score: ptr(88.0), Active: false, CreatedAt: now},
	}

	snapshot, err := ds.Write(ctx, users, lode.D{"source": "parquet-example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Row count: %d\n", snapshot.RowCount)
	fmt.Printf("Codec: %s, Compressor: %s\n", snapshot.Codec, snapshot.Compressor)
	for _, obj := range snapshot.Objects {
		fmt.Printf("  - %s (%d bytes, stats rows=%d)\n", obj.Path, obj.SizeBytes, obj.Stats.RowCount)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Read back as typed structs AND as ad-hoc lode.D maps
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	readUsers, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("failed to read typed structs: %w", err)
	}
	fmt.Printf("Typed User structs (%d):\n", len(readUsers))
	for _, u := range readUsers {
		score := "<nil>"
		if u.Score != nil {
			score = fmt.Sprintf("%.1f", *u.Score)
		}
		fmt.Printf("  id=%d name=%s score=%s active=%v created_at=%s\n",
			u.ID, u.Name, score, u.Active, u.CreatedAt.Format(time.RFC3339))
	}

	// An ad-hoc reader without the User struct can open the same dataset with
	// lode.Parquet[lode.D](schema) because Codec.Describe depends only on schema.
	mapView, err := lode.OpenReadOnly(lode.FS(tmpDir)).Records(ctx, "users",
		lode.Parquet[lode.D](schema), lode.WithAnchor(snapshot.ID))
	if err != nil {
		return fmt.Errorf("failed to open map view: %w", err)
	}
	mapRecords, err := mapView.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("failed to read map records: %w", err)
	}
	fmt.Printf("\nAd-hoc lode.D map records (%d), bob score nil=%v\n",
		len(mapRecords), mapRecords[1]["score"] == nil)

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Parquet codec round-trip complete!")

	return nil
}

func typeName(t lode.ParquetType) string {
	names := map[lode.ParquetType]string{
		lode.ParquetInt32:     "int32",
		lode.ParquetInt64:     "int64",
		lode.ParquetFloat32:   "float32",
		lode.ParquetFloat64:   "float64",
		lode.ParquetString:    "string",
		lode.ParquetBool:      "bool",
		lode.ParquetBytes:     "bytes",
		lode.ParquetTimestamp: "timestamp",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", t)
}

// removeTempDir deletes the example's scratch directory. A failed removal
// does not change the example's outcome, so it is logged rather than returned.
func removeTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("remove temp dir %s: %v", dir, err)
	}
}
