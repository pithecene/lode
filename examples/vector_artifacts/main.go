// Example: Vector Artifact Pipeline
//
// This example demonstrates using Lode for the full vector artifact lifecycle:
//   - Store embedding batches as raw blob snapshots
//   - Store serialized FAISS indices via StreamWrite
//   - Use Latest() as an atomic active-index pointer
//   - Version progression when indices are rebuilt
//   - Rollback to a previous index version by snapshot ID
//   - Rebuild contract: metadata alone enables deterministic reconstruction
//
// Lode is durability infrastructure, not a vector database. It stores embedding
// batches and serialized indices as opaque blobs. Similarity search, index
// construction, and query execution remain the caller's responsibility.
//
// Run with: go run ./examples/vector_artifacts
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"

	"github.com/pithecene/lode/lode"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// embeddingBatch is the metadata of one batch of embeddings.
type embeddingBatch struct {
	Dimension   int    `json:"dimension"`
	VectorCount int    `json:"vector_count"`
	DType       string `json:"dtype"`
	Source      string `json:"source"`
}

// indexBuild is the metadata of one serialized index: what it was built
// from, so that it can be rebuilt.
type indexBuild struct {
	IndexType        string                   `json:"index_type"`
	Dimension        int                      `json:"dimension"`
	VectorCount      int                      `json:"vector_count"`
	SourceEmbeddings []lode.DatasetSnapshotID `json:"source_embeddings"`
	FaissVersion     string                   `json:"faiss_version"`
}

// decodeMetadata decodes a snapshot's metadata into a value of type T.
func decodeMetadata[T any](snap *lode.DatasetSnapshot) (T, error) {
	var v T
	if err := snap.Metadata.Decode(&v); err != nil {
		return v, fmt.Errorf("decode metadata of %s: %w", snap.ID, err)
	}
	return v, nil
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-vector-artifacts-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Open a root over the filesystem store.
	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// STORE EMBEDDING BATCHES
	// -------------------------------------------------------------------------
	fmt.Println("=== STORE EMBEDDING BATCHES ===")

	// Embeddings dataset: a blob dataset. Each snapshot holds a serialized
	// batch of float32 vectors.
	embDS, err := root.Blobs(ctx, "embeddings")
	if err != nil {
		return fmt.Errorf("create embeddings dataset: %w", err)
	}

	// Batch 1: 100 vectors, 128 dimensions
	batch1 := simulateEmbeddings(100, 128)
	embSnap1, err := embDS.Write(ctx, batch1, embeddingBatch{
		Dimension: 128, VectorCount: 100, DType: "float32", Source: "training-set-part-1",
	})
	if err != nil {
		return fmt.Errorf("write embedding batch 1: %w", err)
	}
	fmt.Printf("Batch 1: %s (%d bytes, 100 vectors x 128 dims)\n", embSnap1.ID, embSnap1.Manifest.Files[0].SizeBytes)

	// Batch 2: 50 vectors, 128 dimensions
	batch2 := simulateEmbeddings(50, 128)
	embSnap2, err := embDS.Write(ctx, batch2, embeddingBatch{
		Dimension: 128, VectorCount: 50, DType: "float32", Source: "training-set-part-2",
	})
	if err != nil {
		return fmt.Errorf("write embedding batch 2: %w", err)
	}
	fmt.Printf("Batch 2: %s (%d bytes, 50 vectors x 128 dims)\n\n", embSnap2.ID, embSnap2.Manifest.Files[0].SizeBytes)

	// -------------------------------------------------------------------------
	// STORE SERIALIZED INDEX
	// -------------------------------------------------------------------------
	fmt.Println("=== STORE SERIALIZED INDEX ===")

	// Index dataset: a blob dataset. StreamWrite handles large binary payloads
	// that should be written in a single pass (e.g., faiss.write_index output).
	indexDS, err := root.Blobs(ctx, "index")
	if err != nil {
		return fmt.Errorf("create index dataset: %w", err)
	}

	// Write first index via StreamWrite (built from batch 1 only).
	// StreamWrite is preferred for large blobs: data flows directly to the
	// final object path with no intermediate buffering.
	sw, err := indexDS.StreamWrite(ctx, indexBuild{
		IndexType: "IVF1024,Flat", Dimension: 128, VectorCount: 100,
		SourceEmbeddings: []lode.DatasetSnapshotID{embSnap1.ID}, FaissVersion: "1.7.4",
	})
	if err != nil {
		return fmt.Errorf("start stream write for index v1: %w", err)
	}

	indexBlob1 := simulateIndex(8192)
	if _, err := sw.Write(indexBlob1); err != nil {
		_ = sw.Abort(ctx)
		return fmt.Errorf("write index v1 data: %w", err)
	}

	idxSnap1, err := sw.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit index v1: %w", err)
	}
	fmt.Printf("Index v1: %s (%d bytes)\n", idxSnap1.ID, idxSnap1.Manifest.Files[0].SizeBytes)
	build1, err := decodeMetadata[indexBuild](idxSnap1)
	if err != nil {
		return err
	}
	fmt.Printf("  Built from: %v\n\n", build1.SourceEmbeddings)

	// -------------------------------------------------------------------------
	// LATEST AS ACTIVE POINTER
	// -------------------------------------------------------------------------
	fmt.Println("=== LATEST AS ACTIVE POINTER ===")

	// Latest() returns the most recently committed snapshot — the "active" index.
	// No separate flag or pointer file needed; snapshot ordering is the pointer.
	active, err := indexDS.Latest(ctx)
	if err != nil {
		return fmt.Errorf("get active index: %w", err)
	}
	activeBuild, err := decodeMetadata[indexBuild](active)
	if err != nil {
		return err
	}
	fmt.Printf("Active index: %s\n", active.ID)
	fmt.Printf("  Type: %s\n", activeBuild.IndexType)
	fmt.Printf("  Vectors: %d\n", activeBuild.VectorCount)
	fmt.Printf("  FAISS version: %s\n\n", activeBuild.FaissVersion)

	// -------------------------------------------------------------------------
	// VERSION PROGRESSION
	// -------------------------------------------------------------------------
	fmt.Println("=== VERSION PROGRESSION ===")

	// Build a second index from both embedding batches.
	sw2, err := indexDS.StreamWrite(ctx, indexBuild{
		IndexType: "IVF1024,Flat", Dimension: 128, VectorCount: 150,
		SourceEmbeddings: []lode.DatasetSnapshotID{embSnap1.ID, embSnap2.ID}, FaissVersion: "1.7.4",
	})
	if err != nil {
		return fmt.Errorf("start stream write for index v2: %w", err)
	}

	indexBlob2 := simulateIndex(12288)
	if _, err := sw2.Write(indexBlob2); err != nil {
		_ = sw2.Abort(ctx)
		return fmt.Errorf("write index v2 data: %w", err)
	}

	idxSnap2, err := sw2.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit index v2: %w", err)
	}
	fmt.Printf("Index v2: %s (%d bytes)\n", idxSnap2.ID, idxSnap2.Manifest.Files[0].SizeBytes)

	// Latest() now returns the new version.
	active2, err := indexDS.Latest(ctx)
	if err != nil {
		return fmt.Errorf("get active index after v2: %w", err)
	}
	active2Build, err := decodeMetadata[indexBuild](active2)
	if err != nil {
		return err
	}
	fmt.Printf("Active index updated: %s\n", active2.ID)
	fmt.Printf("  Vectors: %d (was %d)\n\n", active2Build.VectorCount, activeBuild.VectorCount)

	// -------------------------------------------------------------------------
	// ROLLBACK
	// -------------------------------------------------------------------------
	fmt.Println("=== ROLLBACK ===")

	// Access the old index by snapshot ID — no deletion or mutation needed.
	// Both versions remain immutable in storage.
	oldSnap, err := indexDS.Snapshot(ctx, idxSnap1.ID)
	if err != nil {
		return fmt.Errorf("get old index snapshot: %w", err)
	}
	oldBuild, err := decodeMetadata[indexBuild](oldSnap)
	if err != nil {
		return err
	}
	fmt.Printf("Previous index: %s\n", oldSnap.ID)
	fmt.Printf("  Vectors: %d\n", oldBuild.VectorCount)

	// Read the old index data back and verify size.
	oldBlob, err := indexDS.Read(ctx, idxSnap1.ID)
	if err != nil {
		return fmt.Errorf("read old index data: %w", err)
	}
	fmt.Printf("  Read back: %d bytes (matches original: %v)\n", len(oldBlob), bytes.Equal(oldBlob, indexBlob1))

	// List all index snapshots to show version history.
	allSnaps, err := indexDS.Snapshots(ctx)
	if err != nil {
		return fmt.Errorf("list index snapshots: %w", err)
	}
	fmt.Printf("  Total index versions: %d\n\n", len(allSnaps))

	// -------------------------------------------------------------------------
	// REBUILD CONTRACT
	// -------------------------------------------------------------------------
	fmt.Println("=== REBUILD CONTRACT ===")

	// Metadata alone enables deterministic reconstruction:
	// read active index metadata → extract source_embeddings → look up each
	// embedding snapshot → recover the full provenance chain.
	fmt.Println("Provenance chain for active index:")
	fmt.Printf("  Index snapshot: %s\n", active2.ID)
	fmt.Printf("  Index type: %s\n", active2Build.IndexType)

	// The metadata decodes into its struct, so the source IDs keep their type.
	for i, snapID := range active2Build.SourceEmbeddings {
		embSnap, err := embDS.Snapshot(ctx, snapID)
		if err != nil {
			return fmt.Errorf("look up embedding snapshot %s: %w", snapID, err)
		}
		batch, err := decodeMetadata[embeddingBatch](embSnap)
		if err != nil {
			return err
		}
		fmt.Printf("  Source embedding %d: %s\n", i+1, embSnap.ID)
		fmt.Printf("    Vectors: %d, Source: %s\n", batch.VectorCount, batch.Source)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// SUCCESS
	// -------------------------------------------------------------------------
	fmt.Println("=== SUCCESS ===")
	fmt.Println("Vector artifact pipeline complete!")
	fmt.Println("\nKey points demonstrated:")
	fmt.Println("  1. Embedding batches stored as raw blob snapshots (Write)")
	fmt.Println("  2. Serialized indices stored via StreamWrite (large binary payloads)")
	fmt.Println("  3. Latest() is the atomic active-index pointer — no separate flag needed")
	fmt.Println("  4. Version progression: new index replaces active pointer on commit")
	fmt.Println("  5. Rollback: access any prior version by snapshot ID (immutable history)")
	fmt.Println("  6. Rebuild contract: metadata tracks source_embeddings for provenance")
	fmt.Println("  7. Lode is durability — search, training, and execution are the caller's job")

	return nil
}

// simulateEmbeddings generates deterministic float32 vector bytes.
// Returns count * dimension * 4 bytes of IEEE 754 float32 data.
func simulateEmbeddings(count, dimension int) []byte {
	buf := new(bytes.Buffer)
	for i := range count {
		for d := range dimension {
			// Deterministic pseudo-embedding: sin-based pattern
			val := float32(math.Sin(float64(i*dimension+d) * 0.01))
			_ = binary.Write(buf, binary.LittleEndian, val)
		}
	}
	return buf.Bytes()
}

// simulateIndex generates a fake serialized index blob with a header.
func simulateIndex(size int) []byte {
	buf := make([]byte, size)
	// Write a recognizable header (mimics FAISS magic bytes)
	copy(buf, []byte("FAIDX001"))
	// Fill remainder with deterministic pattern
	for i := 8; i < size; i++ {
		buf[i] = byte(i % 251)
	}
	return buf
}
