// Example: Vector Artifact Pipeline
//
// This example demonstrates using Lode for the full vector artifact lifecycle:
//   - Store embedding batches as raw blob snapshots
//   - Store serialized FAISS indices via Blobs.StreamWrite
//   - Use Latest() as an atomic active-index pointer
//   - Version progression when indices are rebuilt
//   - Rollback to a previous index version by snapshot ID
//   - Rebuild contract: metadata alone enables deterministic reconstruction
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

type embeddingBatch struct {
	Dimension   int    `json:"dimension"`
	VectorCount int    `json:"vector_count"`
	DType       string `json:"dtype"`
	Source      string `json:"source"`
}

type indexBuild struct {
	IndexType        string            `json:"index_type"`
	Dimension        int               `json:"dimension"`
	VectorCount      int               `json:"vector_count"`
	SourceEmbeddings []lode.SnapshotID `json:"source_embeddings"`
	FaissVersion     string            `json:"faiss_version"`
}

func decodeMetadata[T any](snap *lode.Snapshot) (T, error) {
	var v T
	if err := snap.Metadata.Decode(&v); err != nil {
		return v, fmt.Errorf("decode metadata of %s: %w", snap.ID, err)
	}
	return v, nil
}

func run() error {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "lode-vector-artifacts-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer removeTempDir(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// STORE EMBEDDING BATCHES
	// -------------------------------------------------------------------------
	fmt.Println("=== STORE EMBEDDING BATCHES ===")

	embDS, err := root.Blobs(ctx, "embeddings")
	if err != nil {
		return fmt.Errorf("create embeddings dataset: %w", err)
	}

	batch1 := simulateEmbeddings(100, 128)
	embSnap1, err := embDS.Write(ctx, batch1, embeddingBatch{
		Dimension: 128, VectorCount: 100, DType: "float32", Source: "training-set-part-1",
	})
	if err != nil {
		return fmt.Errorf("write embedding batch 1: %w", err)
	}
	fmt.Printf("Batch 1: %s (%d bytes, 100 vectors x 128 dims)\n", embSnap1.ID, embSnap1.Objects[0].SizeBytes)

	batch2 := simulateEmbeddings(50, 128)
	embSnap2, err := embDS.Write(ctx, batch2, embeddingBatch{
		Dimension: 128, VectorCount: 50, DType: "float32", Source: "training-set-part-2",
	})
	if err != nil {
		return fmt.Errorf("write embedding batch 2: %w", err)
	}
	fmt.Printf("Batch 2: %s (%d bytes, 50 vectors x 128 dims)\n\n", embSnap2.ID, embSnap2.Objects[0].SizeBytes)

	// -------------------------------------------------------------------------
	// STORE SERIALIZED INDEX
	// -------------------------------------------------------------------------
	fmt.Println("=== STORE SERIALIZED INDEX ===")

	indexDS, err := root.Blobs(ctx, "index", lode.WithChecksum(lode.SHA256()))
	if err != nil {
		return fmt.Errorf("create index dataset: %w", err)
	}

	indexBlob1 := simulateIndex(8192)
	idxSnap1, err := indexDS.StreamWrite(ctx, bytes.NewReader(indexBlob1), indexBuild{
		IndexType: "IVF1024,Flat", Dimension: 128, VectorCount: 100,
		SourceEmbeddings: []lode.SnapshotID{embSnap1.ID}, FaissVersion: "1.7.4",
	})
	if err != nil {
		return fmt.Errorf("stream write index v1: %w", err)
	}
	fmt.Printf("Index v1: %s (%d bytes)\n", idxSnap1.ID, idxSnap1.Objects[0].SizeBytes)
	build1, err := decodeMetadata[indexBuild](idxSnap1)
	if err != nil {
		return err
	}
	fmt.Printf("  Built from: %v\n\n", build1.SourceEmbeddings)

	// -------------------------------------------------------------------------
	// LATEST AS ACTIVE POINTER
	// -------------------------------------------------------------------------
	fmt.Println("=== LATEST AS ACTIVE POINTER ===")

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

	indexBlob2 := simulateIndex(12288)
	idxSnap2, err := indexDS.StreamWrite(ctx, bytes.NewReader(indexBlob2), indexBuild{
		IndexType: "IVF1024,Flat", Dimension: 128, VectorCount: 150,
		SourceEmbeddings: []lode.SnapshotID{embSnap1.ID, embSnap2.ID}, FaissVersion: "1.7.4",
	})
	if err != nil {
		return fmt.Errorf("stream write index v2: %w", err)
	}
	fmt.Printf("Index v2: %s (%d bytes)\n", idxSnap2.ID, idxSnap2.Objects[0].SizeBytes)

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

	oldBlob, err := indexDS.Read(ctx, idxSnap1.ID)
	if err != nil {
		return fmt.Errorf("read old index data: %w", err)
	}
	fmt.Printf("  Read back: %d bytes (matches original: %v)\n", len(oldBlob), bytes.Equal(oldBlob, indexBlob1))

	allSnaps, err := indexDS.Snapshots(ctx)
	if err != nil {
		return fmt.Errorf("list index snapshots: %w", err)
	}
	fmt.Printf("  Total index versions: %d\n\n", len(allSnaps))

	// -------------------------------------------------------------------------
	// REBUILD CONTRACT
	// -------------------------------------------------------------------------
	fmt.Println("=== REBUILD CONTRACT ===")

	fmt.Println("Provenance chain for active index:")
	fmt.Printf("  Index snapshot: %s\n", active2.ID)
	fmt.Printf("  Index type: %s\n", active2Build.IndexType)

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

	fmt.Println("=== SUCCESS ===")
	fmt.Println("Vector artifact pipeline complete!")

	return nil
}

func simulateEmbeddings(count, dimension int) []byte {
	buf := make([]byte, 0, count*dimension*4)
	for i := range count {
		for d := range dimension {
			val := float32(math.Sin(float64(i*dimension+d) * 0.01))
			buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(val))
		}
	}
	return buf
}

func simulateIndex(size int) []byte {
	buf := make([]byte, size)
	copy(buf, []byte("FAIDX001"))
	for i := 8; i < size; i++ {
		buf[i] = byte(i % 251)
	}
	return buf
}

// removeTempDir deletes the example's scratch directory. A failed removal
// does not change the example's outcome, so it is logged rather than returned.
func removeTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("remove temp dir %s: %v", dir, err)
	}
}
