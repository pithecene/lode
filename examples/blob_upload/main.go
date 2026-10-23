// Example: Blob Upload (Raw Bytes)
//
// This example demonstrates the default bundle configuration for raw blob storage:
//   - No codec (raw bytes)
//   - No compression (noop compressor)
//   - No partitioning (noop partitioner)
//   - Default layout
//
// When no codec is configured:
//   - Write expects a single []byte element
//   - Read returns a single []byte element
//   - RowCount in manifest is always 1
//
// Run with: go run ./examples/blob_upload
package main

import (
	"bytes"
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

// blobInfo is the metadata the example persists with each blob.
type blobInfo struct {
	ContentType string `json:"content_type"`
	Filename    string `json:"filename"`
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-blob-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Open a root over the filesystem store.
	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// WRITE: Create dataset with default bundle and write raw blob
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Open a blob dataset: each snapshot holds one blob. Defaults per
	// PUBLIC_API.md:
	//   - Layout: lode.Unpartitioned()
	//   - Compressor: lode.NewNoOpCompressor()
	ds, err := root.Blobs(ctx, "artifacts")
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Prepare raw bytes (could be an image, binary file, etc.)
	blobData := []byte("Hello, Lode! This is raw binary data. 🎉\x00\x01\x02\x03")
	fmt.Printf("Original blob size: %d bytes\n", len(blobData))
	fmt.Printf("Original blob content: %q\n", string(blobData[:min(50, len(blobData))]))

	// Write the blob with typed metadata, persisted as a JSON object
	snapshot, err := ds.Write(ctx, blobData, blobInfo{
		ContentType: "application/octet-stream",
		Filename:    "greeting.bin",
	})
	if err != nil {
		return fmt.Errorf("failed to write blob: %w", err)
	}

	fmt.Printf("\nCreated snapshot: %s\n", snapshot.ID)
	fmt.Printf("Manifest details:\n")
	fmt.Printf("  Row count: %d (always 1 for raw blobs)\n", snapshot.RowCount)
	fmt.Printf("  Codec: %q (empty = no codec)\n", snapshot.Codec)
	fmt.Printf("  Compressor: %s\n", snapshot.Compressor)
	fmt.Printf("  Partitioner: %s\n", snapshot.Partitioner)
	fmt.Printf("  Files:\n")
	for _, f := range snapshot.Files {
		fmt.Printf("    - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Read raw blob back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	// Read returns the snapshot's blob
	readBlob, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("failed to read blob: %w", err)
	}

	fmt.Printf("Read blob size: %d bytes\n", len(readBlob))
	fmt.Printf("Read blob content: %q\n", string(readBlob[:min(50, len(readBlob))]))

	// Verify data integrity
	if !bytes.Equal(blobData, readBlob) {
		return fmt.Errorf("data mismatch: blob was corrupted")
	}
	fmt.Println("\nData integrity verified: blobs match!")

	// -------------------------------------------------------------------------
	// VERIFY: Check metadata from manifest
	// -------------------------------------------------------------------------
	fmt.Println("\n=== VERIFY METADATA ===")

	latest, err := ds.Latest(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	// Decode the persisted metadata back into its struct
	var info blobInfo
	if err := latest.Metadata.Decode(&info); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}
	fmt.Printf("Metadata from manifest:\n")
	fmt.Printf("  content_type: %s\n", info.ContentType)
	fmt.Printf("  filename: %s\n", info.Filename)

	// -------------------------------------------------------------------------
	// STREAM WRITE: Demonstrate streaming write for large blobs
	// -------------------------------------------------------------------------
	fmt.Println("\n=== STREAM WRITE ===")

	// Open another blob dataset for the streaming example
	dsStream, err := root.Blobs(ctx, "artifacts-stream")
	if err != nil {
		return fmt.Errorf("failed to create streaming dataset: %w", err)
	}

	// StreamWrite is for large binary payloads that should be streamed once
	// Data flows directly to the final object path (no temp files)
	sw, err := dsStream.StreamWrite(ctx, lode.D{
		"content_type": "application/octet-stream",
		"description":  "streamed blob",
	})
	if err != nil {
		return fmt.Errorf("failed to start stream write: %w", err)
	}

	// Write data in chunks (simulating streaming from a source)
	chunks := [][]byte{
		[]byte("First chunk of data. "),
		[]byte("Second chunk of data. "),
		[]byte("Final chunk of data."),
	}
	for _, chunk := range chunks {
		if _, err := sw.Write(chunk); err != nil {
			// On error, abort cleans up partial objects (best-effort)
			_ = sw.Abort(ctx)
			return fmt.Errorf("failed to write chunk: %w", err)
		}
	}

	// Commit finalizes the stream and writes the manifest
	// The snapshot becomes visible only after this succeeds
	streamSnap, err := sw.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit stream: %w", err)
	}

	fmt.Printf("Created streaming snapshot: %s\n", streamSnap.ID)
	fmt.Printf("  Row count: %d\n", streamSnap.RowCount)
	fmt.Printf("  File size: %d bytes\n", streamSnap.Manifest.Files[0].SizeBytes)

	// Read back and verify
	streamBlob, err := dsStream.Read(ctx, streamSnap.ID)
	if err != nil {
		return fmt.Errorf("failed to read streamed blob: %w", err)
	}
	fmt.Printf("  Content: %q\n", string(streamBlob))

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Raw blob upload and read complete!")
	fmt.Println("\nKey points demonstrated:")
	fmt.Println("  1. A blob dataset stores one blob per snapshot, without a codec")
	fmt.Println("  2. Write takes the blob as []byte")
	fmt.Println("  3. Read returns the blob as []byte")
	fmt.Println("  4. Metadata is explicit, persisted, and decoded into a struct")
	fmt.Println("  5. RowCount is always 1 for raw blobs")
	fmt.Println("  6. StreamWrite for large blobs (single-pass, no temp files)")
	fmt.Println("  7. Commit makes snapshot visible; Abort leaves no snapshot")

	return nil
}
