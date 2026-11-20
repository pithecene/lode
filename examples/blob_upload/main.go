// Example: Blob Upload (Raw Bytes)
//
// This example demonstrates the default configuration for raw blob storage:
//   - No codec (raw bytes)
//   - No compression (Uncompressed)
//   - No partitioning (Unpartitioned)
//   - Optional SHA-256 end-to-end checksum verification
//   - Single-call streaming via Blobs.StreamWrite(ctx, io.Reader, metadata)
//
// Run with: go run ./examples/blob_upload
package main

import (
	"bytes"
	"context"
	"errors"
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

	tmpDir, err := os.MkdirTemp("", "lode-blob-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	root := lode.Open(lode.FS(tmpDir))

	// -------------------------------------------------------------------------
	// WRITE: Create dataset and write raw blob
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Open a blob dataset with SHA-256 integrity verification. Defaults per
	// PUBLIC_API.md:
	//   - Layout: lode.Unpartitioned()
	//   - Compressor: lode.Uncompressed()
	ds, err := root.Blobs(ctx, "artifacts", lode.WithChecksum(lode.SHA256()))
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	blobData := []byte("Hello, Lode! This is raw binary data. 🎉\x00\x01\x02\x03")
	fmt.Printf("Original blob size: %d bytes\n", len(blobData))
	fmt.Printf("Original blob content: %q\n", string(blobData[:min(50, len(blobData))]))

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
	fmt.Printf("  Checksum: %s\n", snapshot.ChecksumAlgorithm)
	fmt.Printf("  Partitioner: %s\n", snapshot.Partitioner)
	fmt.Printf("  Objects:\n")
	for _, obj := range snapshot.Objects {
		fmt.Printf("    - %s (%d bytes, sha256=%s)\n", obj.Path, obj.SizeBytes, obj.Checksum)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Read raw blob back (verifies SHA-256 digest automatically)
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	readBlob, err := ds.Read(ctx, snapshot.ID)
	if err != nil {
		return fmt.Errorf("failed to read blob: %w", err)
	}

	fmt.Printf("Read blob size: %d bytes\n", len(readBlob))
	fmt.Printf("Read blob content: %q\n", string(readBlob[:min(50, len(readBlob))]))

	if !bytes.Equal(blobData, readBlob) {
		return errors.New("data mismatch: blob was corrupted")
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

	var info blobInfo
	if err := latest.Metadata.Decode(&info); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}
	fmt.Printf("Metadata from manifest:\n")
	fmt.Printf("  content_type: %s\n", info.ContentType)
	fmt.Printf("  filename: %s\n", info.Filename)

	// -------------------------------------------------------------------------
	// STREAM WRITE: Single-call streaming write from an io.Reader
	// -------------------------------------------------------------------------
	fmt.Println("\n=== STREAM WRITE ===")

	dsStream, err := root.Blobs(ctx, "artifacts-stream", lode.WithChecksum(lode.CRC32C()))
	if err != nil {
		return fmt.Errorf("failed to create streaming dataset: %w", err)
	}

	payload := []byte("First chunk of data. Second chunk of data. Final chunk of data.")
	streamSnap, err := dsStream.StreamWrite(ctx, bytes.NewReader(payload), lode.D{
		"content_type": "application/octet-stream",
		"description":  "streamed blob",
	})
	if err != nil {
		return fmt.Errorf("failed to stream write blob: %w", err)
	}

	fmt.Printf("Created streaming snapshot: %s\n", streamSnap.ID)
	fmt.Printf("  Row count: %d\n", streamSnap.RowCount)
	fmt.Printf("  Object size: %d bytes (crc32c=%s)\n", streamSnap.Objects[0].SizeBytes, streamSnap.Objects[0].Checksum)

	streamBlob, err := dsStream.Read(ctx, streamSnap.ID)
	if err != nil {
		return fmt.Errorf("failed to read streamed blob: %w", err)
	}
	fmt.Printf("  Content: %q\n", string(streamBlob))

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Raw blob upload and read complete!")

	return nil
}
