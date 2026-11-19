# Blob Upload — Large Binary Guidance

This example demonstrates raw blob storage, single-call streaming, and
end-to-end checksum verification (`SHA256` and `CRC32C`). For large binary
artifacts, prefer single-pass streaming writes to modeling chunks as logical
records.

## Write APIs

**Buffered write (`Blobs.Write`):**
- Use it for in-memory data or small blobs (`Write(ctx, data, metadata)`).
- Lode buffers, optionally compresses and checksums, and persists the object
  before committing the manifest.

**Streaming write (`Blobs.StreamWrite`):**
- Use it for large binary payloads streamed once (`StreamWrite(ctx, r,
  metadata)`).
- Bytes from `r` (`io.Reader`) stream directly to the final object path in a
  single pass. When uncompressed, `r` passes directly to `Store.Put` without
  an intermediate `io.Pipe` or background goroutine.
- The snapshot becomes visible only after `r` reaches `io.EOF` and Lode
  persists the manifest.
- If `r` returns an error or `ctx` is canceled, Lode writes no manifest and
  creates no snapshot.

## Single-Pass Streaming

`Blobs.StreamWrite` performs single-pass writes:
1. Bytes stream from the caller's `io.Reader` directly to the final object
   key.
2. Lode applies compression and computes the configured checksum (`SHA256`,
   `CRC32C`, `MD5`, or a custom `Checksum`) as the bytes stream.
3. Once the reader completes cleanly, Lode advances the `latest` pointer and
   persists the manifest.
4. On `Blobs.Read` or `BlobView.Read`, Lode verifies the stored bytes against
   the manifest's checksum automatically and returns `ErrChecksumMismatch` if
   they differ.

## Range Reads

For efficient partial reads of raw stored bytes:
- Store a single object and call `ReaderAt(ctx, obj)` (or
  `RangeReader.ReadRange` on the underlying store) to read byte ranges at
  arbitrary offsets.
- If chunking is required, write several blob snapshots or use
  `lode/volume` for sparse byte-range persistence, or record an explicit
  chunk index in snapshot metadata.

## Removal of Partial Objects

Partial objects may remain if a stream fails before the manifest is written.
Lode attempts to remove a completed stream's object if the commit fails
before any manifest write, and `Blobs.Sweep` reclaims unreferenced objects
once they exceed its grace period; see [the retention
guide](../../docs/guides/retention.md).

## Metadata

Record artifact metadata explicitly (media type, original filename, or custom
provenance). Checksum digests are recorded and verified on read whenever the
dataset is configured with `WithChecksum`.
