# Blob Upload — Large Binary Guidance

This example demonstrates raw blob storage with the default components. For
large binary artifacts, prefer single-pass streaming writes to modeling
chunks as logical records.

## Write APIs

**Single write (`Write`):**
- Use it for in-memory data or small blobs.
- The data is buffered and written atomically.

**Streaming write (`StreamWrite`):**
- Use it for large binary payloads that should be streamed once.
- The data is written directly to the final object path, without temporary
  files.
- The snapshot becomes visible only after `Commit` writes the manifest.
- If a stream is aborted or fails, no snapshot is created.

## Single-Pass Streaming

`StreamWrite` performs single-pass writes:
1. The data is written directly to the final object path.
2. Compression and checksums are computed as the data streams.
3. `Commit` finalizes the stream and writes the manifest.
4. `Abort`, or `Close` without `Commit`, leaves no snapshot.

This design avoids temporary files and the cost of copying data at commit.

## Range Reads

For efficient partial reads:
- Store a single object and use `ReadRange` or `ReaderAt` to read at
  offsets.
- If chunking is required, write several objects and record an explicit
  chunk index (object keys, byte ranges, and order) in metadata.

## Removal of Partial Objects

Partial objects may remain if a stream is aborted or fails. Lode attempts
to remove them on abort but does not guarantee their deletion.
`Dataset.Sweep` reclaims such objects once they are older than its grace
period; see [the retention guide](../../docs/guides/retention.md).

## Metadata

Record artifact metadata explicitly (size, media type, chunk index, and so
on). Checksum fields are recorded only when a checksum component is
configured.
