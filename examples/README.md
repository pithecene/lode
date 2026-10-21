# Examples

Each example is self-contained and runnable, uses only the public API, and
documents its steps in comments. Run an example from the repository root
with the command shown.

| Example | Purpose | Run |
|---------|---------|-----|
| [`default_layout`](default_layout) | Writing, listing, and reading with the default layout | `go run ./examples/default_layout` |
| [`hive_layout`](hive_layout) | A partition-first layout with the Hive partitioner | `go run ./examples/hive_layout` |
| [`blob_upload`](blob_upload) | Writing and reading a raw blob (no codec, default components) | `go run ./examples/blob_upload` |
| [`manifest_driven`](manifest_driven) | The manifest as the signal of a commit | `go run ./examples/manifest_driven` |
| [`stream_write_records`](stream_write_records) | Streaming record writes with an iterator | `go run ./examples/stream_write_records` |
| [`parquet`](parquet) | The Parquet codec with schema-typed fields | `go run ./examples/parquet` |
| [`volume_sparse`](volume_sparse) | A sparse Volume: staging, committing, and reading with gaps | `go run ./examples/volume_sparse` |
| [`vector_artifacts`](vector_artifacts) | A vector artifact pipeline: embeddings, indices, and pointers | `go run ./examples/vector_artifacts` |
| [`optimistic_concurrency`](optimistic_concurrency) | CAS conflict detection and the retry pattern | `go run ./examples/optimistic_concurrency` |
| [`retention`](retention) | Expiring runs by partition after a preview, then sweeping | `go run ./examples/retention` |
| [`s3`](s3) | The S3 adapter with LocalStack or MinIO | `go run ./examples/s3` |
