# Examples

Each example is self-contained and runnable, uses only the public API, and
documents its steps in comments. Run an example from the repository root
with the command shown.

| Example | Purpose | Run |
|---------|---------|-----|
| [`default_layout`](default_layout) | Writing, listing, and reading with the default layout | `go run ./examples/default_layout` |
| [`hive_layout`](hive_layout) | A partition-first layout with the Hive partitioner | `go run ./examples/hive_layout` |
| [`vessel_positions`](vessel_positions) | Vessel position reports under a Hive day layout (adapted on `pithecene.sh`) | `go run ./examples/vessel_positions` |
| [`blob_upload`](blob_upload) | Writing, streaming, and checksum-verifying raw blobs | `go run ./examples/blob_upload` |
| [`manifest_driven`](manifest_driven) | The manifest as the signal of a commit | `go run ./examples/manifest_driven` |
| [`stream_write_records`](stream_write_records) | Streaming record writes with an iterator | `go run ./examples/stream_write_records` |
| [`parquet`](parquet) | The generic `Parquet[T]` codec with typed Go structs and map records | `go run ./examples/parquet` |
| [`volume_sparse`](volume_sparse) | A sparse Volume: staging, committing, and reading with gaps | `go run ./examples/volume_sparse` |
| [`depth_grid`](depth_grid) | A 32-by-32 depth grid committed in three sparse Volume snapshots (adapted on `pithecene.sh`) | `go run ./examples/depth_grid` |
| [`vector_artifacts`](vector_artifacts) | A vector artifact pipeline: embeddings, indices, and pointers | `go run ./examples/vector_artifacts` |
| [`optimistic_concurrency`](optimistic_concurrency) | CAS conflict detection and the retry pattern | `go run ./examples/optimistic_concurrency` |
| [`retention`](retention) | Expiring runs by partition after a preview, then sweeping | `go run ./examples/retention` |
| [`s3`](s3) | The S3 adapter with `s3.Bucket` and `lode.Prefixed` on LocalStack or RustFS | `go run ./examples/s3` |
