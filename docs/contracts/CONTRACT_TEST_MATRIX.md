# Contract-to-Test Traceability Matrix

This document maps contract requirements to tests and tracks coverage gaps.

---

## Legend

### Coverage Status

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully covered by test(s) |
| ⚠️ | Partial coverage or documented limitation |
| ❌ | Gap — test needed |

### Gap Codes

Gaps are tracked with codes indicating category and priority:

| Prefix | Category | Description |
|--------|----------|-------------|
| `CORE-` | Core Model | Manifest structure, immutability, metadata rules |
| `WRITE-` | Write API | Dataset.Write, StreamWrite, StreamWriteRecords |
| `STORE-` | Storage | Adapter operations (Put, Get, List, Delete, Range) |
| `LAYOUT-` | Layout | Path topology, partitioning, component rules |
| `READ-` | Read API | Reader operations, manifest loading |
| `ERR-` | Errors | Error sentinels and taxonomy |
| `ITER-` | Iteration | ObjectIterator lifecycle |
| `COMP-` | Composition | Store wrapper ordering |

---

## Coverage by Contract

### CONTRACT_CORE.md — Core Model

**Manifest Required Fields**: All covered ✅

| Field | Test |
|-------|------|
| SchemaName | `TestDatasetReader_GetManifest_InvalidManifest/missing_schema_name` |
| FormatVersion | `TestDatasetReader_GetManifest_InvalidManifest/missing_format_version` |
| DatasetID | `TestDatasetReader_GetManifest_InvalidManifest/missing_dataset_ID` |
| SnapshotID | `TestDatasetReader_GetManifest_InvalidManifest/missing_snapshot_ID` |
| CreatedAt | `TestDatasetReader_GetManifest_InvalidManifest/zero_created_at` |
| Metadata (non-nil) | `TestDatasetReader_GetManifest_InvalidManifest/nil_metadata` |
| Files (non-nil) | `TestDatasetReader_GetManifest_InvalidManifest/nil_files` |
| RowCount (≥0) | `TestDatasetReader_GetManifest_InvalidManifest/negative_row_count` |
| Compressor | `TestDatasetReader_GetManifest_InvalidManifest/missing_compressor` |
| Partitioner | `TestDatasetReader_GetManifest_InvalidManifest/missing_partitioner` |

**Manifest Optional Fields**: All covered ✅

| Field | Test |
|-------|------|
| Codec | Raw blob tests (codec omitted) |
| ParentSnapshotID | First snapshot tests (no parent) |
| MinTimestamp/MaxTimestamp | `TestDataset_Write_Timestamps/non_timestamped_records_omits_min_max` |
| Checksum | `TestDataset_Write_Encoding/without_checksum_omits_checksum` |
| FileRef.Stats (present) | `TestDataset_Write_Stats/parquet_codec_stats_populated` |
| FileRef.Stats (absent) | `TestDataset_Write_Stats/JSONL_codec_stats_nil`, `TestDataset_Write_Stats/raw_blob_stats_nil` |
| FileRef.Stats (JSON round-trip) | `TestFileRef_Stats/JSON_round_trip`, `TestFileRef_Stats/backward_compat`, `TestFileRef_Stats/omitted_when_nil` |

**Metadata Rules**: All covered ✅

- nil metadata coalesced: `TestDataset_Write_Metadata/nil_metadata_coalesces_to_empty`, `TestDataset_StreamWrite_Metadata/nil_metadata_coalesces_to_empty`, `TestDataset_StreamWriteRecords_Metadata/nil_metadata_coalesces_to_empty`
- Empty metadata valid: `TestDataset_Write_Metadata/empty_metadata_valid_and_persisted`, etc.

**Immutability**: All covered ✅

- Data files immutable: `TestStore_Put/err_path_exists`, `TestStore_Put/err_path_exists`, `TestStore_Put/err_path_exists`
- Linear history: `TestDataset_StreamWrite_Lineage/parent_snapshot_linked`, `TestDataset_StreamWriteRecords_Commit/parent_snapshot_linked`

---

### CONTRACT_WRITE_API.md — Write API

**Dataset.Write**: All covered ✅

| Requirement | Test |
|-------------|------|
| Creates snapshot | Multiple write tests |
| nil metadata coalesced | `TestDataset_Write_Metadata/nil_metadata_coalesces_to_empty` |
| Parent snapshot linked | `TestDataset_StreamWrite_Lineage/parent_snapshot_linked` |
| Parent ID resolved O(1) via persistent pointer | `TestDataset_Write_Pointer/latest_pointer_skips_scan` |
| Raw blob RowCount=1 | `TestDataset_StreamWrite_Commit/success` |

**StreamWrite**: All covered ✅

| Requirement | Test |
|-------------|------|
| nil metadata coalesced | `TestDataset_StreamWrite_Metadata/nil_metadata_coalesces_to_empty` |
| Commit writes manifest | `TestDataset_StreamWrite_Commit/success` |
| Snapshot invisible before Commit | `TestDataset_StreamWrite_Visibility/not_visible_before_commit` |
| Abort → no manifest | `TestDataset_StreamWrite_Abort/no_manifest` |
| Close without Commit → abort | `TestDataset_StreamWrite_Interrupted/close_without_commit_behaves_as_abort` |
| Codec configured → error | `TestDataset_StreamWrite_Misuse/with_codec_returns_error` |
| Parent ID resolved O(1) via persistent pointer | `TestDataset_StreamWrite_Lineage/latest_pointer_skips_scan` |
| Checksum computed | `TestDataset_StreamWrite_Encoding/with_checksum_records_checksum` |
| Manifest Put error → no manifest + cleanup | `TestStreamWrite_Cleanup/manifest_put_error_no_manifest_cleanup_attempted` |
| Abort → cleanup attempted | `TestStreamWrite_Cleanup/abort_no_manifest_cleanup_attempted` |
| Close → cleanup attempted | `TestStreamWrite_Cleanup/close_without_commit_no_manifest_cleanup_attempted` |
| Cleanup errors ignored | `TestStreamWrite_Cleanup/cleanup_error_ignored` |
| Fast Put failure → error, no block | `TestStreamWrite_PutFailure/data_put_fails_fast_returns_error` |
| Blocked Put + cancel → no manifest | `TestStreamWrite_PutFailure/blocked_put_context_cancel_no_manifest` |

**StreamWriteRecords**: All covered ✅

| Requirement | Test |
|-------------|------|
| nil metadata coalesced | `TestDataset_StreamWriteRecords_Metadata/nil_metadata_coalesces_to_empty` |
| nil iterator error | `TestDataset_StreamWriteRecords_Rejects/nil_iterator` |
| Non-streaming codec error | `TestDataset_StreamWriteRecords_Rejects/non_streaming_codec` |
| Partitioning error | `TestDataset_StreamWriteRecords_Rejects/with_partitioner` |
| Iterator error → no manifest | `TestDataset_StreamWriteRecords_IteratorError/iterator_error` |
| RowCount = records consumed | `TestDataset_StreamWriteRecords_Commit/success` |
| Iterator error → cleanup attempted | `TestStreamWriteRecords_Cleanup/iterator_error_no_manifest_cleanup_attempted` |
| Parent ID resolved O(1) via persistent pointer | `TestDataset_StreamWriteRecords_Commit/latest_pointer_skips_scan` |
| Manifest Put error → no manifest + cleanup | `TestStreamWriteRecords_Cleanup/manifest_put_error_no_manifest` |
| Fast Put failure → error, no block | `TestStreamWriteRecords_PutFailure/data_put_fails_fast_returns_error` |

**Timestamp Computation**: All covered ✅

| Requirement | Test |
|-------------|------|
| Timestamped interface | `TestDataset_Write_Timestamps/timestamped_records_computes_min_max` |
| Non-timestamped omits | `TestDataset_Write_Timestamps/non_timestamped_records_omits_min_max` |
| Raw blob omits | `TestDataset_Write_Timestamps/raw_blob_omits_timestamps` |

**Per-File Statistics**: All covered ✅

| Requirement | Test |
|-------------|------|
| StatisticalCodec populates stats | `TestDataset_Write_Stats/parquet_codec_stats_populated` |
| Non-statistical codec → nil stats | `TestDataset_Write_Stats/JSONL_codec_stats_nil` |
| Raw blob → nil stats | `TestDataset_Write_Stats/raw_blob_stats_nil` |
| StreamWriteRecords → nil stats (JSONL) | `TestDataset_StreamWriteRecords_Metadata/stats_nil` |
| Parquet basic types stats | `TestParquetCodec_FileStats/basic_types` |
| Parquet nullable field stats | `TestParquetCodec_FileStats_Nulls/nullable_fields` |
| Parquet all-null column stats | `TestParquetCodec_FileStats_Nulls/all_nulls` |
| Parquet single record stats | `TestParquetCodec_FileStats/single_record` |
| Parquet bool/bytes no min/max | `TestParquetCodec_FileStats_Nulls/bool_and_bytes_no_min_max` |
| Parquet timestamp stats | `TestParquetCodec_FileStats_Timestamps` |
| Parquet empty records stats | `TestParquetCodec_FileStats/empty_records` |

**Empty Dataset**: All covered ✅

| Requirement | Test |
|-------------|------|
| Latest() → ErrNoSnapshots | `TestDataset_EmptyDataset/latest_empty_dataset_returns_err_no_snapshots` |
| Snapshots() → empty | `TestDataset_EmptyDataset/snapshots_empty_dataset_returns_empty_list` |
| Snapshot(id) → ErrNotFound | `TestDataset_EmptyDataset/snapshot_empty_dataset_returns_err_not_found` |

---

### CONTRACT_STORAGE.md — Storage Adapter

**Put**: All covered ✅

| Requirement | Test |
|-------------|------|
| ErrPathExists on duplicate (atomic) | `TestStore_PutAtomic/put_atomic_duplicate_returns_err_path_exists` |
| All-or-nothing on reader failure | `TestStore_Put/all_or_nothing`, `TestStore_Put/all_or_nothing`, `TestStore_Put_AllOrNothing` |
| Multipart preflight check | `TestStore_PutMultipartFromFile/pre_existing_returns_err_path_exists` |
| Multipart conditional completion | `TestStore_PutMultipartFromFile/conditional_completion_returns_err_path_exists` |
| Conditional completion documented | `TestStore_PutMultipartFromFile/multipart_conditional_completion` |
| Temp file cleanup | `TestStore_Put/temp_file_cleanup_on_success`, `TestStore_Put/temp_file_cleanup_on_failure` |

**Get/Exists/List/Delete**: All covered ✅

| Operation | Tests |
|-----------|-------|
| Get | `TestStore_Get/success`, `TestStore_Get/err_not_found` |
| Exists | `TestStore_Exists/true`, `TestStore_Exists/false` |
| List | `TestStore_List/with_prefix`, `TestStore_List/empty` |
| Delete | `TestStore_Delete/exists`, `TestStore_Delete/not_exists_idempotent` |

**ReadRange**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| Basic range read | `TestStore_ReadRange/basic` (FS, Memory, S3) |
| ErrNotFound | `TestStore_ReadRange/not_found` |
| Invalid params | `TestStore_ReadRange_Bounds/negative_offset`, `TestStore_ReadRange_Bounds/negative_length`, etc. |
| Beyond EOF | `TestStore_ReadRange/beyond_EOF`, `TestStore_ReadRange/offset_beyond_EOF` |
| Exact bounds; allocation bounded by available bytes | `TestStore_ReadRange_Bounds/exact_bounds`, `TestStore_ReadRange_Bounds/exact_bounds` |

**ReaderAt**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| Basic | `TestStore_ReaderAt/basic` |
| Concurrent reads | `TestStore_ReaderAt/concurrent_reads` |
| ErrNotFound | `TestStore_ReaderAt/not_found` |

---

### CONTRACT_LAYOUT.md — Layout & Components

All covered ✅

| Requirement | Test |
|-------------|------|
| nil layout rejected | `TestNewDataset/nil_layout_returns_error`, `TestNewDatasetReader/nil_layout_returns_error` |
| nil compressor rejected | `TestNewDataset/nil_compressor_returns_error` |
| Raw blob + partitioner rejected | `TestNewDataset/raw_blob_with_partitioner_returns_error` |
| Hive layout keys | `TestHiveLayoutOptions/new_hive_layout_with_keys_success` |

**Compression**: All covered ✅

| Requirement | Test |
|-------------|------|
| Gzip round-trip | `TestDataset_StreamWrite_Encoding/with_gzip_compression` |
| Zstd round-trip (Write) | `TestDataset_Write_Encoding/with_zstd_compression` |
| Zstd round-trip (StreamWrite) | `TestDataset_StreamWrite_Encoding/with_zstd_compression` |
| Zstd round-trip (StreamWriteRecords) | `TestDataset_StreamWriteRecords_Encoding/with_zstd_compression` |
| Compressor mismatch error | `TestDataset_Read/compressor_mismatch_returns_error` |

---

### CONTRACT_READ_API.md — Read API

All covered ✅

| Requirement | Test |
|-------------|------|
| ListDatasets layout error | `TestDatasetReader_ListDatasets/flat_layout_returns_err_datasets_not_modeled` |
| ListDatasets empty storage | `TestDatasetReader_ListDatasets/empty_storage` |
| ListDatasets order / StartAfter | `TestDatasetReader_ListDatasets_Order/sorted`, `TestDatasetReader_ListDatasets_Order/start_after_pages`, `TestDatasetReader_ListDatasets_Limit_Boundary` |
| ErrNoManifests | `TestDatasetReader_ListDatasets/err_no_manifests` |
| Manifest validation errors | Multiple `TestDatasetReader_GetManifest_InvalidManifest_*` tests |
| ListPartitions lexicographic order | `TestDatasetReader_ListPartitions_Order/sorted` |
| ListPartitions Prefix (raw prefix, subtree) | `TestDatasetReader_ListPartitions_Prefix/prefix`, `TestDatasetReader_ListPartitions_Prefix/excludes_other_partitions_of_same_snapshot` |
| ListPartitions Prefix empty subtree | `TestDatasetReader_ListPartitions_Prefix/no_match_returns_empty` |
| ListPartitions Prefix canonical-only (no dataset escape) | `TestDatasetReader_ListPartitions_Prefix/rejects_non_canonical` |
| ListPartitions StartAfter / Limit | `TestDatasetReader_ListPartitions_Order/start_after_pages`, `TestDatasetReader_ListPartitions_Order/start_after_exclusive` |
| ListManifests (Partition, ID) order, deterministic partition | `TestDatasetReader_ListManifests_Order/sorted_and_deterministic_partition`, `TestDatasetReader_ListManifests_Order/default_layout_sorted_by_ID` |
| ListManifests StartAfter / Limit | `TestDatasetReader_ListManifests_Paging/start_after_pages`, `TestDatasetReader_ListManifests_Paging/limit_boundary` |

---

### CONTRACT_ERRORS.md — Error Taxonomy

All error sentinels covered ✅

| Sentinel | Test |
|----------|------|
| ErrNotFound | Multiple tests |
| ErrNoSnapshots | `TestDataset_EmptyDataset/latest_empty_dataset_returns_err_no_snapshots` |
| ErrNoManifests | `TestDatasetReader_ListDatasets/err_no_manifests` |
| ErrPathExists | Multiple store tests |
| ErrInvalidPath | Multiple store tests |
| ErrDatasetsNotModeled | `TestDatasetReader_ListDatasets/flat_layout_returns_err_datasets_not_modeled` |
| ErrPartitionPrefixNotSupported | `TestDatasetReader_ListPartitions_Prefix/default_layout_rejected` |
| ErrManifestInvalid | Multiple validation tests |
| ErrInvalidArgument | `TestErrInvalidArgument_Classification` |
| ErrOptionNotValidFor{Dataset,DatasetReader,Volume} | `TestSentinels_Messages`, option applicability tests |
| ErrCodecConfigured | `TestDataset_StreamWrite_Misuse/with_codec_returns_error` |
| ErrCodecNotStreamable | `TestDataset_StreamWriteRecords_Rejects/non_streaming_codec` |
| ErrNilIterator | `TestDataset_StreamWriteRecords_Rejects/nil_iterator` |
| ErrPartitioningNotSupported | `TestDataset_StreamWriteRecords_Rejects/with_partitioner` |
| ErrRangeMissing | `TestVolume_ReadAt/missing_range_returns_err_range_missing`, `TestVolume_ReadAt/gap_at_start_returns_err_range_missing`, `TestVolume_ReadAt/gap_in_middle_returns_err_range_missing` |
| ErrOverlappingBlocks | `TestVolume_Commit_Overlaps/overlapping_blocks_returns_err_overlapping_blocks`, `TestVolume_Commit_Overlaps/contained_block_overlap`, `TestVolume_Commit_Overlaps/same_start_offset_overlap`, `TestVolume_Commit_Overlaps/overlap_with_existing_rejected`, `TestVolume_Commit_Overlaps/three_block_overlap` |

---

### CONTRACT_VOLUME.md — Volume Persistence

**Construction**: All covered ✅

| Requirement | Test |
|-------------|------|
| Valid construction | `TestNewVolume/success` |
| nil factory rejected | `TestNewVolume/nil_factory_returns_error` |
| Zero length rejected | `TestNewVolume/zero_total_length_returns_error` |
| nil store rejected | `TestNewVolume/nil_store_returns_error` |
| Empty ID rejected | `TestNewVolume/empty_ID_returns_error` |

**StageWriteAt**: All covered ✅

| Requirement | Test |
|-------------|------|
| Negative offset rejected | `TestVolume_StageWriteAt/negative_offset_returns_error` |
| Exceeds bounds rejected | `TestVolume_StageWriteAt/exceeds_bounds_returns_error` |
| Empty reader rejected | `TestVolume_StageWriteAt/empty_reader_returns_error` |
| Exact fit accepted | `TestVolume_StageWriteAt/exact_fit` |
| Path layout correct | `TestVolume_StageWriteAt/path_layout` |
| Checksum computed | `TestVolume_StageWriteAt/with_checksum` |

**Commit**: All covered ✅

| Requirement | Test |
|-------------|------|
| End-to-end round-trip | `TestVolume_EndToEnd/stage_commit_read_at_end_to_end` |
| Cumulative manifest | `TestVolume_Cumulative/cumulative_manifest` |
| nil metadata coalesced | `TestVolume_Commit_Manifest/nil_metadata_coalesces_to_empty` |
| Empty metadata accepted | `TestVolume_Commit_Manifest/empty_metadata_succeeds` |
| Empty blocks rejected | `TestVolume_Commit_Blocks/empty_blocks_returns_error` |
| Empty block path rejected | `TestVolume_Commit_Blocks/empty_block_path_returns_error` |
| Duplicate blocks rejected | `TestVolume_Commit_Blocks/duplicate_blocks_returns_error` |
| Block exceeds bounds rejected | `TestVolume_Commit_Blocks/block_exceeds_bounds_returns_error` |
| Negative block offset rejected | `TestVolume_Commit_Blocks/negative_block_offset_returns_error` |
| Zero block length rejected | `TestVolume_Commit_Blocks/zero_block_length_returns_error` |
| Path mismatch rejected | `TestVolume_Commit_Blocks/path_mismatch_returns_error` |
| Three-snapshot progression | `TestVolume_Cumulative/three_snapshots_cumulative_progression` |
| Checksum persisted | `TestVolume_Commit_Manifest/with_checksum_algorithm_persisted` |
| No checksum → no algorithm | `TestVolume_Commit_Manifest/without_checksum_no_algorithm` |
| Manifest round-trip | `TestVolume_Snapshots/manifest_round_trip_all_fields_preserved` |
| Manifest Put error → no snapshot | `TestVolume_Commit_Manifest/manifest_put_error_no_snapshot` |

**Overlap Detection**: All covered ✅

| Requirement | Test |
|-------------|------|
| Overlapping blocks rejected | `TestVolume_Commit_Overlaps/overlapping_blocks_returns_err_overlapping_blocks` |
| Adjacent blocks valid | `TestVolume_Commit_Blocks/adjacent_blocks_valid` |
| Contained block overlap | `TestVolume_Commit_Overlaps/contained_block_overlap` |
| Same start offset overlap | `TestVolume_Commit_Overlaps/same_start_offset_overlap` |
| Overlap with existing rejected | `TestVolume_Commit_Overlaps/overlap_with_existing_rejected` |
| Three-block overlap | `TestVolume_Commit_Overlaps/three_block_overlap` |

**ReadAt**: All covered ✅

| Requirement | Test |
|-------------|------|
| Spanning blocks | `TestVolume_ReadAt/spanning_blocks` |
| Missing range → ErrRangeMissing | `TestVolume_ReadAt/missing_range_returns_err_range_missing` |
| Exact block boundary | `TestVolume_ReadAt/exact_block_boundary` |
| Within single block | `TestVolume_ReadAt/within_single_block` |
| Full volume | `TestVolume_ReadAt/full_volume` |
| Gap at start → ErrRangeMissing | `TestVolume_ReadAt/gap_at_start_returns_err_range_missing` |
| Gap in middle → ErrRangeMissing | `TestVolume_ReadAt/gap_in_middle_returns_err_range_missing` |
| Negative offset rejected | `TestVolume_ReadAt/negative_offset_returns_error` |
| Zero length rejected | `TestVolume_ReadAt/zero_length_returns_error` |
| Sparse blocks with gaps | `TestVolume_Cumulative/sparse_blocks_with_gaps` |
| Dense packing adjacent | `TestVolume_Cumulative/dense_packing_adjacent` |

**Snapshot Queries**: All covered ✅

| Requirement | Test |
|-------------|------|
| Latest empty → ErrNoSnapshots | `TestVolume_Snapshots/latest_empty_volume_returns_err_no_snapshots` |
| Snapshot not found → ErrNotFound | `TestVolume_Snapshots/snapshot_not_found_returns_err_not_found` |
| Snapshots sorted by created_at | `TestVolume_Snapshots/sorted_by_created_at` |
| ID accessor | `TestVolume_Snapshots/ID_returns_volume_ID` |

**Manifest Validation**: All covered ✅

| Requirement | Test |
|-------------|------|
| Missing schema_name | `TestVolume_Snapshot_InvalidManifest/missing_schema_name` |
| Missing volume_id | `TestVolume_Snapshot_InvalidManifest/missing_volume_ID` |
| Missing snapshot_id | `TestVolume_Snapshot_InvalidManifest/missing_snapshot_ID` |
| Zero created_at | `TestVolume_Snapshot_InvalidManifest/zero_created_at` |
| nil metadata | `TestVolume_Snapshot_InvalidManifest/nil_metadata` |
| nil blocks | `TestVolume_Snapshot_InvalidManifest/nil_blocks` |
| Negative total_length | `TestVolume_Snapshot_InvalidManifest/negative_total_length` |
| Volume ID mismatch | `TestVolume_Snapshot_InvalidManifest/volume_ID_mismatch_returns_error` |
| Total length mismatch | `TestVolume_Snapshot_InvalidManifest/total_length_mismatch_returns_error` |

**Resume**: All covered ✅

| Requirement | Test |
|-------------|------|
| New instance loads latest | `TestVolume_Resume/new_instance_loads_latest` |
| Continue staging after resume | `TestVolume_Resume/continue_staging` |

**FS Integration**: All covered ✅

| Requirement | Test |
|-------------|------|
| FS store round-trip | `TestVolume_EndToEnd/FS_store_stage_commit_read_at` |

---

## Performance: O(1) Resolution and Complexity Guarantees

These tests verify that hot-path operations use O(1) store calls or O(log B) lookups
instead of O(N) full scans. Regressions here would cause wall-clock degradation
proportional to dataset/volume size on remote stores (S3/R2).

**Dataset — Latest Pointer**: Persistent `latest` file enables O(1) resolution.

| Requirement | Test |
|-------------|------|
| Pointer written on Write | `TestDataset_LatestPointer/read_after_write` |
| Pointer tracks most recent | `TestDataset_LatestPointer/updates_across_writes` |
| Backward compat: scan fallback | `TestDataset_LatestPointer/backward_compat` |
| Corrupt pointer: scan fallback | `TestDataset_LatestPointer/corrupt_pointer` |
| Pointer paths for all layouts | `TestDataset_LatestPointer/all_layouts` |
| Write: 0 List calls after pointer | `TestDataset_Write_Pointer/latest_pointer_skips_scan` |
| StreamWrite: 0 List calls | `TestDataset_StreamWrite_Lineage/latest_pointer_skips_scan` |
| StreamWriteRecords: 0 List calls | `TestDataset_StreamWriteRecords_Commit/latest_pointer_skips_scan` |

**Dataset — HiveLayout Canonical Manifest**: O(1) `Snapshot(ctx, id)`.

| Requirement | Test |
|-------------|------|
| Snapshot by ID: 0 List calls | `TestDataset_HiveLayout/snapshot_by_ID_no_list_call` |
| Canonical manifest written | `TestDataset_HiveLayout/canonical_manifest_written` |
| Backward compat: scan fallback | `TestDataset_HiveLayout/backward_compat_fallback_scan` |

**Volume — Latest Pointer**: Same O(1) pattern for volumes.

| Requirement | Test |
|-------------|------|
| Pointer written on Commit | `TestVolume_LatestPointer/read_after_commit` |
| Pointer tracks most recent | `TestVolume_LatestPointer/multiple_commits` |
| Backward compat: scan fallback | `TestVolume_LatestPointer/backward_compat` |

**Volume — Block Operations**: Sorted blocks and O(log B) lookup.

| Requirement | Test |
|-------------|------|
| Blocks sorted in manifest | `TestVolume_Commit_Blocks/blocks_sorted_in_manifest` |
| Block merge correctness | `TestMergeBlocks` (4 subtests) |
| Binary search: sorted input | `TestFindCoveringBlocks_BinarySearch` (6 subtests) |
| Sorted overlap detection | `TestVolume_ValidateNoOverlaps/sorted_input` |
| Sorted overlap rejection | `TestVolume_ValidateNoOverlaps/sorted_overlap` |
| Load-time block sorting | `TestVolume_Snapshots/load_snapshot_sorts_blocks_at_load_time` |

**Pointer Verification**: Corrupt/stale pointers fall back correctly.

| Requirement | Test |
|-------------|------|
| Dataset: nonexistent pointer target → scan fallback | `TestDataset_Write_Pointer/corrupt_pointer_falls_back_to_scan` |
| Volume: nonexistent pointer target → scan fallback | `TestVolume_Commit_Pointer/stale_pointer_falls_back_to_scan` |
| Dataset: existing-but-stale pointer → in-memory cache wins | `TestDataset_Write_Pointer/stale_but_existing_pointer_uses_in_memory_cache` |
| Volume: existing-but-stale pointer → in-memory cache wins | `TestVolume_Commit_Pointer/stale_but_existing_pointer_uses_in_memory_cache` |

**Cross-Process / Cold-Start Pointer**: Pointer correctness across process restarts.

| Requirement | Test |
|-------------|------|
| Dataset: cold start reads correct pointer | `TestDataset_Write_ColdStart/reads_pointer_from_store` |
| Dataset: cold start with corrupt pointer → scan fallback | `TestDataset_Write_ColdStart/corrupt_pointer_falls_back_to_scan` |
| Dataset: pointer write failure aborts commit | `TestDataset_Write_Pointer/pointer_write_failure_aborts_commit` |
| Volume: cold start reads correct pointer | `TestVolume_Commit_ColdStart/reads_pointer_from_store` |
| Volume: cold start with corrupt pointer → scan fallback | `TestVolume_Commit_ColdStart/corrupt_pointer_falls_back_to_scan` |
| Volume: pointer write failure aborts commit | `TestVolume_Commit_Pointer/pointer_write_failure_aborts_commit` |

**Hive Canonical Manifest**: Canonical manifest excluded from listing.

| Requirement | Test |
|-------------|------|
| ListManifests skips canonical for partition layouts | `TestDatasetReader_ListManifests/hive_layout_skips_canonical_manifest` |

**Benchmarks**:

| Benchmark | Validates |
|-----------|-----------|
| `BenchmarkDataset_SequentialWrites` | Pointer eliminates quadratic scan cost |
| `BenchmarkDataset_SequentialWrites_StoreCallCount` | O(1) List calls regardless of snapshot count |
| `BenchmarkFindCoveringBlocks` | O(log B) lookup at 10/100/1K/10K blocks |
| `BenchmarkMergeBlocks` | O(N + K log K) merge at 10/100/1K/10K existing blocks |

### Complexity Violations (all resolved)

| ID | Operation | Resolution | Test |
|----|-----------|------------|------|
| CX-1 | `findCoveringBlocks` sort check | Fixed: sort at load time | `TestVolume_Snapshots/load_snapshot_sorts_blocks_at_load_time` |
| CX-2 | `ListManifests` validation Gets | Reclassified: inherent cost (CONTRACT_ERRORS.md) | — |
| CX-3 | `ListPartitions` double deser | Fixed: single-pass (1 List + M Gets) | `TestDatasetReader_ListPartitions_SingleManifestLoad` |
| CX-4 | `Snapshot(id)` degraded fallback | Reclassified: documented degraded path | — |
| CX-5 | `Write` partitioned memory | Reclassified: O(R aliases), not O(3R copies) | — |
| CX-6 | `Volume.Snapshots` sort checks | Fixed: sort at load time | `TestVolume_Snapshots/load_snapshot_sorts_blocks_at_load_time` |
| CX-7 | `fsStore.List` Walk vs WalkDir | Fixed: `filepath.WalkDir` | Existing `fsStore.List` tests |

### Documented Inherent Costs (not violations)

| Operation | Cost | Reason |
|-----------|------|--------|
| `Store.List` | O(N) memory | Interface returns `[]string` |
| `Dataset.Snapshots()` | O(S) Gets | Must load all manifests for history |
| `Volume.Snapshots()` | O(S × B_avg) memory | Cumulative manifests |
| `Volume.Commit` manifest | O(B) per commit | Cumulative design |
| `Dataset.Read(id)` | O(R_total) memory | Returns all records |
| `latestByScan` | O(N) List | Backward-compat; self-heals via pointer write |
| `ListManifests` validation | M Gets | Contractually required by CONTRACT_ERRORS.md (CX-2) |
| `Snapshot(id)` fallback | O(N) scan | Degraded path for pre-canonical data; self-heals (CX-4) |
| `Write` partitioned memory | O(R interface aliases) | Partition map holds references, not copies (CX-5) |

---

## Open Gaps

### G3-4: Streaming Failure Semantics

The following invariants are now **deterministically tested** via fault injection:

| Invariant | Coverage |
|-----------|----------|
| No manifest on failed stream | ✅ All failure paths |
| Cleanup (Delete) attempted on failure | ✅ All failure paths |
| Cleanup errors ignored (best-effort) | ✅ Explicit test |
| Snapshot invisibility preserved | ✅ All failure paths |

**Deterministic test infrastructure**: `lode/store_fault_test.go` provides a fault-injection
store wrapper with error injection, call observation, and blocking for synchronization.

### Residual Risk: STORE-CTX-CANCEL (Addressed)

**Context cancellation cleanup timing is adapter-dependent.**

| Aspect | Status |
|--------|--------|
| Core invariant: no manifest on cancel | ✅ Deterministically tested with blocked Put |
| Cleanup attempt on cancel | ✅ Deterministically tested with blocked Put |
| Actual cleanup completion | ⚠️ Timing-dependent (adapter-specific) |
| FS adapter timing | ✅ Integration tests in `lode/adapter_timing_test.go` |
| S3 adapter timing | ✅ Integration tests in `lode/s3/integration_test.go` (gated) |

**Why timing matters:**
- In-memory stores complete synchronously before cancellation takes effect
- Real adapters (S3, FS) have varying timing windows
- Whether cleanup Delete runs before or after context deadline is nondeterministic

**Mitigation:**
- Core invariants are tested deterministically via `TestStreamWrite_PutFailure/blocked_put_context_cancel_no_manifest`
- FS adapter timing verified via `TestFSAdapter_ContextCancel/stream_write_context_cancel_no_snapshot`
- S3 adapter timing verified via `TestIntegration_Writes/stream_write_context_cancel` (requires `go test ./lode/s3/... -integration` against the docker-compose services)
- Cleanup is documented as best-effort per CONTRACT_ERRORS.md

**Status**: Core invariants covered. Adapter-specific timing tests exercise real behavior without asserting on inherently nondeterministic outcomes.

---

### Deferred: Requires API Exposure

| Gap | Contract | Blocked By |
|-----|----------|------------|
| ITER-LIFECYCLE | ITERATION | ObjectIterator not yet public |
| COMP-WRAPPER-ORDER | COMPOSITION | Composition API not yet public |
| ERR-RANGE-NOT-SUPPORTED | ERRORS | No non-range adapter exists |

These gaps will be addressed when the relevant APIs are exposed.

---

## Test Baseline

```
$ go test ./...
ok  	github.com/pithecene-io/lode/lode
ok  	github.com/pithecene-io/lode/lode/s3
```

All contracts have test coverage except for deferred gaps and documented residual risk.
