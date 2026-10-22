# Contract-to-Test Traceability Matrix

This document maps contract requirements to tests and records gaps in
coverage.

---

## Legend

### Coverage Status

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully covered by test(s) |
| ⚠️ | Partial coverage or documented limitation |
| ❌ | Gap — test needed |

### Gap Codes

Gaps are recorded with codes that indicate category and priority:

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

**Manifest Settings Fields (format 1.1.0)**: All covered ✅

| Requirement | Test |
|-------------|------|
| A manifest records format 1.1.0, the codec description, the Hive keys, and the default path names | `TestWrite_ManifestRecordsSettings` |
| A flat layout records `flat` and omits the datasets and snapshots directories | `TestWrite_ManifestRecordsSettings` |
| A codec description is canonical and omits the Parquet internal compression; JSONL describes nothing | `TestCodec_Describe` |

**Snapshot ID**: All covered ✅

| Rule | Test |
|------|------|
| `<instant>-<nonce>` form; legacy `<instant>` accepted | `TestSnapshotID/instant_only_from_the_generated_form`, `TestSnapshotID/format_round-trips_the_instant` |
| Unique when the clock repeats | `TestSnapshotID/unique_within_one_instant` |
| New IDs carry the clock's instant | `TestSnapshotID/new_IDs_carry_the_clock` |
| String order follows instant; legacy sorts first on a tie | `TestSnapshotID/string_order_follows_the_instant` |

**Metadata Rules**: All covered ✅

- `nil` metadata converted to empty metadata: `TestDataset_Write_Metadata/nil_metadata_coalesces_to_empty`, `TestDataset_StreamWrite_Metadata/nil_metadata_coalesces_to_empty`, `TestDataset_StreamWriteRecords_Metadata/nil_metadata_coalesces_to_empty`
- Empty metadata is valid: `TestDataset_Write_Metadata/empty_metadata_valid_and_persisted`, etc.

**Immutability**: All covered ✅

- Data files are immutable: `TestStore_Put/err_path_exists`, `TestStore_Put/err_path_exists`, `TestStore_Put/err_path_exists`
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
| A write of no records commits a snapshot without data files on every layout | `TestDataset_Write_EmptyBatch` |

**Settings**: All covered ✅

| Requirement | Test |
|-------------|------|
| A writer whose codec, codec description, compressor, checksum, or partition keys differ is rejected with `ErrSettingsMismatch`, which names the setting | `TestWrite_SettingsMismatch/codec`, `TestWrite_SettingsMismatch/codec_description`, `TestWrite_SettingsMismatch/compressor`, `TestWrite_SettingsMismatch/checksum`, `TestWrite_SettingsMismatch/partition_keys` |
| A writer with matching settings commits | `TestWrite_SettingsMismatch/matching_settings_commit` |
| The loser of two first commits is checked against the winner and rejected, not retried | `TestWrite_SettingsMismatch/the_losing_first_commit_is_rejected,_not_retried` |
| A pending first commit's staged copy records the settings | `TestWrite_SettingsOfAPendingFirstCommit/its_staged_copy_records_the_settings` |
| A pointer naming a snapshot with no manifest, in a dataset with none, is `ErrSnapshotConflict` | `TestWrite_SettingsOfAPendingFirstCommit/without_any_manifest_the_writer_conflicts` |
| After an abandoned later commit, the newest manifest records the settings, and the next commit adopts the abandoned one | `TestWrite_SettingsOfAPendingFirstCommit/an_abandoned_later_commit_falls_back_to_the_newest_manifest` |
| Legacy Hive keys are verified by name and order against the first snapshot with files; the next commit records them | `TestWrite_LegacyHiveKeys/keys_are_verified_against_the_partition_paths` |
| A legacy history without files accepts any keys | `TestWrite_LegacyHiveKeys/a_history_without_files_accepts_any_keys` |
| A legacy chain broken before a snapshot with files is rejected as unverifiable | `TestWrite_LegacyHiveKeys/a_chain_broken_before_a_snapshot_with_files_is_rejected` |

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
| Manifest Put error → data retained (the manifest may have been stored) | `TestStreamWrite_Cleanup/manifest_put_error_retains_data` |
| Pointer error → data removed | `TestStreamWrite_CommitFailure/pointer_error_removes_data` |
| Lost manifest response → the visible snapshot stays readable | `TestStreamWrite_CommitFailure/lost_manifest_response_keeps_the_snapshot_readable` |
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
| Manifest Put error → data retained | `TestStreamWriteRecords_Cleanup/manifest_put_error_no_manifest` |
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
| A failed Put leaves nothing visible (filesystem) | `TestFSStore_Put_FailureLeavesNothingVisible` |
| An object is visible only once complete, with the mode a direct creation under the process umask gives (filesystem) | `TestFSStore_Put_PublishesAtomically` |
| Every adapter rejects a key whose final segment begins with the reserved temporary-name prefix | `TestStore_Put_RejectsReservedNames`, `TestKey` |
| Multipart preflight check | `TestStore_PutMultipartFromFile/pre_existing_returns_err_path_exists` |
| Multipart conditional completion | `TestStore_PutMultipartFromFile/conditional_completion_returns_err_path_exists` |
| Conditional completion documented | `TestStore_PutMultipartFromFile/multipart_conditional_completion` |
| Temp file cleanup | `TestStore_Put/temp_file_cleanup_on_success`, `TestStore_Put/temp_file_cleanup_on_failure` |

**Get/Exists/List/Delete**: All covered ✅

| Operation | Tests |
|-----------|-------|
| Get | `TestStore_Get/success`, `TestStore_Get/err_not_found` |
| Get on a missing path returns an error that matches `ErrNotFound` (filesystem, memory, and S3 adapters) | `TestStore_Get/not_found`, `TestStore_Get/err_not_found` (S3) |
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

**CompareAndSwap (filesystem, memory, and `SingleProcess`)**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| `CompareAndSwap` creates, replaces, and refuses as the semantics table requires | `TestStore_CompareAndSwap/fs`, `TestStore_CompareAndSwap/memory`, `TestStore_CompareAndSwap/single-process` |
| The filesystem store replaces a pointer that a reader holds open, and one beyond MAX_PATH | `TestFSStore_CompareAndSwap_Replacement` |
| `SingleProcess` keeps histories linear under concurrent writers in one process | `TestChaos` with `-chaos -chaos.backends single-process` (on demand) |
| A read through `SingleProcess` waits for a replacement in progress and observes the new content, never the absent path | `TestSingleProcess_ReadsWaitForReplacement` |
| `SingleProcess` returns a nil store as nil, so that a constructor rejects it | `TestSingleProcess_NilStore` |

**CompareAndSwap (S3)**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| `CompareAndSwap` creates, replaces, and refuses as the semantics table requires | `TestStore_CompareAndSwap` |
| Creating the object issues no read | `TestStore_CompareAndSwap_Requests/create_writes_without_reading` |
| Replacing a version that the adapter last wrote by replacement issues no read | `TestStore_CompareAndSwap_Requests/replacing_its_own_version_writes_without_reading` |
| A creation leaves no record, so objects that are created once, such as tombstones, do not accumulate records | `TestStore_CompareAndSwap_Requests/a_creation_leaves_no_record` |
| The record holds a bounded number of versions, and `Delete` discards the record of the deleted object | `TestStore_CompareAndSwap_Requests/the_record_is_bounded_and_forgets_deleted_keys` |
| A record identifies its content by digest and does not retain the content | `TestStore_CompareAndSwap_Requests/a_record_keeps_a_digest,_not_the_content` |
| A recorded version that another writer deleted is read and yields `ErrSnapshotConflict` | `TestStore_CompareAndSwap_Requests/a_version_deleted_by_another_writer_is_read_and_conflicts` |
| An object deleted between the read and the conditional write yields `ErrSnapshotConflict` | `TestStore_CompareAndSwap_Requests/an_object_deleted_between_the_read_and_the_write_conflicts` |
| A version that another writer replaced is read and yields `ErrSnapshotConflict` | `TestStore_CompareAndSwap_Requests/a_version_replaced_by_another_writer_is_read_and_conflicts` |
| An existing empty object satisfies an empty `expected`; any other existing object refuses it | `TestStore_CompareAndSwap_Requests/an_existing_empty_object_satisfies_an_empty_expectation`, `TestStore_CompareAndSwap_Requests/an_existing_object_refuses_an_empty_expectation` |
| After a lost response, the adapter reads the object instead of assuming its version | `TestStore_CompareAndSwap_Requests/a_lost_response_forgets_the_version` |

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
| A dataset ID that is not one canonical path segment is `ErrInvalidArgument`; `=` is permitted | `TestNewDataset_DatasetID`, `TestSegment` |
| ErrSettingsMismatch | `TestWrite_SettingsMismatch` |
| ErrOptionNotValidFor{Dataset,DatasetReader} | `TestSentinels_Messages`, option applicability tests |
| ErrCodecConfigured | `TestDataset_StreamWrite_Misuse/with_codec_returns_error` |
| ErrCodecNotStreamable | `TestDataset_StreamWriteRecords_Rejects/non_streaming_codec` |
| ErrNilIterator | `TestDataset_StreamWriteRecords_Rejects/nil_iterator` |
| ErrPartitioningNotSupported | `TestDataset_StreamWriteRecords_Rejects/with_partitioner` |
| volume.ErrRangeMissing | `TestVolume_ReadAt/missing_range_returns_err_range_missing`, `TestVolume_ReadAt/gap_at_start_returns_err_range_missing`, `TestVolume_ReadAt/gap_in_middle_returns_err_range_missing` |
| volume.ErrOverlappingBlocks | `TestVolume_Commit_Overlaps/overlapping_blocks_returns_err_overlapping_blocks`, `TestVolume_Commit_Overlaps/contained_block_overlap`, `TestVolume_Commit_Overlaps/same_start_offset_overlap`, `TestVolume_Commit_Overlaps/overlap_with_existing_rejected`, `TestVolume_Commit_Overlaps/three_block_overlap` |
| ErrRangeMissing | `TestVolume_ReadAt/missing_range_returns_err_range_missing`, `TestVolume_ReadAt/gap_at_start_returns_err_range_missing`, `TestVolume_ReadAt/gap_in_middle_returns_err_range_missing` |
| ErrOverlappingBlocks | `TestVolume_Commit_Overlaps/overlapping_blocks_returns_err_overlapping_blocks`, `TestVolume_Commit_Overlaps/contained_block_overlap`, `TestVolume_Commit_Overlaps/same_start_offset_overlap`, `TestVolume_Commit_Overlaps/overlap_with_existing_rejected`, `TestVolume_Commit_Overlaps/three_block_overlap` |
| ErrSnapshotExpired | `TestExpire_Selection/by_ID_removes_manifest_and_data`, `TestExpire_Resumption/interrupted_after_the_canonical_delete_is_not_resurrected` |
| ErrHeadUncommitted | `TestExpire_Head/uncommitted_expires_nothing`, `TestExpire_Head/uncommitted_mid-run_reports_completed_work` |

---

### CONTRACT_RETENTION.md — Snapshot Expiry

| Requirement | Test | Status |
|-------------|------|--------|
| An empty request, or a malformed or unsupported `Partition`, is rejected before any store call | `TestExpire_Validation` | ✅ |
| Expiry removes manifests and data and retains a tombstone; `Snapshot` and `Read` then return `ErrSnapshotExpired` | `TestExpire_Selection/by_ID_removes_manifest_and_data`, `TestExpire_Selection/flat_layout` | ✅ |
| `Partition` matches whole path segments | `TestExpire_Partition/matches_whole_segments`, `TestExpire_Partition/a_trailing_slash_is_valid` | ✅ |
| A mixed snapshot is retained under `Partition` and can be expired through `IDs` | `TestExpire_Partition/mixed_snapshot_is_kept`, `TestExpire_Partition/a_resumed_expiry_is_checked_against_the_partition` | ✅ |
| `Before` selects by the snapshot ID's instant, and the selectors intersect | `TestExpire_Selection/before_uses_the_snapshot_ID_instant`, `TestExpire_Selection/selectors_intersect` | ✅ |
| The head is retained and reported, and no tombstone is written for it; a head superseded during the run remains retained | `TestExpire_Head/retained_and_reported`, `TestExpire_Head/superseded_mid-run_stays_retained` | ✅ |
| An uncommitted head terminates expiry, including at the check before a retained head, and the report lists the work completed | `TestExpire_Head/uncommitted_expires_nothing`, `TestExpire_Head/uncommitted_mid-run_reports_completed_work`, `TestExpire_Head/uncommitted_before_a_retained_head_stops_the_run` | ✅ |
| A dry run writes and deletes nothing and fails wherever a real run would fail | `TestExpire_DryRun/changes_nothing`, `TestExpire_DryRun/fails_where_a_real_run_would` | ✅ |
| An existing tombstone for the same ID resumes the expiry, and one for another ID fails it. A tombstone records the whole `Files[]` and the manifest's `CreatedAt`. A tombstone without a commit instant, with a missing or invalid inventory, or of zero length with no manifest remaining is corrupt. Nothing is recorded from a manifest that violates the manifest invariants | `TestExpire_Tombstone/existing_for_the_same_snapshot_resumes`, `TestExpire_Tombstone/existing_for_another_snapshot_fails`, `TestExpire_Tombstone/zero_length_is_replaced`, `TestExpire_Tombstone/records_the_inventory`, `TestExpire_InvalidManifest`, `TestExpire_CorruptTombstone/no_commit_instant`, `TestExpire_CorruptTombstone/a_corrupt_one_does_not_expire_a_legacy_snapshot`, `TestExpire_CorruptTombstone/invalid_inventory`, `TestExpire_CorruptTombstone/zero_length_with_no_manifest_left` | ✅ |
| A recorded expiry resumes from its tombstone when no manifest remains, and each run counts only what it deleted | `TestExpire_Resumption/from_a_tombstone_with_no_manifest_left` | ✅ |
| An ID not in the generated form never becomes a store path. A tombstone that does not describe every manifest copy (by commit instant and inventory) is refused, and a concurrently created tombstone is accepted only if its inventory is the same | `TestExpire_Tombstone/ID_not_in_the_generated_form_never_reaches_a_path`, `TestSnapshot_IDNotInGeneratedForm_IsNotExpired`, `TestExpire_ReusedID/same_files_with_another_commit_instant_is_refused`, `TestExpire_ReusedID/a_fresh_expiry_checks_every_manifest_copy`, `TestExpire_ReusedID/tombstone_of_another_inventory_is_refused`, `TestExpire_ReusedID/recorded_tombstone_must_match_every_manifest_copy`, `TestExpire_ReusedID/copy_reappearing_at_a_derived_path_is_checked`, `TestExpire_ReusedID/manifest_reappearing_for_a_tombstone_is_checked`, `TestExpire_Tombstone/concurrent_one_is_accepted`, `TestExpire_Tombstone/concurrent_one_of_another_inventory_is_refused` | ✅ |
| A dataset with snapshots but no pointer is refused with `ErrHeadUncommitted`, regardless of its IDs; an empty dataset requires no pointer | `TestExpire_HeadResolution/a_pointer-less_dataset_is_refused`, `TestExpire_HeadResolution/a_pointer_missing_beside_new-form_IDs_is_refused`, `TestExpire_HeadResolution/tombstones_without_a_pointer_or_manifest_are_refused`, `TestExpire_HeadResolution/an_empty_dataset_needs_no_pointer`, `TestExpire_HeadResolution/a_pointer_that_is_not_a_snapshot_ID_is_refused`, `TestExpire_HeadResolution/a_pointer_outside_the_generated_form_is_refused` | ✅ |
| The scan fallback does not restore an expired snapshot, and a repeated run completes the expiry | `TestExpire_Resumption/interrupted_after_the_canonical_delete_is_not_resurrected` | ✅ |
| Foreign `Files[]` paths are reported in `Skipped` and never deleted, and no derived path leaves the dataset | `TestExpire_PathSafety/foreign_FileRef_is_skipped`, `TestExpire_PathSafety/a_path_listed_twice_is_counted_once`, `TestExpire_PathSafety/a_snapshot_left_unexpired_reports_no_skipped_paths`, `TestExpire_PathSafety/a_failed_expiry_reports_no_skipped_paths`, `TestExpire_Partition/a_foreign_file_does_not_make_a_snapshot_mixed`, `TestExpire_PathSafety/traversal_in_a_FileRef_deletes_nothing_outside` | ✅ |
| A legacy head with partition manifests only is treated as committed | `TestExpire_HeadResolution/legacy_partition-only_head_is_committed` | ✅ |
| `Sweep` rejects a grace period that is not positive before any listing | `TestSweep_Request/non-positive_grace_is_rejected` | ✅ |
| Orphaned objects older than the grace period are reclaimed, including those of an abandoned commit that the pointer names; younger objects and objects without an instant are retained; auxiliary files are outside the scope | `TestSweep_Reclaim/only_orphans_past_grace`, `TestSweep_Reclaim/an_orphan_whose_manifest_the_listing_omits_is_kept`, `TestSweep_Reclaim/a_crashed_commit's_data_is_an_orphan` | ✅ |
| A live snapshot retains objects that its `Files[]` does not list | `TestSweep_Reclaim/a_live_snapshot_keeps_unlisted_objects` | ✅ |
| Interrupted expiries are completed from the tombstone's inventory, and settled objects are reclaimed irrespective of the grace period; recorded sizes are summed in `Bytes`, and the remaining objects are counted in `Unsized` | `TestSweep_Settle/an_interrupted_expiry`, `TestSweep_Settle/a_manifest_the_listing_omits_is_still_removed`, `TestSweep_Settle/an_inventory_object_the_listing_omits_is_still_removed`, `TestSweep_Settle/a_snapshot_the_listing_shows_nothing_of_is_left_alone`, `TestSweep_Settle/an_expiry_with_no_manifest_left` | ✅ |
| Invalid tombstones, and tombstones that do not describe every remaining manifest copy, are reported and settle nothing. The head is never settled: without a pointer while snapshots exist, `Sweep` is refused, and while a commit is pending, nothing is settled. A held or rejected tombstone retains its data after the grace period | `TestSweep_RejectedTombstone/invalid_ones_settle_nothing`, `TestSweep_RejectedTombstone/same_files_with_another_commit_instant_settles_nothing`, `TestSweep_RejectedTombstone/another_inventory_settles_nothing`, `TestSweep_RejectedTombstone/one_a_partition_copy_contradicts_settles_nothing`, `TestSweep_Head/is_never_settled`, `TestSweep_Head/a_held_tombstone_keeps_its_data`, `TestSweep_RejectedTombstone/its_data_is_held_past_grace`, `TestSweep_RejectedTombstone/one_contradicting_the_head_is_reported`, `TestSweep_HeadResolution/a_pointer_missing_beside_new-form_IDs_is_refused`, `TestSweep_HeadResolution/a_pointer-less_dataset_is_refused`, `TestSweep_HeadResolution/a_pointer-less_dataset_of_orphans_only_is_swept`, `TestSweep_HeadResolution/a_commit_in_flight_settles_nothing`, `TestSweep_HeadResolution/the_pointer's_head_keeps_its_objects_when_the_listing_omits_its_manifests`, `TestSweep_Request/empty_dataset` | ✅ |
| Listed keys that are not canonical, and names under `expired/` that are not IDs (the filesystem store's `.lock` companions), are outside the scope; a completed expiry is not reported again | `TestSweep_Reclaim/non-canonical_keys_are_out_of_scope`, `TestSweep_Reclaim/a_repeated_filesystem_run_is_quiet` | ✅ |
| A dry-run `Sweep` reports what a real run would do and changes nothing | `TestSweep_Request/dry_run_reports_the_real_run` | ✅ |
| A failed `Sweep` reports the work completed | `TestSweep_Request/a_delete_failure_reports_completed_work`, `TestSweep_Request/a_listing_failure_still_reports_invalid_tombstones`, `TestSweep_Request/a_tombstone_read_failure_still_reports_earlier_invalid_ones` | ✅ |
| Enumeration omits manifests deleted after the listing | `TestEnumeration_ToleratesDeletion/listings_skip_a_manifest_deleted_after_listing`, `TestLatestScan/skips_a_manifest_deleted_after_listing`, `TestLatestScan/falls_back_to_another_copy_of_a_snapshot`, `TestLatestScan/passes_over_an_expired_leftover` | ✅ |

---

### CONTRACT_VOLUME.md — Volume Persistence

The tests reside in the package `lode/volume` (experimental;
`V1_API_DECISIONS.md` D9). Argument-error cases, including those for the
Volume retry options, are in `lode/volume/invalid_argument_test.go`.

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

These tests verify that hot-path operations use O(1) store calls or
O(log B) lookups instead of O(N) scans. A regression here would increase
elapsed time in proportion to the size of the dataset or volume on remote
stores (S3, R2).

**Dataset — Latest Pointer**: the persisted `latest` object enables O(1)
resolution.

| Requirement | Test |
|-------------|------|
| Pointer written on Write | `TestDataset_LatestPointer/read_after_write` |
| Pointer tracks most recent | `TestDataset_LatestPointer/updates_across_writes` |
| Backward compat: scan fallback | `TestDataset_LatestPointer/backward_compat` |
| Corrupt pointer: scan fallback | `TestDataset_LatestPointer/corrupt_pointer` |
| Pointer names a snapshot without a manifest: `Latest` does not replace the pointer | `TestDataset_LatestPointer_PendingCommit`, `TestLatest/stale_pointer_is_left_for_the_next_commit` |
| Pointer names a snapshot without a manifest: the next Write adopts it as parent | `TestDataset_Write_ParentsOnPendingCommit`, `TestResolve/stale_pointer_is_the_parent` |
| Parent resolution through the pointer: 1 Get, no manifest check | `TestResolve/pointer_costs_one_Get_and_no_manifest_check` |
| Pointer read fails with an error other than not-found: `Write` returns the error and does not scan | `TestDataset_TransientHeadReadErrors/Write_returns_a_failed_pointer_read`, `TestResolve/failed_pointer_read_is_returned,_not_scanned` |
| Pointer or head-manifest read fails with an error other than not-found: `Latest` returns the error and does not scan | `TestDataset_TransientHeadReadErrors/Latest_returns_a_failed_head_load`, `TestLatest/failed_pointer_read_is_returned,_not_scanned`, `TestLatest/failed_head_load_is_returned,_not_scanned` |
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

**Volume — Latest Pointer**: the same O(1) pattern for volumes.

| Requirement | Test |
|-------------|------|
| Pointer written on Commit | `TestVolume_LatestPointer/read_after_commit` |
| Pointer tracks most recent | `TestVolume_LatestPointer/multiple_commits` |
| Backward compat: scan fallback | `TestVolume_LatestPointer/backward_compat` |

**Volume — Block Operations**: sorted blocks and O(log B) lookup.

| Requirement | Test |
|-------------|------|
| Blocks sorted in manifest | `TestVolume_Commit_Blocks/blocks_sorted_in_manifest` |
| Block merge correctness | `TestMergeBlocks` (4 subtests) |
| Binary search: sorted input | `TestFindCoveringBlocks_BinarySearch` (6 subtests) |
| Sorted overlap detection | `TestVolume_ValidateNoOverlaps/sorted_input` |
| Sorted overlap rejection | `TestVolume_ValidateNoOverlaps/sorted_overlap` |
| Load-time block sorting | `TestVolume_Snapshots/load_snapshot_sorts_blocks_at_load_time` |

**Pointer Verification**: Each kind of bad pointer is handled as specified.

| Requirement | Test |
|-------------|------|
| Dataset: pointer holds no snapshot ID → scan fallback | `TestDataset_Write_Pointer/corrupt_pointer_falls_back_to_scan` |
| Volume: pointer holds no snapshot ID → scan fallback | `TestVolume_Commit_Pointer/stale_pointer_falls_back_to_scan` |
| Volume: pointer names a snapshot without a canonical manifest → the next commit loads its staged copy as parent and merges its blocks | `TestVolume_Commit_AbandonedCommit`, `TestResolveParent_Staged/an_abandoned_commit's_staged_copy_is_the_parent`, `TestResolveParent_Staged/a_staged_copy_removed_meanwhile_falls_back_to_the_canonical_manifest` |
| Volume: pointer names a snapshot with neither a canonical manifest nor a staged copy → `ErrSnapshotConflict` | `TestVolume_Commit_PendingCommit`, `TestResolveParent/pointer's_manifest_missing_is_a_conflict`, `TestResolveParent_Staged/neither_copy_is_a_conflict` |
| Volume: a failed read of a staged copy, or of the canonical manifest after the staged copy was removed, is returned | `TestResolveParent_Staged/an_unreadable_staged_copy_is_returned`, `TestResolveParent_Staged/an_unreadable_canonical_manifest_after_a_removed_staged_copy_is_returned` |
| Volume: a commit persists its staged copy before the pointer and removes it after the canonical manifest; a staging failure writes nothing else | `TestAdvance_Staged/staged_copy_precedes_the_pointer_and_is_removed_after_the_manifests`, `TestAdvance_Staged/staging_failure_is_wrapped_and_nothing_else_is_written` |
| Volume: the staged copy is retained after a manifest failure and after a pointer failure that the store applied, and is removed after a pointer update that another commit won | `TestAdvance_Staged/manifest_failure_retains_the_staged_copy`, `TestAdvance_Staged/pointer_failure_the_store_applied_retains_the_staged_copy`, `TestAdvance_Staged/lost_race_discards_the_staged_copy` |
| Dataset: existing-but-stale pointer → in-memory cache wins | `TestDataset_Write_Pointer/stale_but_existing_pointer_uses_in_memory_cache` |
| Volume: existing-but-stale pointer → in-memory cache wins | `TestVolume_Commit_Pointer/stale_but_existing_pointer_uses_in_memory_cache` |
| Dataset: only the first commit stages its manifest, and removes the staged copy after the canonical manifest | `TestWrite_StagesOnlyTheFirstCommit`, `TestAdvance_StagesOnlyAcceptedCommits` |
| A staged copy loads by snapshot ID; a kind that stages nothing reports not found | `TestLoadStaged` |

**Cross-Process and Cold-Start Pointer**: the pointer remains correct
across process restarts.

| Requirement | Test |
|-------------|------|
| Dataset: cold start reads correct pointer | `TestDataset_Write_ColdStart/reads_pointer_from_store` |
| Dataset: cold start with corrupt pointer → scan fallback | `TestDataset_Write_ColdStart/corrupt_pointer_falls_back_to_scan` |
| Dataset: pointer write failure aborts commit | `TestDataset_Write_Pointer/pointer_write_failure_aborts_commit` |
| Volume: cold start reads correct pointer | `TestVolume_Commit_ColdStart/reads_pointer_from_store` |
| Volume: cold start with corrupt pointer → scan fallback | `TestVolume_Commit_ColdStart/corrupt_pointer_falls_back_to_scan` |
| Volume: pointer write failure aborts commit | `TestVolume_Commit_Pointer/pointer_write_failure_aborts_commit` |

**Hive Canonical Manifest**: the canonical manifest is excluded from
listing.

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

### Allocation Gates

| Requirement | Test |
|-------------|------|
| Each Dataset hot path (`Write`, `Read`, `StreamWrite`, `StreamWriteRecords`, and JSON Lines decoding) stays within its ceilings on allocations and bytes per operation | `TestAllocationGates` (`lode`) |
| Each Volume hot path (`ReadAt` and `Commit`) stays within its ceilings on allocations and bytes per operation | `TestAllocationGates` (`lode/volume`) |
| The JSON Lines decoder grows its buffer to the longest line, decodes a line far longer than its initial buffer, and refuses a line beyond its limit | `TestJSONLCodec_Decode_LineLength` |

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

## Fuzzing

Fuzz targets check properties over arbitrary input rather than listed
cases. `go test` runs their seed inputs; `task fuzz` explores further (see
`scripts/fuzz.sh`).

| Property | Target |
|----------|--------|
| An accepted store key or prefix is canonical, cannot escape the root, names no reserved temporary file (keys only), and normalizes idempotently | `FuzzKey`, `FuzzPrefix` |
| An accepted snapshot ID is path-safe and round-trips through its instant and nonce | `FuzzInstant`, `FuzzFormatID` |
| A path that retention may delete is a canonical key under its dataset with no parent segment | `FuzzLayout_IsObjectOf` |
| Every path a layout builds parses back to its components | `FuzzLayout_RoundTrip` |
| An accepted partition prefix resolves beneath the dataset's partitions directory | `FuzzPartitionPrefix` |
| Tombstone validation never panics and accepts only complete records | `FuzzValidTombstone` |

---

## Chaos Harness

`internal/chaos` runs concurrent writers and readers, each with its own
`Dataset` handle, against a shared store for a fixed duration, and then
checks the history they left. It runs on demand (`task chaos`, or the Chaos
workflow) against memory, the filesystem, LocalStack, RustFS, any
S3-compatible store configured through `CHAOS_S3_*` variables, or
`lode.SingleProcess` over a memory store without CAS (`single-process`). The
invariants assume CAS (CONTRACT_WRITE_API §Concurrency), which every store
provides. A run fails if the harness cannot remove what it created on the
store.

| Invariant | Check |
|-----------|-------|
| Every commit the library acknowledged has a manifest | `acknowledged-commits-persist` |
| `Latest` returns the settling commit; no snapshot has two children (CONTRACT_WRITE_API §Concurrency: linear under CAS); the chain has no more ends than missing parents account for; and, with no missing parent, the chain from the head reaches every snapshot | `history-shape` |
| A parent without a manifest is an abandoned commit, which no writer was told had committed, or an expired snapshot | `parents-explained` |
| Every snapshot reads back its manifest's row count, and an acknowledged one its writer's rows | `contents-match-manifests` |
| During the run, the head and acknowledged snapshots read back in full | `read-head`, `read-commit` |

After the workload stops, a clean handle makes one settling commit, which
`Latest` must then return.

Every configuration also checks that the run released its resources. The
limits on retained heap allow a fixed one mebibyte and sixty-four bytes per
acknowledged commit, more than ten times the figures measured when they
were set; `TestMemoryBaseline_DetectsLeaks` confirms that both checks fail
on a leaked goroutine, including one masked by a goroutine of the baseline
that exits meanwhile or one beside an idle HTTP/1.1 or HTTP/2 connection,
and on retained heap. A response body left open on an S3 backend holds a
connection too, but it also retains memory with every request, which the
heap check reports. With `-chaos.memprofile=<dir>`
(`MEMPROFILE` for `task chaos`), each configuration also writes a heap
profile to the directory after its run.

| Invariant | Check |
|-----------|-------|
| Once the run ends and its backend is released, every goroutine started during the run has exited within five seconds, compared by goroutine identity, except, on S3 backends, those of the HTTP connections that the AWS SDK keeps open for reuse | `goroutines-released` |
| On a store that keeps no data in memory, the heap that the finished run retains stays within the limits | `heap-retained` |

With `-chaos.faults=mild` or `heavy`, every handle reaches the store
through a faulty store that delays calls, fails them before or after they
take effect (a lost response), omits keys from listings, and kills the
handle's process at a random call, possibly in the middle of a commit. A
killed writer restarts with a new handle. Faults may leave abandoned
commits (CONTRACT_CORE §Head Pointer), which `parents-explained` accepts
as missing parents and `history-shape` as one extra end of chain each;
every other invariant holds unchanged.

With `-chaos.retention`, a janitor runs `Expire` (by instant and by ID) and
`Sweep` concurrently with the writers, with a grace period longer than the
run, and the run ends with a final `Sweep`.

| Invariant | Check |
|-----------|-------|
| An acknowledged commit without a manifest reads as expired | `acknowledged-commits-persist` |
| A report never lists its retained head as expired | `head-retained` |
| Every snapshot a report listed as expired reads as expired | `expired-snapshots-read-as-expired` |
| After the final sweep, a dry-run `Sweep` finds nothing to settle, delete, or reject as unparseable | `expiries-settled` |
| Every read that failed during the run concerned a snapshot that is now expired | `failed-reads-were-expired` |

With `-chaos.volume`, each writer stages and commits blocks in its own
stripe of one Volume's address space. After the workload stops, a clean
handle commits one block in a stripe of its own. That settling commit must
succeed even after an abandoned commit, because it loads the abandoned
commit's staged manifest (CONTRACT_VOLUME §Error Semantics); a settling
commit that fails is a violation. An abandoned commit has no canonical
manifest, so it appears in the history only as a missing parent, which
`history-shape` accepts as one extra end of chain; an acknowledged commit
without a manifest violates `acknowledged-commits-persist`.

| Invariant | Check |
|-----------|-------|
| Every commit the library acknowledged has a manifest | `acknowledged-commits-persist` |
| `Latest` returns the settling commit; the history is linear and the head reaches every snapshot | `history-shape` |
| The head's cumulative manifest holds no overlapping blocks | `blocks-disjoint` |
| The head's manifest holds every acknowledged block, and each reads back its bytes | `blocks-read-back` |

---

## Open Gaps

### G3-4: Streaming Failure Semantics

The following invariants are now **tested deterministically** through
fault injection:

| Invariant | Coverage |
|-----------|----------|
| No manifest on failed stream | ✅ All failure paths |
| Cleanup (Delete) attempted on failure | ✅ All failure paths |
| Cleanup errors ignored (best-effort) | ✅ Explicit test |
| Snapshot invisibility preserved | ✅ All failure paths |

**Deterministic test infrastructure**: `internal/testutil/faultstore.go`
provides a store wrapper that injects errors, records calls, and blocks for
synchronization.

### Residual Risk: STORE-CTX-CANCEL (Addressed)

**The timing of removal after context cancellation depends on the
adapter.**

| Aspect | Status |
|--------|--------|
| Core invariant: no manifest on cancel | ✅ Deterministically tested with blocked Put |
| Cleanup attempt on cancel | ✅ Deterministically tested with blocked Put |
| Actual cleanup completion | ⚠️ Timing-dependent (adapter-specific) |
| FS adapter timing | ✅ Integration tests in `lode/adapter_timing_test.go` |
| S3 adapter timing | ✅ Integration tests in `lode/s3/integration_test.go` (gated) |

**Why the timing matters:**
- In-memory stores complete synchronously before cancellation takes
  effect.
- Real adapters (S3, filesystem) have intervals of varying length.
- Whether the removing `Delete` runs before or after the context deadline
  is nondeterministic.

**Mitigation:**
- The core invariants are tested deterministically through `TestStreamWrite_PutFailure/blocked_put_context_cancel_no_manifest`
- The filesystem adapter's timing is verified through `TestFSAdapter_ContextCancel/stream_write_context_cancel_no_snapshot`
- The S3 adapter's timing is verified through `TestIntegration_Writes/stream_write_context_cancel` (requires `go test ./lode/s3/... -integration` against the docker-compose services)
- Removal is documented as best-effort in CONTRACT_ERRORS.md.

**Status**: the core invariants are covered. Timing tests specific to each
adapter exercise real behavior without asserting on outcomes that are
inherently nondeterministic.

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
ok  	github.com/pithecene/lode/lode
ok  	github.com/pithecene/lode/lode/s3
```

Every contract has test coverage, apart from the deferred gaps and the
documented residual risk.
