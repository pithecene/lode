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
| SchemaName | `TestDatasetReader_GetManifest_InvalidManifest_MissingSchemaName` |
| FormatVersion | `TestDatasetReader_GetManifest_InvalidManifest_MissingFormatVersion` |
| DatasetID | `TestDatasetReader_GetManifest_InvalidManifest_MissingDatasetID` |
| SnapshotID | `TestDatasetReader_GetManifest_InvalidManifest_MissingSnapshotID` |
| CreatedAt | `TestDatasetReader_GetManifest_InvalidManifest_ZeroCreatedAt` |
| Metadata (non-nil) | `TestDatasetReader_GetManifest_InvalidManifest_NilMetadata` |
| Files (non-nil) | `TestDatasetReader_GetManifest_InvalidManifest_NilFiles` |
| RowCount (≥0) | `TestDatasetReader_GetManifest_InvalidManifest_NegativeRowCount` |
| Compressor | `TestDatasetReader_GetManifest_InvalidManifest_MissingCompressor` |
| Partitioner | `TestDatasetReader_GetManifest_InvalidManifest_MissingPartitioner` |

**Manifest Optional Fields**: All covered ✅

| Field | Test |
|-------|------|
| Codec | Raw blob tests (codec omitted) |
| ParentSnapshotID | First snapshot tests (no parent) |
| MinTimestamp/MaxTimestamp | `TestDataset_Write_NonTimestampedRecords_OmitsMinMax` |
| Checksum | `TestDataset_Write_WithoutChecksum_OmitsChecksum` |
| FileRef.Stats (present) | `TestDataset_Write_ParquetCodec_StatsPopulated` |
| FileRef.Stats (absent) | `TestDataset_Write_JSONLCodec_StatsNil`, `TestDataset_Write_RawBlob_StatsNil` |
| FileRef.Stats (JSON round-trip) | `TestFileRef_Stats_JSONRoundTrip`, `TestFileRef_Stats_BackwardCompat`, `TestFileRef_Stats_OmittedWhenNil` |

**Metadata Rules**: All covered ✅

- nil metadata coalesced: `TestDataset_Write_NilMetadata_CoalescesToEmpty`, `TestDataset_StreamWrite_NilMetadata_CoalescesToEmpty`, `TestDataset_StreamWriteRecords_NilMetadata_CoalescesToEmpty`
- Empty metadata valid: `TestDataset_Write_EmptyMetadata_ValidAndPersisted`, etc.

**Immutability**: All covered ✅

- Data files immutable: `TestFSStore_Put_ErrPathExists`, `TestMemoryStore_Put_ErrPathExists`, `TestStore_Put_ErrPathExists`
- Linear history: `TestDataset_StreamWrite_ParentSnapshotLinked`, `TestDataset_StreamWriteRecords_ParentSnapshotLinked`

---

### CONTRACT_WRITE_API.md — Write API

**Dataset.Write**: All covered ✅

| Requirement | Test |
|-------------|------|
| Creates snapshot | Multiple write tests |
| nil metadata coalesced | `TestDataset_Write_NilMetadata_CoalescesToEmpty` |
| Parent snapshot linked | `TestDataset_StreamWrite_ParentSnapshotLinked` |
| Parent ID resolved O(1) via persistent pointer | `TestDataset_Write_LatestPointer_SkipsScan` |
| Raw blob RowCount=1 | `TestDataset_StreamWrite_Success` |

**StreamWrite**: All covered ✅

| Requirement | Test |
|-------------|------|
| nil metadata coalesced | `TestDataset_StreamWrite_NilMetadata_CoalescesToEmpty` |
| Commit writes manifest | `TestDataset_StreamWrite_Success` |
| Snapshot invisible before Commit | `TestDataset_StreamWrite_NotVisibleBeforeCommit` |
| Abort → no manifest | `TestDataset_StreamWrite_Abort_NoManifest` |
| Close without Commit → abort | `TestDataset_StreamWrite_CloseWithoutCommit_BehavesAsAbort` |
| Codec configured → error | `TestDataset_StreamWrite_WithCodec_ReturnsError` |
| Parent ID resolved O(1) via persistent pointer | `TestDataset_StreamWrite_LatestPointer_SkipsScan` |
| Checksum computed | `TestDataset_StreamWrite_WithChecksum_RecordsChecksum` |
| Manifest Put error → no manifest + cleanup | `TestStreamWrite_ManifestPutError_NoManifest_CleanupAttempted` |
| Abort → cleanup attempted | `TestStreamWrite_Abort_NoManifest_CleanupAttempted` |
| Close → cleanup attempted | `TestStreamWrite_CloseWithoutCommit_NoManifest_CleanupAttempted` |
| Cleanup errors ignored | `TestStreamWrite_CleanupErrorIgnored` |
| Fast Put failure → error, no block | `TestStreamWrite_DataPutFailsFast_ReturnsError` |
| Blocked Put + cancel → no manifest | `TestStreamWrite_BlockedPut_ContextCancel_NoManifest` |

**StreamWriteRecords**: All covered ✅

| Requirement | Test |
|-------------|------|
| nil metadata coalesced | `TestDataset_StreamWriteRecords_NilMetadata_CoalescesToEmpty` |
| nil iterator error | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| Non-streaming codec error | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |
| Partitioning error | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |
| Iterator error → no manifest | `TestDataset_StreamWriteRecords_IteratorError` |
| RowCount = records consumed | `TestDataset_StreamWriteRecords_Success` |
| Iterator error → cleanup attempted | `TestStreamWriteRecords_IteratorError_NoManifest_CleanupAttempted` |
| Parent ID resolved O(1) via persistent pointer | `TestDataset_StreamWriteRecords_LatestPointer_SkipsScan` |
| Manifest Put error → no manifest + cleanup | `TestStreamWriteRecords_ManifestPutError_NoManifest` |
| Fast Put failure → error, no block | `TestStreamWriteRecords_DataPutFailsFast_ReturnsError` |

**Timestamp Computation**: All covered ✅

| Requirement | Test |
|-------------|------|
| Timestamped interface | `TestDataset_Write_TimestampedRecords_ComputesMinMax` |
| Non-timestamped omits | `TestDataset_Write_NonTimestampedRecords_OmitsMinMax` |
| Raw blob omits | `TestDataset_Write_RawBlob_OmitsTimestamps` |

**Per-File Statistics**: All covered ✅

| Requirement | Test |
|-------------|------|
| StatisticalCodec populates stats | `TestDataset_Write_ParquetCodec_StatsPopulated` |
| Non-statistical codec → nil stats | `TestDataset_Write_JSONLCodec_StatsNil` |
| Raw blob → nil stats | `TestDataset_Write_RawBlob_StatsNil` |
| StreamWriteRecords → nil stats (JSONL) | `TestDataset_StreamWriteRecords_StatsNil` |
| Parquet basic types stats | `TestParquetCodec_FileStats_BasicTypes` |
| Parquet nullable field stats | `TestParquetCodec_FileStats_NullableFields` |
| Parquet all-null column stats | `TestParquetCodec_FileStats_AllNulls` |
| Parquet single record stats | `TestParquetCodec_FileStats_SingleRecord` |
| Parquet bool/bytes no min/max | `TestParquetCodec_FileStats_BoolAndBytes_NoMinMax` |
| Parquet timestamp stats | `TestParquetCodec_FileStats_Timestamps` |
| Parquet empty records stats | `TestParquetCodec_FileStats_EmptyRecords` |

**Empty Dataset**: All covered ✅

| Requirement | Test |
|-------------|------|
| Latest() → ErrNoSnapshots | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| Snapshots() → empty | `TestDataset_Snapshots_EmptyDataset_ReturnsEmptyList` |
| Snapshot(id) → ErrNotFound | `TestDataset_Snapshot_EmptyDataset_ReturnsErrNotFound` |

---

### CONTRACT_STORAGE.md — Storage Adapter

**Put**: All covered ✅

| Requirement | Test |
|-------------|------|
| ErrPathExists on duplicate (atomic) | `TestStore_Put_Atomic_Duplicate_ReturnsErrPathExists` |
| All-or-nothing on reader failure | `TestFSStore_Put_AllOrNothing`, `TestMemoryStore_Put_AllOrNothing`, `TestStore_Put_AllOrNothing` |
| Multipart preflight check | `TestStore_PutMultipartFromFile_PreExisting_ReturnsErrPathExists` |
| Multipart conditional completion | `TestStore_PutMultipartFromFile_ConditionalCompletion_ReturnsErrPathExists` |
| Conditional completion documented | `TestStore_Multipart_ConditionalCompletion` |
| Temp file cleanup | `TestStore_Put_TempFileCleanup_OnSuccess`, `TestStore_Put_TempFileCleanup_OnFailure` |

**Get/Exists/List/Delete**: All covered ✅

| Operation | Tests |
|-----------|-------|
| Get | `TestStore_Get_Success`, `TestStore_Get_ErrNotFound` |
| Exists | `TestStore_Exists_True`, `TestStore_Exists_False` |
| List | `TestStore_List_WithPrefix`, `TestStore_List_Empty` |
| Delete | `TestStore_Delete_Exists`, `TestStore_Delete_NotExists_Idempotent` |

**ReadRange**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| Basic range read | `TestStore_ReadRange_Basic` (FS, Memory, S3) |
| ErrNotFound | `TestStore_ReadRange_NotFound` |
| Invalid params | `TestStore_ReadRange_NegativeOffset`, `TestStore_ReadRange_NegativeLength`, etc. |
| Beyond EOF | `TestStore_ReadRange_BeyondEOF`, `TestStore_ReadRange_OffsetBeyondEOF` |
| Exact bounds; allocation bounded by available bytes | `TestFSStore_ReadRange_Bounds`, `TestMemoryStore_ReadRange_Bounds` |

**ReaderAt**: All covered ✅

| Requirement | Tests |
|-------------|-------|
| Basic | `TestStore_ReaderAt_Basic` |
| Concurrent reads | `TestStore_ReaderAt_ConcurrentReads` |
| ErrNotFound | `TestStore_ReaderAt_NotFound` |

---

### CONTRACT_LAYOUT.md — Layout & Components

All covered ✅

| Requirement | Test |
|-------------|------|
| nil layout rejected | `TestNewDataset_NilLayout_ReturnsError`, `TestNewDatasetReader_NilLayout_ReturnsError` |
| nil compressor rejected | `TestNewDataset_NilCompressor_ReturnsError` |
| Raw blob + partitioner rejected | `TestNewDataset_RawBlobWithPartitioner_ReturnsError` |
| Hive layout keys | `TestNewHiveLayout_WithKeys_Success` |

**Compression**: All covered ✅

| Requirement | Test |
|-------------|------|
| Gzip round-trip | `TestDataset_StreamWrite_WithGzipCompression` |
| Zstd round-trip (Write) | `TestDataset_Write_WithZstdCompression` |
| Zstd round-trip (StreamWrite) | `TestDataset_StreamWrite_WithZstdCompression` |
| Zstd round-trip (StreamWriteRecords) | `TestDataset_StreamWriteRecords_WithZstdCompression` |
| Compressor mismatch error | `TestDataset_Read_CompressorMismatch_ReturnsError` |

---

### CONTRACT_READ_API.md — Read API

All covered ✅

| Requirement | Test |
|-------------|------|
| ListDatasets layout error | `TestDatasetReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled` |
| ListDatasets empty storage | `TestDatasetReader_ListDatasets_EmptyStorage` |
| ListDatasets order / StartAfter | `TestDatasetReader_ListDatasets_Sorted`, `TestDatasetReader_ListDatasets_StartAfter_Pages`, `TestDatasetReader_ListDatasets_Limit_Boundary` |
| ErrNoManifests | `TestDatasetReader_ListDatasets_ErrNoManifests` |
| Manifest validation errors | Multiple `TestDatasetReader_GetManifest_InvalidManifest_*` tests |
| ListPartitions lexicographic order | `TestDatasetReader_ListPartitions_Sorted` |
| ListPartitions Prefix (raw prefix, subtree) | `TestDatasetReader_ListPartitions_Prefix`, `TestDatasetReader_ListPartitions_Prefix_ExcludesOtherPartitionsOfSameSnapshot` |
| ListPartitions Prefix empty subtree | `TestDatasetReader_ListPartitions_Prefix_NoMatch_ReturnsEmpty` |
| ListPartitions Prefix canonical-only (no dataset escape) | `TestDatasetReader_ListPartitions_Prefix_RejectsNonCanonical` |
| ListPartitions StartAfter / Limit | `TestDatasetReader_ListPartitions_StartAfter_Pages`, `TestDatasetReader_ListPartitions_StartAfter_Exclusive` |
| ListManifests (Partition, ID) order, deterministic partition | `TestDatasetReader_ListManifests_SortedAndDeterministicPartition`, `TestDatasetReader_ListManifests_DefaultLayout_SortedByID` |
| ListManifests StartAfter / Limit | `TestDatasetReader_ListManifests_StartAfter_Pages`, `TestDatasetReader_ListManifests_Limit_Boundary` |

---

### CONTRACT_ERRORS.md — Error Taxonomy

All error sentinels covered ✅

| Sentinel | Test |
|----------|------|
| ErrNotFound | Multiple tests |
| ErrNoSnapshots | `TestDataset_Latest_EmptyDataset_ReturnsErrNoSnapshots` |
| ErrNoManifests | `TestDatasetReader_ListDatasets_ErrNoManifests` |
| ErrPathExists | Multiple store tests |
| ErrInvalidPath | Multiple store tests |
| ErrDatasetsNotModeled | `TestDatasetReader_ListDatasets_FlatLayout_ReturnsErrDatasetsNotModeled` |
| ErrPartitionPrefixNotSupported | `TestDatasetReader_ListPartitions_Prefix_DefaultLayout_Rejected` |
| ErrManifestInvalid | Multiple validation tests |
| ErrInvalidArgument | `TestErrInvalidArgument_Classification` |
| ErrOptionNotValidFor{Dataset,DatasetReader,Volume} | `TestSentinels_Messages`, option applicability tests |
| ErrCodecConfigured | `TestDataset_StreamWrite_WithCodec_ReturnsError` |
| ErrCodecNotStreamable | `TestDataset_StreamWriteRecords_NonStreamingCodec_ReturnsError` |
| ErrNilIterator | `TestDataset_StreamWriteRecords_NilIterator_ReturnsError` |
| ErrPartitioningNotSupported | `TestDataset_StreamWriteRecords_WithPartitioner_ReturnsError` |
| ErrRangeMissing | `TestVolume_ReadAt_MissingRange_ReturnsErrRangeMissing`, `TestVolume_ReadAt_GapAtStart_ReturnsErrRangeMissing`, `TestVolume_ReadAt_GapInMiddle_ReturnsErrRangeMissing` |
| ErrOverlappingBlocks | `TestVolume_Commit_OverlappingBlocks_ReturnsErrOverlappingBlocks`, `TestVolume_Commit_ContainedBlock_Overlap`, `TestVolume_Commit_SameStartOffset_Overlap`, `TestVolume_Commit_OverlapWithExisting_Rejected`, `TestVolume_Commit_ThreeBlockOverlap` |
| ErrSnapshotExpired | `TestExpire_ByID_RemovesManifestAndData`, `TestExpire_InterruptedAfterCanonicalDelete_IsNotResurrected` |
| ErrHeadUncommitted | `TestExpire_UncommittedHead_ExpiresNothing`, `TestExpire_UncommittedHeadMidRun_ReportsCompletedWork` |

---

### CONTRACT_RETENTION.md — Snapshot Expiry

| Requirement | Test | Status |
|-------------|------|--------|
| Empty request, malformed or unsupported `Partition` rejected before any store call | `TestExpire_Validation` | ✅ |
| Expiry removes manifests and data, keeps a tombstone; `Snapshot`/`Read` return `ErrSnapshotExpired` | `TestExpire_ByID_RemovesManifestAndData`, `TestExpire_FlatLayout` | ✅ |
| `Partition` matches whole segments | `TestExpire_Partition_MatchesWholeSegments`, `TestExpire_TrailingSlashPartitionIsValid` | ✅ |
| Mixed snapshots kept under `Partition`, expirable by `IDs` | `TestExpire_Partition_MixedSnapshotIsKept` | ✅ |
| `Before` selects by snapshot ID instant; selectors intersect | `TestExpire_Before_UsesSnapshotIDInstant`, `TestExpire_IntersectsSelectors` | ✅ |
| Head retained and reported, no tombstone written for it; a head superseded mid-run stays retained | `TestExpire_HeadIsRetainedAndReported`, `TestExpire_NoPointer_ProtectsScannedHead`, `TestExpire_HeadSupersededMidRun_StaysRetained` | ✅ |
| Uncommitted head stops expiry; report lists completed work | `TestExpire_UncommittedHead_ExpiresNothing`, `TestExpire_UncommittedHeadMidRun_ReportsCompletedWork` | ✅ |
| Dry run writes and deletes nothing, and fails where a real run would | `TestExpire_DryRun_ChangesNothing`, `TestExpire_DryRun_FailsWhereARealRunWould` | ✅ |
| Existing tombstone: same ID resumes, other ID fails (CAS and non-CAS); tombstone records the whole `Files[]` | `TestExpire_ExistingTombstone`, `TestExpire_TombstoneRecordsWholeManifest` | ✅ |
| Expired snapshot not resurrected by the scan fallback; rerun completes | `TestExpire_InterruptedAfterCanonicalDelete_IsNotResurrected` | ✅ |
| Foreign `Files[]` paths reported in `Skipped`, never deleted | `TestExpire_ForeignFileRef_IsSkipped` | ✅ |
| Enumeration skips manifests deleted after listing | `TestEnumeration_SkipsManifestDeletedAfterListing`, `TestLatestScan_SkipsManifestDeletedAfterListing` | ✅ |

---

### CONTRACT_VOLUME.md — Volume Persistence

**Construction**: All covered ✅

| Requirement | Test |
|-------------|------|
| Valid construction | `TestNewVolume_Success` |
| nil factory rejected | `TestNewVolume_NilFactory_ReturnsError` |
| Zero length rejected | `TestNewVolume_ZeroTotalLength_ReturnsError` |
| nil store rejected | `TestNewVolume_NilStore_ReturnsError` |
| Empty ID rejected | `TestNewVolume_EmptyID_ReturnsError` |

**StageWriteAt**: All covered ✅

| Requirement | Test |
|-------------|------|
| Negative offset rejected | `TestVolume_StageWriteAt_NegativeOffset_ReturnsError` |
| Exceeds bounds rejected | `TestVolume_StageWriteAt_ExceedsBounds_ReturnsError` |
| Empty reader rejected | `TestVolume_StageWriteAt_EmptyReader_ReturnsError` |
| Exact fit accepted | `TestVolume_StageWriteAt_ExactFit` |
| Path layout correct | `TestVolume_StageWriteAt_PathLayout` |
| Checksum computed | `TestVolume_StageWriteAt_WithChecksum` |

**Commit**: All covered ✅

| Requirement | Test |
|-------------|------|
| End-to-end round-trip | `TestVolume_StageCommitReadAt_EndToEnd` |
| Cumulative manifest | `TestVolume_CumulativeManifest` |
| nil metadata coalesced | `TestVolume_Commit_NilMetadata_CoalescesToEmpty` |
| Empty metadata accepted | `TestVolume_Commit_EmptyMetadata_Succeeds` |
| Empty blocks rejected | `TestVolume_Commit_EmptyBlocks_ReturnsError` |
| Empty block path rejected | `TestVolume_Commit_EmptyBlockPath_ReturnsError` |
| Duplicate blocks rejected | `TestVolume_Commit_DuplicateBlocks_ReturnsError` |
| Block exceeds bounds rejected | `TestVolume_Commit_BlockExceedsBounds_ReturnsError` |
| Negative block offset rejected | `TestVolume_Commit_NegativeBlockOffset_ReturnsError` |
| Zero block length rejected | `TestVolume_Commit_ZeroBlockLength_ReturnsError` |
| Path mismatch rejected | `TestVolume_Commit_PathMismatch_ReturnsError` |
| Three-snapshot progression | `TestVolume_ThreeSnapshots_CumulativeProgression` |
| Checksum persisted | `TestVolume_Commit_WithChecksum_AlgorithmPersisted` |
| No checksum → no algorithm | `TestVolume_Commit_WithoutChecksum_NoAlgorithm` |
| Manifest round-trip | `TestVolume_ManifestRoundTrip_AllFieldsPreserved` |
| Manifest Put error → no snapshot | `TestVolume_Commit_ManifestPutError_NoSnapshot` |

**Overlap Detection**: All covered ✅

| Requirement | Test |
|-------------|------|
| Overlapping blocks rejected | `TestVolume_Commit_OverlappingBlocks_ReturnsErrOverlappingBlocks` |
| Adjacent blocks valid | `TestVolume_Commit_AdjacentBlocks_Valid` |
| Contained block overlap | `TestVolume_Commit_ContainedBlock_Overlap` |
| Same start offset overlap | `TestVolume_Commit_SameStartOffset_Overlap` |
| Overlap with existing rejected | `TestVolume_Commit_OverlapWithExisting_Rejected` |
| Three-block overlap | `TestVolume_Commit_ThreeBlockOverlap` |

**ReadAt**: All covered ✅

| Requirement | Test |
|-------------|------|
| Spanning blocks | `TestVolume_ReadAt_SpanningBlocks` |
| Missing range → ErrRangeMissing | `TestVolume_ReadAt_MissingRange_ReturnsErrRangeMissing` |
| Exact block boundary | `TestVolume_ReadAt_ExactBlockBoundary` |
| Within single block | `TestVolume_ReadAt_WithinSingleBlock` |
| Full volume | `TestVolume_ReadAt_FullVolume` |
| Gap at start → ErrRangeMissing | `TestVolume_ReadAt_GapAtStart_ReturnsErrRangeMissing` |
| Gap in middle → ErrRangeMissing | `TestVolume_ReadAt_GapInMiddle_ReturnsErrRangeMissing` |
| Negative offset rejected | `TestVolume_ReadAt_NegativeOffset_ReturnsError` |
| Zero length rejected | `TestVolume_ReadAt_ZeroLength_ReturnsError` |
| Sparse blocks with gaps | `TestVolume_SparseBlocks_WithGaps` |
| Dense packing adjacent | `TestVolume_DensePacking_Adjacent` |

**Snapshot Queries**: All covered ✅

| Requirement | Test |
|-------------|------|
| Latest empty → ErrNoSnapshots | `TestVolume_Latest_EmptyVolume_ReturnsErrNoSnapshots` |
| Snapshot not found → ErrNotFound | `TestVolume_Snapshot_NotFound_ReturnsErrNotFound` |
| Snapshots sorted by created_at | `TestVolume_Snapshots_SortedByCreatedAt` |
| ID accessor | `TestVolume_ID_ReturnsVolumeID` |

**Manifest Validation**: All covered ✅

| Requirement | Test |
|-------------|------|
| Missing schema_name | `TestVolume_Snapshot_InvalidManifest_MissingSchemaName` |
| Missing volume_id | `TestVolume_Snapshot_InvalidManifest_MissingVolumeID` |
| Missing snapshot_id | `TestVolume_Snapshot_InvalidManifest_MissingSnapshotID` |
| Zero created_at | `TestVolume_Snapshot_InvalidManifest_ZeroCreatedAt` |
| nil metadata | `TestVolume_Snapshot_InvalidManifest_NilMetadata` |
| nil blocks | `TestVolume_Snapshot_InvalidManifest_NilBlocks` |
| Negative total_length | `TestVolume_Snapshot_InvalidManifest_NegativeTotalLength` |
| Volume ID mismatch | `TestVolume_Snapshot_VolumeIDMismatch_ReturnsError` |
| Total length mismatch | `TestVolume_Snapshot_TotalLengthMismatch_ReturnsError` |

**Resume**: All covered ✅

| Requirement | Test |
|-------------|------|
| New instance loads latest | `TestVolume_Resume_NewInstance_LoadsLatest` |
| Continue staging after resume | `TestVolume_Resume_ContinueStaging` |

**FS Integration**: All covered ✅

| Requirement | Test |
|-------------|------|
| FS store round-trip | `TestVolume_FSStore_StageCommitReadAt` |

---

## Performance: O(1) Resolution and Complexity Guarantees

These tests verify that hot-path operations use O(1) store calls or O(log B) lookups
instead of O(N) full scans. Regressions here would cause wall-clock degradation
proportional to dataset/volume size on remote stores (S3/R2).

**Dataset — Latest Pointer**: Persistent `latest` file enables O(1) resolution.

| Requirement | Test |
|-------------|------|
| Pointer written on Write | `TestDataset_LatestPointer_ReadAfterWrite` |
| Pointer tracks most recent | `TestDataset_LatestPointer_UpdatesAcrossWrites` |
| Backward compat: scan fallback | `TestDataset_LatestPointer_BackwardCompat` |
| Corrupt pointer: scan fallback | `TestDataset_LatestPointer_CorruptPointer` |
| Pointer paths for all layouts | `TestDataset_LatestPointer_AllLayouts` |
| Write: 0 List calls after pointer | `TestDataset_Write_LatestPointer_SkipsScan` |
| StreamWrite: 0 List calls | `TestDataset_StreamWrite_LatestPointer_SkipsScan` |
| StreamWriteRecords: 0 List calls | `TestDataset_StreamWriteRecords_LatestPointer_SkipsScan` |

**Dataset — HiveLayout Canonical Manifest**: O(1) `Snapshot(ctx, id)`.

| Requirement | Test |
|-------------|------|
| Snapshot by ID: 0 List calls | `TestDataset_HiveLayout_SnapshotByID_NoListCall` |
| Canonical manifest written | `TestDataset_HiveLayout_CanonicalManifestWritten` |
| Backward compat: scan fallback | `TestDataset_HiveLayout_BackwardCompat_FallbackScan` |

**Volume — Latest Pointer**: Same O(1) pattern for volumes.

| Requirement | Test |
|-------------|------|
| Pointer written on Commit | `TestVolume_LatestPointer_ReadAfterCommit` |
| Pointer tracks most recent | `TestVolume_LatestPointer_MultipleCommits` |
| Backward compat: scan fallback | `TestVolume_LatestPointer_BackwardCompat` |

**Volume — Block Operations**: Sorted blocks and O(log B) lookup.

| Requirement | Test |
|-------------|------|
| Blocks sorted in manifest | `TestVolume_Commit_BlocksSortedInManifest` |
| Block merge correctness | `TestMergeBlocks` (4 subtests) |
| Binary search: sorted input | `TestFindCoveringBlocks_BinarySearch` (6 subtests) |
| Sorted overlap detection | `TestVolume_ValidateNoOverlaps_SortedInput` |
| Sorted overlap rejection | `TestVolume_ValidateNoOverlaps_SortedOverlap` |
| Load-time block sorting | `TestVolume_LoadSnapshot_SortsBlocksAtLoadTime` |

**Pointer Verification**: Corrupt/stale pointers fall back correctly.

| Requirement | Test |
|-------------|------|
| Dataset: nonexistent pointer target → scan fallback | `TestDataset_Write_CorruptPointer_FallsBackToScan` |
| Volume: nonexistent pointer target → scan fallback | `TestVolume_Commit_StalePointer_FallsBackToScan` |
| Dataset: existing-but-stale pointer → in-memory cache wins | `TestDataset_Write_StaleButExistingPointer_UsesInMemoryCache` |
| Volume: existing-but-stale pointer → in-memory cache wins | `TestVolume_Commit_StaleButExistingPointer_UsesInMemoryCache` |

**Cross-Process / Cold-Start Pointer**: Pointer correctness across process restarts.

| Requirement | Test |
|-------------|------|
| Dataset: cold start reads correct pointer | `TestDataset_Write_ColdStart_ReadsPointerFromStore` |
| Dataset: cold start with corrupt pointer → scan fallback | `TestDataset_Write_ColdStart_CorruptPointer_FallsBackToScan` |
| Dataset: pointer write failure aborts commit | `TestDataset_Write_PointerWriteFailure_AbortsCommit` |
| Volume: cold start reads correct pointer | `TestVolume_Commit_ColdStart_ReadsPointerFromStore` |
| Volume: cold start with corrupt pointer → scan fallback | `TestVolume_Commit_ColdStart_CorruptPointer_FallsBackToScan` |
| Volume: pointer write failure aborts commit | `TestVolume_Commit_PointerWriteFailure_AbortsCommit` |

**Hive Canonical Manifest**: Canonical manifest excluded from listing.

| Requirement | Test |
|-------------|------|
| ListManifests skips canonical for partition layouts | `TestDatasetReader_ListManifests_HiveLayout_SkipsCanonicalManifest` |

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
| CX-1 | `findCoveringBlocks` sort check | Fixed: sort at load time | `TestVolume_LoadSnapshot_SortsBlocksAtLoadTime` |
| CX-2 | `ListManifests` validation Gets | Reclassified: inherent cost (CONTRACT_ERRORS.md) | — |
| CX-3 | `ListPartitions` double deser | Fixed: single-pass (1 List + M Gets) | `TestDatasetReader_ListPartitions_SingleManifestLoad` |
| CX-4 | `Snapshot(id)` degraded fallback | Reclassified: documented degraded path | — |
| CX-5 | `Write` partitioned memory | Reclassified: O(R aliases), not O(3R copies) | — |
| CX-6 | `Volume.Snapshots` sort checks | Fixed: sort at load time | `TestVolume_LoadSnapshot_SortsBlocksAtLoadTime` |
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
- Core invariants are tested deterministically via `TestStreamWrite_BlockedPut_ContextCancel_NoManifest`
- FS adapter timing verified via `TestFSAdapter_StreamWrite_ContextCancel_NoSnapshot`
- S3 adapter timing verified via `TestLocalStack_StreamWrite_ContextCancel_NoSnapshot` (requires `LODE_S3_TESTS=1`)
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
