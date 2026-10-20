package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
)

const (
	manifestSchemaName    = "lode-manifest"
	manifestFormatVersion = "1.0.0"
)

// -----------------------------------------------------------------------------
// Dataset Configuration
// -----------------------------------------------------------------------------

// datasetConfig holds the resolved configuration for a dataset.
type datasetConfig struct {
	layout     layout
	compressor Compressor
	codec      Codec
	checksum   Checksum
	retry      retryConfig
}

// Option configures dataset, reader, or volume construction.
// Options implement methods for the constructors they support.
// Using an option with an unsupported constructor returns an error.
type Option interface {
	applyDataset(*datasetConfig) error
	applyReader(*readerConfig) error
	applyVolume(*volumeConfig) error
}

// layoutOption implements Option for WithLayout.
type layoutOption struct {
	layout layout
}

// WithLayout sets the layout for the dataset or reader.
// Layout configures both path topology AND partitioning.
// Default: NewDefaultLayout() (flat, no partitions).
func WithLayout(l layout) Option {
	return &layoutOption{layout: l}
}

func (o *layoutOption) applyDataset(cfg *datasetConfig) error {
	cfg.layout = o.layout
	return nil
}

func (o *layoutOption) applyReader(cfg *readerConfig) error {
	cfg.layout = o.layout
	return nil
}

func (o *layoutOption) applyVolume(*volumeConfig) error {
	return fmt.Errorf("lode: WithLayout: %w", ErrOptionNotValidForVolume)
}

// hiveLayoutOption implements Option for WithHiveLayout.
type hiveLayoutOption struct {
	keys []string
}

// WithHiveLayout creates a Hive (partition-first) layout option with the specified partition keys.
//
// This is the preferred way to configure Hive layout for fluent callsites.
// At least one partition key is required; validation occurs when the option is applied.
//
// Example:
//
//	ds, err := lode.NewDataset("events", factory,
//	    lode.WithHiveLayout("day", "region"),
//	    lode.WithCodec(lode.NewJSONLCodec()),
//	)
func WithHiveLayout(keys ...string) Option {
	return &hiveLayoutOption{keys: keys}
}

func (o *hiveLayoutOption) applyDataset(cfg *datasetConfig) error {
	l, err := NewHiveLayout(o.keys...)
	if err != nil {
		return err
	}
	cfg.layout = l
	return nil
}

func (o *hiveLayoutOption) applyReader(cfg *readerConfig) error {
	l, err := NewHiveLayout(o.keys...)
	if err != nil {
		return err
	}
	cfg.layout = l
	return nil
}

func (o *hiveLayoutOption) applyVolume(*volumeConfig) error {
	return fmt.Errorf("lode: WithHiveLayout: %w", ErrOptionNotValidForVolume)
}

// compressorOption implements Option for WithCompressor (dataset-only).
type compressorOption struct {
	compressor Compressor
}

// WithCompressor sets the compressor for the dataset.
// Default: NewNoOpCompressor().
// This option is only valid for NewDataset.
func WithCompressor(c Compressor) Option {
	return &compressorOption{compressor: c}
}

func (o *compressorOption) applyDataset(cfg *datasetConfig) error {
	cfg.compressor = o.compressor
	return nil
}

func (o *compressorOption) applyReader(*readerConfig) error {
	return fmt.Errorf("lode: WithCompressor: %w", ErrOptionNotValidForDatasetReader)
}

func (o *compressorOption) applyVolume(*volumeConfig) error {
	return fmt.Errorf("lode: WithCompressor: %w", ErrOptionNotValidForVolume)
}

// codecOption implements Option for WithCodec (dataset-only).
type codecOption struct {
	codec Codec
}

// WithCodec sets the codec for the dataset.
// Default: none (raw blob storage).
// This option is only valid for NewDataset.
//
// When a codec is set, Write expects structured records that will be
// serialized using the codec. When nil (default), Write expects a single
// []byte element and stores it as a raw blob.
func WithCodec(c Codec) Option {
	return &codecOption{codec: c}
}

func (o *codecOption) applyDataset(cfg *datasetConfig) error {
	cfg.codec = o.codec
	return nil
}

func (o *codecOption) applyReader(*readerConfig) error {
	return fmt.Errorf("lode: WithCodec: %w", ErrOptionNotValidForDatasetReader)
}

func (o *codecOption) applyVolume(*volumeConfig) error {
	return fmt.Errorf("lode: WithCodec: %w", ErrOptionNotValidForVolume)
}

// checksumOption implements Option for WithChecksum (dataset-only).
type checksumOption struct {
	checksum Checksum
}

// WithChecksum sets the checksum algorithm for the dataset.
// Default: none (no checksums).
// This option is only valid for NewDataset.
//
// When a checksum is configured, checksums are computed during write
// and recorded in the manifest for each data file.
func WithChecksum(c Checksum) Option {
	return &checksumOption{checksum: c}
}

func (o *checksumOption) applyDataset(cfg *datasetConfig) error {
	cfg.checksum = o.checksum
	return nil
}

func (o *checksumOption) applyReader(*readerConfig) error {
	return fmt.Errorf("lode: WithChecksum: %w", ErrOptionNotValidForDatasetReader)
}

func (o *checksumOption) applyVolume(cfg *volumeConfig) error {
	cfg.checksum = o.checksum
	return nil
}

// retryOption implements Option for all WithRetry* constructors.
// Retry options apply identically to datasets and volumes (both have a
// retryConfig), and are never valid for readers.
type retryOption struct {
	name  string
	apply func(*retryConfig) error
}

func (o *retryOption) applyDataset(cfg *datasetConfig) error { return o.apply(&cfg.retry) }
func (o *retryOption) applyVolume(cfg *volumeConfig) error   { return o.apply(&cfg.retry) }

func (o *retryOption) applyReader(*readerConfig) error {
	return fmt.Errorf("lode: %s: %w", o.name, ErrOptionNotValidForDatasetReader)
}

// WithRetryCount enables automatic CAS retry on snapshot conflict.
// When n > 0, the commit path retries up to n times on ErrSnapshotConflict,
// refreshing the parent state between attempts.
// Default: 0 (no retry; ErrSnapshotConflict returned immediately).
func WithRetryCount(n int) Option {
	return &retryOption{
		name: "WithRetryCount",
		apply: func(r *retryConfig) error {
			if n < 0 {
				return invalidArgument("WithRetryCount: n must be non-negative")
			}
			r.maxAttempts = n
			return nil
		},
	}
}

// WithRetryBaseDelay sets the base delay for exponential backoff on CAS retry.
// The actual delay doubles with each attempt and is jittered.
// Default: 10ms.
func WithRetryBaseDelay(d time.Duration) Option {
	return &retryOption{
		name: "WithRetryBaseDelay",
		apply: func(r *retryConfig) error {
			if d <= 0 {
				return invalidArgument("WithRetryBaseDelay: delay must be positive")
			}
			r.baseDelay = d
			return nil
		},
	}
}

// WithRetryMaxDelay sets the maximum delay for exponential backoff on CAS retry.
// Default: 2s.
func WithRetryMaxDelay(d time.Duration) Option {
	return &retryOption{
		name: "WithRetryMaxDelay",
		apply: func(r *retryConfig) error {
			if d <= 0 {
				return invalidArgument("WithRetryMaxDelay: delay must be positive")
			}
			r.maxDelay = d
			return nil
		},
	}
}

// WithRetryJitter sets the jitter factor for CAS retry backoff.
// 0.0 means no jitter (deterministic delay), 1.0 means full jitter.
// Default: 1.0 (full jitter).
func WithRetryJitter(j float64) Option {
	return &retryOption{
		name: "WithRetryJitter",
		apply: func(r *retryConfig) error {
			if j < 0 || j > 1 {
				return invalidArgument("WithRetryJitter: jitter must be between 0.0 and 1.0")
			}
			r.jitter = j
			return nil
		},
	}
}

// WithRetryObserver registers a callback invoked once per CAS retry attempt.
// The callback receives the 1-indexed retry attempt number and is called
// synchronously on the committing goroutine after a snapshot conflict is
// detected, before the backoff sleep. It is never called for the initial
// attempt, so the number of calls equals the number of retries performed.
// The callback must be fast and must not block; it is intended for
// observability (e.g. incrementing a retry counter metric).
// Has no effect unless WithRetryCount(n) is configured with n > 0.
func WithRetryObserver(fn func(attempt int)) Option {
	return &retryOption{
		name: "WithRetryObserver",
		apply: func(r *retryConfig) error {
			if fn == nil {
				return invalidArgument("WithRetryObserver: fn must not be nil")
			}
			r.observer = fn
			return nil
		},
	}
}

// -----------------------------------------------------------------------------
// Dataset Implementation
// -----------------------------------------------------------------------------

// dataset implements the Dataset interface.
type dataset struct {
	id         DatasetID
	store      Store
	layout     layout
	compressor Compressor
	codec      Codec
	checksum   Checksum
	retry      retryConfig

	// history owns the snapshot sequence and its head pointer (history.go).
	history history[DatasetSnapshotID, Manifest]
}

// datasetKind is the dataset's snapshot grammar for history: paths come from
// the layout, manifests are read without validation (the reader validates).
type datasetKind struct {
	id     DatasetID
	layout layout
}

func (k datasetKind) snapshotsPrefix() string { return k.layout.snapshotsPrefix(k.id) }
func (k datasetKind) pointerPath() string     { return k.layout.latestPointerPath(k.id) }
func (k datasetKind) manifestPath(id DatasetSnapshotID) string {
	return k.layout.manifestPath(k.id, id)
}

func (k datasetKind) parseManifest(p string) (DatasetSnapshotID, bool) {
	if !k.layout.isManifest(p) {
		return "", false
	}
	id := k.layout.parseSnapshotID(p)
	return id, id != ""
}

func (k datasetKind) decode(r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("lode: failed to decode manifest: %w", err)
	}
	return &m, nil
}

// datasetKind is a snapshotKind; stated so the methods are visibly used.
var _ snapshotKind[DatasetSnapshotID, Manifest] = datasetKind{}

func toDatasetSnapshot(s *snapshot[DatasetSnapshotID, Manifest]) *DatasetSnapshot {
	return &DatasetSnapshot{ID: s.ID, Manifest: s.Manifest}
}

// NewDataset creates a dataset with documented defaults.
//
// Default bundle (per PUBLIC_API.md):
//   - Layout: NewDefaultLayout() (flat, no partitions)
//   - Compressor: NewNoOpCompressor()
//   - Codec: none (raw blob storage)
//
// Use option functions to override defaults:
//   - WithLayout(l) to use a different layout (configures both paths AND partitioning)
//   - WithCompressor(c) to use compression
//   - WithCodec(c) to use structured records with a codec
//   - WithChecksum(c) to enable file checksums
func NewDataset(id DatasetID, factory StoreFactory, opts ...Option) (Dataset, error) {
	if factory == nil {
		return nil, invalidArgument("store factory is required")
	}

	cfg := &datasetConfig{
		layout:     NewDefaultLayout(),
		compressor: NewNoOpCompressor(),
		codec:      nil,
		retry:      defaultRetryConfig(),
	}

	for _, opt := range opts {
		if err := opt.applyDataset(cfg); err != nil {
			return nil, err // option errors carry the lode: prefix
		}
	}

	if cfg.layout == nil {
		return nil, invalidArgument("layout must not be nil")
	}
	if cfg.compressor == nil {
		return nil, invalidArgument("compressor must not be nil")
	}
	// Raw blob mode (no codec) cannot use partitioning - there are no record fields to extract keys from
	if cfg.codec == nil && !cfg.layout.partitioner().isNoop() {
		return nil, invalidArgument("raw blob mode (no codec) requires a layout with noop partitioner")
	}

	// Arguments are checked before any store work (CONTRACT_ERRORS §5).
	store, err := factory()
	if err != nil {
		return nil, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if store == nil {
		return nil, invalidArgument("store factory returned nil store")
	}

	return &dataset{
		id:         id,
		store:      store,
		layout:     cfg.layout,
		compressor: cfg.compressor,
		codec:      cfg.codec,
		checksum:   cfg.checksum,
		retry:      cfg.retry,
		history:    newHistory(store, datasetKind{id: id, layout: cfg.layout}, "manifest"),
	}, nil
}

func (d *dataset) ID() DatasetID {
	return d.id
}

func (d *dataset) Write(ctx context.Context, data []any, metadata Metadata) (*DatasetSnapshot, error) {
	// Data files are written once; on a commit conflict only the manifest is
	// re-parented, never the data re-uploaded.
	pending := d.newPending(metadata)
	if err := d.stage(ctx, &pending, data); err != nil {
		return nil, err
	}
	return d.commit(ctx, &pending)
}

// Snapshot loads a snapshot by ID. A dataset written before canonical
// manifests existed is found by scan when the canonical path is absent.
// An expired snapshot is reported before that scan: its partition manifests
// may outlive the canonical one, and the scan would resurrect it.
func (d *dataset) Snapshot(ctx context.Context, id DatasetSnapshotID) (*DatasetSnapshot, error) {
	s, err := d.history.Snapshot(ctx, id)
	if errors.Is(err, ErrNotFound) {
		if expired := d.tombstones().expiredError(ctx, id); expired != nil {
			return nil, expired
		}
		s, err = d.history.find(ctx, id)
	}
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("lode: failed to get manifest: %w", err)
	}
	return toDatasetSnapshot(s), nil
}

// Snapshots lists every snapshot, oldest first.
func (d *dataset) Snapshots(ctx context.Context) ([]*DatasetSnapshot, error) {
	all, err := d.history.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]*DatasetSnapshot, len(all))
	for i, s := range all {
		snapshots[i] = toDatasetSnapshot(s)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Manifest.CreatedAt.Before(snapshots[j].Manifest.CreatedAt)
	})
	return snapshots, nil
}

func (d *dataset) Read(ctx context.Context, id DatasetSnapshotID) ([]any, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := d.validateComponentsMatch(snapshot.Manifest); err != nil {
		return nil, err
	}

	if d.codec == nil {
		if len(snapshot.Manifest.Files) != 1 {
			return nil, fmt.Errorf("lode: raw blob snapshot must have exactly one file, got %d", len(snapshot.Manifest.Files))
		}
		data, err := d.readRawBlob(ctx, snapshot.Manifest.Files[0].Path)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read blob %s: %w", snapshot.Manifest.Files[0].Path, err)
		}
		return []any{data}, nil
	}

	var allRecords []any
	for _, fileRef := range snapshot.Manifest.Files {
		records, err := d.readDataFile(ctx, fileRef.Path)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read data file %s: %w", fileRef.Path, err)
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

// Latest returns the head snapshot.
func (d *dataset) Latest(ctx context.Context) (*DatasetSnapshot, error) {
	s, err := d.history.Latest(ctx)
	if err != nil {
		return nil, err
	}
	return toDatasetSnapshot(s), nil
}

func (d *dataset) StreamWrite(ctx context.Context, metadata Metadata) (StreamWriter, error) {
	if d.codec != nil {
		return nil, ErrCodecConfigured
	}
	// Surface an unreadable head now rather than at Commit.
	if _, _, err := d.history.Resolve(ctx); err != nil {
		return nil, err
	}

	pending := d.newPending(metadata)
	stream, err := d.openStream(ctx, pending.id, blobFile)
	if err != nil {
		return nil, err
	}
	return &streamWriter{ds: d, ctx: ctx, pending: pending, stream: stream}, nil
}

func (d *dataset) StreamWriteRecords(ctx context.Context, records RecordIterator, metadata Metadata) (*DatasetSnapshot, error) {
	if records == nil {
		return nil, ErrNilIterator
	}
	if d.codec == nil {
		return nil, invalidArgument("StreamWriteRecords requires a codec")
	}
	streamCodec, ok := d.codec.(StreamingRecordCodec)
	if !ok {
		return nil, ErrCodecNotStreamable
	}
	// A single streamed file cannot be partitioned: that would require
	// buffering the records to determine their keys.
	if !d.layout.partitioner().isNoop() {
		return nil, ErrPartitioningNotSupported
	}

	pending := d.newPending(metadata)
	stream, err := d.openRecordStream(ctx, pending.id, streamCodec)
	if err != nil {
		return nil, err
	}
	if err := stream.encode(records); err != nil {
		return nil, err
	}
	ref, err := stream.finish()
	if err != nil {
		return nil, err
	}

	pending.add(ref, "")
	pending.rowCount = stream.rows
	pending.codec = d.codec.Name()
	pending.timestamps = stream.timestamps
	pending.streamed = ref.Path
	return d.commit(ctx, &pending)
}

func (d *dataset) readRawBlob(ctx context.Context, filePath string) ([]byte, error) {
	rc, err := d.store.Get(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("lode: read blob: %w", err)
	}
	defer closer(rc)()

	decompReader, err := d.compressor.Decompress(rc)
	if err != nil {
		return nil, fmt.Errorf("lode: read blob: %w", err)
	}
	defer closer(decompReader)()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(decompReader); err != nil {
		return nil, fmt.Errorf("lode: read blob: %w", err)
	}
	return buf.Bytes(), nil
}

func (d *dataset) readDataFile(ctx context.Context, filePath string) ([]any, error) {
	rc, err := d.store.Get(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("lode: read data file: %w", err)
	}
	defer closer(rc)()

	decompReader, err := d.compressor.Decompress(rc)
	if err != nil {
		return nil, fmt.Errorf("lode: read data file: %w", err)
	}
	defer closer(decompReader)()

	return d.codec.Decode(decompReader)
}

func (d *dataset) validateComponentsMatch(m *Manifest) error {
	var expectedCodec string
	if d.codec != nil {
		expectedCodec = d.codec.Name()
	}
	if m.Codec != expectedCodec {
		return fmt.Errorf("lode: codec mismatch: snapshot uses %q but dataset configured with %q",
			m.Codec, expectedCodec)
	}
	if m.Compressor != d.compressor.Name() {
		return fmt.Errorf("lode: compressor mismatch: snapshot uses %q but dataset configured with %q",
			m.Compressor, d.compressor.Name())
	}
	return nil
}
