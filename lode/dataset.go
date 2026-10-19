package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
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

// ErrOptionNotValidForDatasetReader indicates an option was used with NewDatasetReader
// that only applies to NewDataset.
var ErrOptionNotValidForDatasetReader = errors.New("option not valid for reader")

// ErrOptionNotValidForDataset indicates an option was used with NewDataset
// that only applies to NewDatasetReader.
var ErrOptionNotValidForDataset = errors.New("option not valid for dataset")

// ErrOptionNotValidForVolume indicates an option was used with NewVolume
// that only applies to Dataset or DatasetReader.
var ErrOptionNotValidForVolume = errors.New("option not valid for volume")

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
	return fmt.Errorf("WithLayout: %w", ErrOptionNotValidForVolume)
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
	return fmt.Errorf("WithHiveLayout: %w", ErrOptionNotValidForVolume)
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
	return fmt.Errorf("WithCompressor: %w", ErrOptionNotValidForDatasetReader)
}

func (o *compressorOption) applyVolume(*volumeConfig) error {
	return fmt.Errorf("WithCompressor: %w", ErrOptionNotValidForVolume)
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
	return fmt.Errorf("WithCodec: %w", ErrOptionNotValidForDatasetReader)
}

func (o *codecOption) applyVolume(*volumeConfig) error {
	return fmt.Errorf("WithCodec: %w", ErrOptionNotValidForVolume)
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
	return fmt.Errorf("WithChecksum: %w", ErrOptionNotValidForDatasetReader)
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
	return fmt.Errorf("%s: %w", o.name, ErrOptionNotValidForDatasetReader)
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
				return errors.New("WithRetryCount: n must be non-negative")
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
				return errors.New("WithRetryBaseDelay: delay must be positive")
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
				return errors.New("WithRetryMaxDelay: delay must be positive")
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
				return errors.New("WithRetryJitter: jitter must be between 0.0 and 1.0")
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
				return errors.New("WithRetryObserver: fn must not be nil")
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

func (k datasetKind) snapshotsPrefix() string { return k.layout.segmentsPrefix(k.id) }
func (k datasetKind) pointerPath() string     { return k.layout.latestPointerPath(k.id) }
func (k datasetKind) manifestPath(id DatasetSnapshotID) string {
	return k.layout.manifestPath(k.id, id)
}

func (k datasetKind) parseManifest(p string) (DatasetSnapshotID, bool) {
	if !k.layout.isManifest(p) {
		return "", false
	}
	id := k.layout.parseSegmentID(p)
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
		return nil, errors.New("lode: store factory is required")
	}

	store, err := factory()
	if err != nil {
		return nil, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if store == nil {
		return nil, errors.New("lode: store factory returned nil store")
	}

	cfg := &datasetConfig{
		layout:     NewDefaultLayout(),
		compressor: NewNoOpCompressor(),
		codec:      nil,
		retry:      defaultRetryConfig(),
	}

	for _, opt := range opts {
		if err := opt.applyDataset(cfg); err != nil {
			return nil, fmt.Errorf("lode: %w", err)
		}
	}

	if cfg.layout == nil {
		return nil, errors.New("lode: layout must not be nil")
	}
	if cfg.compressor == nil {
		return nil, errors.New("lode: compressor must not be nil")
	}
	// Raw blob mode (no codec) cannot use partitioning - there are no record fields to extract keys from
	if cfg.codec == nil && !cfg.layout.partitioner().isNoop() {
		return nil, errors.New("lode: raw blob mode (no codec) requires a layout with noop partitioner")
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
	if metadata == nil {
		metadata = Metadata{}
	}

	// Phase 1: Write data files once. File paths include the snapshot ID but
	// data is immutable — on retry we re-parent the manifest, not re-upload.
	snapshotID := DatasetSnapshotID(generateID())

	var files []FileRef
	var rowCount int64
	var partitionKeys []string
	var codecName string

	if d.codec == nil {
		// Raw blob mode
		if len(data) != 1 {
			return nil, errors.New("lode: raw blob mode requires exactly one data element")
		}
		blob, ok := data[0].([]byte)
		if !ok {
			return nil, fmt.Errorf("lode: raw blob mode requires []byte, got %T", data[0])
		}

		fileRef, err := d.writeRawBlob(ctx, snapshotID, blob)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to write blob: %w", err)
		}
		files = []FileRef{fileRef}
		rowCount = 1
		partitionKeys = []string{""}
		codecName = ""
	} else {
		// Structured records mode
		partitions, err := d.partitionRecords(data)
		if err != nil {
			return nil, fmt.Errorf("lode: partitioning failed: %w", err)
		}

		for partKey, partRecords := range partitions {
			fileRef, err := d.writeDataFile(ctx, snapshotID, partKey, partRecords)
			if err != nil {
				return nil, fmt.Errorf("lode: failed to write data file: %w", err)
			}
			files = append(files, fileRef)
			partitionKeys = append(partitionKeys, partKey)
		}

		rowCount = int64(len(data))
		codecName = d.codec.Name()
	}

	// Extract timestamps from records that implement Timestamped
	minTs, maxTs := extractTimestamps(data)

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	// Phase 2: Resolve parent and commit. On CAS conflict, retry by
	// refreshing state and re-parenting the manifest.
	return d.commitWithRetry(ctx, snapshotID, files, partitionKeys, metadata, rowCount, codecName, minTs, maxTs, "")
}

// Snapshot loads a snapshot by ID. A dataset written before canonical
// manifests existed is found by scan when the canonical path is absent.
func (d *dataset) Snapshot(ctx context.Context, id DatasetSnapshotID) (*DatasetSnapshot, error) {
	s, err := d.history.Snapshot(ctx, id)
	if errors.Is(err, ErrNotFound) {
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
	if metadata == nil {
		metadata = Metadata{}
	}
	if d.codec != nil {
		return nil, ErrCodecConfigured
	}

	parentID, expectedPointer, err := d.history.Resolve(ctx)
	if err != nil {
		return nil, err
	}

	snapshotID := DatasetSnapshotID(generateID())
	fileName := "blob" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, "", fileName)

	// Create pipe for streaming to store
	pr, pw := io.Pipe()

	// Wrap with counting writer to track compressed size
	cw := &countingWriter{w: pw}

	// Set up base writer, optionally with checksum
	var baseWriter io.Writer = cw
	var hasher HashWriter
	if d.checksum != nil {
		hasher = d.checksum.NewHasher()
		baseWriter = io.MultiWriter(cw, hasher)
	}

	// Wrap with compression
	compWriter, err := d.compressor.Compress(baseWriter)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create compressor: %w", err)
	}

	put := startPut(ctx, d.store, filePath, pr)

	return &streamWriter{
		ds:              d,
		ctx:             ctx,
		metadata:        metadata,
		snapshotID:      snapshotID,
		parentID:        parentID,
		expectedPointer: expectedPointer,
		filePath:        filePath,
		pipeWriter:      pw,
		compWriter:      compWriter,
		countWriter:     cw,
		hasher:          hasher,
		put:             put,
	}, nil
}

func (d *dataset) StreamWriteRecords(ctx context.Context, records RecordIterator, metadata Metadata) (*DatasetSnapshot, error) {
	if metadata == nil {
		metadata = Metadata{}
	}
	if records == nil {
		return nil, ErrNilIterator
	}
	if d.codec == nil {
		return nil, errors.New("lode: StreamWriteRecords requires a codec")
	}
	streamCodec, ok := d.codec.(StreamingRecordCodec)
	if !ok {
		return nil, ErrCodecNotStreamable
	}
	// StreamWriteRecords writes to a single file and cannot partition records
	// since that would require buffering to determine partition keys
	if !d.layout.partitioner().isNoop() {
		return nil, ErrPartitioningNotSupported
	}

	snapshotID := DatasetSnapshotID(generateID())
	fileName := "data" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, "", fileName)

	// Create pipe for streaming to store
	pr, pw := io.Pipe()

	// Wrap with counting writer to track compressed size
	cw := &countingWriter{w: pw}

	// Set up base writer, optionally with checksum
	var baseWriter io.Writer = cw
	var hasher HashWriter
	if d.checksum != nil {
		hasher = d.checksum.NewHasher()
		baseWriter = io.MultiWriter(cw, hasher)
	}

	// Wrap with compression
	compWriter, err := d.compressor.Compress(baseWriter)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create compressor: %w", err)
	}

	// Create streaming encoder
	encoder, err := streamCodec.NewStreamEncoder(compWriter)
	if err != nil {
		_ = compWriter.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("lode: failed to create stream encoder: %w", err)
	}

	put := startPut(ctx, d.store, filePath, pr)

	// Stream records through encoder, tracking count and timestamps
	var rowCount int64
	var ts timestampRange

	// A buffering compressor can absorb records without touching the pipe, so a
	// Put that has already failed must be noticed before every iterator call
	// (which may block) and before accepting each record — never only after an
	// iterator that may never end.
	putFailed := func() error {
		if finished, putErr := put.poll(); finished && putErr != nil {
			cleanupStreamWrite(encoder, compWriter, pw, put, putErr)
			return fmt.Errorf("lode: failed to write data: %w", putErr)
		}
		return nil
	}
	if err := putFailed(); err != nil {
		return nil, err
	}
	for records.Next() {
		// Before and after Record(), which a producer-backed iterator may block in.
		if err := putFailed(); err != nil {
			return nil, err
		}
		record := records.Record()
		if err := putFailed(); err != nil {
			return nil, err
		}

		if err := encoder.WriteRecord(record); err != nil {
			cleanupStreamWrite(encoder, compWriter, pw, put, err)
			return nil, fmt.Errorf("lode: failed to write record: %w", err)
		}

		rowCount++
		ts.observe(record)

		// Before the next (possibly blocking) call into the iterator.
		if err := putFailed(); err != nil {
			return nil, err
		}
	}

	// A Put that failed while the iterator was finishing is the root cause and
	// takes precedence over whatever the iterator reports. Check before calling
	// Err, which may block, and again after, since Put may complete during it.
	if err := putFailed(); err != nil {
		return nil, err
	}
	iterErr := records.Err()
	if err := putFailed(); err != nil {
		return nil, err
	}
	if iterErr != nil {
		cleanupStreamWrite(encoder, compWriter, pw, put, iterErr)
		return nil, fmt.Errorf("lode: record iterator error: %w", iterErr)
	}

	// Close encoder (finalizes format)
	if err := encoder.Close(); err != nil {
		return nil, finalizeError(put, "encoder", err, func(cause error) {
			cleanupStreamWrite(nil, compWriter, pw, put, cause)
		})
	}

	// Collect per-file stats if the stream encoder supports it
	var fileStats *FileStats
	if se, ok := encoder.(StatisticalStreamEncoder); ok {
		fileStats = se.FileStats()
	}

	// Close compression (flushes final data)
	if err := compWriter.Close(); err != nil {
		return nil, finalizeError(put, "compressor", err, func(cause error) {
			cleanupStreamWrite(nil, nil, pw, put, cause)
		})
	}

	// Close pipe (signals EOF to store.Put)
	if err := pw.Close(); err != nil {
		return nil, finalizeError(put, "pipe", err, func(error) { _ = put.wait() })
	}

	// Wait for store.Put to complete. A failed Put leaves nothing behind
	// (CONTRACT_STORAGE §Put), so there is nothing for the writer to clean up.
	if err := put.wait(); err != nil {
		return nil, fmt.Errorf("lode: failed to write data: %w", err)
	}

	// Build file reference with optional checksum and stats
	fileRef := FileRef{
		Path:      filePath,
		SizeBytes: cw.n,
		Stats:     fileStats,
	}
	if hasher != nil {
		fileRef.Checksum = hasher.Sum()
	}

	// Commit with retry. Data file is already persisted; on conflict we
	// only re-parent the manifest and retry the pointer CAS.
	return d.commitWithRetry(ctx, snapshotID, []FileRef{fileRef}, []string{""}, metadata, rowCount, d.codec.Name(), ts.min, ts.max, filePath)
}

// cleanupStreamWrite tears down the streaming write pipeline on error and
// waits for the background Put to return. It never deletes the data object:
// a failed Put leaves the path as it was (CONTRACT_STORAGE §Put), and a Put
// that returned ErrPathExists refused a path that belongs to another writer.
// Arguments may be nil when the failure occurs before the corresponding
// resource is created; nil values are skipped.
func cleanupStreamWrite(encoder RecordStreamEncoder, compWriter io.WriteCloser, pw *io.PipeWriter, put *putResult, cause error) {
	if encoder != nil {
		_ = encoder.Close()
	}
	if compWriter != nil {
		_ = compWriter.Close()
	}
	if pw != nil {
		_ = pw.CloseWithError(cause)
	}
	_ = put.wait()
}

// putResult tracks the background store.Put of a streamed data object.
type putResult struct {
	done chan struct{} // closed once the result below is published

	mu       sync.Mutex
	finished bool
	err      error
}

// startPut runs store.Put in the background. The result is published before
// the read end is closed with Put's error, so a writer blocked on the pipe
// fails instead of hanging (a fast failure such as ErrPathExists never drains
// the pipe) and a Write issued after Put returned always observes the result.
func startPut(ctx context.Context, store Store, path string, pr *io.PipeReader) *putResult {
	r := &putResult{done: make(chan struct{})}
	go func() {
		err := store.Put(ctx, path, pr)
		r.mu.Lock()
		r.err, r.finished = err, true
		r.mu.Unlock()
		_ = pr.CloseWithError(err)
		close(r.done)
	}()
	return r
}

// wait blocks until Put has returned and returns its error.
func (r *putResult) wait() error {
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// poll reports Put's result if it has already returned, without blocking.
func (r *putResult) poll() (finished bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finished, r.err
}

// rootCause picks the error to surface when a finalization step fails: a Put
// that has already failed is the root cause of anything that broke after it.
func (r *putResult) rootCause(stepErr error) (fromPut bool, cause error) {
	if finished, putErr := r.poll(); finished && putErr != nil {
		return true, putErr
	}
	return false, stepErr
}

// finalizeError runs teardown with the root cause of a failed finalization
// step and returns the error to report: the Put failure when one preceded
// the step, otherwise the step's own error.
func finalizeError(put *putResult, step string, stepErr error, teardown func(cause error)) error {
	fromPut, cause := put.rootCause(stepErr)
	teardown(cause)
	if fromPut {
		return fmt.Errorf("lode: failed to write data: %w", cause)
	}
	return fmt.Errorf("lode: failed to close %s: %w", step, cause)
}

// commitWithRetry resolves the parent, builds a manifest, and commits.
// On ErrSnapshotConflict it refreshes state and retries up to d.retry.maxAttempts
// times. Data files must already be persisted before calling.
//
// The snapshotID is stable across retries. Data files are written under paths
// derived from the snapshotID, so changing it would break layout co-location
// (manifest and data must share the same snapshot directory). On CAS failure,
// no manifest was written for this ID — it is safe to reuse.
//
// Parameters:
//   - snapshotID: snapshot ID (stable across retries)
//   - files: immutable file references (stable across retries)
//   - partitionKeys: partition keys for manifest fan-out
//   - metadata: user metadata
//   - rowCount: total row count
//   - codec: codec name (empty for raw blob)
//   - minTs, maxTs: timestamp bounds
//   - cleanupPath: data file path to delete on final failure (empty = no cleanup)
func (d *dataset) commitWithRetry(
	ctx context.Context,
	snapshotID DatasetSnapshotID,
	files []FileRef,
	partitionKeys []string,
	metadata Metadata,
	rowCount int64,
	codec string,
	minTs, maxTs *time.Time,
	cleanupPath string,
) (*DatasetSnapshot, error) {
	var manifest *Manifest
	err := d.history.CommitLoop(ctx, &d.retry, func() error {
		parentID, expectedPointer, err := d.history.Resolve(ctx)
		if err != nil {
			return err
		}
		manifest = d.buildManifest(snapshotID, parentID, metadata, files, rowCount, codec, minTs, maxTs)
		return d.history.Advance(ctx, expectedPointer, snapshotID, manifest, d.manifestPaths(snapshotID, partitionKeys))
	})
	if err != nil {
		d.cleanupDataFile(ctx, cleanupPath)
		return nil, err
	}
	return &DatasetSnapshot{ID: snapshotID, Manifest: manifest}, nil
}

// cleanupDataFile deletes a data file on commit failure. No-op if path is empty.
func (d *dataset) cleanupDataFile(ctx context.Context, path string) {
	if path != "" {
		_ = d.store.Delete(ctx, path)
	}
}

func (d *dataset) partitionRecords(records []any) (map[string][]any, error) {
	partitions := make(map[string][]any)
	part := d.layout.partitioner()

	for _, record := range records {
		key, err := part.partitionKey(record)
		if err != nil {
			return nil, err
		}
		partitions[key] = append(partitions[key], record)
	}

	return partitions, nil
}

// buildManifest constructs a Manifest with common fields derived from the
// dataset configuration. Only fields that vary across call sites are parameters.
func (d *dataset) buildManifest(
	snapshotID DatasetSnapshotID,
	parentID DatasetSnapshotID,
	metadata Metadata,
	files []FileRef,
	rowCount int64,
	codec string,
	minTs, maxTs *time.Time,
) *Manifest {
	m := &Manifest{
		SchemaName:       manifestSchemaName,
		FormatVersion:    manifestFormatVersion,
		DatasetID:        d.id,
		SnapshotID:       snapshotID,
		CreatedAt:        time.Now().UTC(),
		Metadata:         metadata,
		Files:            files,
		ParentSnapshotID: parentID,
		RowCount:         rowCount,
		MinTimestamp:     minTs,
		MaxTimestamp:     maxTs,
		Codec:            codec,
		Compressor:       d.compressor.Name(),
		Partitioner:      d.layout.partitioner().name(),
	}
	if d.checksum != nil {
		m.ChecksumAlgorithm = d.checksum.Name()
	}
	return m
}

// checksumBytes computes a checksum of data using the dataset's configured checksum
// algorithm. Returns empty string if no checksum is configured.
func (d *dataset) checksumBytes(data []byte) string {
	if d.checksum == nil {
		return ""
	}
	hasher := d.checksum.NewHasher()
	_, _ = hasher.Write(data)
	return hasher.Sum()
}

func (d *dataset) writeRawBlob(ctx context.Context, snapshotID DatasetSnapshotID, data []byte) (FileRef, error) {
	fileName := "blob" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, "", fileName)

	var buf bytes.Buffer
	compWriter, err := d.compressor.Compress(&buf)
	if err != nil {
		return FileRef{}, fmt.Errorf("lode: write blob: %w", err)
	}

	if _, err := compWriter.Write(data); err != nil {
		_ = compWriter.Close()
		return FileRef{}, fmt.Errorf("lode: write blob: %w", err)
	}

	if err := compWriter.Close(); err != nil {
		return FileRef{}, fmt.Errorf("lode: write blob: %w", err)
	}

	compressedData := buf.Bytes()
	if err := d.store.Put(ctx, filePath, bytes.NewReader(compressedData)); err != nil {
		return FileRef{}, fmt.Errorf("lode: write blob: %w", err)
	}

	fileRef := FileRef{
		Path:      filePath,
		SizeBytes: int64(len(compressedData)),
	}

	fileRef.Checksum = d.checksumBytes(compressedData)

	return fileRef, nil
}

func (d *dataset) writeDataFile(ctx context.Context, snapshotID DatasetSnapshotID, partKey string, records []any) (FileRef, error) {
	fileName := "data" + d.compressor.Extension()
	filePath := d.layout.dataFilePath(d.id, snapshotID, partKey, fileName)

	var buf bytes.Buffer
	compWriter, err := d.compressor.Compress(&buf)
	if err != nil {
		return FileRef{}, fmt.Errorf("lode: write data file: %w", err)
	}

	if err := d.codec.Encode(compWriter, records); err != nil {
		_ = compWriter.Close()
		return FileRef{}, fmt.Errorf("lode: write data file: %w", err)
	}

	if err := compWriter.Close(); err != nil {
		return FileRef{}, fmt.Errorf("lode: write data file: %w", err)
	}

	data := buf.Bytes()
	if err := d.store.Put(ctx, filePath, bytes.NewReader(data)); err != nil {
		return FileRef{}, fmt.Errorf("lode: write data file: %w", err)
	}

	fileRef := FileRef{
		Path:      filePath,
		SizeBytes: int64(len(data)),
	}

	fileRef.Checksum = d.checksumBytes(data)

	// Collect per-file stats if the codec supports it
	if sc, ok := d.codec.(StatisticalCodec); ok {
		fileRef.Stats = sc.FileStats()
	}

	return fileRef, nil
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

// manifestPaths returns where a snapshot's manifest is written: always the
// canonical path (O(1) Snapshot lookup) and, on layouts that place segments
// under partitions, one copy per partition the snapshot touches so partition
// listings can find it. Partitionless layouts have only the canonical path.
func (d *dataset) manifestPaths(snapshotID DatasetSnapshotID, partitionKeys []string) []string {
	canonical := d.layout.manifestPath(d.id, snapshotID)
	paths := []string{canonical}
	if !d.layout.supportsPartitions() {
		return paths
	}
	seen := map[string]bool{canonical: true}
	for _, pk := range partitionKeys {
		if pk == "" {
			continue
		}
		p := d.layout.manifestPathInPartition(d.id, snapshotID, pk)
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}
	return paths
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

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// extractTimestamps iterates over records and extracts min/max timestamps
// from records that implement the Timestamped interface.
// Returns nil pointers if no records implement Timestamped.
func extractTimestamps(data []any) (minTs, maxTs *time.Time) {
	var hasTimestamp bool

	for _, record := range data {
		ts, ok := record.(Timestamped)
		if !ok {
			continue
		}

		t := ts.Timestamp()
		if !hasTimestamp {
			minTs = &t
			maxTs = &t
			hasTimestamp = true
			continue
		}

		if t.Before(*minTs) {
			minTs = &t
		}
		if t.After(*maxTs) {
			maxTs = &t
		}
	}

	return minTs, maxTs
}

// -----------------------------------------------------------------------------
// StreamWriter Implementation
// -----------------------------------------------------------------------------

// streamWriter implements StreamWriter for raw binary streaming writes.
type streamWriter struct {
	ds              *dataset
	ctx             context.Context
	metadata        Metadata
	snapshotID      DatasetSnapshotID
	parentID        DatasetSnapshotID
	expectedPointer string // current pointer content for CAS
	filePath        string
	pipeWriter      *io.PipeWriter
	compWriter      io.WriteCloser
	countWriter     *countingWriter
	hasher          HashWriter
	put             *putResult

	mu        sync.Mutex
	committed bool
	aborted   bool
	closed    bool
	writeErr  error
}

func (sw *streamWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()
	if sw.closed || sw.aborted || sw.committed {
		sw.mu.Unlock()
		return 0, errors.New("lode: stream is closed")
	}
	if sw.writeErr != nil {
		sw.mu.Unlock()
		return 0, sw.writeErr
	}
	sw.mu.Unlock()

	// A compressor may buffer p without touching the pipe, so a Put that has
	// already failed would otherwise go unnoticed until Commit.
	if finished, putErr := sw.put.poll(); finished && putErr != nil {
		sw.mu.Lock()
		sw.writeErr = putErr
		sw.mu.Unlock()
		return 0, putErr
	}

	n, err = sw.compWriter.Write(p)
	if err != nil {
		sw.mu.Lock()
		sw.writeErr = err
		sw.mu.Unlock()
	}
	return n, err
}

func (sw *streamWriter) Commit(ctx context.Context) (*DatasetSnapshot, error) {
	sw.mu.Lock()
	if sw.committed {
		sw.mu.Unlock()
		return nil, errors.New("lode: stream already committed")
	}
	if sw.aborted || sw.closed {
		sw.mu.Unlock()
		return nil, errors.New("lode: stream is closed")
	}
	// Mark as closed to prevent concurrent commits, but not committed until success
	sw.closed = true
	sw.mu.Unlock()

	// Close compression writer (flushes final data). A Put that already failed
	// is the root cause of a failing close and is the error reported.
	if err := sw.compWriter.Close(); err != nil {
		return nil, finalizeError(sw.put, "compressor", err, func(cause error) {
			_ = sw.pipeWriter.CloseWithError(cause)
			_ = sw.put.wait()
		})
	}

	// Close pipe writer (signals EOF to store.Put)
	if err := sw.pipeWriter.Close(); err != nil {
		return nil, finalizeError(sw.put, "pipe", err, func(error) { _ = sw.put.wait() })
	}

	// Wait for store.Put to complete. A failed Put leaves nothing behind
	// (CONTRACT_STORAGE §Put), so there is nothing for the writer to clean up.
	if err := sw.put.wait(); err != nil {
		return nil, fmt.Errorf("lode: failed to write data: %w", err)
	}

	// Build file reference with optional checksum
	fileRef := FileRef{
		Path:      sw.filePath,
		SizeBytes: sw.countWriter.n,
	}
	if sw.hasher != nil {
		fileRef.Checksum = sw.hasher.Sum()
	}

	// Commit with retry. Data file is already persisted; on conflict we
	// only re-parent the manifest and retry the pointer CAS.
	snapshot, err := sw.ds.commitWithRetry(ctx, sw.snapshotID, []FileRef{fileRef}, []string{""}, sw.metadata, 1, "", nil, nil, sw.filePath)
	if err != nil {
		return nil, err
	}

	sw.mu.Lock()
	sw.committed = true
	sw.mu.Unlock()

	return snapshot, nil
}

func (sw *streamWriter) Abort(_ context.Context) error {
	sw.mu.Lock()
	if sw.committed {
		sw.mu.Unlock()
		return errors.New("lode: cannot abort committed stream")
	}
	if sw.aborted {
		sw.mu.Unlock()
		return nil // already aborted
	}
	sw.aborted = true
	sw.mu.Unlock()

	// Close pipe with error to cancel store.Put, then wait for it to return.
	// The failed Put leaves no object behind (CONTRACT_STORAGE §Put); nothing
	// to delete.
	_ = sw.pipeWriter.CloseWithError(errors.New("stream aborted"))
	_ = sw.put.wait()

	return nil
}

func (sw *streamWriter) Close() error {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return nil
	}
	sw.closed = true
	alreadyCommitted := sw.committed
	alreadyAborted := sw.aborted
	sw.mu.Unlock()

	if alreadyCommitted || alreadyAborted {
		return nil
	}

	// Close without commit = abort
	return sw.Abort(sw.ctx)
}

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// timestampRange accumulates the earliest and latest Timestamp over the
// records it observes; records that are not Timestamped are ignored.
type timestampRange struct {
	min, max *time.Time
}

func (r *timestampRange) observe(record any) {
	ts, ok := record.(Timestamped)
	if !ok {
		return
	}
	t := ts.Timestamp()
	if r.min == nil || t.Before(*r.min) {
		r.min = &t
	}
	if r.max == nil || t.After(*r.max) {
		r.max = &t
	}
}
