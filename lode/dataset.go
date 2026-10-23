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

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/retry"
	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

const (
	manifestSchemaName = "lode-manifest"
	// manifestFormatVersion 1.1.0 adds codec_description, partition_keys,
	// and paths; a manifest without paths is 1.0.0 (legacy).
	manifestFormatVersion = "1.1.0"
)

// -----------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------

// datasetConfig holds a writer handle's configuration.
type datasetConfig struct {
	layout     Layout
	compressor Compressor
	checksum   Checksum
	retry      retry.Config
}

// viewConfig holds a view's custom components, which a view uses for the
// snapshots whose manifests name them.
type viewConfig struct {
	compressor Compressor
	checksum   Checksum
}

// DatasetOption configures a writer handle: Root.Records or Root.Blobs.
type DatasetOption interface {
	applyDataset(cfg *datasetConfig) error
}

// ViewOption configures a view: ReadRoot.Records or ReadRoot.Blobs. A view
// takes its layout and components from the dataset's manifests; only a
// custom compressor or checksum, which Lode cannot reconstruct from its
// name, is passed to it.
type ViewOption interface {
	applyView(cfg *viewConfig) error
}

// ComponentOption configures a writer handle or a view.
type ComponentOption interface {
	DatasetOption
	ViewOption
}

// datasetOption adapts a function to DatasetOption.
type datasetOption func(cfg *datasetConfig) error

func (o datasetOption) applyDataset(cfg *datasetConfig) error { return o(cfg) }

// componentOption sets a component on a writer handle and on a view.
type componentOption struct {
	dataset func(cfg *datasetConfig) error
	view    func(cfg *viewConfig) error
}

func (o componentOption) applyDataset(cfg *datasetConfig) error { return o.dataset(cfg) }
func (o componentOption) applyView(cfg *viewConfig) error       { return o.view(cfg) }

// WithLayout sets how a dataset is partitioned: Unpartitioned (the default)
// or Hive.
func WithLayout(l Layout) DatasetOption {
	return datasetOption(func(cfg *datasetConfig) error {
		if validate.Nil(l) {
			return validate.Argument("WithLayout: layout must not be nil")
		}
		cfg.layout = l
		return nil
	})
}

// WithCompressor sets the compressor of a writer handle (default:
// NewNoOpCompressor). For a view it supplies a custom compressor, used for
// the snapshots whose manifests name it; Lode reconstructs the bundled
// compressors by name.
func WithCompressor(c Compressor) ComponentOption {
	return componentOption{
		dataset: func(cfg *datasetConfig) error {
			if validate.Nil(c) {
				return validate.Argument("compressor must not be nil")
			}
			cfg.compressor = c
			return nil
		},
		view: func(cfg *viewConfig) error {
			if validate.Nil(c) {
				return validate.Argument("WithCompressor: compressor must not be nil")
			}
			cfg.compressor = c
			return nil
		},
	}
}

// WithChecksum sets the checksum algorithm of a writer handle (default:
// none), whose checksums are computed during writes and recorded in the
// manifest. For a view it supplies a custom checksum, matched by name. A
// nil checksum is none; a non-nil interface that holds a nil value is
// rejected.
func WithChecksum(c Checksum) ComponentOption {
	set := func(dst *Checksum) error {
		if c != nil && validate.Nil(c) {
			return validate.Argument("WithChecksum: checksum holds a nil value")
		}
		*dst = c
		return nil
	}
	return componentOption{
		dataset: func(cfg *datasetConfig) error { return set(&cfg.checksum) },
		view:    func(cfg *viewConfig) error { return set(&cfg.checksum) },
	}
}

// retryOption builds a retry option whose validation failure is an invalid
// argument named after the option, as package lode/volume's options are.
func retryOption(name string, set func(r *retry.Config) error) DatasetOption {
	return datasetOption(func(cfg *datasetConfig) error {
		if err := set(&cfg.retry); err != nil {
			return validate.Argument("%s: %v", name, err)
		}
		return nil
	})
}

// WithRetry enables automatic retry of a commit on snapshot conflict: up
// to n retries on ErrSnapshotConflict, refreshing the parent between
// attempts. Default: 0 (ErrSnapshotConflict is returned at once).
func WithRetry(n int) DatasetOption {
	return retryOption("WithRetry", func(r *retry.Config) error { return r.SetMaxAttempts(n) })
}

// WithRetryBaseDelay sets the base delay for exponential backoff on CAS retry.
// The actual delay doubles with each attempt and is jittered.
// Default: 10ms.
func WithRetryBaseDelay(d time.Duration) DatasetOption {
	return retryOption("WithRetryBaseDelay", func(r *retry.Config) error { return r.SetBaseDelay(d) })
}

// WithRetryMaxDelay sets the maximum delay for exponential backoff on CAS retry.
// Default: 2s.
func WithRetryMaxDelay(d time.Duration) DatasetOption {
	return retryOption("WithRetryMaxDelay", func(r *retry.Config) error { return r.SetMaxDelay(d) })
}

// WithRetryJitter sets the jitter factor for CAS retry backoff.
// 0.0 means no jitter (deterministic delay), 1.0 means full jitter.
// Default: 1.0 (full jitter).
func WithRetryJitter(j float64) DatasetOption {
	return retryOption("WithRetryJitter", func(r *retry.Config) error { return r.SetJitter(j) })
}

// WithRetryObserver registers a callback invoked once per CAS retry attempt.
// The callback receives the 1-indexed retry attempt number and is called
// synchronously on the committing goroutine after a snapshot conflict is
// detected, before the backoff sleep. It is never called for the initial
// attempt, so the number of calls equals the number of retries performed.
// The callback must be fast and must not block; it is intended for
// observability (e.g. incrementing a retry counter metric).
// Has no effect unless WithRetry(n) is configured with n > 0.
func WithRetryObserver(fn func(attempt int)) DatasetOption {
	return retryOption("WithRetryObserver", func(r *retry.Config) error { return r.SetObserver(fn) })
}

// resolveDatasetConfig applies opts over the defaults.
func resolveDatasetConfig(opts []DatasetOption) (*datasetConfig, error) {
	cfg := &datasetConfig{layout: Unpartitioned(), compressor: NewNoOpCompressor(), retry: retry.Default()}
	for _, opt := range opts {
		if opt == nil {
			return nil, validate.Argument("option must not be nil")
		}
		if err := opt.applyDataset(cfg); err != nil {
			return nil, err
		}
	}
	if validate.Nil(cfg.compressor) {
		return nil, validate.Argument("compressor must not be nil")
	}
	return cfg, nil
}

// resolveViewConfig applies opts.
func resolveViewConfig(opts []ViewOption) (*viewConfig, error) {
	cfg := &viewConfig{}
	for _, opt := range opts {
		if opt == nil {
			return nil, validate.Argument("option must not be nil")
		}
		if err := opt.applyView(cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// -----------------------------------------------------------------------------
// The dataset core
// -----------------------------------------------------------------------------

// dataset is what every handle of one dataset shares: its paths, its
// history, its components, and the writer's state. Records, Blobs, and the
// views wrap it; a view's store refuses writes (readOnlyStore), and a view
// never calls the writer's methods.
type dataset struct {
	id         DatasetID
	store      Store
	layout     *pathLayout
	compressor Compressor // the writer's; nil for a view
	checksum   Checksum
	custom     viewConfig // a view's custom components
	retry      retry.Config

	// codecName and codecDescription are the codec the manifests record;
	// "" for a blob dataset.
	codecName, codecDescription string

	// history owns the snapshot sequence and its head pointer (internal/history).
	history history.History[DatasetSnapshotID, Manifest]

	// settled is true once this writer's settings were verified against
	// the dataset's (checkSettings) or recorded by its own commit.
	settled bool

	// adoptedStaged is the staged manifest of an abandoned first commit
	// whose settings checkSettings read; this writer's commit, a child of
	// it, removes it ("" when there is none).
	adoptedStaged string
}

// datasetKind is the dataset's snapshot grammar for history: paths come from
// the layout, manifests are read without validation (the reader validates).
type datasetKind struct {
	id      DatasetID
	paths   *pathLayout
	store   Store      // checks canonical manifests of tombstoned snapshots
	expired tombstones // retention's expiries hide snapshots from the scan
}

func newDatasetKind(id DatasetID, l *pathLayout, store Store) datasetKind {
	return datasetKind{id: id, paths: l, store: store, expired: tombstones{store: store, paths: l, dataset: id}}
}

// StagedPath makes a dataset a history.Stager, so that a writer can check
// its settings against a pending first commit (CONTRACT_CORE §Head Pointer).
func (k datasetKind) StagedPath(id DatasetSnapshotID) string {
	return k.paths.stagedManifestPath(k.id, id)
}

// StagesCommit is true only for a commit that expects no pointer, a
// dataset's first: every later commit has a manifest to check against.
func (datasetKind) StagesCommit(expected string) bool { return expected == "" }

func (k datasetKind) SnapshotsPrefix() string { return k.paths.snapshotsPrefix(k.id) }
func (k datasetKind) ManifestPath(id DatasetSnapshotID) string {
	return k.paths.manifestPath(k.id, id)
}

func (k datasetKind) ParseManifest(p string) (DatasetSnapshotID, bool) {
	sp, ok := k.paths.manifest(p)
	return sp.snapshot, ok && sp.snapshot != ""
}

// NotExpired is false only for an expired snapshot: one with a tombstone
// and no canonical manifest (CONTRACT_RETENTION §Expired Snapshot). An
// expiring snapshot, its canonical manifest still present, is readable.
func (k datasetKind) NotExpired(ctx context.Context, id DatasetSnapshotID) (bool, error) {
	absent, err := k.expired.absent(ctx, id)
	if err != nil || absent {
		return absent, err
	}
	present, err := k.store.Exists(ctx, k.ManifestPath(id))
	if err != nil {
		return false, fmt.Errorf("lode: failed to check manifest: %w", err)
	}
	return present, nil
}

func (k datasetKind) Decode(r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("lode: failed to decode manifest: %w", err)
	}
	return &m, nil
}

// datasetKind is a history.Kind and Stager; stated so the methods are
// visibly used.
var (
	_ history.Kind[DatasetSnapshotID, Manifest] = datasetKind{}
	_ history.Stager[DatasetSnapshotID]         = datasetKind{}
)

func toDatasetSnapshot(s *history.Snapshot[DatasetSnapshotID, Manifest]) *DatasetSnapshot {
	return &DatasetSnapshot{ID: s.ID, Manifest: s.Manifest}
}

// fromHistory converts a history load result, passing its error through.
func fromHistory(s *history.Snapshot[DatasetSnapshotID, Manifest], err error) (*DatasetSnapshot, error) {
	if err != nil {
		return nil, err
	}
	return toDatasetSnapshot(s), nil
}

// newDataset assembles the core of a dataset handle over store.
func newDataset(id DatasetID, store Store, l *pathLayout) *dataset {
	return &dataset{
		id:      id,
		store:   store,
		layout:  l,
		retry:   retry.Default(),
		history: history.New(store, newDatasetKind(id, l, store), l.latestPointerPath(id), "manifest", historyErrors),
	}
}

// ID returns the dataset's identifier.
func (d *dataset) ID() DatasetID {
	return d.id
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
		s, err = d.history.Find(ctx, id)
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
		return snapshots[i].CreatedAt.Before(snapshots[j].CreatedAt)
	})
	return snapshots, nil
}

// latest returns the head snapshot for a writer, which creates a missing
// pointer from the scan result.
func (d *dataset) latest(ctx context.Context) (*DatasetSnapshot, error) {
	return fromHistory(d.history.Latest(ctx))
}

// head returns the head snapshot for a view, which never writes.
func (d *dataset) head(ctx context.Context) (*DatasetSnapshot, error) {
	return fromHistory(d.history.Head(ctx))
}

// readFile reads one data file of a snapshot through the compressor its
// manifest names, passing the decompressed stream to decode.
func (d *dataset) readFile(ctx context.Context, m *Manifest, path string, decode func(io.Reader) error) error {
	compressor, err := d.compressorFor(m)
	if err != nil {
		return err
	}
	if err := d.matchChecksum(m); err != nil {
		return err
	}
	rc, err := d.store.Get(ctx, path)
	if err != nil {
		return err
	}
	defer closer(rc)()
	decompressed, err := compressor.Decompress(rc)
	if err != nil {
		return err
	}
	defer closer(decompressed)()
	return decode(decompressed)
}

// readBlob reads the single blob of a blob snapshot.
func (d *dataset) readBlob(ctx context.Context, id DatasetSnapshotID) ([]byte, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := matchCodec(snapshot.Manifest, "", ""); err != nil {
		return nil, err
	}
	if len(snapshot.Files) != 1 {
		return nil, fmt.Errorf("lode: blob snapshot must have exactly one file, got %d", len(snapshot.Files))
	}
	path := snapshot.Files[0].Path
	var buf bytes.Buffer
	err = d.readFile(ctx, snapshot.Manifest, path, func(r io.Reader) error {
		_, err := buf.ReadFrom(r)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("lode: failed to read blob %s: %w", path, err)
	}
	return buf.Bytes(), nil
}

// readRecords reads every record of a record snapshot with codec, which
// must be the one the manifest names.
func readRecords[T any](ctx context.Context, d *dataset, codec Codec[T], id DatasetSnapshotID) ([]T, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := matchCodec(snapshot.Manifest, codec.Name(), codec.Describe()); err != nil {
		return nil, err
	}
	var all []T
	for _, f := range snapshot.Files {
		err := d.readFile(ctx, snapshot.Manifest, f.Path, func(r io.Reader) error {
			records, err := codec.Decode(r)
			all = append(all, records...)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read data file %s: %w", f.Path, err)
		}
	}
	return all, nil
}

// matchCodec rejects a snapshot written with another codec, or, unless its
// manifest is legacy, with another codec configuration. It reads the two
// codec fields directly rather than through recordedSettings, because it
// runs on every read and needs no other setting.
func matchCodec(m *Manifest, name, description string) error {
	if m.Codec != name {
		return fmt.Errorf("lode: codec mismatch: snapshot %s uses %q, handle has %q: %w", m.SnapshotID, m.Codec, name, ErrSettingsMismatch)
	}
	if !legacy(m) && m.CodecDescription != description {
		return fmt.Errorf("lode: codec description mismatch: snapshot %s records %q, handle has %q: %w", m.SnapshotID, m.CodecDescription, description, ErrSettingsMismatch)
	}
	return nil
}

// bundledCompressors are the compressors Lode reconstructs from the name a
// manifest records.
var bundledCompressors = map[string]func() Compressor{
	"noop": NewNoOpCompressor,
	"gzip": NewGzipCompressor,
	"zstd": NewZstdCompressor,
}

// compressorFor returns the compressor a snapshot's manifest names: the
// handle's own or custom one when its name matches, else the bundled one.
func (d *dataset) compressorFor(m *Manifest) (Compressor, error) {
	for _, c := range []Compressor{d.compressor, d.custom.compressor} {
		if c != nil && c.Name() == m.Compressor {
			return c, nil
		}
	}
	if newCompressor, ok := bundledCompressors[m.Compressor]; ok {
		return newCompressor(), nil
	}
	return nil, fmt.Errorf("lode: snapshot %s uses compressor %q, which is not bundled; pass it with WithCompressor: %w", m.SnapshotID, m.Compressor, ErrSettingsMismatch)
}

// bundledChecksums are the checksums Lode reconstructs from the name a
// manifest records.
var bundledChecksums = map[string]func() Checksum{
	"md5": NewMD5Checksum,
}

// matchChecksum returns ErrSettingsMismatch when a snapshot's manifest
// names a checksum that is neither bundled nor this handle's, as
// compressorFor does for compressors (V1_API_DECISIONS D11, Options).
// Reads do not verify checksums; the name is matched so that a view states
// every custom component of the snapshots it reads.
func (d *dataset) matchChecksum(m *Manifest) error {
	name := m.ChecksumAlgorithm
	if _, bundled := bundledChecksums[name]; name == "" || bundled {
		return nil
	}
	for _, c := range []Checksum{d.checksum, d.custom.checksum} {
		if c != nil && c.Name() == name {
			return nil
		}
	}
	return fmt.Errorf("lode: snapshot %s uses checksum %q, which is not bundled; pass it with WithChecksum: %w", m.SnapshotID, name, ErrSettingsMismatch)
}

// validateDatasetID rejects an ID that is not one canonical path segment
// (storepath.Segment): Lode cleans the paths it builds from an ID, so that
// "a/../events" would otherwise address "events" (CONTRACT_ERRORS §Dataset
// IDs).
func validateDatasetID(id DatasetID) error {
	if !storepath.Segment(string(id)) {
		return validate.Argument("dataset ID %q must be one path segment: not empty, \".\" or \"..\", without \"/\" or a backslash, and not beginning with %q", id, storepath.ReservedPrefix)
	}
	return nil
}

// historyErrors are the sentinels every history in this package returns, so
// history errors keep lode's public identities.
var historyErrors = history.Errors{
	NotFound:         ErrNotFound,
	NoSnapshots:      ErrNoSnapshots,
	SnapshotConflict: ErrSnapshotConflict,
	HeadUncommitted:  ErrHeadUncommitted,
}
