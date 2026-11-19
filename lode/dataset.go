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
	manifestSchemaName    = "lode-manifest"
	manifestFormatVersion = "1.2.0"
)

// -----------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------

type datasetConfig struct {
	layout     Layout
	compressor Compressor
	checksum   Checksum
	retry      retry.Config
}

type components struct {
	compressor Compressor
	checksum   Checksum
}

// DatasetOption configures a writer handle: Root.Records or Root.Blobs, as
// well as retry settings for Root.RemoveDataset.
type DatasetOption interface {
	applyDataset(cfg *datasetConfig) error
}

// ViewOption configures a view: ReadRoot.Records or ReadRoot.Blobs.
type ViewOption interface {
	applyView(cfg *viewConfig) error
}

type viewConfig struct {
	components components
	layout     *partitionScheme
	anchor     viewAnchor
}

type viewOption func(cfg *viewConfig) error

func (o viewOption) applyView(cfg *viewConfig) error { return o(cfg) }

// WithAnchor opens a view at snapshot id in place of the dataset's head.
// Opening reads id's canonical manifest once (touching neither the head
// pointer nor the head manifest), validates codec and layout settings against
// it, and retains that immutable manifest so an immediate Snapshot(ctx, id) or
// Read(ctx, id) issues zero additional manifest Gets. Other methods such as
// Latest() still consult the dataset's live history.
func WithAnchor(id SnapshotID) ViewOption {
	return viewOption(func(cfg *viewConfig) error {
		if !storepath.Segment(string(id)) {
			return validate.Argument("WithAnchor: snapshot ID %q is not %s", id, storepath.SegmentRule)
		}
		cfg.anchor = snapshotAnchor{id: id}
		return nil
	})
}

// ComponentOption configures either a writer handle or a view.
type ComponentOption interface {
	DatasetOption
	ViewOption
}

type datasetOption func(cfg *datasetConfig) error

func (o datasetOption) applyDataset(cfg *datasetConfig) error { return o(cfg) }

type componentOption struct {
	dataset func(cfg *datasetConfig) error
	view    func(cfg *viewConfig) error
}

func (o componentOption) applyDataset(cfg *datasetConfig) error { return o.dataset(cfg) }
func (o componentOption) applyView(cfg *viewConfig) error       { return o.view(cfg) }

// WithLayout sets how a dataset is partitioned: Unpartitioned (the default) or
// Hive. On a view it states the expected layout, avoiding dual-grammar probes.
func WithLayout(l Layout) ComponentOption {
	return componentOption{
		dataset: func(cfg *datasetConfig) error {
			if validate.Nil(l) {
				return validate.Argument("WithLayout: layout must not be nil")
			}
			cfg.layout = l
			return nil
		},
		view: func(cfg *viewConfig) error {
			if validate.Nil(l) {
				return validate.Argument("WithLayout: layout must not be nil")
			}
			scheme, err := l.scheme()
			if err != nil {
				return err
			}
			cfg.layout = &scheme
			return nil
		},
	}
}

// WithCompressor sets the compressor of a writer handle (default:
// Uncompressed) or supplies a custom compressor to a view.
func WithCompressor(c Compressor) ComponentOption {
	return componentOption{
		dataset: func(cfg *datasetConfig) error {
			if validate.Nil(c) {
				return validate.Argument("compressor must not be nil")
			}
			if ext := c.Extension(); !storepath.Segment(dataFile + ext) {
				return validate.Argument("WithCompressor: extension %q must keep a data file name %s", ext, storepath.SegmentRule)
			}
			cfg.compressor = c
			return nil
		},
		view: func(cfg *viewConfig) error {
			if validate.Nil(c) {
				return validate.Argument("WithCompressor: compressor must not be nil")
			}
			cfg.components.compressor = c
			return nil
		},
	}
}

// WithChecksum sets the checksum algorithm of a writer handle (default: none)
// or supplies a custom checksum algorithm to a view. Bundled algorithms
// (SHA256, CRC32C, MD5) are reconstructed automatically by name on read.
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
		view:    func(cfg *viewConfig) error { return set(&cfg.components.checksum) },
	}
}

func retryOption(name string, set func(r *retry.Config) error) DatasetOption {
	return datasetOption(func(cfg *datasetConfig) error {
		if err := set(&cfg.retry); err != nil {
			return validate.Argument("%s: %v", name, err)
		}
		return nil
	})
}

// WithRetry enables automatic retry on ErrSnapshotConflict up to n retries.
func WithRetry(n int) DatasetOption {
	return retryOption("WithRetry", func(r *retry.Config) error { return r.SetMaxAttempts(n) })
}

// WithRetryBaseDelay sets the base delay for exponential backoff on CAS retry.
func WithRetryBaseDelay(d time.Duration) DatasetOption {
	return retryOption("WithRetryBaseDelay", func(r *retry.Config) error { return r.SetBaseDelay(d) })
}

// WithRetryMaxDelay sets the maximum delay for exponential backoff on CAS retry.
func WithRetryMaxDelay(d time.Duration) DatasetOption {
	return retryOption("WithRetryMaxDelay", func(r *retry.Config) error { return r.SetMaxDelay(d) })
}

// WithRetryJitter sets the jitter factor [0, 1] for CAS retry backoff.
func WithRetryJitter(j float64) DatasetOption {
	return retryOption("WithRetryJitter", func(r *retry.Config) error { return r.SetJitter(j) })
}

// WithRetryObserver registers a synchronous callback invoked on each CAS retry.
func WithRetryObserver(fn func(attempt int)) DatasetOption {
	return retryOption("WithRetryObserver", func(r *retry.Config) error { return r.SetObserver(fn) })
}

func resolveDatasetConfig(opts []DatasetOption) (*datasetConfig, error) {
	cfg := &datasetConfig{layout: Unpartitioned(), compressor: Uncompressed(), retry: retry.Default()}
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

func resolveViewConfig(opts []ViewOption) (*viewConfig, error) {
	cfg := &viewConfig{anchor: headAnchor{}}
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

type dataset struct {
	id         DatasetID
	store      Store
	layout     *pathLayout
	codec      codecIdentity
	components components
	anchor     *Snapshot // immutable anchor retained when opened with WithAnchor
	history    history.History[SnapshotID, Manifest]
}

type writer struct {
	*dataset
	retry   retry.Config
	settled bool
}

type codecIdentity struct {
	name, description string
}

var blobCodec = codecIdentity{}

func codecOf[T any](c Codec[T]) codecIdentity {
	return codecIdentity{name: c.Name(), description: c.Describe()}
}

type datasetKind struct {
	id      DatasetID
	paths   *pathLayout
	store   Store
	expired tombstones
}

func newDatasetKind(id DatasetID, l *pathLayout, store Store) datasetKind {
	return datasetKind{id: id, paths: l, store: store, expired: tombstones{store: store, paths: l, dataset: id}}
}

func (k datasetKind) StagedPath(id SnapshotID) string {
	return k.paths.stagedManifestPath(k.id, id)
}

func (datasetKind) StagesCommit(expected string) bool { return expected == "" }

func (k datasetKind) SnapshotsPrefix() string { return k.paths.snapshotsPrefix(k.id) }
func (k datasetKind) ManifestPath(id SnapshotID) string {
	return k.paths.manifestPath(k.id, id)
}

func (k datasetKind) ParseManifest(p string) (SnapshotID, bool) {
	sp, ok := k.paths.manifest(p)
	return sp.snapshot, ok && sp.snapshot != ""
}

func (k datasetKind) NotExpired(ctx context.Context, id SnapshotID) (bool, error) {
	absent, err := k.expired.absent(ctx, id)
	if err != nil || absent {
		return absent, err
	}
	present, err := k.store.Exists(ctx, k.ManifestPath(id))
	if err != nil {
		return false, fmt.Errorf("failed to check manifest: %w", err)
	}
	return present, nil
}

func (k datasetKind) Decode(id SnapshotID, r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("lode: failed to decode manifest: %w", err)
	}
	if err := validateManifest(&m); err != nil {
		return nil, err
	}
	if m.DatasetID != k.id {
		return nil, &validate.ManifestError{
			Field:   "dataset_id",
			Message: fmt.Sprintf("got %q, want %q", m.DatasetID, k.id),
		}
	}
	if m.SnapshotID != id {
		return nil, &validate.ManifestError{
			Field:   "snapshot_id",
			Message: fmt.Sprintf("got %q, want %q", m.SnapshotID, id),
		}
	}
	return &m, nil
}

var (
	_ history.Kind[SnapshotID, Manifest] = datasetKind{}
	_ history.Stager[SnapshotID]         = datasetKind{}
)

func toSnapshot(s *history.Snapshot[SnapshotID, Manifest]) *Snapshot {
	return &Snapshot{ID: s.ID, Manifest: s.Manifest}
}

func fromHistory(s *history.Snapshot[SnapshotID, Manifest], err error) (*Snapshot, error) {
	if err != nil {
		return nil, err
	}
	return toSnapshot(s), nil
}

func newDataset(id DatasetID, store Store, l *pathLayout) *dataset {
	return &dataset{
		id:      id,
		store:   store,
		layout:  l,
		history: history.New(store, newDatasetKind(id, l, store), l.latestPointerPath(id), "manifest", historyErrors),
	}
}

// ID returns the dataset's identifier.
func (d *dataset) ID() DatasetID { return d.id }

// Snapshot loads a snapshot by ID. When opened with WithAnchor(id), the
// already-loaded immutable anchor snapshot is returned without another Get.
func (d *dataset) Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	if err := validateSnapshotID(id); err != nil {
		return nil, err
	}
	if d.anchor != nil && d.anchor.ID == id {
		return &Snapshot{ID: d.anchor.ID, Manifest: d.anchor.Manifest}, nil
	}
	return findSnapshot(ctx, []*dataset{d}, id)
}

func findSnapshot(ctx context.Context, candidates []*dataset, id SnapshotID) (*Snapshot, error) {
	canonical := func(d *dataset) (*Snapshot, error) { return loadedManifest(d.history.Snapshot(ctx, id)) }
	scanned := func(d *dataset) (*Snapshot, error) { return loadedManifest(d.history.Find(ctx, id)) }
	s, err := firstFound(candidates, canonical)
	if !isAbsence(err) {
		return s, err
	}
	if expired := candidates[0].tombstones().expiredError(ctx, id); expired != nil {
		return nil, expired
	}
	s, err = firstFound(candidates, scanned)
	if isAbsence(err) {
		if _, ptrErr := candidates[0].history.ReadPointer(ctx); errors.Is(ptrErr, ErrRemoved) {
			return nil, ptrErr
		}
	}
	return s, err
}

func loadedManifest(s *history.Snapshot[SnapshotID, Manifest], err error) (*Snapshot, error) {
	if err != nil && !isAbsence(err) {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	return fromHistory(s, err)
}

func firstFound(candidates []*dataset, load func(*dataset) (*Snapshot, error)) (*Snapshot, error) {
	var err error
	for _, d := range candidates {
		var s *Snapshot
		if s, err = load(d); !isAbsence(err) {
			return s, err
		}
	}
	return nil, err
}

func isAbsence(err error) bool {
	if errors.Is(err, ErrSnapshotExpired) {
		return false
	}
	return errors.Is(err, ErrNotFound) || errors.Is(err, ErrNoSnapshots)
}

func (s *Snapshot) partitioning() partitionScheme {
	return partitionScheme{name: partitioner(s.Partitioner), keys: s.PartitionKeys}
}

// Snapshots lists every committed snapshot, oldest first.
func (d *dataset) Snapshots(ctx context.Context) ([]*Snapshot, error) {
	all, err := d.history.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		if _, ptrErr := d.history.ReadPointer(ctx); errors.Is(ptrErr, ErrRemoved) {
			return nil, ptrErr
		}
	}
	snapshots := make([]*Snapshot, len(all))
	for i, s := range all {
		snapshots[i] = toSnapshot(s)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].CreatedAt.Before(snapshots[j].CreatedAt)
	})
	return snapshots, nil
}

// Latest returns the head snapshot without creating a missing pointer.
func (d *dataset) Latest(ctx context.Context) (*Snapshot, error) {
	return fromHistory(d.history.Head(ctx))
}

// Latest returns the head snapshot for a writer, creating a missing pointer
// from the scan result when absent.
func (d *writer) Latest(ctx context.Context) (*Snapshot, error) {
	return fromHistory(d.history.Latest(ctx))
}

// readObject reads one data object of a snapshot, verifying its stored-byte
// checksum when the manifest records a checksum algorithm and streaming the
// decompressed bytes to decode.
func (d *dataset) readObject(ctx context.Context, m *Manifest, obj ObjectRef, decode func(io.Reader) error) error {
	rc, err := d.openObjectStream(ctx, m, obj)
	if err != nil {
		return err
	}
	defer closer(rc)()

	if err := decode(rc); err != nil {
		return rc.verifyOnError(err)
	}
	return rc.verifyComplete()
}

type verifiedObjectStream struct {
	decompressed io.ReadCloser
	source       io.ReadCloser
	raw          io.Reader
	hasher       HashWriter
	obj          ObjectRef
	verified     bool
	verifyErr    error
}

func (v *verifiedObjectStream) Read(p []byte) (int, error) {
	n, err := v.decompressed.Read(p)
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		if vErr := v.verifyComplete(); vErr != nil {
			return n, vErr
		}
		return n, io.EOF
	}
	return n, v.verifyOnError(err)
}

func (v *verifiedObjectStream) verifyComplete() error {
	if v.verified {
		return v.verifyErr
	}
	v.verified = true
	v.verifyErr = verifyStoredBytes(v.raw, v.hasher, v.obj)
	return v.verifyErr
}

func (v *verifiedObjectStream) verifyOnError(cause error) error {
	if v.verified {
		if errors.Is(v.verifyErr, ErrChecksumMismatch) {
			return v.verifyErr
		}
		return cause
	}
	v.verified = true
	v.verifyErr = verifyStoredBytesOnError(v.raw, v.hasher, v.obj, cause)
	return v.verifyErr
}

func (v *verifiedObjectStream) Close() error {
	dErr := v.decompressed.Close()
	sErr := v.source.Close()
	if dErr != nil {
		return dErr
	}
	return sErr
}

func (d *dataset) openObjectStream(ctx context.Context, m *Manifest, obj ObjectRef) (*verifiedObjectStream, error) {
	compressor, err := d.compressorFor(m)
	if err != nil {
		return nil, err
	}
	var checksum Checksum
	if m.ChecksumAlgorithm != "" {
		checksum, err = d.checksumFor(m)
		if err != nil {
			return nil, err
		}
		if obj.Checksum == "" {
			return nil, fmt.Errorf("lode: object %s in snapshot %s is missing %s checksum: %w", obj.Path, m.SnapshotID, m.ChecksumAlgorithm, ErrChecksumMismatch)
		}
	}

	rc, err := d.store.Get(ctx, obj.Path)
	if err != nil {
		return nil, err
	}

	raw := io.Reader(rc)
	var hasher HashWriter
	if checksum != nil {
		hasher = checksum.NewHasher()
		raw = io.TeeReader(rc, hasher)
	}

	decompressed, err := compressor.Decompress(raw)
	if err != nil {
		vErr := verifyStoredBytesOnError(raw, hasher, obj, err)
		_ = rc.Close()
		return nil, vErr
	}
	return &verifiedObjectStream{
		decompressed: decompressed,
		source:       rc,
		raw:          raw,
		hasher:       hasher,
		obj:          obj,
	}, nil
}

func verifyStoredBytes(raw io.Reader, hasher HashWriter, obj ObjectRef) error {
	if hasher == nil {
		return nil
	}
	// Ensure any trailing framing bytes after the compressed payload are
	// consumed so the digest covers the complete stored object.
	if _, err := io.Copy(io.Discard, raw); err != nil {
		return fmt.Errorf("lode: failed to read trailing object bytes: %w", err)
	}
	if got := hasher.Sum(); got != obj.Checksum {
		return fmt.Errorf("lode: checksum mismatch for %s: got %s, want %s: %w", obj.Path, got, obj.Checksum, ErrChecksumMismatch)
	}
	return nil
}

func verifyStoredBytesOnError(raw io.Reader, hasher HashWriter, obj ObjectRef, cause error) error {
	if err := verifyStoredBytes(raw, hasher, obj); errors.Is(err, ErrChecksumMismatch) {
		return err
	}
	return cause
}

func (d *dataset) readBlob(ctx context.Context, id SnapshotID) ([]byte, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := matchCodec(snapshot.Manifest, blobCodec); err != nil {
		return nil, err
	}
	if len(snapshot.Objects) != 1 {
		return nil, fmt.Errorf("lode: blob snapshot must have exactly one object, got %d", len(snapshot.Objects))
	}
	obj := snapshot.Objects[0]
	if !d.layout.isObjectOf(d.id, snapshot.ID, dataLeaf, obj.Path) {
		return nil, &validate.ManifestError{
			Field:   "objects[0].path",
			Message: fmt.Sprintf("path %q is outside snapshot %s of dataset %s", obj.Path, snapshot.ID, d.id),
		}
	}
	var buf bytes.Buffer
	err = d.readObject(ctx, snapshot.Manifest, obj, func(r io.Reader) error {
		_, err := buf.ReadFrom(r)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read blob %s: %w", obj.Path, err)
	}
	return buf.Bytes(), nil
}

func readRecords[T any](ctx context.Context, d *dataset, codec Codec[T], id SnapshotID) ([]T, error) {
	snapshot, err := d.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := matchCodec(snapshot.Manifest, codecOf(codec)); err != nil {
		return nil, err
	}
	var all []T
	for i, obj := range snapshot.Objects {
		if !d.layout.isObjectOf(d.id, snapshot.ID, dataLeaf, obj.Path) {
			return nil, &validate.ManifestError{
				Field:   fmt.Sprintf("objects[%d].path", i),
				Message: fmt.Sprintf("path %q is outside snapshot %s of dataset %s", obj.Path, snapshot.ID, d.id),
			}
		}
		err := d.readObject(ctx, snapshot.Manifest, obj, func(r io.Reader) error {
			records, err := codec.Decode(r)
			all = append(all, records...)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read data object %s: %w", obj.Path, err)
		}
	}
	return all, nil
}

func matchCodec(m *Manifest, codec codecIdentity) error {
	if m.Codec != codec.name {
		return fmt.Errorf("lode: codec mismatch: snapshot %s uses %q, handle has %q: %w", m.SnapshotID, m.Codec, codec.name, ErrSettingsMismatch)
	}
	if !legacy(m) && m.CodecDescription != codec.description {
		return fmt.Errorf("lode: codec description mismatch: snapshot %s records %q, handle has %q: %w", m.SnapshotID, m.CodecDescription, codec.description, ErrSettingsMismatch)
	}
	return nil
}

var bundledCompressors = map[string]func() Compressor{
	"noop": Uncompressed,
	"gzip": Gzip,
	"zstd": Zstd,
}

func (d *dataset) compressorFor(m *Manifest) (Compressor, error) {
	if c := d.components.compressor; c != nil && c.Name() == m.Compressor {
		return c, nil
	}
	if newCompressor, ok := bundledCompressors[m.Compressor]; ok {
		return newCompressor(), nil
	}
	return nil, fmt.Errorf("lode: snapshot %s uses compressor %q, which is not bundled; pass it with WithCompressor: %w", m.SnapshotID, m.Compressor, ErrSettingsMismatch)
}

var bundledChecksums = map[string]func() Checksum{
	"sha256": SHA256,
	"crc32c": CRC32C,
	"md5":    MD5,
}

func (d *dataset) checksumFor(m *Manifest) (Checksum, error) {
	name := m.ChecksumAlgorithm
	if c := d.components.checksum; c != nil && c.Name() == name {
		return c, nil
	}
	if newChecksum, ok := bundledChecksums[name]; ok {
		return newChecksum(), nil
	}
	return nil, fmt.Errorf("lode: snapshot %s uses checksum %q, which is not bundled; pass it with WithChecksum: %w", m.SnapshotID, name, ErrSettingsMismatch)
}

func validateDatasetID(id DatasetID) error {
	if !storepath.Segment(string(id)) {
		return validate.Argument("dataset ID %q must be %s", id, storepath.SegmentRule)
	}
	return nil
}

func validateSnapshotID(id SnapshotID) error {
	if !storepath.Segment(string(id)) {
		return fmt.Errorf("lode: snapshot %q: a snapshot ID is %s: %w", id, storepath.SegmentRule, ErrNotFound)
	}
	return nil
}

var historyErrors = history.Errors{
	NotFound:         ErrNotFound,
	NoSnapshots:      ErrNoSnapshots,
	SnapshotConflict: ErrSnapshotConflict,
	HeadUncommitted:  ErrHeadUncommitted,
	Removed:          ErrRemoved,
	PathExists:       ErrPathExists,
}
