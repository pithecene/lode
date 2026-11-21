package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"reflect"

	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Concrete Dataset Handles & Views
// -----------------------------------------------------------------------------

// RecordView reads a dataset of records of type T. ReadRoot.Records returns
// a *RecordView[T].
type RecordView[T any] struct {
	*dataset
	codec Codec[T]
}

// Read returns every record of snapshot id, decoded with the view's codec and
// decompressed and checksum-verified according to the snapshot's manifest.
func (v *RecordView[T]) Read(ctx context.Context, id SnapshotID) ([]T, error) {
	return readRecords(ctx, v.dataset, v.codec, id)
}

// Records writes, reads, and maintains retention on a dataset of records of
// type T. Root.Records returns a *Records[T]. A handle is not safe for
// concurrent use by multiple goroutines; open separate handles for concurrent
// writers (CONTRACT_WRITE_API §Concurrency).
type Records[T any] struct {
	*writer
	codec        Codec[T]
	partitionKey func(T) (string, error)
	timestamped  bool
}

// Read returns every record of snapshot id, decoded with the handle's codec.
func (r *Records[T]) Read(ctx context.Context, id SnapshotID) ([]T, error) {
	return readRecords(ctx, r.dataset, r.codec, id)
}

// Write commits records and metadata as an immutable snapshot. metadata is any
// value that encodes as a JSON object (a struct, a map, or nil, which persists
// as {}).
func (r *Records[T]) Write(ctx context.Context, records []T, metadata any) (*Snapshot, error) {
	pending, err := r.newPending(metadata)
	if err != nil {
		return nil, err
	}
	if err := stageRecords(ctx, r, &pending, records); err != nil {
		return nil, err
	}
	return r.commit(ctx, &pending)
}

// StreamWrite consumes records from an iterator and streams them through the
// codec, which must implement StreamingCodec[T], into one data object. An
// error yielded by the iterator aborts the write without creating a snapshot.
// Partitioned layouts are rejected with ErrPartitioningNotSupported.
func (r *Records[T]) StreamWrite(ctx context.Context, records iter.Seq2[T, error], metadata any) (*Snapshot, error) {
	if records == nil {
		return nil, validate.Argument("records iterator must not be nil")
	}
	streaming, ok := r.codec.(StreamingCodec[T])
	if !ok {
		return nil, ErrCodecNotStreamable
	}
	if !r.layout.partitioning().isNoop() {
		return nil, ErrPartitioningNotSupported
	}
	pending, err := r.newPending(metadata)
	if err != nil {
		return nil, err
	}
	stream, err := openRecordStream(ctx, r.writer, pending.id, streaming, r.timestamped)
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
	pending.timestamps = stream.timestamps
	pending.streamed = ref.Path
	return r.commit(ctx, &pending)
}

// BlobView reads a dataset whose snapshots each hold one raw binary object.
// ReadRoot.Blobs returns a *BlobView.
type BlobView struct {
	*dataset
}

// Read returns the raw bytes of snapshot id's blob.
func (v *BlobView) Read(ctx context.Context, id SnapshotID) ([]byte, error) {
	return v.readBlob(ctx, id)
}

// Blobs writes, streams, reads, and maintains retention on a dataset whose
// snapshots each hold one raw binary object. Root.Blobs returns a *Blobs.
type Blobs struct {
	*writer
}

// Read returns the raw bytes of snapshot id's blob.
func (b *Blobs) Read(ctx context.Context, id SnapshotID) ([]byte, error) {
	return b.readBlob(ctx, id)
}

// Write commits one in-memory blob and metadata as an immutable snapshot.
func (b *Blobs) Write(ctx context.Context, data []byte, metadata any) (*Snapshot, error) {
	pending, err := b.newPending(metadata)
	if err != nil {
		return nil, err
	}
	if err := b.stageBlob(ctx, &pending, data); err != nil {
		return nil, err
	}
	return b.commit(ctx, &pending)
}

// StreamWrite streams bytes from r into a single immutable blob object and
// commits the snapshot when r reaches io.EOF. If r fails or ctx is canceled,
// no snapshot is committed.
func (b *Blobs) StreamWrite(ctx context.Context, r io.Reader, metadata any) (*Snapshot, error) {
	if validate.Nil(r) {
		return nil, validate.Argument("blob reader must not be nil")
	}
	pending, err := b.newPending(metadata)
	if err != nil {
		return nil, err
	}
	if _, _, err := b.history.Resolve(ctx); err != nil {
		return nil, err
	}
	ref, err := b.streamBlob(ctx, pending.id, r)
	if err != nil {
		return nil, err
	}
	pending.add(ref, "")
	pending.rowCount = 1
	pending.streamed = ref.Path
	return b.commit(ctx, &pending)
}

// -----------------------------------------------------------------------------
// Root handle constructors
// -----------------------------------------------------------------------------

// Records opens dataset id for writing records of type T, inferred from codec.
func (r *Root) Records[T any](ctx context.Context, id DatasetID, codec Codec[T], opts ...DatasetOption) (*Records[T], error) {
	if r.err != nil {
		return nil, r.err
	}
	if err := checkCodec(codec); err != nil {
		return nil, err
	}
	d, cfg, err := r.newWriter(id, opts)
	if err != nil {
		return nil, err
	}
	partitionKey, err := partitionKeyFor[T](cfg.layout)
	if err != nil {
		return nil, err
	}
	d.codec = codecOf(codec)
	if err := d.checkAtOpen(ctx); err != nil {
		return nil, err
	}
	return &Records[T]{writer: d, codec: codec, partitionKey: partitionKey, timestamped: timestampedRecords[T]()}, nil
}

// Blobs opens dataset id for writing blobs, one per snapshot.
func (r *Root) Blobs(ctx context.Context, id DatasetID, opts ...DatasetOption) (*Blobs, error) {
	if r.err != nil {
		return nil, r.err
	}
	d, _, err := r.newWriter(id, opts)
	if err != nil {
		return nil, err
	}
	if !d.layout.partitioning().isNoop() {
		return nil, validate.Argument("a blob dataset cannot be partitioned: use Unpartitioned")
	}
	if err := d.checkAtOpen(ctx); err != nil {
		return nil, err
	}
	return &Blobs{writer: d}, nil
}

func (r *Root) newWriter(id DatasetID, opts []DatasetOption) (*writer, *datasetConfig, error) {
	if err := validateDatasetID(id); err != nil {
		return nil, nil, err
	}
	cfg, err := resolveDatasetConfig(opts)
	if err != nil {
		return nil, nil, err
	}
	scheme, err := cfg.layout.scheme()
	if err != nil {
		return nil, nil, err
	}
	d := &writer{dataset: newDataset(id, r.store, newPathLayout(r.flat, scheme)), retry: cfg.retry, recorded: cfg.recorded}
	d.components = components{compressor: cfg.compressor, checksum: cfg.checksum}
	return d, cfg, nil
}

func (d *writer) checkAtOpen(ctx context.Context) error {
	parent, _, err := d.history.Resolve(ctx)
	if err != nil {
		return err
	}
	if !d.recorded || parent == "" {
		return d.checkSettings(ctx, parent)
	}
	head, err := d.settingsManifest(ctx, parent)
	if err != nil {
		return err
	}
	if err := d.adoptRecordedComponents(head); err != nil {
		return err
	}
	return d.checkManifestSettings(ctx, head)
}

func (d *writer) adoptRecordedComponents(m *Manifest) error {
	compressor, err := d.compressorFor(m)
	if err != nil {
		return err
	}
	var checksum Checksum
	if m.ChecksumAlgorithm != "" {
		if checksum, err = d.checksumFor(m); err != nil {
			return err
		}
	}
	d.components = components{compressor: compressor, checksum: checksum}
	return nil
}

func checkCodec[T any](codec Codec[T]) error {
	if validate.Nil(codec) {
		return validate.Argument("codec is required")
	}
	if codec.Name() == "" {
		return validate.Argument("codec name must not be empty: an empty name denotes a blob dataset")
	}
	if c, ok := codec.(interface{ configError() error }); ok {
		return c.configError()
	}
	return nil
}

func timestampedRecords[T any]() bool {
	t := reflect.TypeFor[T]()
	return t.Kind() == reflect.Interface || t.Implements(reflect.TypeFor[Timestamped]())
}

// -----------------------------------------------------------------------------
// Views
// -----------------------------------------------------------------------------

// Records opens a read-only view of dataset id's records of type T.
func (r *ReadRoot) Records[T any](ctx context.Context, id DatasetID, codec Codec[T], opts ...ViewOption) (*RecordView[T], error) {
	if r.err != nil {
		return nil, r.err
	}
	if err := checkCodec(codec); err != nil {
		return nil, err
	}
	d, err := r.openView(ctx, id, opts, codecOf(codec))
	if err != nil {
		return nil, err
	}
	return &RecordView[T]{dataset: d, codec: codec}, nil
}

// Blobs opens a read-only view of dataset id's blobs.
func (r *ReadRoot) Blobs(ctx context.Context, id DatasetID, opts ...ViewOption) (*BlobView, error) {
	if r.err != nil {
		return nil, r.err
	}
	d, err := r.openView(ctx, id, opts, blobCodec)
	if err != nil {
		return nil, err
	}
	return &BlobView{dataset: d}, nil
}

func (r *ReadRoot) openView(ctx context.Context, id DatasetID, opts []ViewOption, codec codecIdentity) (*dataset, error) {
	if err := validateDatasetID(id); err != nil {
		return nil, err
	}
	cfg, err := resolveViewConfig(opts)
	if err != nil {
		return nil, err
	}
	opened, err := resolveAnchor(ctx, r.store, id, viewCandidates(r.store, r.flat, id, cfg.layout), cfg.anchor)
	if err != nil {
		return nil, err
	}
	recorded, err := r.checkOpened(opened.Manifest, codec, fmt.Sprintf("%s of %s", cfg.anchor, id))
	if err != nil {
		return nil, err
	}
	d := newDataset(id, r.store, newPathLayout(r.flat, cfg.partitioning(opened)))
	if err := d.checkExpectedLayout(ctx, cfg.layout, opened.Manifest, recorded); err != nil {
		return nil, err
	}
	d.components = cfg.components
	d.codec = codec
	if _, anchored := cfg.anchor.(snapshotAnchor); anchored {
		d.anchorID = opened.ID
		d.anchorManifest, err = json.Marshal(opened.Manifest)
		if err != nil {
			return nil, err
		}
		d.anchorLegacyPaths = opened.legacyPaths
	}
	return d, nil
}

func (r *ReadRoot) checkOpened(m *Manifest, codec codecIdentity, at string) (settings, error) {
	if err := validateManifest(m); err != nil {
		return settings{}, fmt.Errorf("%s: %w", at, err)
	}
	if err := matchCodec(m, codec); err != nil {
		return settings{}, err
	}
	recorded, err := recordedSettings(m, r.flat)
	if err != nil {
		return settings{}, err
	}
	if recorded.customV11Paths != nil {
		return settings{}, fmt.Errorf("lode: path names mismatch: dataset records custom 1.1.0 paths %#v: %w", *recorded.customV11Paths, ErrSettingsMismatch)
	}
	if r.flat != recorded.flat {
		return settings{}, fmt.Errorf("lode: flat paths mismatch: dataset records flat=%t, root has flat=%t: %w", recorded.flat, r.flat, ErrSettingsMismatch)
	}
	return recorded, nil
}

func (cfg *viewConfig) partitioning(opened *Snapshot) partitionScheme {
	if cfg.layout != nil {
		return *cfg.layout
	}
	return opened.partitioning()
}

func resolveAnchor(ctx context.Context, store Store, id DatasetID, cores viewCores, anchor viewAnchor) (*Snapshot, error) {
	opened, err := anchor.resolve(ctx, cores)
	if !isAbsence(err) {
		return opened, err
	}
	layouts := make([]*pathLayout, 0, len(cores.search))
	for _, c := range cores.search {
		layouts = append(layouts, c.layout)
	}
	if foreign := rejectForeignManifest(ctx, store, id, layouts...); foreign != nil {
		return nil, foreign
	}
	return nil, err
}

type viewAnchor interface {
	resolve(ctx context.Context, cores viewCores) (*Snapshot, error)
	String() string
}

type headAnchor struct{}

func (headAnchor) resolve(ctx context.Context, cores viewCores) (*Snapshot, error) {
	id := cores.search[0].id
	pointed, err := cores.search[0].history.ReadPointer(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if err == nil {
		s, err := firstFound(cores.search, func(d *dataset) (*Snapshot, error) { return fromHistory(d.history.Snapshot(ctx, pointed)) })
		if err == nil {
			return s, nil
		}
		if !isAbsence(err) {
			return nil, fmt.Errorf("failed to resolve the head of %s: %w", id, err)
		}
		if err := cores.rejectExcluded(ctx, pointed); err != nil {
			return nil, err
		}
	}
	s, err := firstFound(cores.search, func(d *dataset) (*Snapshot, error) { return fromHistory(d.history.Newest(ctx)) })
	if err == nil {
		return s, nil
	}
	if !isAbsence(err) {
		return nil, fmt.Errorf("failed to resolve the head of %s: %w", id, err)
	}
	return nil, fmt.Errorf("lode: dataset %s has no snapshot: %w", id, ErrNotFound)
}

func (headAnchor) String() string { return "head" }

// snapshotAnchor opens a view at one named snapshot (WithAnchor) and retains
// that single immutable manifest on the view so Snapshot(id)/Read(id) avoids a
// redundant second Get.
type snapshotAnchor struct {
	id SnapshotID
}

func (a snapshotAnchor) resolve(ctx context.Context, cores viewCores) (*Snapshot, error) {
	id := cores.search[0].id
	if err := cores.search[0].history.CheckLive(ctx); err != nil {
		return nil, err
	}
	s, err := findSnapshot(ctx, cores.search, a.id)
	if err == nil {
		return s, nil
	}
	if errors.Is(err, ErrSnapshotExpired) {
		return nil, err
	}
	if isAbsence(err) {
		return nil, fmt.Errorf("lode: snapshot %s of %s: %w", a.id, id, ErrNotFound)
	}
	return nil, fmt.Errorf("failed to resolve snapshot %s of %s: %w", a.id, id, err)
}

func (a snapshotAnchor) String() string { return "snapshot " + string(a.id) }

var hiveGrammar = partitionScheme{name: hivePartitioner}

type viewCores struct {
	search   []*dataset
	excluded []*dataset
}

func viewCandidates(store Store, flat bool, id DatasetID, expected *partitionScheme) viewCores {
	core := func(s partitionScheme) *dataset { return newDataset(id, store, newPathLayout(flat, s)) }
	if expected == nil {
		return viewCores{search: []*dataset{core(noopScheme), core(hiveGrammar)}}
	}
	other := hiveGrammar
	if !expected.isNoop() {
		other = noopScheme
	}
	return viewCores{search: []*dataset{core(*expected)}, excluded: []*dataset{core(other)}}
}

func (c viewCores) rejectExcluded(ctx context.Context, id SnapshotID) error {
	for _, d := range c.excluded {
		p := d.layout.manifestPath(d.id, id)
		exists, err := d.store.Exists(ctx, p)
		if err != nil {
			return fmt.Errorf("failed to resolve the head of %s: %w", d.id, err)
		}
		if exists {
			return fmt.Errorf("lode: layout mismatch: the head %s has the manifest %s, which the view's layout does not persist: %w", id, p, ErrSettingsMismatch)
		}
	}
	return nil
}
