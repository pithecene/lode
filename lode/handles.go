package lode

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"

	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Writer handles
// -----------------------------------------------------------------------------

// Records opens the dataset id for writing records of type T, which the
// codec fixes: Go infers T from it. Opening reads the dataset's head under
// ctx and checks the handle's settings against the head's manifest
// (CONTRACT_WRITE_API §Settings); it writes nothing. A writer whose
// settings differ is rejected with ErrSettingsMismatch.
func (r *Root) Records[T any](ctx context.Context, id DatasetID, codec Codec[T], opts ...DatasetOption) (Records[T], error) {
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
	return &recordDataset[T]{writer: d, codec: codec, partitionKey: partitionKey, timestamped: timestampedRecords[T]()}, nil
}

// Blobs opens the dataset id for writing blobs, one per snapshot. A blob has
// no fields to partition by, so its layout is Unpartitioned. Opening checks
// settings as Records does.
func (r *Root) Blobs(ctx context.Context, id DatasetID, opts ...DatasetOption) (Blobs, error) {
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
	return &blobDataset{writer: d}, nil
}

// newWriter validates a writer's arguments and assembles its core.
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
	d := &writer{dataset: newDataset(id, r.store, newPathLayout(r.names, scheme)), retry: cfg.retry}
	d.components = components{compressor: cfg.compressor, checksum: cfg.checksum}
	return d, cfg, nil
}

// checkAtOpen resolves the head and checks the writer's settings against
// it, so that a misconfigured writer fails when it opens rather than at its
// first commit. The commit checks again: the head may change in between.
func (d *writer) checkAtOpen(ctx context.Context) error {
	parent, _, err := d.history.Resolve(ctx)
	if err != nil {
		return err
	}
	return d.checkSettings(ctx, parent)
}

// checkCodec rejects a nil codec and reports a construction error that a
// codec recorded (Parquet with an invalid schema).
func checkCodec[T any](codec Codec[T]) error {
	if validate.Nil(codec) {
		return validate.Argument("codec is required")
	}
	// A manifest without a codec name records a blob dataset.
	if codec.Name() == "" {
		return validate.Argument("codec name must not be empty: an empty name denotes a blob dataset")
	}
	if c, ok := codec.(interface{ configError() error }); ok {
		return c.configError()
	}
	return nil
}

// timestampedRecords reports whether records of type T may implement
// Timestamped: the type does, or it is an interface a record may satisfy.
// Other types are never observed, so a write converts no record to an
// interface for the check.
func timestampedRecords[T any]() bool {
	t := reflect.TypeFor[T]()
	return t.Kind() == reflect.Interface || t.Implements(reflect.TypeFor[Timestamped]())
}

// recordDataset implements Records[T].
type recordDataset[T any] struct {
	*writer
	codec        Codec[T]
	partitionKey func(T) (string, error)
	timestamped  bool
}

func (r *recordDataset[T]) Read(ctx context.Context, id DatasetSnapshotID) ([]T, error) {
	return readRecords(ctx, r.dataset, r.codec, id)
}

func (r *recordDataset[T]) Write(ctx context.Context, records []T, metadata any) (*DatasetSnapshot, error) {
	// Data files are written once; on a commit conflict only the manifest is
	// re-parented, never the data re-uploaded.
	pending, err := r.newPending(metadata)
	if err != nil {
		return nil, err
	}
	if err := stageRecords(ctx, r, &pending, records); err != nil {
		return nil, err
	}
	return r.commit(ctx, &pending)
}

func (r *recordDataset[T]) StreamWrite(ctx context.Context, records iter.Seq2[T, error], metadata any) (*DatasetSnapshot, error) {
	if records == nil {
		return nil, ErrNilIterator
	}
	streaming, ok := r.codec.(StreamingCodec[T])
	if !ok {
		return nil, ErrCodecNotStreamable
	}
	// A single streamed file cannot be partitioned: that would require
	// buffering the records to determine their keys.
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

// blobDataset implements Blobs.
type blobDataset struct {
	*writer
}

func (b *blobDataset) Read(ctx context.Context, id DatasetSnapshotID) ([]byte, error) {
	return b.readBlob(ctx, id)
}

func (b *blobDataset) Write(ctx context.Context, data []byte, metadata any) (*DatasetSnapshot, error) {
	pending, err := b.newPending(metadata)
	if err != nil {
		return nil, err
	}
	if err := b.stageBlob(ctx, &pending, data); err != nil {
		return nil, err
	}
	return b.commit(ctx, &pending)
}

func (b *blobDataset) StreamWrite(ctx context.Context, metadata any) (StreamWriter, error) {
	pending, err := b.newPending(metadata)
	if err != nil {
		return nil, err
	}
	// Surface an unreadable head now rather than at Commit.
	if _, _, err := b.history.Resolve(ctx); err != nil {
		return nil, err
	}
	stream, err := b.openStream(ctx, pending.id, blobFile)
	if err != nil {
		return nil, err
	}
	return &streamWriter{ds: b.writer, ctx: ctx, pending: pending, stream: stream}, nil
}

// -----------------------------------------------------------------------------
// Views
// -----------------------------------------------------------------------------

// Records opens a view of the dataset id's records of type T. The view
// takes the dataset's layout from its manifests, and reads each snapshot
// with the compressor its manifest names: a bundled one by name, or the
// custom one passed with WithCompressor. Opening reads the head (one Get of
// the pointer and at most two of its manifest) and writes nothing; it
// returns ErrNotFound for a dataset without snapshots, and
// ErrSettingsMismatch when the head was written with another codec or path
// names.
func (r *ReadRoot) Records[T any](ctx context.Context, id DatasetID, codec Codec[T], opts ...ViewOption) (RecordView[T], error) {
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
	return &recordView[T]{dataset: d, codec: codec}, nil
}

// Blobs opens a view of the dataset id's blobs, as Records does.
func (r *ReadRoot) Blobs(ctx context.Context, id DatasetID, opts ...ViewOption) (BlobView, error) {
	if r.err != nil {
		return nil, r.err
	}
	d, err := r.openView(ctx, id, opts, blobCodec)
	if err != nil {
		return nil, err
	}
	return &blobView{dataset: d}, nil
}

// openView resolves a view's layout from the dataset's head and checks the
// head against the view's codec and the root's names.
func (r *ReadRoot) openView(ctx context.Context, id DatasetID, opts []ViewOption, codec codecIdentity) (*dataset, error) {
	if err := validateDatasetID(id); err != nil {
		return nil, err
	}
	cfg, err := resolveViewConfig(opts)
	if err != nil {
		return nil, err
	}
	head, scheme, err := resolveViewHead(ctx, r.store, r.names, id)
	if err != nil {
		return nil, err
	}
	if err := validateManifest(head); err != nil {
		return nil, fmt.Errorf("lode: head of %s: %w", id, err)
	}
	if err := matchCodec(head, codec); err != nil {
		return nil, err
	}
	recorded, err := recordedSettings(head, r.names.Flat)
	if err != nil {
		return nil, err
	}
	if want, have := r.names, recorded.paths; have != want {
		return nil, fmt.Errorf("lode: path names mismatch: dataset records %#v, root has %#v: %w", have, want, ErrSettingsMismatch)
	}
	d := newDataset(id, r.store, newPathLayout(r.names, scheme))
	d.components = *cfg
	d.codec = codec
	return d, nil
}

// hiveGrammar stands for every Hive scheme while a view's layout is not yet
// known: reading needs only the grammar, not the keys.
var hiveGrammar = partitionScheme{name: hivePartitioner}

// resolveViewHead finds the dataset's head manifest without knowing its
// layout. The pointer names the head but not whether the dataset is
// partitioned, so the canonical manifest is read under the snapshots
// directory, then the segments directory: because the names are distinct,
// at most one exists. Without a pointer, or when the pointer names a
// snapshot whose manifest is not persisted, each grammar's scan selects the
// head, as Latest does, without creating the pointer. It returns the head's
// manifest and the partitioning it records; ErrSettingsMismatch when the
// dataset's directory holds a manifest under other path names; ErrNotFound
// when the dataset has no snapshot.
func resolveViewHead(ctx context.Context, store Store, names ManifestPaths, id DatasetID) (*Manifest, partitionScheme, error) {
	candidates := []*dataset{
		newDataset(id, store, newPathLayout(names, noopScheme)),
		newDataset(id, store, newPathLayout(names, hiveGrammar)),
	}
	pointed, err := candidates[0].history.ReadPointer(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, partitionScheme{}, err
	}
	loaders := make([]func(*dataset) (*DatasetSnapshot, error), 0, 2)
	if err == nil {
		loaders = append(loaders, func(d *dataset) (*DatasetSnapshot, error) { return fromHistory(d.history.Snapshot(ctx, pointed)) })
	}
	loaders = append(loaders, func(d *dataset) (*DatasetSnapshot, error) { return fromHistory(d.history.Newest(ctx)) })
	for _, load := range loaders {
		for _, d := range candidates {
			s, err := load(d)
			if err == nil {
				return s.Manifest, partitionScheme{name: partitioner(s.Partitioner), keys: s.PartitionKeys}, nil
			}
			if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrNoSnapshots) {
				return nil, partitionScheme{}, fmt.Errorf("lode: failed to resolve the head of %s: %w", id, err)
			}
		}
	}
	if err := rejectForeignManifest(ctx, store, id, candidates[0].layout, candidates[1].layout); err != nil {
		return nil, partitionScheme{}, err
	}
	return nil, partitionScheme{}, fmt.Errorf("lode: dataset %s has no snapshot: %w", id, ErrNotFound)
}

// recordView implements RecordView[T].
type recordView[T any] struct {
	*dataset
	codec Codec[T]
}

func (v *recordView[T]) Read(ctx context.Context, id DatasetSnapshotID) ([]T, error) {
	return readRecords(ctx, v.dataset, v.codec, id)
}

// blobView implements BlobView.
type blobView struct {
	*dataset
}

func (v *blobView) Read(ctx context.Context, id DatasetSnapshotID) ([]byte, error) {
	return v.readBlob(ctx, id)
}

// The handles implement the public interfaces; stated so a missing method
// fails the build here.
var (
	_ Records[D]    = (*recordDataset[D])(nil)
	_ Blobs         = (*blobDataset)(nil)
	_ RecordView[D] = (*recordView[D])(nil)
	_ BlobView      = (*blobView)(nil)
)
