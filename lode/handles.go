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
// takes the dataset's layout from its manifests, unless WithLayout states
// the layout it expects, and reads each snapshot with the compressor its
// manifest names: a bundled one by name, or the custom one passed with
// WithCompressor. Opening reads the head (one Get of the pointer and at
// most two of its manifest, or one with WithLayout) and writes nothing; it
// returns ErrNotFound for a dataset without snapshots, and
// ErrSettingsMismatch when the head was written with another codec, path
// names, or layout than the view expects.
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

// openView resolves the snapshot at which the view opens (its anchor),
// takes the view's layout from that snapshot or checks the layout the view
// expects against it, and checks the snapshot against the view's codec and
// the root's names.
func (r *ReadRoot) openView(ctx context.Context, id DatasetID, opts []ViewOption, codec codecIdentity) (*dataset, error) {
	if err := validateDatasetID(id); err != nil {
		return nil, err
	}
	cfg, err := resolveViewConfig(opts)
	if err != nil {
		return nil, err
	}
	opened, err := resolveAnchor(ctx, r.store, id, viewCandidates(r.store, r.names, id, cfg.layout), cfg.anchor)
	if err != nil {
		return nil, err
	}
	recorded, err := r.checkOpened(opened.Manifest, codec, fmt.Sprintf("%s of %s", cfg.anchor, id))
	if err != nil {
		return nil, err
	}
	d := newDataset(id, r.store, newPathLayout(r.names, cfg.partitioning(opened)))
	if err := d.checkExpectedLayout(ctx, cfg.layout, opened.Manifest, recorded); err != nil {
		return nil, err
	}
	d.components = cfg.components
	d.codec = codec
	return d, nil
}

// checkOpened validates m, the manifest of the snapshot at which a view
// opens, which a validation error names as at, and checks m against the
// view's codec and the root's names. It returns the settings m records.
func (r *ReadRoot) checkOpened(m *Manifest, codec codecIdentity, at string) (settings, error) {
	if err := validateManifest(m); err != nil {
		return settings{}, fmt.Errorf("%s: %w", at, err)
	}
	if err := matchCodec(m, codec); err != nil {
		return settings{}, err
	}
	recorded, err := recordedSettings(m, r.names.Flat)
	if err != nil {
		return settings{}, err
	}
	if want, have := r.names, recorded.paths; have != want {
		return settings{}, fmt.Errorf("lode: path names mismatch: dataset records %#v, root has %#v: %w", have, want, ErrSettingsMismatch)
	}
	return recorded, nil
}

// partitioning is the view's partitioning: the layout it expects, or the
// one the snapshot it opened at records.
func (cfg *viewConfig) partitioning(opened *DatasetSnapshot) partitionScheme {
	if cfg.layout != nil {
		return *cfg.layout
	}
	return opened.partitioning()
}

// resolveAnchor finds the anchor's snapshot of dataset id in the cores that
// the view searches. When the anchor has no snapshot, it returns
// ErrSettingsMismatch if the dataset's directory holds a manifest that no
// searched core's layout persists, under other path names or another
// layout, and the anchor's absence otherwise.
func resolveAnchor(ctx context.Context, store Store, id DatasetID, cores viewCores, anchor viewAnchor) (*DatasetSnapshot, error) {
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

// viewAnchor is the snapshot at which a view opens: the dataset's head
// (headAnchor) or the snapshot that WithPinnedSnapshot names (snapshotAnchor).
type viewAnchor interface {
	// resolve finds the anchor's snapshot in the cores that the view
	// searches: one core per grammar, because a view without WithLayout does
	// not know its layout until it reads a manifest, or the core of the
	// layout it expects. It returns an absence (isAbsence) when the anchor
	// has no snapshot.
	resolve(ctx context.Context, cores viewCores) (*DatasetSnapshot, error)

	// String names the anchor in error messages.
	String() string
}

// headAnchor opens a view at the dataset's head.
type headAnchor struct{}

// resolve finds the head without knowing the layout. The pointer names the
// head but not whether the dataset is partitioned, so the head's canonical
// manifest is read under each searched grammar. Without a pointer, or when
// the pointer names a snapshot whose manifest is not persisted, each
// grammar's scan selects the head, as Latest does, without creating the
// pointer. A view that expects a layout first rejects a head persisted
// under another one (viewCores.rejectExcluded), which its scan would pass
// over for an older snapshot.
func (headAnchor) resolve(ctx context.Context, cores viewCores) (*DatasetSnapshot, error) {
	id := cores.search[0].id
	pointed, err := cores.search[0].history.ReadPointer(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	// If-chains, not tagless switches: gremlins reports switch case
	// expressions as not covered (go-gremlins/gremlins#298).
	if err == nil {
		s, err := firstFound(cores.search, func(d *dataset) (*DatasetSnapshot, error) { return fromHistory(d.history.Snapshot(ctx, pointed)) })
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
	s, err := firstFound(cores.search, func(d *dataset) (*DatasetSnapshot, error) { return fromHistory(d.history.Newest(ctx)) })
	if err == nil {
		return s, nil
	}
	if !isAbsence(err) {
		return nil, fmt.Errorf("failed to resolve the head of %s: %w", id, err)
	}
	return nil, fmt.Errorf("lode: dataset %s has no snapshot: %w", id, ErrNotFound)
}

func (headAnchor) String() string { return "head" }

// snapshotAnchor opens a view at one named snapshot. The view keeps none of
// it: like any view, it reads each snapshot from the store when asked.
type snapshotAnchor struct {
	id DatasetSnapshotID
}

// resolve finds the snapshot as Snapshot does (findSnapshot), with the
// searched cores in place of the handle's own core.
func (a snapshotAnchor) resolve(ctx context.Context, cores viewCores) (*DatasetSnapshot, error) {
	id := cores.search[0].id
	s, err := findSnapshot(ctx, cores.search, a.id)
	// An if-chain, not a tagless switch, as in headAnchor.resolve.
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

// hiveGrammar stands for every Hive scheme while a view's layout is not yet
// known: reading needs only the grammar, not the keys.
var hiveGrammar = partitionScheme{name: hivePartitioner}

// viewCores are the dataset cores of a view's search. A canonical manifest
// lies under the snapshots directory or the segments directory; because
// the names are distinct, at most one exists.
type viewCores struct {
	// search holds the cores in which the view looks for its snapshot: the
	// core of the layout it expects, or one core of each grammar when it
	// expects none.
	search []*dataset

	// excluded holds the core of the grammar that an expected layout rules
	// out, empty when the view expects no layout.
	excluded []*dataset
}

// viewCandidates returns the cores of a view of dataset id that expects
// the layout expected (nil when it expects none).
func viewCandidates(store Store, names ManifestPaths, id DatasetID, expected *partitionScheme) viewCores {
	core := func(s partitionScheme) *dataset { return newDataset(id, store, newPathLayout(names, s)) }
	if expected == nil {
		return viewCores{search: []*dataset{core(noopScheme), core(hiveGrammar)}}
	}
	other := hiveGrammar
	if !expected.isNoop() {
		other = noopScheme
	}
	return viewCores{search: []*dataset{core(*expected)}, excluded: []*dataset{core(other)}}
}

// rejectExcluded returns ErrSettingsMismatch when snapshot id, which the
// pointer names, has a canonical manifest under an excluded grammar: the
// head was persisted under another layout than the one the view expects.
// It checks with one Exists per excluded core, which is called only when
// the expected layout holds no manifest of id.
func (c viewCores) rejectExcluded(ctx context.Context, id DatasetSnapshotID) error {
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
