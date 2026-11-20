package lode

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Roots
// -----------------------------------------------------------------------------

// Root opens the datasets of one store for writing. Open returns one. Every
// dataset of a root shares the root's one store value and its path topology.
// A Root is safe for concurrent use; the handles it returns are not
// (CONTRACT_WRITE_API §Concurrency).
type Root struct {
	store Store
	flat  bool
	err   error // a factory or option error, which every method returns
}

// ReadRoot opens the datasets of one store for reading. OpenReadOnly
// returns one. Its views never write, so its store need not provide
// CompareAndSwap.
type ReadRoot struct {
	store Store // the store, behind readOnlyStore
	flat  bool
	err   error
}

// Open calls the factory once and returns a root over the store it creates.
// A factory error, or a nil option, is recorded and returned by every method
// of the root, so a caller checks one error:
//
//	events, err := lode.Open(lode.FS("/data")).Records(ctx, "events", lode.JSONL[Event]())
func Open[S Store](f Factory[S], opts ...RootOption) *Root {
	store, flat, err := openStore(f, opts)
	if err != nil {
		return &Root{err: err}
	}
	return &Root{store: store, flat: flat}
}

// OpenReadOnly calls the factory once and returns a root whose views read
// the store's datasets and never write. Errors are recorded as by Open.
func OpenReadOnly[S ObjectStore](f Factory[S], opts ...RootOption) *ReadRoot {
	store, flat, err := openStore(f, opts)
	if err != nil {
		return &ReadRoot{err: err}
	}
	return &ReadRoot{store: newReadOnlyStore(store), flat: flat}
}

func openStore[S ObjectStore](f Factory[S], opts []RootOption) (S, bool, error) {
	var zero S
	flat, err := resolveRootConfig(opts)
	if err != nil {
		return zero, false, err
	}
	if f == nil {
		return zero, false, validate.Argument("store factory is required")
	}
	store, err := f()
	if err != nil {
		if errors.Is(err, ErrInvalidArgument) {
			return zero, false, err
		}
		return zero, false, fmt.Errorf("store factory failed: %w", err)
	}
	if validate.Nil(store) {
		return zero, false, validate.Argument("store factory returned nil store")
	}
	return store, flat, nil
}

// ListDatasets returns the IDs of the root's committed datasets in
// lexicographic order. Returns ErrDatasetsNotModeled when opened with
// WithFlatPaths().
func (r *Root) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	if r.err != nil {
		return nil, r.err
	}
	return listDatasets(ctx, r.store, r.flat, opts)
}

// ListDatasets returns the IDs of the root's committed datasets, as
// Root.ListDatasets does.
func (r *ReadRoot) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	if r.err != nil {
		return nil, r.err
	}
	return listDatasets(ctx, r.store, r.flat, opts)
}

// RemoveDataset permanently removes dataset id from the root's store. It
// first replaces the dataset's head pointer with the removal marker, so
// subsequent opens, reads of Latest, and commits return ErrRemoved, and then
// deletes every manifest, staged manifest, tombstone, and data object under
// the dataset's prefix. The removal pointer stays as a fence, and a second
// call completes an interrupted removal.
//
// WithRetry and the other retry DatasetOptions retry pointer CAS conflicts;
// other DatasetOptions have no effect. Returns ErrHeadUncommitted (leaving
// objects intact behind the fence) if the fenced head belongs to a pending
// commit whose canonical manifest has not been persisted yet.
func (r *Root) RemoveDataset(ctx context.Context, id DatasetID, opts ...DatasetOption) error {
	if r.err != nil {
		return r.err
	}
	if err := validateDatasetID(id); err != nil {
		return err
	}
	cfg, err := resolveDatasetConfig(opts)
	if err != nil {
		return err
	}
	unpartLayout := newPathLayout(r.flat, noopScheme)
	hiveLayout := newPathLayout(r.flat, hiveGrammar)
	pointer := unpartLayout.latestPointerPath(id)

	previous, err := history.Remove(ctx, r.store, pointer, historyErrors, &cfg.retry)
	if err != nil {
		return err
	}
	if previous != "" && previous != history.RemovedPointer {
		if err := pendingDatasetCommit(ctx, r.store, id, SnapshotID(previous), unpartLayout, hiveLayout); err != nil {
			return err
		}
	}

	keys, err := r.store.List(ctx, unpartLayout.datasetPrefix(id))
	if err != nil {
		return fmt.Errorf("failed to list dataset objects: %w", err)
	}
	lockKey := pointer + ".lock"
	for _, k := range keys {
		if k == pointer || k == lockKey {
			continue
		}
		if err := r.store.Delete(ctx, k); err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("failed to delete dataset object %s: %w", k, err)
		}
	}
	return nil
}

func pendingDatasetCommit(ctx context.Context, store Store, id DatasetID, head SnapshotID, layouts ...*pathLayout) error {
	if !history.IsGeneratedID(head) {
		return nil
	}
	for _, l := range layouts {
		committed, err := store.Exists(ctx, l.manifestPath(id, head))
		if err != nil {
			return fmt.Errorf("failed to resolve head: %w", err)
		}
		if committed {
			return nil
		}
	}
	for _, l := range layouts {
		if !l.supportsPartitions() {
			continue
		}
		keys, err := store.List(ctx, l.partitionsPrefix(id, ""))
		if err != nil {
			return fmt.Errorf("failed to resolve head: %w", err)
		}
		for _, k := range keys {
			if sp, ok := l.manifest(k); ok && sp.dataset == id && sp.snapshot == head {
				return nil
			}
		}
	}
	return fmt.Errorf("lode: latest pointer named %s, whose manifest has not been persisted: %w", head, ErrHeadUncommitted)
}

// -----------------------------------------------------------------------------
// Read-only store wrapper (preserves RangeReader & PrefixLister)
// -----------------------------------------------------------------------------

type readOnlyStore struct {
	ObjectStore
}

func (readOnlyStore) Put(context.Context, string, io.Reader) error { return ErrReadOnly }

func (readOnlyStore) Delete(context.Context, string) error { return ErrReadOnly }

func (readOnlyStore) CompareAndSwap(context.Context, string, string, string) error {
	return ErrReadOnly
}

type readOnlyRangeStore struct {
	readOnlyStore
	RangeReader
}

type readOnlyPrefixStore struct {
	readOnlyStore
	PrefixLister
}

type readOnlyFullStore struct {
	readOnlyStore
	RangeReader
	PrefixLister
}

func newReadOnlyStore(store ObjectStore) Store {
	base := readOnlyStore{store}
	ranges, hasRanges := store.(RangeReader)
	prefixes, hasPrefixes := store.(PrefixLister)
	switch {
	case hasRanges && hasPrefixes:
		return readOnlyFullStore{base, ranges, prefixes}
	case hasRanges:
		return readOnlyRangeStore{base, ranges}
	case hasPrefixes:
		return readOnlyPrefixStore{base, prefixes}
	default:
		return base
	}
}

// -----------------------------------------------------------------------------
// Root options
// -----------------------------------------------------------------------------

// RootOption configures a Root or ReadRoot. Without options, datasets live
// under datasets/<id>/snapshots/<snapshot>/manifest.json (or segments/ when
// partitioned). Custom bucket prefixes are configured via lode.Prefixed.
type RootOption interface {
	applyRoot(cfg *rootConfig) error
}

type rootConfig struct {
	flat bool
}

type flatOption struct{}

func (flatOption) applyRoot(cfg *rootConfig) error {
	cfg.flat = true
	return nil
}

// WithFlatPaths places each dataset directly at the store root and, without
// partitions, each snapshot directly under its dataset (<id>/<snapshot>/manifest.json).
// Flat paths cannot enumerate datasets (ListDatasets returns ErrDatasetsNotModeled).
func WithFlatPaths() RootOption {
	return flatOption{}
}

func resolveRootConfig(opts []RootOption) (bool, error) {
	var cfg rootConfig
	for _, opt := range opts {
		if opt == nil {
			return false, validate.Argument("root option must not be nil")
		}
		if err := opt.applyRoot(&cfg); err != nil {
			return false, err
		}
	}
	return cfg.flat, nil
}
