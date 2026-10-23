package lode

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Roots
// -----------------------------------------------------------------------------

// Root opens the datasets of one store for writing. Open returns one. Every
// dataset of a root shares the root's one store value and its path names.
// A Root is safe for concurrent use; the handles it returns are not
// (CONTRACT_WRITE_API §Concurrency).
type Root struct {
	store Store
	names ManifestPaths
	err   error // a factory or path-option error, which every method returns
}

// ReadRoot opens the datasets of one store for reading. OpenReadOnly
// returns one. Its views never write, so its store need not provide
// CompareAndSwap.
type ReadRoot struct {
	store Store // the store, behind readOnlyStore
	names ManifestPaths
	err   error
}

// Open calls the factory once and returns a root over the store it
// creates. A factory error, or an invalid path option, is recorded and
// returned by every method of the root, so a caller checks one error:
//
//	events, err := lode.Open(lode.FS("/data")).Records(ctx, "events", lode.JSONL[Event]())
//
// Open requires a Store, which provides CompareAndSwap; a factory of a
// store without it does not compile here (OpenReadOnly accepts one).
func Open[S Store](f Factory[S], opts ...RootOption) *Root {
	store, names, err := openStore(f, opts)
	if err != nil {
		return &Root{err: err}
	}
	return &Root{store: store, names: names}
}

// OpenReadOnly calls the factory once and returns a root whose views read
// the store's datasets and never write. Errors are recorded as by Open.
func OpenReadOnly[S ObjectStore](f Factory[S], opts ...RootOption) *ReadRoot {
	store, names, err := openStore(f, opts)
	if err != nil {
		return &ReadRoot{err: err}
	}
	return &ReadRoot{store: readOnlyStore{store}, names: names}
}

// openStore validates the path options, then calls the factory: arguments
// are checked before any store work (CONTRACT_ERRORS §5).
func openStore[S ObjectStore](f Factory[S], opts []RootOption) (S, ManifestPaths, error) {
	var zero S
	names, err := resolveRootConfig(opts)
	if err != nil {
		return zero, ManifestPaths{}, err
	}
	if f == nil {
		return zero, ManifestPaths{}, validate.Argument("store factory is required")
	}
	store, err := f()
	if err != nil {
		return zero, ManifestPaths{}, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if isNilStore(store) {
		return zero, ManifestPaths{}, validate.Argument("store factory returned nil store")
	}
	return store, names, nil
}

// isNilStore reports whether a factory returned no store: a nil interface,
// or a nil pointer, map, slice, function, or channel inside one.
func isNilStore(store any) bool {
	return validate.Nil(store)
}

// ListDatasets returns the IDs of the root's datasets, in lexicographic
// order. Returns ErrDatasetsNotModeled for flat paths, which have no common
// prefix.
func (r *Root) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	if r.err != nil {
		return nil, r.err
	}
	return listDatasets(ctx, r.store, r.names, opts)
}

// ListDatasets returns the IDs of the root's datasets, as Root.ListDatasets
// does.
func (r *ReadRoot) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	if r.err != nil {
		return nil, r.err
	}
	return listDatasets(ctx, r.store, r.names, opts)
}

// readOnlyStore is a ReadRoot's store: every read passes through, and every
// write is refused, because a view never writes (it resolves the head
// without creating a pointer). A view's handle shares the writer's dataset
// core, so the refusal is what keeps a method that writes, such as Sweep
// reached by a type assertion, from changing the store.
type readOnlyStore struct {
	ObjectStore
}

func (readOnlyStore) Put(context.Context, string, io.Reader) error { return ErrReadOnly }

func (readOnlyStore) Delete(context.Context, string) error { return ErrReadOnly }

func (readOnlyStore) CompareAndSwap(context.Context, string, string, string) error {
	return ErrReadOnly
}

// -----------------------------------------------------------------------------
// Path options
// -----------------------------------------------------------------------------

// RootOption sets one of the names within a root's dataset paths. Without
// options a root uses the default names, which every earlier release wrote:
//
//	datasets/<id>/snapshots/<snapshot>/manifest.json
//
// The names are part of a dataset's address, as the store is: a root with
// other names addresses other objects. Each manifest records its names, and
// a writer whose names differ is rejected with ErrSettingsMismatch
// (CONTRACT_LAYOUT §Path Names). The pointer's name, latest, is not an
// option: it is where the writers of a dataset coordinate.
type RootOption interface {
	applyRoot(cfg *rootConfig) error
}

// rootConfig is the names a root's options set, and which options set them.
type rootConfig struct {
	names ManifestPaths
	set   map[string]bool
}

// nameOption sets one name of the convention.
type nameOption struct {
	field pathField
	value string
}

func (o nameOption) applyRoot(cfg *rootConfig) error {
	*o.field.of(&cfg.names) = o.value
	cfg.set[o.field.option] = true
	return nil
}

// nameOptionFor is the option, named option, that sets one name.
func nameOptionFor(option, value string) RootOption {
	i := slices.IndexFunc(pathFields, func(f pathField) bool { return f.option == option })
	return nameOption{field: pathFields[i], value: value}
}

// flatOption selects flat paths.
type flatOption struct{}

func (flatOption) applyRoot(cfg *rootConfig) error {
	cfg.names.Flat = true
	return nil
}

// WithDatasetsDir names the directory that holds every dataset (default
// "datasets").
func WithDatasetsDir(name string) RootOption { return nameOptionFor("WithDatasetsDir", name) }

// WithSnapshotsDir names the directory of an unpartitioned dataset's
// snapshots (default "snapshots").
func WithSnapshotsDir(name string) RootOption { return nameOptionFor("WithSnapshotsDir", name) }

// WithPartitionsDir names the directory of a Hive dataset's partitions
// (default "partitions").
func WithPartitionsDir(name string) RootOption { return nameOptionFor("WithPartitionsDir", name) }

// WithSegmentsDir names the directory of a Hive dataset's snapshots, within
// a partition and beside the canonical manifests (default "segments").
func WithSegmentsDir(name string) RootOption { return nameOptionFor("WithSegmentsDir", name) }

// WithManifestFile names every manifest file (default "manifest.json").
func WithManifestFile(name string) RootOption { return nameOptionFor("WithManifestFile", name) }

// WithDataDir names the directory of a snapshot's data files (default
// "data").
func WithDataDir(name string) RootOption { return nameOptionFor("WithDataDir", name) }

// WithExpiredDir names the directory of a dataset's retention tombstones
// (default "expired").
func WithExpiredDir(name string) RootOption { return nameOptionFor("WithExpiredDir", name) }

// WithStagedDir names the directory of a dataset's staged first manifest
// (default "staged").
func WithStagedDir(name string) RootOption { return nameOptionFor("WithStagedDir", name) }

// WithFlatPaths selects flat paths, which place each dataset at the store
// root and, without partitions, each snapshot directly under its dataset:
//
//	<id>/<snapshot>/manifest.json
//
// Flat paths cannot list datasets (ErrDatasetsNotModeled), and cannot be
// combined with WithDatasetsDir or WithSnapshotsDir, which name the
// directories they omit.
func WithFlatPaths() RootOption {
	return flatOption{}
}

// resolveRootConfig applies opts over the default names and validates the
// result. Flat paths omit the datasets and snapshots directories, so an
// option that names one is rejected, and the defaults are cleared.
func resolveRootConfig(opts []RootOption) (ManifestPaths, error) {
	cfg := &rootConfig{names: defaultPaths(false), set: map[string]bool{}}
	for _, opt := range opts {
		if opt == nil {
			return ManifestPaths{}, validate.Argument("root option must not be nil")
		}
		if err := opt.applyRoot(cfg); err != nil {
			return ManifestPaths{}, err
		}
	}
	if cfg.names.Flat {
		if cfg.set["WithDatasetsDir"] || cfg.set["WithSnapshotsDir"] {
			return ManifestPaths{}, validate.Argument("WithFlatPaths cannot be combined with WithDatasetsDir or WithSnapshotsDir: flat paths omit those directories")
		}
		cfg.names.Datasets, cfg.names.Snapshots = "", ""
	}
	if problem := cfg.names.problem(func(f pathField) string { return f.option }); problem != "" {
		return ManifestPaths{}, validate.Argument("%s", problem)
	}
	return cfg.names, nil
}

// pathName is one name of a convention, labeled by the option or manifest
// field that sets it.
type pathName struct{ label, value string }

// nameProblem returns why names do not form a convention, or "": each must
// be one canonical path segment (storepath.Segment) without "=", other than
// the pointer's name, and distinct from the others. Roots and manifests
// apply the same rule (CONTRACT_LAYOUT §Path Names).
func nameProblem(names []pathName) string {
	seen := map[string]string{latestFile: "the latest pointer"}
	for _, nm := range names {
		if !storepath.Segment(nm.value) || strings.Contains(nm.value, "=") {
			return fmt.Sprintf("%s: name %q must be one path segment without \"=\"", nm.label, nm.value)
		}
		if other, dup := seen[nm.value]; dup {
			return fmt.Sprintf("%s: name %q is already used by %s", nm.label, nm.value, other)
		}
		seen[nm.value] = nm.label
	}
	return ""
}
