package lode

import (
	"context"
	"fmt"
	"io"
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
	names pathNames
	err   error // a factory or path-option error, which every method returns
}

// ReadRoot opens the datasets of one store for reading. OpenReadOnly
// returns one. Its views never write, so its store need not provide
// CompareAndSwap.
type ReadRoot struct {
	store Store // the store, behind readOnlyStore
	names pathNames
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
func openStore[S ObjectStore](f Factory[S], opts []RootOption) (S, pathNames, error) {
	var zero S
	names, err := resolveRootConfig(opts)
	if err != nil {
		return zero, pathNames{}, err
	}
	if f == nil {
		return zero, pathNames{}, validate.Argument("store factory is required")
	}
	store, err := f()
	if err != nil {
		return zero, pathNames{}, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if isNilStore(store) {
		return zero, pathNames{}, validate.Argument("store factory returned nil store")
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

// rootConfig is the names a root's options set, and which were set.
type rootConfig struct {
	names pathNames
	set   map[string]bool
}

// nameOption sets one named field of the path names.
type nameOption struct {
	option string // the option's name, for errors
	field  func(n *pathNames) *string
	value  string
}

func (o nameOption) applyRoot(cfg *rootConfig) error {
	*o.field(&cfg.names) = o.value
	cfg.set[o.option] = true
	return nil
}

// flatOption selects flat paths.
type flatOption struct{}

func (flatOption) applyRoot(cfg *rootConfig) error {
	cfg.names.flat = true
	return nil
}

// WithDatasetsDir names the directory that holds every dataset (default
// "datasets").
func WithDatasetsDir(name string) RootOption {
	return nameOption{"WithDatasetsDir", func(n *pathNames) *string { return &n.datasets }, name}
}

// WithSnapshotsDir names the directory of an unpartitioned dataset's
// snapshots (default "snapshots").
func WithSnapshotsDir(name string) RootOption {
	return nameOption{"WithSnapshotsDir", func(n *pathNames) *string { return &n.snapshots }, name}
}

// WithPartitionsDir names the directory of a Hive dataset's partitions
// (default "partitions").
func WithPartitionsDir(name string) RootOption {
	return nameOption{"WithPartitionsDir", func(n *pathNames) *string { return &n.partitions }, name}
}

// WithSegmentsDir names the directory of a Hive dataset's snapshots, within
// a partition and beside the canonical manifests (default "segments").
func WithSegmentsDir(name string) RootOption {
	return nameOption{"WithSegmentsDir", func(n *pathNames) *string { return &n.segments }, name}
}

// WithManifestFile names every manifest file (default "manifest.json").
func WithManifestFile(name string) RootOption {
	return nameOption{"WithManifestFile", func(n *pathNames) *string { return &n.manifest }, name}
}

// WithDataDir names the directory of a snapshot's data files (default
// "data").
func WithDataDir(name string) RootOption {
	return nameOption{"WithDataDir", func(n *pathNames) *string { return &n.data }, name}
}

// WithExpiredDir names the directory of a dataset's retention tombstones
// (default "expired").
func WithExpiredDir(name string) RootOption {
	return nameOption{"WithExpiredDir", func(n *pathNames) *string { return &n.expired }, name}
}

// WithStagedDir names the directory of a dataset's staged first manifest
// (default "staged").
func WithStagedDir(name string) RootOption {
	return nameOption{"WithStagedDir", func(n *pathNames) *string { return &n.staged }, name}
}

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
// result.
func resolveRootConfig(opts []RootOption) (pathNames, error) {
	cfg := &rootConfig{names: defaultPathNames(), set: map[string]bool{}}
	for _, opt := range opts {
		if opt == nil {
			return pathNames{}, validate.Argument("root option must not be nil")
		}
		if err := opt.applyRoot(cfg); err != nil {
			return pathNames{}, err
		}
	}
	if err := validateNames(cfg); err != nil {
		return pathNames{}, err
	}
	return cfg.names, nil
}

// validateNames requires each name a root uses to be one canonical path
// segment (storepath.Segment) without "=", other than the pointer's name,
// and distinct from the others, so that every path parses to one meaning.
func validateNames(cfg *rootConfig) error {
	n := cfg.names
	if n.flat && (cfg.set["WithDatasetsDir"] || cfg.set["WithSnapshotsDir"]) {
		return validate.Argument("WithFlatPaths cannot be combined with WithDatasetsDir or WithSnapshotsDir: flat paths omit those directories")
	}
	named := []pathName{
		{"WithPartitionsDir", n.partitions},
		{"WithSegmentsDir", n.segments},
		{"WithManifestFile", n.manifest},
		{"WithDataDir", n.data},
		{"WithExpiredDir", n.expired},
		{"WithStagedDir", n.staged},
	}
	if !n.flat {
		named = append(named, pathName{"WithDatasetsDir", n.datasets}, pathName{"WithSnapshotsDir", n.snapshots})
	}
	if problem := nameProblem(named); problem != "" {
		return validate.Argument("%s", problem)
	}
	return nil
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
