package lode

import (
	"path"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// layout is everything a dataset or reader needs from one path grammar and
// its partitioner. Per CONTRACT_LAYOUT.md the two are unified: users
// configure both through Layout constructors. It is composed from one
// interface per concern, so a consumer that needs only one concern holds
// only that one.
type layout interface {
	snapshotPaths
	partitionPaths
	datasetPaths
	tombstonePaths
	stagedPaths
	partitioner() partitioner
}

// snapshotPaths is where a dataset's snapshots live: the pointer, every
// manifest copy and data object, and parsing those paths back.
type snapshotPaths interface {
	// snapshotsPrefix covers every manifest of the dataset.
	snapshotsPrefix(dataset DatasetID) string
	manifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string
	manifestPathInPartition(dataset DatasetID, snapshot DatasetSnapshotID, partition string) string
	dataFilePath(dataset DatasetID, snapshot DatasetSnapshotID, partition, filename string) string
	latestPointerPath(dataset DatasetID) string
	// parse reads a path as a manifest or data object of this layout.
	parse(p string) (snapshotPath, bool)
	// manifest is parse restricted to manifest paths.
	manifest(p string) (snapshotPath, bool)
	// isObjectOf reports whether p is, in canonical form, an object of the
	// given kind (manifest or data) of this snapshot of this dataset.
	isObjectOf(dataset DatasetID, snapshot DatasetSnapshotID, kind pathKind, p string) bool
}

// partitionPaths is how partitions appear in paths; only meaningful when
// supportsPartitions().
type partitionPaths interface {
	supportsPartitions() bool
	snapshotsPrefixForPartition(dataset DatasetID, partition string) string
	// partitionsPrefix returns the storage prefix covering every segment whose
	// partition path starts with partitionPrefix (a raw string prefix, no
	// key boundary implied).
	partitionsPrefix(dataset DatasetID, partitionPrefix string) string
	// extractPartitionPath is the partition of a manifest or data path, ""
	// when it has none.
	extractPartitionPath(filePath string) string
}

// datasetPaths is where datasets themselves are enumerated.
type datasetPaths interface {
	supportsDatasetEnumeration() bool
	datasetsPrefix() string
}

// stagedPaths is where a dataset's first commit stages its manifest before
// it advances the pointer (CONTRACT_CORE §Head Pointer), the convention a
// manifest records, and which objects under a dataset's directory belong
// to this layout, so that a writer can detect a history persisted under
// another layout (CONTRACT_WRITE_API §Settings).
type stagedPaths interface {
	stagedManifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string
	convention() ManifestPaths
	datasetPrefix(dataset DatasetID) string
	ownsObject(dataset DatasetID, p string) bool
	manifestName() string
}

// tombstonePaths is where Dataset.Expire records an expired snapshot;
// tombstonesPrefix lists every tombstone of a dataset.
type tombstonePaths interface {
	tombstonePath(dataset DatasetID, snapshot DatasetSnapshotID) string
	tombstonesPrefix(dataset DatasetID) string
}

// Layout constants
const (
	datasetsDir   = "datasets"
	snapshotsDir  = "snapshots"
	manifestFile  = "manifest.json"
	dataDir       = "data"
	partitionsDir = "partitions"
	segmentsDir   = "segments"
	latestFile    = "latest"
	tombstonesDir = "expired"
	stagedDir     = "staged"
)

// -----------------------------------------------------------------------------
// Path grammar
// -----------------------------------------------------------------------------

// pathGrammar describes a layout's object paths as data. Every layout is one
// grammar plus a partitioner; building and parsing paths are written once.
//
//	[<root>/]<dataset>/[partitions/<k=v>.../]<snapshotDir>/<snapshot>/manifest.json
//	[<root>/]<dataset>/[partitions/<k=v>.../]<snapshotDir>/<snapshot>/data/<file>
//
// root, partitions and snapshotDir are each optional per grammar.
type pathGrammar struct {
	root        string // common prefix for all datasets; "" puts datasets at the store root
	snapshotDir string // directory holding snapshots under a dataset (or partition); "" for none
	partitioned bool   // partitions/<k=v>... may sit between the dataset and snapshotDir
}

// pathKind is what the leaf of a parsed path names.
type pathKind int

const (
	otherPath pathKind = iota
	manifestLeaf
	dataLeaf
)

// snapshotPath is a path parsed against a grammar: which dataset, which
// partition ("" when none) and which snapshot it belongs to.
type snapshotPath struct {
	dataset   DatasetID
	partition string
	snapshot  DatasetSnapshotID
	kind      pathKind
}

// join is path.Join over the non-empty parts. A grammar's optional pieces
// (root, partition, snapshotDir) are "" when absent and are skipped. The
// result is built in one allocation, sized first; path.Join allocates twice
// (its buffer and the string). Clean returns an already clean path without
// allocating, so the result equals path.Join's.
//
// path.Join is the specification: join MUST return exactly what path.Join
// returns for any number of parts. FuzzJoin enforces this on every test run
// and in the nightly fuzz job; a change that diverges is a bug in join.
// Reverting to path.Join preserves every path, but it adds an allocation per
// path, so the allocation gates in alloc_test.go must be raised with it.
func join(parts ...string) string {
	size, n := 0, 0
	for _, part := range parts {
		if part != "" {
			size += len(part)
			n++
		}
	}
	if n == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(size + n - 1)
	for _, part := range parts {
		if part == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('/')
		}
		b.WriteString(part)
	}
	return path.Clean(b.String())
}

// partitionParts is the partitions directory and the partition, or two
// empty strings when the grammar has no partitions or none is given.
func (g pathGrammar) partitionParts(partition string) (dir, part string) {
	if !g.partitioned || partition == "" {
		return "", ""
	}
	return partitionsDir, partition
}

// datasetDir is the directory that holds everything about one dataset.
func (g pathGrammar) datasetDir(dataset DatasetID) string {
	return join(g.root, string(dataset))
}

// snapshotDirFor is the directory holding snapshots of a dataset, under the
// given partition when the grammar is partitioned and partition is non-empty.
func (g pathGrammar) snapshotDirFor(dataset DatasetID, partition string) string {
	partDir, part := g.partitionParts(partition)
	return join(g.root, string(dataset), partDir, part, g.snapshotDir)
}

// objectPath is the path of one object in a snapshot's directory: the
// manifest (dir "", file manifestFile) or a data file (dir dataDir).
func (g pathGrammar) objectPath(dataset DatasetID, partition string, snapshot DatasetSnapshotID, dir, file string) string {
	partDir, part := g.partitionParts(partition)
	return join(g.root, string(dataset), partDir, part, g.snapshotDir, string(snapshot), dir, file)
}

// parse reads a path against the grammar. ok is false for any path that is
// not a manifest or data object of this layout.
func (g pathGrammar) parse(p string) (sp snapshotPath, ok bool) {
	parts := strings.Split(p, "/")
	next := func() (string, bool) {
		if len(parts) == 0 {
			return "", false
		}
		head := parts[0]
		parts = parts[1:]
		return head, true
	}
	expect := func(want string) bool {
		got, ok := next()
		return ok && got == want
	}

	if g.root != "" && !expect(g.root) {
		return sp, false
	}
	ds, ok := next()
	if !ok || ds == "" {
		return sp, false
	}
	sp.dataset = DatasetID(ds)

	if g.partitioned && len(parts) > 0 && parts[0] == partitionsDir {
		parts = parts[1:]
		end := slices.Index(parts, g.snapshotDir)
		if end <= 0 {
			return sp, false // no partition segments, or no snapshot dir after them
		}
		sp.partition = strings.Join(parts[:end], "/")
		parts = parts[end:]
	}
	if g.snapshotDir != "" && !expect(g.snapshotDir) {
		return sp, false
	}
	id, ok := next()
	if !ok || id == "" {
		return sp, false
	}
	sp.snapshot = DatasetSnapshotID(id)

	// The leaf decides the kind. If-chain rather than a tagless switch so
	// the mutation gate measures these guards (go-gremlins/gremlins#298).
	if len(parts) == 1 && parts[0] == manifestFile {
		sp.kind = manifestLeaf
		return sp, true
	}
	if len(parts) == 2 && parts[0] == dataDir && parts[1] != "" {
		sp.kind = dataLeaf
		return sp, true
	}
	return sp, false
}

// -----------------------------------------------------------------------------
// The one layout implementation
// -----------------------------------------------------------------------------

// pathLayout implements layout from a grammar and a partitioner.
type pathLayout struct {
	grammar pathGrammar
	part    partitioner
}

// NewDefaultLayout creates the default novice-friendly layout:
//
//	/datasets/<dataset>/snapshots/<snapshot>/
//	  manifest.json
//	  /data/<file>
//
// Flat (noop partitioning); the default for NewDataset and NewDatasetReader.
func NewDefaultLayout() layout {
	return &pathLayout{
		grammar: pathGrammar{root: datasetsDir, snapshotDir: snapshotsDir},
		part:    newNoopPartitioner(),
	}
}

// NewHiveLayout creates a Hive (partition-first) layout with the specified partition keys:
//
//	/datasets/<dataset>/partitions/<k=v>/segments/<snapshot>/
//	  manifest.json
//	  /data/<file>
//
// plus a canonical manifest at /datasets/<dataset>/segments/<snapshot>/manifest.json
// for O(1) lookup by ID. At least one partition key is required; for
// unpartitioned data use NewDefaultLayout. Records must be map[string]any
// with the keys present.
//
// Example:
//
//	layout, err := NewHiveLayout("day", "region")
//	// Records will be partitioned by day=<value>/region=<value>
func NewHiveLayout(keys ...string) (layout, error) {
	if len(keys) == 0 {
		return nil, validate.Argument("NewHiveLayout requires at least one partition key; use NewDefaultLayout for unpartitioned data")
	}
	for _, key := range keys {
		if !storepath.Segment(key) || strings.Contains(key, "=") {
			return nil, validate.Argument("NewHiveLayout: partition key %q must be one path segment without \"=\", so that a partition path names its keys unambiguously", key)
		}
	}
	return &pathLayout{
		grammar: pathGrammar{root: datasetsDir, snapshotDir: segmentsDir, partitioned: true},
		part:    newHivePartitioner(keys...),
	}, nil
}

// NewFlatLayout creates a minimal layout with dataset names at the store root:
//
//	/<dataset>/<snapshot>/
//	  manifest.json
//	  /data/<file>
//
// No partitions, and no dataset enumeration (there is no common prefix).
func NewFlatLayout() layout {
	return &pathLayout{grammar: pathGrammar{}, part: newNoopPartitioner()}
}

func (l *pathLayout) supportsDatasetEnumeration() bool { return l.grammar.root != "" }
func (l *pathLayout) supportsPartitions() bool         { return l.grammar.partitioned }
func (l *pathLayout) partitioner() partitioner         { return l.part }

func (l *pathLayout) datasetsPrefix() string {
	if l.grammar.root == "" {
		return ""
	}
	return l.grammar.root + "/"
}

// snapshotsPrefix covers every manifest of the dataset. On a partitioned
// grammar that is the whole dataset directory (partitions sit inside it).
func (l *pathLayout) snapshotsPrefix(dataset DatasetID) string {
	if l.grammar.partitioned {
		return l.grammar.datasetDir(dataset) + "/"
	}
	return l.grammar.snapshotDirFor(dataset, "") + "/"
}

func (l *pathLayout) snapshotsPrefixForPartition(dataset DatasetID, partition string) string {
	if partition == "" || !l.grammar.partitioned {
		return l.snapshotsPrefix(dataset)
	}
	return l.grammar.snapshotDirFor(dataset, partition) + "/"
}

func (l *pathLayout) partitionsPrefix(dataset DatasetID, partitionPrefix string) string {
	if !l.grammar.partitioned {
		return l.snapshotsPrefix(dataset) // callers guard on supportsPartitions()
	}
	// Raw concatenation, not path.Join: a partial final token such as
	// "day=2026-10" must stay a prefix of "day=2026-10-18".
	return join(l.grammar.root, string(dataset), partitionsDir) + "/" + partitionPrefix
}

func (l *pathLayout) manifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string {
	return l.grammar.objectPath(dataset, "", snapshot, "", manifestFile)
}

func (l *pathLayout) manifestPathInPartition(dataset DatasetID, snapshot DatasetSnapshotID, partition string) string {
	return l.grammar.objectPath(dataset, partition, snapshot, "", manifestFile)
}

func (l *pathLayout) dataFilePath(dataset DatasetID, snapshot DatasetSnapshotID, partition, filename string) string {
	return l.grammar.objectPath(dataset, partition, snapshot, dataDir, filename)
}

func (l *pathLayout) latestPointerPath(dataset DatasetID) string {
	return join(l.grammar.root, string(dataset), latestFile)
}

// stagedManifestPath lies beside the dataset's tombstones, where no grammar
// parses a manifest, so a staged copy is never listed as a snapshot.
func (l *pathLayout) stagedManifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string {
	return join(l.grammar.datasetDir(dataset), stagedDir, string(snapshot)+".json")
}

// manifestName is the file name of every manifest of this layout.
func (l *pathLayout) manifestName() string { return manifestFile }

// datasetPrefix covers every object of the dataset.
func (l *pathLayout) datasetPrefix(dataset DatasetID) string {
	return l.grammar.datasetDir(dataset) + "/"
}

// ownsObject reports whether p, an object under the dataset's prefix, is
// one this layout persists: the pointer, a manifest or data object its
// grammar parses, a staged manifest (staged/<id>.json), or a tombstone
// (expired/<id>). The pointer and tombstones are written by
// CompareAndSwap, so each may have the companion lock file that the
// filesystem store keeps beside it (CONTRACT_STORAGE §Implementation
// Notes).
func (l *pathLayout) ownsObject(dataset DatasetID, p string) bool {
	if sp, ok := l.grammar.parse(p); ok && sp.dataset == dataset {
		return true
	}
	pointer := l.latestPointerPath(dataset)
	stagedPrefix := join(l.grammar.datasetDir(dataset), stagedDir) + "/"
	return p == pointer || p == pointer+".lock" ||
		snapshotLeaf(p, stagedPrefix, ".json") ||
		snapshotLeaf(p, l.tombstonesPrefix(dataset), "") ||
		snapshotLeaf(p, l.tombstonesPrefix(dataset), ".lock")
}

// snapshotLeaf reports whether p is prefix, a generated snapshot ID, and
// suffix, with nothing else: one object directly under prefix.
func snapshotLeaf(p, prefix, suffix string) bool {
	rest, ok := strings.CutPrefix(p, prefix)
	if !ok {
		return false
	}
	id, ok := strings.CutSuffix(rest, suffix)
	return ok && history.IsGeneratedID(DatasetSnapshotID(id))
}

// convention is the names this layout's paths use. A layout without a
// datasets directory has the flat paths.
func (l *pathLayout) convention() ManifestPaths {
	p := ManifestPaths{
		Datasets: datasetsDir, Snapshots: snapshotsDir, Partitions: partitionsDir,
		Segments: segmentsDir, Manifest: manifestFile, Data: dataDir,
		Expired: tombstonesDir, Staged: stagedDir,
	}
	if l.grammar.root == "" {
		p.Datasets, p.Snapshots, p.Flat = "", "", true
	}
	return p
}

func (l *pathLayout) tombstonePath(dataset DatasetID, snapshot DatasetSnapshotID) string {
	return join(l.grammar.datasetDir(dataset), tombstonesDir, string(snapshot))
}

func (l *pathLayout) tombstonesPrefix(dataset DatasetID) string {
	return join(l.grammar.datasetDir(dataset), tombstonesDir) + "/"
}

func (l *pathLayout) parse(p string) (snapshotPath, bool) {
	return l.grammar.parse(p)
}

// leafDir is the directory under a snapshot that holds each kind of leaf.
var leafDir = map[pathKind]string{manifestLeaf: "", dataLeaf: dataDir}

// isObjectOf requires p to be a key every store accepts, already in clean
// form, and to survive a parse-and-rebuild round trip unchanged: no "..",
// backslash, empty or foreign segment can make a delete land outside the
// snapshot or fail in the store.
func (l *pathLayout) isObjectOf(dataset DatasetID, snapshot DatasetSnapshotID, kind pathKind, p string) bool {
	if key, ok := storepath.Key(p); !ok || key != p {
		return false
	}
	sp, ok := l.grammar.parse(p)
	if !ok || sp.kind != kind || sp.dataset != dataset || sp.snapshot != snapshot {
		return false
	}
	return l.grammar.objectPath(dataset, sp.partition, snapshot, leafDir[kind], path.Base(p)) == p
}

// withinPartition reports whether partition p is partition or lies beneath
// it, comparing whole segments: "run_id=1" does not contain "run_id=10".
func withinPartition(p, partition string) bool {
	return p == partition || strings.HasPrefix(p, partition+"/")
}

// manifest parses p as a manifest path of this layout; false, with the zero
// snapshotPath, for anything else, so callers never see partial fields.
func (l *pathLayout) manifest(p string) (snapshotPath, bool) {
	if sp, ok := l.grammar.parse(p); ok && sp.kind == manifestLeaf {
		return sp, true
	}
	return snapshotPath{}, false
}

// extractPartitionPath returns the partition of a manifest or data path, ""
// when it has none or is not a path of this layout.
func (l *pathLayout) extractPartitionPath(filePath string) string {
	if sp, ok := l.grammar.parse(filePath); ok {
		return sp.partition
	}
	return ""
}
