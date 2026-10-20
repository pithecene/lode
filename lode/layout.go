package lode

import (
	"path"
	"strings"

	"github.com/pithecene-io/lode/internal/storepath"
)

// layout is the internal interface that combines path topology with partitioning.
// Per CONTRACT_LAYOUT.md, these are unified - users configure both through Layout constructors.
type layout interface {
	// Path topology methods
	supportsDatasetEnumeration() bool
	supportsPartitions() bool
	datasetsPrefix() string
	snapshotsPrefix(dataset DatasetID) string
	snapshotsPrefixForPartition(dataset DatasetID, partition string) string
	// partitionsPrefix returns the storage prefix covering every segment whose
	// partition path starts with partitionPrefix (a raw string prefix, no
	// key boundary implied). Only meaningful when supportsPartitions().
	partitionsPrefix(dataset DatasetID, partitionPrefix string) string
	isManifest(p string) bool
	parseDatasetID(manifestPath string) DatasetID
	parseSnapshotID(manifestPath string) DatasetSnapshotID
	parsePartitionFromManifest(manifestPath string) string
	extractPartitionPath(filePath string) string
	manifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string
	manifestPathInPartition(dataset DatasetID, snapshot DatasetSnapshotID, partition string) string
	dataFilePath(dataset DatasetID, snapshot DatasetSnapshotID, partition, filename string) string
	latestPointerPath(dataset DatasetID) string
	// tombstonePath is where Dataset.Expire records an expired snapshot;
	// tombstonesPrefix lists every tombstone of a dataset.
	tombstonePath(dataset DatasetID, snapshot DatasetSnapshotID) string
	tombstonesPrefix(dataset DatasetID) string
	// parse reads a path as a manifest or data object of this layout.
	parse(p string) (snapshotPath, bool)
	// isObjectOf reports whether p is, in canonical form, an object of the
	// given kind (manifest or data) of this snapshot of this dataset.
	isObjectOf(dataset DatasetID, snapshot DatasetSnapshotID, kind pathKind, p string) bool

	// Partitioning (unified with layout)
	partitioner() partitioner
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
// (root, partition, snapshotDir) are "" when absent; dropping them up front
// means one Join per path and nothing for Clean to repair.
func join(parts ...string) string {
	n := 0
	for _, part := range parts {
		if part != "" {
			parts[n] = part
			n++
		}
	}
	return path.Join(parts[:n]...)
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
		end := indexOf(parts, g.snapshotDir)
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

func indexOf(parts []string, want string) int {
	for i, p := range parts {
		if p == want {
			return i
		}
	}
	return -1
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
		return nil, invalidArgument("NewHiveLayout requires at least one partition key; use NewDefaultLayout for unpartitioned data")
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

// manifest parses p as a manifest path of this layout; the zero snapshotPath
// for anything else, so callers never see partial fields.
func (l *pathLayout) manifest(p string) snapshotPath {
	if sp, ok := l.grammar.parse(p); ok && sp.kind == manifestLeaf {
		return sp
	}
	return snapshotPath{}
}

func (l *pathLayout) isManifest(p string) bool {
	return l.manifest(p).kind == manifestLeaf
}

func (l *pathLayout) parseDatasetID(manifestPath string) DatasetID {
	return l.manifest(manifestPath).dataset
}

func (l *pathLayout) parseSnapshotID(manifestPath string) DatasetSnapshotID {
	return l.manifest(manifestPath).snapshot
}

func (l *pathLayout) parsePartitionFromManifest(manifestPath string) string {
	return l.manifest(manifestPath).partition
}

// extractPartitionPath returns the partition of a manifest or data path, ""
// when it has none or is not a path of this layout.
func (l *pathLayout) extractPartitionPath(filePath string) string {
	if sp, ok := l.grammar.parse(filePath); ok {
		return sp.partition
	}
	return ""
}
