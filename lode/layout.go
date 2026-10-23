package lode

import (
	"path"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/storepath"
)

// latestFile names every dataset's head pointer. It is not configurable:
// the pointer is where the writers of a dataset coordinate, and two roots
// that named it differently would commit independent histories over shared
// snapshot paths (V1_API_DECISIONS D11).
const latestFile = "latest"

// pathNames are the names within a dataset's paths, which a root's path
// options set (V1_API_DECISIONS D11). defaultPathNames returns the names
// every earlier release wrote. Flat paths have no datasets directory and,
// without partitions, no snapshots directory.
type pathNames struct {
	datasets, snapshots, partitions, segments string
	manifest, data, expired, staged           string
	flat                                      bool
}

func defaultPathNames() pathNames {
	return pathNames{
		datasets: "datasets", snapshots: "snapshots", partitions: "partitions", segments: "segments",
		manifest: "manifest.json", data: "data", expired: "expired", staged: "staged",
	}
}

// grammar returns the path grammar of these names, with or without
// partitions. A partitioned grammar places snapshots in the segments
// directory, flat or not.
func (n pathNames) grammar(partitioned bool) pathGrammar {
	g := pathGrammar{names: n, partitioned: partitioned}
	if !n.flat {
		g.root, g.snapshotDir = n.datasets, n.snapshots
	}
	if partitioned {
		g.snapshotDir = n.segments
	}
	return g
}

// convention is the record of these names that a manifest persists.
func (n pathNames) convention() ManifestPaths {
	p := ManifestPaths{
		Datasets: n.datasets, Snapshots: n.snapshots, Partitions: n.partitions,
		Segments: n.segments, Manifest: n.manifest, Data: n.data,
		Expired: n.expired, Staged: n.staged,
	}
	if n.flat {
		p.Datasets, p.Snapshots, p.Flat = "", "", true
	}
	return p
}

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
	root        string    // common prefix for all datasets; "" puts datasets at the store root
	snapshotDir string    // directory holding snapshots under a dataset (or partition); "" for none
	partitioned bool      // partitions/<k=v>... may sit between the dataset and snapshotDir
	names       pathNames // the partitions, manifest, and data names
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
	return g.names.partitions, partition
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
// manifest (dir "", file names.manifest) or a data file (dir names.data).
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

	if g.partitioned && len(parts) > 0 && parts[0] == g.names.partitions {
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
	if len(parts) == 1 && parts[0] == g.names.manifest {
		sp.kind = manifestLeaf
		return sp, true
	}
	if len(parts) == 2 && parts[0] == g.names.data && parts[1] != "" {
		sp.kind = dataLeaf
		return sp, true
	}
	return sp, false
}

// -----------------------------------------------------------------------------
// The dataset layout
// -----------------------------------------------------------------------------

// pathLayout is everything a dataset handle needs from its paths: one path
// grammar, from the root's path names, and the dataset's partitioning
// (CONTRACT_LAYOUT.md). It is the only layout: paths differ by data (the
// grammar), not by type.
type pathLayout struct {
	grammar pathGrammar
	part    partitionScheme
}

// newPathLayout lays out a dataset with the root's names and the dataset's
// partitioning. With the default names, an unpartitioned layout writes:
//
//	datasets/<dataset>/snapshots/<snapshot>/manifest.json
//	datasets/<dataset>/snapshots/<snapshot>/data/<file>
//
// and a Hive layout writes each manifest under every partition it touches,
// plus a canonical copy for lookup by ID:
//
//	datasets/<dataset>/partitions/<k=v>.../segments/<snapshot>/manifest.json
//	datasets/<dataset>/segments/<snapshot>/manifest.json
//
// Flat names omit the datasets directory and, without partitions, the
// snapshots directory: <dataset>/<snapshot>/manifest.json.
func newPathLayout(names pathNames, part partitionScheme) *pathLayout {
	return &pathLayout{grammar: names.grammar(!part.isNoop()), part: part}
}

func (l *pathLayout) supportsPartitions() bool      { return l.grammar.partitioned }
func (l *pathLayout) partitioning() partitionScheme { return l.part }

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
	return join(l.grammar.root, string(dataset), l.grammar.names.partitions) + "/" + partitionPrefix
}

func (l *pathLayout) manifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string {
	return l.grammar.objectPath(dataset, "", snapshot, "", l.grammar.names.manifest)
}

func (l *pathLayout) manifestPathInPartition(dataset DatasetID, snapshot DatasetSnapshotID, partition string) string {
	return l.grammar.objectPath(dataset, partition, snapshot, "", l.grammar.names.manifest)
}

func (l *pathLayout) dataFilePath(dataset DatasetID, snapshot DatasetSnapshotID, partition, filename string) string {
	return l.grammar.objectPath(dataset, partition, snapshot, l.grammar.names.data, filename)
}

func (l *pathLayout) latestPointerPath(dataset DatasetID) string {
	return join(l.grammar.root, string(dataset), latestFile)
}

// stagedManifestPath lies beside the dataset's tombstones, where no grammar
// parses a manifest, so a staged copy is never listed as a snapshot.
func (l *pathLayout) stagedManifestPath(dataset DatasetID, snapshot DatasetSnapshotID) string {
	return join(l.grammar.datasetDir(dataset), l.grammar.names.staged, string(snapshot)+".json")
}

// manifestName is the file name of every manifest of this layout.
func (l *pathLayout) manifestName() string { return l.grammar.names.manifest }

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
	stagedPrefix := join(l.grammar.datasetDir(dataset), l.grammar.names.staged) + "/"
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

// convention is the names this layout's paths use.
func (l *pathLayout) convention() ManifestPaths { return l.grammar.names.convention() }

func (l *pathLayout) tombstonePath(dataset DatasetID, snapshot DatasetSnapshotID) string {
	return join(l.grammar.datasetDir(dataset), l.grammar.names.expired, string(snapshot))
}

func (l *pathLayout) tombstonesPrefix(dataset DatasetID) string {
	return join(l.grammar.datasetDir(dataset), l.grammar.names.expired) + "/"
}

func (l *pathLayout) parse(p string) (snapshotPath, bool) {
	return l.grammar.parse(p)
}

// leafDir is the directory under a snapshot that holds each kind of leaf.
func (g pathGrammar) leafDir(kind pathKind) string {
	if kind == dataLeaf {
		return g.names.data
	}
	return ""
}

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
	return l.grammar.objectPath(dataset, sp.partition, snapshot, l.grammar.leafDir(kind), path.Base(p)) == p
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
