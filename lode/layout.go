package lode

import (
	"path"
	"strings"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/storepath"
)

// Fixed internal path segment names (CONTRACT_LAYOUT.md). Custom bucket
// placement uses lode.Prefixed; WithFlatPaths omits datasetsDir and, on
// unpartitioned layouts, snapshotsDir.
const (
	latestFile    = "latest"
	datasetsDir   = "datasets"
	snapshotsDir  = "snapshots"
	partitionsDir = "partitions"
	segmentsDir   = "segments"
	manifestFile  = "manifest.json"
	dataDir       = "data"
	expiredDir    = "expired"
	stagedDir     = "staged"
)

// defaultLegacyPaths returns the 1.1.0 paths object corresponding to flat,
// used to check compatibility when reading format 1.1.0 manifests.
func defaultLegacyPaths(flat bool) legacyManifestPaths {
	p := legacyManifestPaths{
		Partitions: partitionsDir,
		Segments:   segmentsDir,
		Manifest:   manifestFile,
		Data:       dataDir,
		Expired:    expiredDir,
		Staged:     stagedDir,
		Flat:       flat,
	}
	if !flat {
		p.Datasets = datasetsDir
		p.Snapshots = snapshotsDir
	}
	return p
}

// -----------------------------------------------------------------------------
// Path grammar
// -----------------------------------------------------------------------------

// pathGrammar describes a layout's object paths as data.
//
//	[<root>/]<dataset>/[partitions/<k=v>.../]<snapshotDir>/<snapshot>/manifest.json
//	[<root>/]<dataset>/[partitions/<k=v>.../]<snapshotDir>/<snapshot>/data/<file>
type pathGrammar struct {
	root        string // "datasets", or "" when flat
	snapshotDir string // "snapshots" (unpartitioned), "segments" (partitioned), or "" (flat unpartitioned)
	partitioned bool
	flat        bool
}

func newPathGrammar(flat, partitioned bool) pathGrammar {
	g := pathGrammar{partitioned: partitioned, flat: flat}
	if !flat {
		g.root = datasetsDir
		g.snapshotDir = snapshotsDir
	}
	if partitioned {
		g.snapshotDir = segmentsDir
	}
	return g
}

// pathKind is what the leaf of a parsed path names.
type pathKind int

const (
	otherPath pathKind = iota
	manifestLeaf
	dataLeaf
)

// snapshotPath is a path parsed against a grammar.
type snapshotPath struct {
	dataset   DatasetID
	partition string
	snapshot  SnapshotID
	kind      pathKind
}

// join is path.Join over the non-empty parts. FuzzJoin enforces exact parity
// with path.Join.
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

func (g pathGrammar) partitionParts(partition string) (dir, part string) {
	if !g.partitioned || partition == "" {
		return "", ""
	}
	return partitionsDir, partition
}

func (g pathGrammar) datasetDir(dataset DatasetID) string {
	return join(g.root, string(dataset))
}

func (g pathGrammar) snapshotDirFor(dataset DatasetID, partition string) string {
	partDir, part := g.partitionParts(partition)
	return join(g.root, string(dataset), partDir, part, g.snapshotDir)
}

func (g pathGrammar) objectPath(dataset DatasetID, partition string, snapshot SnapshotID, dir, file string) string {
	partDir, part := g.partitionParts(partition)
	return join(g.root, string(dataset), partDir, part, g.snapshotDir, string(snapshot), dir, file)
}

// parse reads a path against the grammar without allocating.
func (g pathGrammar) parse(p string) (sp snapshotPath, ok bool) {
	rem, hasMore := p, true
	next := func() (string, bool) {
		if !hasMore {
			return "", false
		}
		head, tail, found := strings.Cut(rem, "/")
		rem, hasMore = tail, found
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

	if g.partitioned && hasMore {
		if first, tail, found := strings.Cut(rem, "/"); first == partitionsDir {
			if !found {
				return sp, false
			}
			partRem, partMore := tail, true
			partLen := 0
			matchedSnapshotDir := false
			for partMore {
				seg, nextTail, nextFound := strings.Cut(partRem, "/")
				if seg == g.snapshotDir {
					if partLen == 0 {
						return sp, false
					}
					sp.partition = tail[:partLen-1]
					rem, hasMore = partRem, true
					matchedSnapshotDir = true
					break
				}
				partLen += len(seg) + 1
				partRem, partMore = nextTail, nextFound
			}
			if !matchedSnapshotDir {
				return sp, false
			}
		}
	}
	if g.snapshotDir != "" && !expect(g.snapshotDir) {
		return sp, false
	}
	id, ok := next()
	if !ok || id == "" {
		return sp, false
	}
	sp.snapshot = SnapshotID(id)

	leaf, ok := next()
	if !ok {
		return sp, false
	}
	if !hasMore && leaf == manifestFile {
		sp.kind = manifestLeaf
		return sp, true
	}
	if hasMore && leaf == dataDir {
		file, ok := next()
		if ok && !hasMore && file != "" {
			sp.kind = dataLeaf
			return sp, true
		}
	}
	return sp, false
}

// -----------------------------------------------------------------------------
// The dataset layout
// -----------------------------------------------------------------------------

type pathLayout struct {
	grammar pathGrammar
	part    partitionScheme
}

func newPathLayout(flat bool, part partitionScheme) *pathLayout {
	return &pathLayout{grammar: newPathGrammar(flat, !part.isNoop()), part: part}
}

func (l *pathLayout) supportsPartitions() bool      { return l.grammar.partitioned }
func (l *pathLayout) partitioning() partitionScheme { return l.part }
func (l *pathLayout) isFlat() bool                  { return l.grammar.flat }

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
		return l.snapshotsPrefix(dataset)
	}
	return join(l.grammar.root, string(dataset), partitionsDir) + "/" + partitionPrefix
}

func (l *pathLayout) manifestPath(dataset DatasetID, snapshot SnapshotID) string {
	return l.grammar.objectPath(dataset, "", snapshot, "", manifestFile)
}

func (l *pathLayout) manifestPathInPartition(dataset DatasetID, snapshot SnapshotID, partition string) string {
	return l.grammar.objectPath(dataset, partition, snapshot, "", manifestFile)
}

func (l *pathLayout) dataFilePath(dataset DatasetID, snapshot SnapshotID, partition, filename string) string {
	return l.grammar.objectPath(dataset, partition, snapshot, dataDir, filename)
}

func (l *pathLayout) latestPointerPath(dataset DatasetID) string {
	return join(l.grammar.root, string(dataset), latestFile)
}

func (l *pathLayout) stagedManifestPath(dataset DatasetID, snapshot SnapshotID) string {
	return join(l.grammar.datasetDir(dataset), stagedDir, string(snapshot)+".json")
}

func (l *pathLayout) datasetPrefix(dataset DatasetID) string {
	return l.grammar.datasetDir(dataset) + "/"
}

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

func snapshotLeaf(p, prefix, suffix string) bool {
	rest, ok := strings.CutPrefix(p, prefix)
	if !ok {
		return false
	}
	id, ok := strings.CutSuffix(rest, suffix)
	return ok && history.IsGeneratedID(SnapshotID(id))
}

func (l *pathLayout) tombstonePath(dataset DatasetID, snapshot SnapshotID) string {
	return join(l.grammar.datasetDir(dataset), expiredDir, string(snapshot))
}

func (l *pathLayout) tombstonesPrefix(dataset DatasetID) string {
	return join(l.grammar.datasetDir(dataset), expiredDir) + "/"
}

func (l *pathLayout) parse(p string) (snapshotPath, bool) {
	return l.grammar.parse(p)
}

func (l *pathLayout) isObjectOf(dataset DatasetID, snapshot SnapshotID, kind pathKind, p string) bool {
	if key, ok := storepath.Key(p); !ok || key != p {
		return false
	}
	sp, ok := l.grammar.parse(p)
	if !ok || sp.kind != kind || sp.dataset != dataset || sp.snapshot != snapshot {
		return false
	}
	return validPartitionPath(sp.partition)
}

func withinPartition(p, partition string) bool {
	return p == partition || strings.HasPrefix(p, partition+"/")
}

func (l *pathLayout) manifest(p string) (snapshotPath, bool) {
	if sp, ok := l.grammar.parse(p); ok && sp.kind == manifestLeaf {
		return sp, true
	}
	return snapshotPath{}, false
}

func (l *pathLayout) extractPartitionPath(filePath string) string {
	if sp, ok := l.grammar.parse(filePath); ok {
		return sp.partition
	}
	return ""
}
