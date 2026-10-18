package lode

import (
	"errors"
	"path"
	"strings"
)

// layout is the internal interface that combines path topology with partitioning.
// Per CONTRACT_LAYOUT.md, these are unified - users configure both through Layout constructors.
type layout interface {
	// Path topology methods
	supportsDatasetEnumeration() bool
	supportsPartitions() bool
	datasetsPrefix() string
	segmentsPrefix(dataset DatasetID) string
	segmentsPrefixForPartition(dataset DatasetID, partition string) string
	// partitionsPrefix returns the storage prefix covering every segment whose
	// partition path starts with partitionPrefix (a raw string prefix, no
	// key boundary implied). Only meaningful when supportsPartitions().
	partitionsPrefix(dataset DatasetID, partitionPrefix string) string
	isManifest(p string) bool
	parseDatasetID(manifestPath string) DatasetID
	parseSegmentID(manifestPath string) DatasetSnapshotID
	parsePartitionFromManifest(manifestPath string) string
	extractPartitionPath(filePath string) string
	manifestPath(dataset DatasetID, segment DatasetSnapshotID) string
	manifestPathInPartition(dataset DatasetID, segment DatasetSnapshotID, partition string) string
	dataFilePath(dataset DatasetID, segment DatasetSnapshotID, partition, filename string) string
	latestPointerPath(dataset DatasetID) string

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
)

// -----------------------------------------------------------------------------
// Default Layout
// -----------------------------------------------------------------------------

// defaultLayout implements the reference layout from CONTRACT_READ_API.md:
//
//	/datasets/<dataset>/snapshots/<segment_id>/
//	  manifest.json
//	  /data/
//	    filename
//
// This layout uses noop partitioning (flat, unpartitioned).
type defaultLayout struct {
	part partitioner
}

// NewDefaultLayout creates the default novice-friendly layout.
//
// This layout:
//   - Uses segment-first organization (datasets/snapshots/segment)
//   - Does not support partitioning (flat data organization)
//   - Is the default for NewDataset when no layout is specified
func NewDefaultLayout() layout {
	return &defaultLayout{part: newNoopPartitioner()}
}

func (l *defaultLayout) supportsDatasetEnumeration() bool { return true }
func (l *defaultLayout) supportsPartitions() bool         { return false }
func (l *defaultLayout) datasetsPrefix() string           { return datasetsDir + "/" }

func (l *defaultLayout) segmentsPrefix(dataset DatasetID) string {
	return path.Join(datasetsDir, string(dataset), snapshotsDir) + "/"
}

func (l *defaultLayout) segmentsPrefixForPartition(dataset DatasetID, _ string) string {
	return l.segmentsPrefix(dataset)
}

func (l *defaultLayout) partitionsPrefix(dataset DatasetID, _ string) string {
	return l.segmentsPrefix(dataset) // no partitions: callers guard on supportsPartitions()
}

func (l *defaultLayout) manifestPath(dataset DatasetID, segment DatasetSnapshotID) string {
	return path.Join(datasetsDir, string(dataset), snapshotsDir, string(segment), manifestFile)
}

func (l *defaultLayout) manifestPathInPartition(dataset DatasetID, segment DatasetSnapshotID, _ string) string {
	return l.manifestPath(dataset, segment)
}

func (l *defaultLayout) isManifest(p string) bool {
	parts := strings.Split(p, "/")
	if len(parts) != 5 {
		return false
	}
	return parts[0] == datasetsDir &&
		parts[1] != "" &&
		parts[2] == snapshotsDir &&
		parts[3] != "" &&
		parts[4] == manifestFile
}

func (l *defaultLayout) parseDatasetID(manifestPath string) DatasetID {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return DatasetID(parts[1])
}

func (l *defaultLayout) parseSegmentID(manifestPath string) DatasetSnapshotID {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return DatasetSnapshotID(parts[3])
}

func (l *defaultLayout) parsePartitionFromManifest(_ string) string {
	return "" // Default layout doesn't encode partitions in manifest paths
}

func (l *defaultLayout) extractPartitionPath(_ string) string {
	return "" // Default layout doesn't support partitions
}

func (l *defaultLayout) dataFilePath(dataset DatasetID, segment DatasetSnapshotID, _, filename string) string {
	return path.Join(datasetsDir, string(dataset), snapshotsDir, string(segment), dataDir, filename)
}

func (l *defaultLayout) latestPointerPath(dataset DatasetID) string {
	return path.Join(datasetsDir, string(dataset), "latest")
}

func (l *defaultLayout) partitioner() partitioner {
	return l.part
}

// -----------------------------------------------------------------------------
// Hive Layout
// -----------------------------------------------------------------------------

// hiveLayout implements a partition-first layout:
//
//	/datasets/<dataset>/partitions/<k=v>/segments/<segment_id>/
//	  manifest.json
//	  /data/
//	    filename
//
// This layout places partitions at the dataset level, enabling efficient
// partition pruning at the storage layer.
type hiveLayout struct {
	part partitioner
}

// NewHiveLayout creates a Hive (partition-first) layout with the specified partition keys.
//
// At least one partition key is required. For unpartitioned data, use NewDefaultLayout instead.
//
// The keys specify which record fields to use for partitioning.
// Records must be map[string]any with the specified keys present.
//
// Example:
//
//	layout, err := NewHiveLayout("day", "region")
//	// Records will be partitioned by day=<value>/region=<value>
func NewHiveLayout(keys ...string) (layout, error) {
	if len(keys) == 0 {
		return nil, errors.New("NewHiveLayout requires at least one partition key; use NewDefaultLayout for unpartitioned data")
	}
	return &hiveLayout{part: newHivePartitioner(keys...)}, nil
}

func (l *hiveLayout) supportsDatasetEnumeration() bool { return true }
func (l *hiveLayout) supportsPartitions() bool         { return true }
func (l *hiveLayout) datasetsPrefix() string           { return datasetsDir + "/" }

func (l *hiveLayout) segmentsPrefix(dataset DatasetID) string {
	return path.Join(datasetsDir, string(dataset)) + "/"
}

func (l *hiveLayout) segmentsPrefixForPartition(dataset DatasetID, partition string) string {
	if partition == "" {
		return l.segmentsPrefix(dataset)
	}
	return path.Join(datasetsDir, string(dataset), partitionsDir, partition, segmentsDir) + "/"
}

func (l *hiveLayout) partitionsPrefix(dataset DatasetID, partitionPrefix string) string {
	// Raw concatenation, not path.Join: a partial final token such as
	// "day=2026-10" must stay a prefix of "day=2026-10-18".
	return path.Join(datasetsDir, string(dataset), partitionsDir) + "/" + partitionPrefix
}

func (l *hiveLayout) manifestPath(dataset DatasetID, segment DatasetSnapshotID) string {
	return path.Join(datasetsDir, string(dataset), segmentsDir, string(segment), manifestFile)
}

func (l *hiveLayout) manifestPathInPartition(dataset DatasetID, segment DatasetSnapshotID, partition string) string {
	if partition == "" {
		return l.manifestPath(dataset, segment)
	}
	return path.Join(datasetsDir, string(dataset), partitionsDir, partition, segmentsDir, string(segment), manifestFile)
}

func (l *hiveLayout) isManifest(p string) bool {
	parts := strings.Split(p, "/")
	if len(parts) < 4 {
		return false
	}
	if parts[0] != datasetsDir || parts[1] == "" {
		return false
	}
	if parts[len(parts)-1] != manifestFile {
		return false
	}
	for i := 2; i < len(parts)-2; i++ {
		if parts[i] == segmentsDir && parts[i+2] == manifestFile {
			return parts[i+1] != ""
		}
	}
	return false
}

func (l *hiveLayout) parseDatasetID(manifestPath string) DatasetID {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return DatasetID(parts[1])
}

func (l *hiveLayout) parseSegmentID(manifestPath string) DatasetSnapshotID {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	for i := 2; i < len(parts)-2; i++ {
		if parts[i] == segmentsDir {
			return DatasetSnapshotID(parts[i+1])
		}
	}
	return ""
}

func (l *hiveLayout) parsePartitionFromManifest(manifestPath string) string {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")

	partitionsIdx := -1
	for i := 2; i < len(parts); i++ {
		if parts[i] == partitionsDir {
			partitionsIdx = i
			break
		}
	}
	if partitionsIdx < 0 {
		return ""
	}

	segmentsIdx := -1
	for i := partitionsIdx + 1; i < len(parts); i++ {
		if parts[i] == segmentsDir {
			segmentsIdx = i
			break
		}
	}
	if segmentsIdx < 0 || segmentsIdx <= partitionsIdx+1 {
		return ""
	}

	return strings.Join(parts[partitionsIdx+1:segmentsIdx], "/")
}

func (l *hiveLayout) extractPartitionPath(filePath string) string {
	parts := strings.Split(filePath, "/")

	partitionsIdx := -1
	for i := 2; i < len(parts); i++ {
		if parts[i] == partitionsDir {
			partitionsIdx = i
			break
		}
	}
	if partitionsIdx < 0 {
		return ""
	}

	segmentsIdx := -1
	for i := partitionsIdx + 1; i < len(parts); i++ {
		if parts[i] == segmentsDir {
			segmentsIdx = i
			break
		}
	}
	if segmentsIdx < 0 || segmentsIdx <= partitionsIdx+1 {
		return ""
	}

	return strings.Join(parts[partitionsIdx+1:segmentsIdx], "/")
}

func (l *hiveLayout) dataFilePath(dataset DatasetID, segment DatasetSnapshotID, partition, filename string) string {
	if partition == "" {
		return path.Join(datasetsDir, string(dataset), segmentsDir, string(segment), dataDir, filename)
	}
	return path.Join(datasetsDir, string(dataset), partitionsDir, partition, segmentsDir, string(segment), dataDir, filename)
}

func (l *hiveLayout) latestPointerPath(dataset DatasetID) string {
	return path.Join(datasetsDir, string(dataset), "latest")
}

func (l *hiveLayout) partitioner() partitioner {
	return l.part
}

// -----------------------------------------------------------------------------
// Flat Layout
// -----------------------------------------------------------------------------

// flatLayout implements a minimal flat layout:
//
//	/<dataset>/<segment>/
//	  manifest.json
//	  /data/
//	    filename
//
// This layout has no partition support and no "datasets" prefix.
type flatLayout struct {
	part partitioner
}

// NewFlatLayout creates a minimal flat layout for simple use cases.
//
// This layout:
//   - Has no "datasets" prefix (dataset names are at root level)
//   - Does not support partitioning
//   - Does not support dataset enumeration (no common prefix)
func NewFlatLayout() layout {
	return &flatLayout{part: newNoopPartitioner()}
}

func (l *flatLayout) supportsDatasetEnumeration() bool { return false }
func (l *flatLayout) supportsPartitions() bool         { return false }
func (l *flatLayout) datasetsPrefix() string           { return "" }

func (l *flatLayout) segmentsPrefix(dataset DatasetID) string {
	return string(dataset) + "/"
}

func (l *flatLayout) segmentsPrefixForPartition(dataset DatasetID, _ string) string {
	return l.segmentsPrefix(dataset)
}

func (l *flatLayout) partitionsPrefix(dataset DatasetID, _ string) string {
	return l.segmentsPrefix(dataset) // no partitions: callers guard on supportsPartitions()
}

func (l *flatLayout) manifestPath(dataset DatasetID, segment DatasetSnapshotID) string {
	return path.Join(string(dataset), string(segment), manifestFile)
}

func (l *flatLayout) manifestPathInPartition(dataset DatasetID, segment DatasetSnapshotID, _ string) string {
	return l.manifestPath(dataset, segment)
}

func (l *flatLayout) isManifest(p string) bool {
	parts := strings.Split(p, "/")
	return len(parts) == 3 &&
		parts[0] != "" &&
		parts[1] != "" &&
		parts[2] == manifestFile
}

func (l *flatLayout) parseDatasetID(manifestPath string) DatasetID {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return DatasetID(parts[0])
}

func (l *flatLayout) parseSegmentID(manifestPath string) DatasetSnapshotID {
	if !l.isManifest(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return DatasetSnapshotID(parts[1])
}

func (l *flatLayout) parsePartitionFromManifest(_ string) string {
	return ""
}

func (l *flatLayout) extractPartitionPath(_ string) string {
	return ""
}

func (l *flatLayout) dataFilePath(dataset DatasetID, segment DatasetSnapshotID, _, filename string) string {
	return path.Join(string(dataset), string(segment), dataDir, filename)
}

func (l *flatLayout) latestPointerPath(dataset DatasetID) string {
	return path.Join(string(dataset), "latest")
}

func (l *flatLayout) partitioner() partitioner {
	return l.part
}
