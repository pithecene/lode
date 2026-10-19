package lode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// Data file roles: a dataset without a codec stores one blob per snapshot, a
// dataset with a codec stores encoded records. The compressor's extension is
// appended to the role to name the file.
const (
	blobFile = "blob"
	dataFile = "data"
)

// pendingSnapshot is a snapshot whose data files are persisted but whose
// manifest is not yet committed. Everything in it is stable across commit
// retries: only the parent changes when the head moves underneath it.
type pendingSnapshot struct {
	id         DatasetSnapshotID
	metadata   Metadata
	files      []FileRef
	partitions []string // partition key of each file, for manifest fan-out
	rowCount   int64
	codec      string
	timestamps timestampRange

	// streamed names a streamed data object, which is deleted best-effort when
	// the commit fails for good (PUBLIC_API §Cleanup Behavior). Buffered writes
	// leave their files in place.
	streamed string
}

// newPending starts a snapshot with a fresh ID. Data files are written under
// paths derived from the ID, so it must not change once staging begins.
func (d *dataset) newPending(metadata Metadata) pendingSnapshot {
	if metadata == nil {
		metadata = Metadata{}
	}
	return pendingSnapshot{id: DatasetSnapshotID(generateID()), metadata: metadata}
}

// add records a persisted data file and the partition it belongs to.
func (p *pendingSnapshot) add(ref FileRef, partition string) {
	p.files = append(p.files, ref)
	p.partitions = append(p.partitions, partition)
}

// commit publishes a pending snapshot: resolve the head, write the manifest
// under the current parent, and advance the pointer. On conflict the loop
// refreshes and retries with the same snapshot ID, since its data files are
// already in place and no manifest was written for it.
func (d *dataset) commit(ctx context.Context, p *pendingSnapshot) (*DatasetSnapshot, error) {
	slices.SortFunc(p.files, func(a, b FileRef) int { return strings.Compare(a.Path, b.Path) })

	var manifest *Manifest
	err := d.history.CommitLoop(ctx, &d.retry, func() error {
		parent, expected, err := d.history.Resolve(ctx)
		if err != nil {
			return err
		}
		manifest = d.manifest(p, parent)
		return d.history.Advance(ctx, expected, p.id, manifest, d.manifestPaths(p.id, p.partitions))
	})
	if err != nil {
		d.discard(ctx, p.streamed)
		return nil, err
	}
	return &DatasetSnapshot{ID: p.id, Manifest: manifest}, nil
}

// manifest describes a pending snapshot under the given parent.
func (d *dataset) manifest(p *pendingSnapshot, parent DatasetSnapshotID) *Manifest {
	m := &Manifest{
		SchemaName:       manifestSchemaName,
		FormatVersion:    manifestFormatVersion,
		DatasetID:        d.id,
		SnapshotID:       p.id,
		CreatedAt:        time.Now().UTC(),
		Metadata:         p.metadata,
		Files:            p.files,
		ParentSnapshotID: parent,
		RowCount:         p.rowCount,
		MinTimestamp:     p.timestamps.min,
		MaxTimestamp:     p.timestamps.max,
		Codec:            p.codec,
		Compressor:       d.compressor.Name(),
		Partitioner:      d.layout.partitioner().name(),
	}
	if d.checksum != nil {
		m.ChecksumAlgorithm = d.checksum.Name()
	}
	return m
}

// manifestPaths returns where a snapshot's manifest is written: always the
// canonical path (O(1) Snapshot lookup) and, on layouts that place segments
// under partitions, one copy per partition the snapshot touches so partition
// listings can find it. Partitionless layouts have only the canonical path.
func (d *dataset) manifestPaths(snapshotID DatasetSnapshotID, partitionKeys []string) []string {
	canonical := d.layout.manifestPath(d.id, snapshotID)
	paths := []string{canonical}
	if !d.layout.supportsPartitions() {
		return paths
	}
	seen := map[string]bool{canonical: true}
	for _, pk := range partitionKeys {
		if pk == "" {
			continue
		}
		p := d.layout.manifestPathInPartition(d.id, snapshotID, pk)
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}
	return paths
}

// discard deletes a data file whose commit failed. No-op for an empty path.
func (d *dataset) discard(ctx context.Context, path string) {
	if path != "" {
		_ = d.store.Delete(ctx, path)
	}
}

// -----------------------------------------------------------------------------
// Buffered staging (Write)
// -----------------------------------------------------------------------------

// stage persists data as this dataset's kind of files: one blob without a
// codec, one encoded file per partition with one.
func (d *dataset) stage(ctx context.Context, p *pendingSnapshot, data []any) error {
	if d.codec == nil {
		return d.stageBlob(ctx, p, data)
	}
	return d.stageRecords(ctx, p, data)
}

func (d *dataset) stageBlob(ctx context.Context, p *pendingSnapshot, data []any) error {
	if len(data) != 1 {
		return errors.New("lode: raw blob mode requires exactly one data element")
	}
	blob, ok := data[0].([]byte)
	if !ok {
		return fmt.Errorf("lode: raw blob mode requires []byte, got %T", data[0])
	}
	ref, err := d.putFile(ctx, d.filePath(p.id, "", blobFile), blobContent(blob))
	if err != nil {
		return fmt.Errorf("lode: failed to write blob: %w", err)
	}
	p.add(ref, "")
	p.rowCount = 1
	return nil
}

func (d *dataset) stageRecords(ctx context.Context, p *pendingSnapshot, data []any) error {
	partitions, err := d.partitionRecords(data)
	if err != nil {
		return fmt.Errorf("lode: partitioning failed: %w", err)
	}
	for key, records := range partitions {
		ref, err := d.putFile(ctx, d.filePath(p.id, key, dataFile), encodedRecords{d.codec, records})
		if err != nil {
			return fmt.Errorf("lode: failed to write data file: %w", err)
		}
		ref.Stats = fileStats(d.codec)
		p.add(ref, key)
	}
	p.rowCount = int64(len(data))
	p.codec = d.codec.Name()
	for _, record := range data {
		p.timestamps.observe(record)
	}
	return nil
}

func (d *dataset) partitionRecords(records []any) (map[string][]any, error) {
	partitions := make(map[string][]any)
	part := d.layout.partitioner()

	for _, record := range records {
		key, err := part.partitionKey(record)
		if err != nil {
			return nil, err
		}
		partitions[key] = append(partitions[key], record)
	}

	return partitions, nil
}

// filePath places a data file of the given role in the snapshot's directory.
func (d *dataset) filePath(id DatasetSnapshotID, partition, role string) string {
	return d.layout.dataFilePath(d.id, id, partition, role+d.compressor.Extension())
}

// putFile compresses content into memory and stores it as one immutable
// object, returning a reference that describes the stored bytes.
func (d *dataset) putFile(ctx context.Context, path string, content fileContent) (FileRef, error) {
	var buf bytes.Buffer
	comp, err := d.compressor.Compress(&buf)
	if err != nil {
		return FileRef{}, err
	}
	if err := content.writeTo(comp); err != nil {
		_ = comp.Close()
		return FileRef{}, err
	}
	if err := comp.Close(); err != nil {
		return FileRef{}, err
	}

	data := buf.Bytes()
	if err := d.store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return FileRef{}, err
	}
	return FileRef{Path: path, SizeBytes: int64(len(data)), Checksum: d.checksumBytes(data)}, nil
}

// checksumBytes computes a checksum of data using the dataset's configured
// checksum algorithm. Returns empty string if no checksum is configured.
func (d *dataset) checksumBytes(data []byte) string {
	if d.checksum == nil {
		return ""
	}
	hasher := d.checksum.NewHasher()
	_, _ = hasher.Write(data)
	return hasher.Sum()
}

// fileContent is what a buffered data file holds before compression: a raw
// blob, or a record batch that encodes through the codec on demand.
type fileContent interface {
	writeTo(w io.Writer) error
}

type blobContent []byte

func (b blobContent) writeTo(w io.Writer) error {
	_, err := w.Write(b)
	return err
}

type encodedRecords struct {
	codec   Codec
	records []any
}

func (e encodedRecords) writeTo(w io.Writer) error { return e.codec.Encode(w, e.records) }

// fileStats returns per-file statistics when the encoder reports them
// (StatisticalCodec, StatisticalStreamEncoder), nil otherwise.
func fileStats(encoder any) *FileStats {
	if s, ok := encoder.(interface{ FileStats() *FileStats }); ok {
		return s.FileStats()
	}
	return nil
}

// timestampRange accumulates the earliest and latest Timestamp over the
// records it observes; records that are not Timestamped are ignored.
type timestampRange struct {
	min, max *time.Time
}

func (r *timestampRange) observe(record any) {
	ts, ok := record.(Timestamped)
	if !ok {
		return
	}
	t := ts.Timestamp()
	if r.min == nil || t.Before(*r.min) {
		r.min = &t
	}
	if r.max == nil || t.After(*r.max) {
		r.max = &t
	}
}
