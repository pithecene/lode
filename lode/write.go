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

	"github.com/pithecene/lode/internal/history"
)

const (
	blobFile = "blob"
	dataFile = "data"
)

type pendingSnapshot struct {
	id         SnapshotID
	metadata   Metadata
	objects    []ObjectRef
	partitions []string
	rowCount   int64
	timestamps timestampRange
	streamed   string
}

func (d *writer) newPending(metadata any) (pendingSnapshot, error) {
	m, err := NewMetadata(metadata)
	if err != nil {
		return pendingSnapshot{}, err
	}
	return pendingSnapshot{id: SnapshotID(history.NewID()), metadata: m, objects: []ObjectRef{}}, nil
}

func (p *pendingSnapshot) add(ref ObjectRef, partition string) {
	p.objects = append(p.objects, ref)
	p.partitions = append(p.partitions, partition)
}

func (d *writer) commit(ctx context.Context, p *pendingSnapshot) (*Snapshot, error) {
	slices.SortFunc(p.objects, func(a, b ObjectRef) int { return strings.Compare(a.Path, b.Path) })

	var manifest *Manifest
	err := d.history.CommitLoop(ctx, &d.retry, func() error {
		parent, expected, err := d.history.Resolve(ctx)
		if err != nil {
			return err
		}
		if err := d.checkSettings(ctx, parent); err != nil {
			return err
		}
		manifest = d.manifest(p, parent)
		return d.history.Advance(ctx, expected, p.id, manifest, d.manifestPaths(p.id, p.partitions))
	})
	if err != nil {
		err = d.classifyConflict(ctx, err)
		if !errors.Is(err, history.ErrManifestUncertain) {
			d.discard(ctx, p.streamed)
		}
		return nil, err
	}
	d.settled = true
	return &Snapshot{ID: p.id, Manifest: manifest}, nil
}

func (d *writer) classifyConflict(ctx context.Context, err error) error {
	if d.settled || errors.Is(err, history.ErrManifestUncertain) || !errors.Is(err, ErrSnapshotConflict) {
		return err
	}
	if d.history.Refresh(ctx) != nil {
		return err
	}
	parent, _, resolveErr := d.history.Resolve(ctx)
	if resolveErr != nil || parent == "" {
		return err
	}
	if mismatch := d.checkSettings(ctx, parent); errors.Is(mismatch, ErrSettingsMismatch) {
		return mismatch
	}
	return err
}

func (d *writer) manifest(p *pendingSnapshot, parent SnapshotID) *Manifest {
	m := &Manifest{
		SchemaName:       manifestSchemaName,
		FormatVersion:    manifestFormatVersion,
		DatasetID:        d.id,
		SnapshotID:       p.id,
		CreatedAt:        time.Now().UTC(),
		Metadata:         p.metadata,
		Objects:          p.objects,
		ParentSnapshotID: parent,
		RowCount:         p.rowCount,
		MinTimestamp:     p.timestamps.min,
		MaxTimestamp:     p.timestamps.max,
	}
	d.settings().record(m)
	return m
}

func (d *dataset) manifestPaths(snapshotID SnapshotID, partitionKeys []string) []string {
	canonical := d.layout.manifestPath(d.id, snapshotID)
	paths := []string{canonical}
	if !d.layout.supportsPartitions() {
		return paths
	}
	seen := map[string]struct{}{canonical: {}}
	for _, pk := range partitionKeys {
		if pk == "" {
			continue
		}
		p := d.layout.manifestPathInPartition(d.id, snapshotID, pk)
		if _, dup := seen[p]; !dup {
			seen[p] = struct{}{}
			paths = append(paths, p)
		}
	}
	return paths
}

const streamCleanupTimeout = 10 * time.Second

func (d *writer) discard(ctx context.Context, path string) {
	if path != "" {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), streamCleanupTimeout)
		defer cancel()
		_ = d.store.Delete(cleanupCtx, path)
	}
}

// -----------------------------------------------------------------------------
// Buffered staging (Write)
// -----------------------------------------------------------------------------

func (d *writer) stageBlob(ctx context.Context, p *pendingSnapshot, blob []byte) error {
	ref, err := d.putObject(ctx, d.filePath(p.id, "", blobFile), blobContent(blob))
	if err != nil {
		return fmt.Errorf("failed to write blob: %w", err)
	}
	p.add(ref, "")
	p.rowCount = 1
	return nil
}

func stageRecords[T any](ctx context.Context, r *Records[T], p *pendingSnapshot, records []T) error {
	d := r.writer
	partitions, err := groupRecords(records, r.partitionKey, !d.layout.partitioning().isNoop())
	if err != nil {
		return fmt.Errorf("partitioning failed: %w", err)
	}
	for key, group := range partitions {
		var stats *ObjectStats
		content := &encodedRecords[T]{codec: r.codec, records: group, stats: &stats}
		ref, err := d.putObject(ctx, d.filePath(p.id, key, dataFile), content)
		if err != nil {
			return fmt.Errorf("failed to write data object: %w", err)
		}
		ref.Stats = stats
		p.add(ref, key)
	}
	p.rowCount = int64(len(records))
	if r.timestamped {
		for i := range records {
			p.timestamps.observe(records[i])
		}
	}
	return nil
}

func (d *writer) filePath(id SnapshotID, partition, role string) string {
	return d.layout.dataFilePath(d.id, id, partition, role+d.components.compressor.Extension())
}

func (d *writer) putObject(ctx context.Context, path string, content objectContent) (ObjectRef, error) {
	ref, err := d.storeObject(ctx, path, content)
	if err != nil {
		return ObjectRef{}, fmt.Errorf("%s: %w", content.op(), err)
	}
	return ref, nil
}

func (d *writer) storeObject(ctx context.Context, path string, content objectContent) (ObjectRef, error) {
	var buf bytes.Buffer
	comp, err := d.components.compressor.Compress(&buf)
	if err != nil {
		return ObjectRef{}, err
	}
	if err := content.writeTo(comp); err != nil {
		_ = comp.Close()
		return ObjectRef{}, err
	}
	if err := comp.Close(); err != nil {
		return ObjectRef{}, err
	}

	data := buf.Bytes()
	if err := d.store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return ObjectRef{}, err
	}
	return ObjectRef{Path: path, SizeBytes: int64(len(data)), Checksum: d.checksumBytes(data)}, nil
}

func (d *writer) checksumBytes(data []byte) string {
	if d.components.checksum == nil {
		return ""
	}
	hasher := d.components.checksum.NewHasher()
	_, _ = hasher.Write(data)
	return hasher.Sum()
}

type objectContent interface {
	writeTo(w io.Writer) error
	op() string
}

type blobContent []byte

func (b blobContent) writeTo(w io.Writer) error {
	_, err := w.Write(b)
	return err
}

func (blobContent) op() string { return "write blob" }

type encodedRecords[T any] struct {
	codec   Codec[T]
	records []T
	stats   **ObjectStats
}

func (e *encodedRecords[T]) writeTo(w io.Writer) error {
	if sc, ok := e.codec.(StatisticalCodec[T]); ok {
		s, err := sc.EncodeWithStats(w, e.records)
		if err != nil {
			return err
		}
		*e.stats = s
		return nil
	}
	return e.codec.Encode(w, e.records)
}

func (*encodedRecords[T]) op() string { return "write data object" }

func streamEncoderStats(encoder any) *ObjectStats {
	if s, ok := encoder.(StatisticalStreamEncoder); ok {
		return s.ObjectStats()
	}
	return nil
}

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
