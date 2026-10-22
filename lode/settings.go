package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/storepath"
)

// datasetSettings are what every writer of a dataset must share: the
// components and names its manifests record (CONTRACT_WRITE_API §Settings).
type datasetSettings struct {
	codec            string
	codecDescription string
	compressor       string
	checksum         string
	partitioner      string
	partitionKeys    []string
	paths            ManifestPaths
}

// settings are this writer's settings, as its manifests record them.
func (d *dataset) settings() datasetSettings {
	s := datasetSettings{
		compressor:    d.compressor.Name(),
		partitioner:   d.layout.partitioner().name(),
		partitionKeys: d.layout.partitioner().keys(),
		paths:         d.layout.convention(),
	}
	if d.codec != nil {
		s.codec, s.codecDescription = d.codec.Name(), d.codec.Describe()
	}
	if d.checksum != nil {
		s.checksum = d.checksum.Name()
	}
	return s
}

// settingComparison is one setting a manifest must share with the writer.
type settingComparison struct {
	name       string
	want, have string
}

// legacy reports whether m was written before manifests recorded the codec
// description, partition keys, and path names (format version 1.0.0).
func legacy(m *Manifest) bool { return m.Paths == (ManifestPaths{}) }

// mismatch returns the first setting of m that differs from s, or nil. A
// legacy manifest is compared by the settings it records; its partition keys
// are verified separately (verifyLegacyKeys), and its path names are those
// under which it was found.
func (s datasetSettings) mismatch(m *Manifest) error {
	comparisons := []settingComparison{
		{"codec", s.codec, m.Codec},
		{"compressor", s.compressor, m.Compressor},
		{"checksum", s.checksum, m.ChecksumAlgorithm},
		{"partitioner", s.partitioner, m.Partitioner},
	}
	// %q and %#v quote every string, so distinct key lists and path names
	// never format alike ("a,b" against "a", "b").
	if !legacy(m) {
		comparisons = append(comparisons,
			settingComparison{"codec description", s.codecDescription, m.CodecDescription},
			settingComparison{"partition keys", fmt.Sprintf("%q", s.partitionKeys), fmt.Sprintf("%q", m.PartitionKeys)},
			settingComparison{"path names", fmt.Sprintf("%#v", s.paths), fmt.Sprintf("%#v", m.Paths)},
		)
	}
	for _, c := range comparisons {
		if c.want != c.have {
			return fmt.Errorf("lode: %s mismatch: dataset records %q, writer has %q: %w", c.name, c.have, c.want, ErrSettingsMismatch)
		}
	}
	return nil
}

// checkSettings verifies this writer's settings against the manifest of
// parent, the head a commit resolved. Every writer is checked, so every
// later snapshot records the same settings, and a writer checks until a
// check against a 1.1.0 manifest, or its own commit, settles it. A check
// against a legacy manifest compares fewer settings, so a concurrent
// commit may record a description that differs; the writer checks again
// after a conflict. With no parent the dataset is empty, unless its
// directory holds objects persisted under another layout.
func (d *dataset) checkSettings(ctx context.Context, parent DatasetSnapshotID) error {
	if d.settled {
		return nil
	}
	if parent == "" {
		return d.rejectForeignHistory(ctx)
	}
	head, err := d.settingsManifest(ctx, parent)
	if err != nil {
		return err
	}
	if err := d.settings().mismatch(head); err != nil {
		return err
	}
	if legacy(head) && !d.layout.partitioner().isNoop() {
		if err := d.verifyLegacyKeys(ctx, head); err != nil {
			return err
		}
	}
	d.settled = !legacy(head)
	return nil
}

// rejectForeignHistory returns ErrSettingsMismatch when the dataset's
// directory holds an object this layout does not persist: a history written
// under another layout or other path names, whose manifests the configured
// layout cannot find. A writer that took the dataset for empty would
// otherwise begin a second history beside it.
func (d *dataset) rejectForeignHistory(ctx context.Context) error {
	keys, err := d.store.List(ctx, d.layout.datasetPrefix(d.id))
	if err != nil {
		return fmt.Errorf("lode: failed to list the dataset: %w", err)
	}
	for _, k := range keys {
		// A key no store accepts in this form was not persisted by a layout.
		if key, ok := storepath.Key(k); !ok || key != k {
			continue
		}
		if !d.layout.ownsObject(d.id, k) {
			return fmt.Errorf("lode: layout mismatch: dataset holds %s, which the configured layout does not persist: %w", k, ErrSettingsMismatch)
		}
	}
	return nil
}

// settingsManifest is the manifest a writer's settings are checked
// against: that of parent, the snapshot the pointer names; its staged copy
// while a first commit is pending; otherwise the newest manifest, as Latest
// selects it. A pointer that names a snapshot with no manifest in a dataset
// with no other manifest belongs to a pending first commit whose settings
// are unknown, or to one abandoned before staging existed: the writer
// conflicts rather than commit unchecked settings.
func (d *dataset) settingsManifest(ctx context.Context, parent DatasetSnapshotID) (*Manifest, error) {
	loaders := []func() (*DatasetSnapshot, error){
		func() (*DatasetSnapshot, error) { return d.Snapshot(ctx, parent) },
		func() (*DatasetSnapshot, error) { return fromHistory(d.history.LoadStaged(ctx, parent)) },
		func() (*DatasetSnapshot, error) { return fromHistory(d.history.Newest(ctx)) },
	}
	for _, load := range loaders {
		s, err := load()
		if err == nil {
			return s.Manifest, nil
		}
		if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrNoSnapshots) {
			return nil, fmt.Errorf("lode: failed to load the head's settings: %w", err)
		}
	}
	if err := d.rejectForeignHistory(ctx); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("lode: latest pointer names %s, and no manifest records the dataset's settings: %w", parent, ErrSnapshotConflict)
}

// verifyLegacyKeys verifies the writer's partition keys against a dataset
// whose manifests predate recorded keys: by name and in order, against the
// partition path of the first snapshot with files, following parents from
// head. A chain without files contains no partition path, so any keys are
// consistent with it; a chain that ends at a missing or expired parent first
// cannot be verified.
func (d *dataset) verifyLegacyKeys(ctx context.Context, head *Manifest) error {
	want := d.layout.partitioner().keys()
	for m := head; ; {
		if len(m.Files) > 0 {
			have := partitionKeyNames(d.layout.extractPartitionPath(m.Files[0].Path))
			if !slices.Equal(want, have) {
				return fmt.Errorf("lode: partition keys mismatch: dataset has %q, writer has %q: %w", have, want, ErrSettingsMismatch)
			}
			return nil
		}
		if m.ParentSnapshotID == "" {
			return nil
		}
		parent, err := d.Snapshot(ctx, m.ParentSnapshotID)
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("lode: cannot verify partition keys: snapshot %s has no manifest: %w", m.ParentSnapshotID, ErrSettingsMismatch)
		}
		if err != nil {
			return err
		}
		m = parent.Manifest
	}
}

// partitionKeyNames returns the key of each segment of a Hive partition
// path ("day=1/region=eu" yields day, region).
func partitionKeyNames(partition string) []string {
	if partition == "" {
		return nil
	}
	segments := strings.Split(partition, "/")
	names := make([]string, len(segments))
	for i, segment := range segments {
		names[i], _, _ = strings.Cut(segment, "=")
	}
	return names
}
