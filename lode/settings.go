package lode

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// settings are what every writer of a dataset must share: the components
// and names its manifests record (CONTRACT_WRITE_API §Settings). A writer
// has settings, and each manifest records them; the format difference
// between manifests is resolved once, when a manifest's settings are read
// (recordedSettings), so no consumer of settings tests the format.
type settings struct {
	codec            string
	codecDescription string
	compressor       string
	checksum         string
	partitioning     partitionScheme
	paths            ManifestPaths

	// legacy marks settings a manifest of format 1.0.0 recorded: it records
	// no codec description or partition keys, so those are unknown, not
	// empty, and its paths are the default names.
	legacy bool
}

// settings are this writer's settings, as its manifests record them.
func (d *writer) settings() settings {
	s := settings{
		codec:            d.codec.name,
		codecDescription: d.codec.description,
		compressor:       d.components.compressor.Name(),
		partitioning:     d.layout.partitioning(),
		paths:            d.layout.convention(),
	}
	if d.components.checksum != nil {
		s.checksum = d.components.checksum.Name()
	}
	return s
}

// record writes s into m, the only place a manifest's settings are set.
// The partition keys are copied: a returned manifest must not alias the
// layout.
func (s settings) record(m *Manifest) {
	m.Codec, m.CodecDescription = s.codec, s.codecDescription
	m.Compressor, m.ChecksumAlgorithm = s.compressor, s.checksum
	m.Partitioner, m.PartitionKeys = string(s.partitioning.name), slices.Clone(s.partitioning.keys)
	m.Paths = s.paths
}

// recordedSettings returns the settings m records, the only place the
// manifest format is told apart. A legacy manifest records no path names:
// it was persisted under the default names, flat or not according to where
// it lies, which is where a layout whose paths are flat exactly when flat
// found it (V1_API_DECISIONS D11, Paths). A manifest of format 1.1.0 must
// record its path names.
func recordedSettings(m *Manifest, flat bool) (settings, error) {
	if err := missingPaths(m); err != nil {
		return settings{}, fmt.Errorf("manifest of snapshot %s: %w", m.SnapshotID, err)
	}
	s := settings{
		codec:            m.Codec,
		codecDescription: m.CodecDescription,
		compressor:       m.Compressor,
		checksum:         m.ChecksumAlgorithm,
		partitioning:     partitionScheme{name: partitioner(m.Partitioner), keys: m.PartitionKeys},
		paths:            m.Paths,
		legacy:           legacy(m),
	}
	if s.legacy {
		s.codecDescription, s.partitioning.keys, s.paths = "", nil, defaultPaths(flat)
	}
	return s, nil
}

// settingComparison is one setting a manifest must share with the writer.
type settingComparison struct {
	name       string
	want, have string
}

// legacy reports whether m was written before manifests recorded the codec
// description, partition keys, and path names (format version 1.0.0).
func legacy(m *Manifest) bool { return m.FormatVersion == legacyFormatVersion }

// legacyFormatVersion is the manifest format before the settings fields.
const legacyFormatVersion = "1.0.0"

// missingPaths is the validation error of a manifest of format 1.1.0 or
// later without the path names it must record.
func missingPaths(m *Manifest) error {
	if legacy(m) {
		return nil
	}
	if m.Paths == (ManifestPaths{}) {
		return &validate.ManifestError{Field: "paths", Message: "is required from format version 1.1.0"}
	}
	if problem := m.Paths.problem(func(f pathField) string { return f.field }); problem != "" {
		return &validate.ManifestError{Field: "paths", Message: problem}
	}
	return nil
}

// mismatch returns the first setting of have, the settings a manifest
// records, that differs from s, or nil. A setting have does not know (a
// legacy manifest's codec description and partition keys) is not
// compared; legacy partition keys are verified separately
// (verifyLegacyKeys).
func (s settings) mismatch(have settings) error {
	// %q and %#v quote every string, so distinct key lists and path names
	// never format alike ("a,b" against "a", "b").
	comparisons := []settingComparison{
		{"codec", s.codec, have.codec},
		{"compressor", s.compressor, have.compressor},
		{"checksum", s.checksum, have.checksum},
		{"partitioner", string(s.partitioning.name), string(have.partitioning.name)},
		{"path names", fmt.Sprintf("%#v", s.paths), fmt.Sprintf("%#v", have.paths)},
	}
	if !have.legacy {
		comparisons = append(comparisons,
			settingComparison{"codec description", s.codecDescription, have.codecDescription},
			settingComparison{"partition keys", fmt.Sprintf("%q", s.partitioning.keys), fmt.Sprintf("%q", have.partitioning.keys)},
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
func (d *writer) checkSettings(ctx context.Context, parent DatasetSnapshotID) error {
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
	mine := d.settings()
	recorded, err := recordedSettings(head, mine.paths.Flat)
	if err != nil {
		return err
	}
	if err := mine.mismatch(recorded); err != nil {
		return err
	}
	if recorded.legacy && !mine.partitioning.isNoop() {
		if err := d.verifyLegacyKeys(ctx, head); err != nil {
			return err
		}
	}
	d.settled = !recorded.legacy
	return nil
}

// removeAdoptedStaged removes the staged manifest of a pending first
// commit that this writer's commit adopted as its parent. Later commits
// stage nothing, so no other commit would remove it. Best-effort: a staged
// copy that remains is never listed as a snapshot. If the pending commit's
// writer is still running, it persists its canonical manifest regardless,
// and a writer that checks its settings meanwhile reads this writer's
// manifest, which records the same settings.
func (d *writer) removeAdoptedStaged(ctx context.Context) {
	if d.adoptedStaged == "" {
		return
	}
	_ = d.store.Delete(ctx, d.adoptedStaged)
	d.adoptedStaged = ""
}

// rejectForeignHistory returns ErrSettingsMismatch when the dataset's
// directory holds a manifest this layout does not persist: a history
// written under another layout, whose manifests the configured layout
// cannot find. A writer that took the dataset for empty would otherwise
// begin a second history beside it.
func (d *writer) rejectForeignHistory(ctx context.Context) error {
	return rejectForeignManifest(ctx, d.store, d.id, d.layout)
}

// rejectForeignManifest returns ErrSettingsMismatch when the directory of
// dataset id holds a manifest that none of layouts persists. Only manifests
// count: data objects that another writer persisted before its first
// commit are not a history, and counting them would make two first writers
// with different layouts reject each other before either commits. A
// manifest is recognized by its file name, from the listing alone: the
// layouts' manifest name, or the default name, which every manifest
// persisted before the names became root options bears.
func rejectForeignManifest(ctx context.Context, store Store, id DatasetID, layouts ...*pathLayout) error {
	keys, err := store.List(ctx, layouts[0].datasetPrefix(id))
	if err != nil {
		return fmt.Errorf("failed to list the dataset: %w", err)
	}
	names := []string{layouts[0].manifestName(), defaultPaths(false).Manifest}
	for _, k := range keys {
		// A key no store accepts in this form was not persisted by a layout.
		if key, ok := storepath.Key(k); !ok || key != k {
			continue
		}
		if !slices.Contains(names, path.Base(k)) {
			continue
		}
		if slices.ContainsFunc(layouts, func(l *pathLayout) bool { return l.ownsObject(id, k) }) {
			continue
		}
		return fmt.Errorf("lode: layout mismatch: dataset holds the manifest %s, which the configured layout does not persist: %w", k, ErrSettingsMismatch)
	}
	return nil
}

// settingsManifest is the manifest a writer's settings are checked
// against: the canonical manifest of parent, the snapshot the pointer
// names; its staged copy while a first commit is pending; otherwise the
// newest manifest, as Latest selects it, by scan. The first two are single
// Gets, so a pending first commit is checked without a listing. A pointer that names a snapshot with no manifest in a dataset
// with no other manifest belongs to a pending first commit whose settings
// are unknown, or to one abandoned before staging existed: the writer
// conflicts rather than commit unchecked settings.
func (d *writer) settingsManifest(ctx context.Context, parent DatasetSnapshotID) (*Manifest, error) {
	loaders := []func() (*DatasetSnapshot, error){
		func() (*DatasetSnapshot, error) { return fromHistory(d.history.Snapshot(ctx, parent)) },
		func() (*DatasetSnapshot, error) {
			s, err := fromHistory(d.history.LoadStaged(ctx, parent))
			if err == nil {
				d.adoptedStaged = d.layout.stagedManifestPath(d.id, parent)
			}
			return s, err
		},
		func() (*DatasetSnapshot, error) { return fromHistory(d.history.Newest(ctx)) },
	}
	for _, load := range loaders {
		s, err := load()
		if err == nil {
			return s.Manifest, nil
		}
		if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrNoSnapshots) {
			return nil, fmt.Errorf("failed to load the head's settings: %w", err)
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
// consistent with it; a chain that ends at a missing or expired parent, or
// repeats a snapshot, before a snapshot with files cannot be verified.
func (d *writer) verifyLegacyKeys(ctx context.Context, head *Manifest) error {
	want := d.layout.partitioning().keys
	visited := map[DatasetSnapshotID]bool{}
	for m := head; ; {
		if visited[m.SnapshotID] {
			return fmt.Errorf("lode: cannot verify partition keys: the parent chain repeats snapshot %s: %w", m.SnapshotID, ErrSettingsMismatch)
		}
		visited[m.SnapshotID] = true
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
