package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"

	"github.com/pithecene/lode/internal/errs"
	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// settings are what every writer of a dataset must share: the components
// and topology its manifests record (CONTRACT_WRITE_API §Settings).
type settings struct {
	codec            string
	codecDescription string
	compressor       string
	checksum         string
	partitioning     partitionScheme
	flat             bool
	customV11Paths   *legacyManifestPaths // non-nil only if a 1.1.0 manifest recorded custom directory names
	legacy           bool                 // format 1.0.0
}

func (d *writer) settings() settings {
	s := settings{
		codec:            d.codec.name,
		codecDescription: d.codec.description,
		compressor:       d.writes.compressor.Name(),
		partitioning:     d.layout.partitioning(),
		flat:             d.layout.isFlat(),
	}
	if d.writes.checksum != nil {
		s.checksum = d.writes.checksum.Name()
	}
	return s
}

func (s settings) record(m *Manifest) {
	m.Codec, m.CodecDescription = s.codec, s.codecDescription
	m.Compressor, m.ChecksumAlgorithm = s.compressor, s.checksum
	m.Partitioner, m.PartitionKeys = string(s.partitioning.name), slices.Clone(s.partitioning.keys)
	m.Flat = s.flat
}

func recordedSettings(m *Manifest, flat bool) (settings, error) {
	if err := checkLegacyPaths(m); err != nil {
		return settings{}, fmt.Errorf("manifest of snapshot %s: %w", m.SnapshotID, err)
	}
	s := settings{
		codec:            m.Codec,
		codecDescription: m.CodecDescription,
		compressor:       m.Compressor,
		checksum:         m.ChecksumAlgorithm,
		partitioning:     partitionScheme{name: partitioner(m.Partitioner), keys: m.PartitionKeys},
		flat:             m.Flat,
		legacy:           legacy(m),
	}
	if s.legacy {
		s.codecDescription, s.partitioning.keys, s.flat = "", nil, flat
	}
	if m.legacyPaths != nil && *m.legacyPaths != defaultLegacyPaths(m.legacyPaths.Flat) {
		s.customV11Paths = m.legacyPaths
	}
	return s, nil
}

type settingComparison struct {
	name       string
	want, have string
}

const (
	legacyFormatVersion = "1.0.0"
	v11FormatVersion    = "1.1.0"
)

func legacy(m *Manifest) bool { return m.FormatVersion == legacyFormatVersion }

// checkLegacyPaths validates the "paths" object when reading a format 1.1.0
// manifest, preserving backward compatibility with v0.12–v0.17 data.
func checkLegacyPaths(m *Manifest) error {
	if m.FormatVersion != v11FormatVersion {
		return nil
	}
	if m.legacyPaths == nil || *m.legacyPaths == (legacyManifestPaths{}) {
		return &validate.ManifestError{Field: "paths", Message: "is required in format version 1.1.0"}
	}
	if m.legacyPaths.Flat && (m.legacyPaths.Datasets != "" || m.legacyPaths.Snapshots != "") {
		return &validate.ManifestError{Field: "paths", Message: "of flat paths must not name the datasets or snapshots directory"}
	}
	return nil
}

func (s settings) mismatch(have settings) error {
	if have.customV11Paths != nil {
		return fmt.Errorf("lode: path names mismatch: dataset records custom 1.1.0 paths %#v, which v1.0 roots do not configure (use lode.Prefixed): %w", *have.customV11Paths, ErrSettingsMismatch)
	}
	comparisons := []settingComparison{
		{"codec", s.codec, have.codec},
		{"compressor", s.compressor, have.compressor},
		{"checksum", s.checksum, have.checksum},
		{"partitioner", string(s.partitioning.name), string(have.partitioning.name)},
		{"flat paths", fmt.Sprintf("%t", s.flat), fmt.Sprintf("%t", have.flat)},
	}
	if !have.legacy {
		comparisons = append(comparisons,
			settingComparison{"codec description", s.codecDescription, have.codecDescription},
			settingComparison{"partition keys", fmt.Sprintf("%q", s.partitioning.keys), fmt.Sprintf("%q", have.partitioning.keys)},
		)
	}
	return firstMismatch("writer", comparisons)
}

func layoutMismatch(want partitionScheme, have settings) error {
	comparisons := []settingComparison{{"partitioner", string(want.name), string(have.partitioning.name)}}
	if !have.legacy {
		comparisons = append(comparisons, settingComparison{"partition keys", fmt.Sprintf("%q", want.keys), fmt.Sprintf("%q", have.partitioning.keys)})
	}
	return firstMismatch("view", comparisons)
}

func (d *dataset) checkExpectedLayout(ctx context.Context, expected *partitionScheme, m *Manifest, recorded settings) error {
	if expected == nil {
		return nil
	}
	if err := layoutMismatch(*expected, recorded); err != nil {
		return err
	}
	if recorded.legacy && !expected.isNoop() {
		return d.verifyLegacyKeys(ctx, m, "view")
	}
	return nil
}

func firstMismatch(holder string, comparisons []settingComparison) error {
	for _, c := range comparisons {
		if c.want != c.have {
			return fmt.Errorf("lode: %s mismatch: dataset records %q, %s has %q: %w", c.name, c.have, holder, c.want, ErrSettingsMismatch)
		}
	}
	return nil
}

func (d *writer) checkSettings(ctx context.Context, parent SnapshotID) error {
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
	return d.checkManifestSettings(ctx, head)
}

func (d *writer) checkManifestSettings(ctx context.Context, head *Manifest) error {
	mine := d.settings()
	recorded, err := recordedSettings(head, mine.flat)
	if err != nil {
		return err
	}
	if err := mine.mismatch(recorded); err != nil {
		return err
	}
	if recorded.legacy && !mine.partitioning.isNoop() {
		if err := d.verifyLegacyKeys(ctx, head, "writer"); err != nil {
			return err
		}
	}
	d.settled = !recorded.legacy
	return nil
}

func (d *writer) rejectForeignHistory(ctx context.Context) error {
	return rejectForeignManifest(ctx, d.store, d.id, d.layout)
}

func rejectForeignManifest(ctx context.Context, store Store, id DatasetID, layouts ...*pathLayout) error {
	keys, err := store.List(ctx, layouts[0].datasetPrefix(id))
	if err != nil {
		return fmt.Errorf("failed to list the dataset: %w", err)
	}
	for _, k := range keys {
		if key, ok := storepath.Key(k); !ok || key != k {
			continue
		}
		if path.Base(k) != manifestFile {
			continue
		}
		if slices.ContainsFunc(layouts, func(l *pathLayout) bool { return l.ownsObject(id, k) }) {
			continue
		}
		// A caller's own file can have a manifest's name.
		lodes, err := isForeignManifest(ctx, store, id, k, layouts[0].isFlat())
		if err != nil {
			return err
		}
		if !lodes {
			continue
		}
		return fmt.Errorf("lode: layout mismatch: dataset holds the manifest %s, which the configured layout does not persist: %w", k, ErrSettingsMismatch)
	}
	return nil
}

// manifestProbe is how much of a candidate is read: a manifest's schema name is
// its first field.
const manifestProbe = 4 << 10

// isForeignManifest reports whether the object is a manifest of some layout: any
// object in a bundled layout's manifest directory, so that a damaged manifest
// counts, and elsewhere one that declares Lode's schema.
func isForeignManifest(ctx context.Context, store Store, id DatasetID, key string, flat bool) (bool, error) {
	if isBundledManifest(id, key, flat) {
		return true, nil
	}
	head, err := readProbe(ctx, store, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to read %s: %w", key, err)
	}
	return declaresManifest(head), nil
}

// isBundledManifest reports whether the key is a manifest path of the dataset in
// either bundled layout of the root (CONTRACT_LAYOUT §Canonical Path Grammars).
func isBundledManifest(id DatasetID, key string, flat bool) bool {
	for _, partitioned := range []bool{false, true} {
		sp, ok := newPathGrammar(flat, partitioned).parse(key)
		if ok && sp.kind == manifestLeaf && sp.dataset == id && history.IsGeneratedID(sp.snapshot) && isHivePartition(sp.partition) {
			return true
		}
	}
	return false
}

// readProbe reads the start of an object, with a ranged read where the store
// has one.
func readProbe(ctx context.Context, store Store, key string) ([]byte, error) {
	if ranged, ok := store.(RangeReader); ok {
		return ranged.ReadRange(ctx, key, 0, manifestProbe)
	}
	rc, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer errs.DiscardClose(rc)
	return io.ReadAll(io.LimitReader(rc, manifestProbe))
}

// declaresManifest reports whether the start of a JSON object has the top-level
// schema name of a manifest.
func declaresManifest(head []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(head))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return false
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return false
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return false
		}
		if key == "schema_name" {
			var name string
			if json.Unmarshal(value, &name) == nil && name == manifestSchemaName {
				return true
			}
		}
	}
	return false
}

func (d *writer) settingsManifest(ctx context.Context, parent SnapshotID) (*Manifest, error) {
	canonical := func() (*Snapshot, error) { return fromHistory(d.history.Snapshot(ctx, parent)) }
	loaders := []func() (*Snapshot, error){
		canonical,
		func() (*Snapshot, error) { return fromHistory(d.history.LoadStaged(ctx, parent)) },
		canonical,
		func() (*Snapshot, error) { return fromHistory(d.history.Newest(ctx)) },
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

func (d *dataset) verifyLegacyKeys(ctx context.Context, head *Manifest, holder string) error {
	want := d.layout.partitioning().keys
	visited := map[SnapshotID]struct{}{}
	for m := head; ; {
		if _, repeated := visited[m.SnapshotID]; repeated {
			return fmt.Errorf("lode: cannot verify partition keys: the parent chain repeats snapshot %s: %w", m.SnapshotID, ErrSettingsMismatch)
		}
		visited[m.SnapshotID] = struct{}{}
		if len(m.Objects) > 0 {
			have := partitionKeyNames(d.layout.extractPartitionPath(m.Objects[0].Path))
			if !slices.Equal(want, have) {
				return fmt.Errorf("lode: partition keys mismatch: dataset has %q, %s has %q: %w", have, holder, want, ErrSettingsMismatch)
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
