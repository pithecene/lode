package lode

import "testing"

func TestSentinelErrors_ErrorMessages(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrNotFound", ErrNotFound, "not found"},
		{"ErrNoSnapshots", ErrNoSnapshots, "no snapshots"},
		{"ErrPathExists", ErrPathExists, "path exists"},
		{"ErrNoManifests", ErrNoManifests, "no manifests found (storage contains objects)"},
		{"ErrRangeReadNotSupported", ErrRangeReadNotSupported, "range read not supported"},
		{"ErrCodecNotStreamable", ErrCodecNotStreamable, "codec does not support streaming"},
		{"ErrNilIterator", ErrNilIterator, "records iterator must be non-nil"},
		{"ErrPartitioningNotSupported", ErrPartitioningNotSupported, "StreamWrite does not support partitioning; use Write for partitioned data"},
		{"ErrPartitionPrefixNotSupported", ErrPartitionPrefixNotSupported, "partition prefix requires a partition-aware layout"},
		{"ErrSchemaViolation", ErrSchemaViolation, "schema violation"},
		{"ErrInvalidFormat", ErrInvalidFormat, "invalid format"},
		{"ErrSnapshotConflict", ErrSnapshotConflict, "snapshot conflict: another writer committed"},
		{"ErrDatasetsNotModeled", ErrDatasetsNotModeled, "datasets not modeled by this layout"},
		{"ErrSettingsMismatch", ErrSettingsMismatch, "settings mismatch"},
		{"ErrReadOnly", ErrReadOnly, "read-only store"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.want {
				t.Errorf("%s.Error() = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}
