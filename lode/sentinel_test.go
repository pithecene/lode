package lode

import (
	"errors"
	"testing"
)

func TestSentinelErrors_ErrorMessages(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrNotFound", ErrNotFound, "not found"},
		{"ErrNoSnapshots", ErrNoSnapshots, "no snapshots"},
		{"ErrPathExists", ErrPathExists, "path exists"},
		{"ErrRangeReadNotSupported", ErrRangeReadNotSupported, "range read not supported"},
		{"ErrCodecNotStreamable", ErrCodecNotStreamable, "codec does not support streaming"},
		{"ErrPartitioningNotSupported", ErrPartitioningNotSupported, "StreamWrite does not support partitioning; use Write for partitioned data"},
		{"ErrNotPartitioned", ErrNotPartitioned, "operation requires a partitioned layout"},
		{"ErrSchemaViolation", ErrSchemaViolation, "schema violation"},
		{"ErrInvalidFormat", ErrInvalidFormat, "invalid format"},
		{"ErrChecksumMismatch", ErrChecksumMismatch, "checksum mismatch"},
		{"ErrSnapshotConflict", ErrSnapshotConflict, "snapshot conflict: another writer committed"},
		{"ErrInvalidArgument", ErrInvalidArgument, "invalid argument"},
		{"ErrInvalidPath", ErrInvalidPath, "invalid path"},
		{"ErrSnapshotExpired", ErrSnapshotExpired, "snapshot expired"},
		{"ErrRemoved", ErrRemoved, "removed"},
		{"ErrHeadUncommitted", ErrHeadUncommitted, "head snapshot is not committed yet"},
		{"ErrSettingsMismatch", ErrSettingsMismatch, "settings mismatch"},
		{"ErrManifestInvalid", ErrManifestInvalid, "invalid manifest"},
		{"ErrReadOnly", ErrReadOnly, "read-only store"},
		{"ErrDatasetsNotModeled", ErrDatasetsNotModeled, "datasets not modeled by this layout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.want {
				t.Errorf("%s.Error() = %q, want %q", tt.name, got, tt.want)
			}
		})
	}

	if !errors.Is(ErrSnapshotExpired, ErrNotFound) {
		t.Error("ErrSnapshotExpired must match ErrNotFound")
	}
}
