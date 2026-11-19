package lode

import (
	"testing"
)

func TestSentinelErrors_ErrorMessages(t *testing.T) {
	wantMessages := map[error]string{
		ErrNotFound:                 "not found",
		ErrNoSnapshots:              "no snapshots",
		ErrPathExists:               "path exists",
		ErrRangeReadNotSupported:    "range read not supported",
		ErrCodecNotStreamable:       "codec does not support streaming",
		ErrPartitioningNotSupported: "StreamWrite does not support partitioning; use Write for partitioned data",
		ErrNotPartitioned:           "operation requires a partitioned layout",
		ErrSchemaViolation:          "schema violation",
		ErrInvalidFormat:            "invalid format",
		ErrChecksumMismatch:         "checksum mismatch",
		ErrSnapshotConflict:         "snapshot conflict: another writer committed",
		ErrInvalidArgument:          "invalid argument",
		ErrInvalidPath:              "invalid path",
		ErrSnapshotExpired:          "snapshot expired",
		ErrRemoved:                  "removed",
		ErrHeadUncommitted:          "head snapshot is not committed yet",
		ErrSettingsMismatch:         "settings mismatch",
		ErrManifestInvalid:          "invalid manifest",
		ErrReadOnly:                 "read-only store",
		ErrDatasetsNotModeled:       "datasets not modeled by this layout",
	}
	for sentinel, want := range wantMessages {
		if got := sentinel.Error(); got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	}
}
