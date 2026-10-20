package volume

import (
	"fmt"

	"github.com/pithecene-io/lode/internal/history"
	"github.com/pithecene-io/lode/lode"
)

// historyErrors are the sentinels a volume's history returns, so history
// errors keep lode's public identities.
var historyErrors = history.Errors{
	NotFound:         lode.ErrNotFound,
	NoSnapshots:      lode.ErrNoSnapshots,
	SnapshotConflict: lode.ErrSnapshotConflict,
}

// invalidArgument reports a caller-supplied value that cannot be used. The
// message names the value; the error matches lode.ErrInvalidArgument and
// reads as lode's own argument errors do.
func invalidArgument(format string, args ...any) error {
	return fmt.Errorf("lode: %w: %s", lode.ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// manifestValidationError provides details about manifest validation
// failures; it matches lode.ErrManifestInvalid and reads as lode's dataset
// manifest errors do.
type manifestValidationError struct {
	Field   string
	Message string
}

func (e *manifestValidationError) Error() string {
	return fmt.Sprintf("invalid manifest: %s: %s", e.Field, e.Message)
}

func (e *manifestValidationError) Unwrap() error {
	return lode.ErrManifestInvalid
}

// fieldCheck is one manifest validation rule: invalid is true when the field
// violates it. validateManifest is a table of these, so the rules read as
// data and the error shape is uniform.
type fieldCheck struct {
	invalid bool
	field   string
	message string
}

// firstInvalid returns the validation error for the first failing check, or
// nil when every check passes.
func firstInvalid(checks []fieldCheck) error {
	for _, c := range checks {
		if c.invalid {
			return &manifestValidationError{Field: c.field, Message: c.message}
		}
	}
	return nil
}
