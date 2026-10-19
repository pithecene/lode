package lode

import "fmt"

// fieldCheck is one manifest validation rule: invalid is true when the field
// violates it. Manifest validators are tables of these (reader.go,
// volume.go), so the rules read as data and the error shape is uniform.
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

// invalidArgument reports a caller-supplied value that cannot be used. The
// message names the value; the error matches ErrInvalidArgument.
func invalidArgument(format string, args ...any) error {
	return fmt.Errorf("lode: %w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
