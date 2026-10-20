// Package validate holds the argument and manifest validation errors every
// Lode package reports. It owns the two sentinels behind them; package lode
// re-exports them as lode.ErrInvalidArgument and lode.ErrManifestInvalid, so
// identity under errors.Is and the messages are the same from any package.
package validate

import "fmt"

var (
	// ErrInvalidArgument is re-exported as lode.ErrInvalidArgument.
	ErrInvalidArgument error = errInvalidArgument{}

	// ErrManifestInvalid is re-exported as lode.ErrManifestInvalid.
	ErrManifestInvalid error = errManifestInvalid{}
)

type errInvalidArgument struct{}

func (errInvalidArgument) Error() string { return "invalid argument" }

type errManifestInvalid struct{}

func (errManifestInvalid) Error() string { return "invalid manifest" }

// Argument reports a caller-supplied value that cannot be used. The
// message names the value; the error matches ErrInvalidArgument.
func Argument(format string, args ...any) error {
	return fmt.Errorf("lode: %w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// ManifestError provides details about manifest validation failures.
type ManifestError struct {
	Field   string
	Message string
}

func (e *ManifestError) Error() string {
	return fmt.Sprintf("invalid manifest: %s: %s", e.Field, e.Message)
}

// Unwrap makes the error match ErrManifestInvalid.
func (e *ManifestError) Unwrap() error {
	return ErrManifestInvalid
}

// Check is one manifest validation rule: Invalid is true when the field
// violates it. Manifest validators are tables of these, so the rules read
// as data and the error shape is uniform.
type Check struct {
	Invalid bool
	Field   string
	Message string
}

// FirstInvalid returns the validation error for the first failing check, or
// nil when every check passes.
func FirstInvalid(checks []Check) error {
	for _, c := range checks {
		if c.Invalid {
			return &ManifestError{Field: c.Field, Message: c.Message}
		}
	}
	return nil
}
