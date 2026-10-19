package lode

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
