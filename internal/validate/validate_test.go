package validate

import (
	"errors"
	"testing"
)

func TestArgument(t *testing.T) {
	err := Argument("offset %d must be non-negative", -1)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
	if got, want := err.Error(), "lode: invalid argument: offset -1 must be non-negative"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFirstInvalid(t *testing.T) {
	if err := FirstInvalid([]Check{{Invalid: false, Field: "a", Message: "x"}}); err != nil {
		t.Fatalf("all valid: got %v", err)
	}
	err := FirstInvalid([]Check{
		{Invalid: false, Field: "a", Message: "x"},
		{Invalid: true, Field: "b", Message: "is required"},
		{Invalid: true, Field: "c", Message: "is required"},
	})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("got %v, want ErrManifestInvalid", err)
	}
	if got, want := err.Error(), "invalid manifest: b: is required"; got != want {
		t.Fatalf("got %q, want the first failing check %q", got, want)
	}
}

func TestSentinelMessages(t *testing.T) {
	if ErrInvalidArgument.Error() != "invalid argument" || ErrManifestInvalid.Error() != "invalid manifest" {
		t.Fatalf("messages changed: %q, %q", ErrInvalidArgument, ErrManifestInvalid)
	}
}
