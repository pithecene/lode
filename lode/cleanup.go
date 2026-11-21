package lode

import (
	"io"

	"github.com/pithecene/lode/internal/errs"
)

// closer returns a function that closes c, discarding the error.
// Use with defer for cleanup-only io.Closer values where the
// error is intentionally ignored (e.g., response bodies, read-only files).
func closer(c io.Closer) func() {
	return func() { errs.DiscardClose(c) }
}
