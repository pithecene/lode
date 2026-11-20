// Package testutil provides helpers for examples and tests.
package testutil

import (
	"os"

	"github.com/pithecene/lode/internal/errs"
)

// RemoveAll removes the path and any children. Errors are ignored.
// Use for defer cleanup in examples and tests.
//
// Usage:
//
//	defer testutil.RemoveAll(tmpDir)
func RemoveAll(path string) { errs.IgnoreError(os.RemoveAll(path)) }
