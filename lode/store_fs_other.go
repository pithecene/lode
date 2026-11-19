//go:build !windows

package lode

import (
	"fmt"
	"os"
)

// openRead opens a stored object for reading. Outside Windows an open file
// never prevents another process from replacing or deleting the object.
func openRead(name string) (*os.File, error) {
	return os.Open(name)
}

// replaceFile atomically renames from to to, replacing an existing to.
func replaceFile(from, to string) error {
	return os.Rename(from, to)
}

// syncDir flushes directory entries to stable storage.
func syncDir(name string) error {
	d, err := os.Open(name) //nolint:gosec // name is resolved inside f.root
	if err != nil {
		return fmt.Errorf("fs: sync dir: %w", err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("fs: sync dir: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("fs: sync dir: %w", closeErr)
	}
	return nil
}
