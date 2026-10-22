//go:build windows

package lode

import (
	"os"

	"golang.org/x/sys/windows"
)

// lockExclusive blocks until it holds an exclusive lock on the first byte of
// f and returns the function that releases it. Locking a byte range beyond
// the end of the file is permitted, so the lock file may stay empty.
func lockExclusive(f *os.File) (unlock func(), err error) {
	h := windows.Handle(f.Fd())
	overlapped := new(windows.Overlapped)
	if err := windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, overlapped); err != nil {
		return nil, err
	}
	return func() { _ = windows.UnlockFileEx(h, 0, 1, 0, overlapped) }, nil
}
