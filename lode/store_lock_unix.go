//go:build unix

package lode

import (
	"os"
	"syscall"
)

// lockExclusive blocks until it holds an exclusive advisory lock on f and
// returns the function that releases it.
func lockExclusive(f *os.File) (unlock func(), err error) {
	fd := int(f.Fd())
	if err := syscall.Flock(fd, syscall.LOCK_EX); err != nil {
		return nil, err
	}
	return func() { _ = syscall.Flock(fd, syscall.LOCK_UN) }, nil
}
