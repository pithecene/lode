//go:build windows

package lode

import (
	"context"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

const (
	initialLockPoll = 500 * time.Microsecond
	maxLockPoll     = 5 * time.Millisecond
)

// lockExclusive acquires an exclusive lock on the first byte of f while
// honoring ctx cancellation, returning the function that releases it. Locking
// a byte range beyond the end of the file is permitted, so the lock file may
// stay empty.
func lockExclusive(ctx context.Context, f *os.File) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h := windows.Handle(f.Fd())
	overlapped := new(windows.Overlapped)
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)

	err = windows.LockFileEx(h, flags, 0, 1, 0, overlapped)
	if err == nil {
		return func() { _ = windows.UnlockFileEx(h, 0, 1, 0, overlapped) }, nil
	}
	if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return nil, err
	}

	wait := initialLockPoll
	timer := time.NewTimer(wait)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		err = windows.LockFileEx(h, flags, 0, 1, 0, overlapped)
		if err == nil {
			return func() { _ = windows.UnlockFileEx(h, 0, 1, 0, overlapped) }, nil
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, err
		}
		wait = min(wait*2, maxLockPoll)
		timer.Reset(wait)
	}
}
