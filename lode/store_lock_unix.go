//go:build unix

package lode

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	"github.com/pithecene/lode/internal/errs"
)

const (
	initialLockPoll = 500 * time.Microsecond
	maxLockPoll     = 5 * time.Millisecond
)

// lockExclusive acquires an exclusive advisory lock on f while honoring ctx
// cancellation, returning the function that releases it.
func lockExclusive(ctx context.Context, f *os.File) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd := int(f.Fd())
	err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return func() { errs.IgnoreError(syscall.Flock(fd, syscall.LOCK_UN)) }, nil
	}
	if !isLockContended(err) {
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
		err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { errs.IgnoreError(syscall.Flock(fd, syscall.LOCK_UN)) }, nil
		}
		if !isLockContended(err) {
			return nil, err
		}
		wait = min(wait*2, maxLockPoll)
		timer.Reset(wait)
	}
}

func isLockContended(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR)
}
