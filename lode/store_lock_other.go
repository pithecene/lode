//go:build !unix && !windows

package lode

import (
	"errors"
	"fmt"
	"os"
)

// lockExclusive reports that this platform offers the filesystem store no
// exclusive file lock, so its CompareAndSwap cannot be atomic across
// processes. It fails rather than lock within the process alone, which
// would silently stop protecting writers in other processes.
func lockExclusive(*os.File) (unlock func(), err error) {
	return nil, fmt.Errorf("the filesystem store has no exclusive file lock on this platform: %w", errors.ErrUnsupported)
}
