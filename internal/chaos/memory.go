package chaos

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
)

const (
	// retainedAllowance is the heap a run may retain whatever its length:
	// bounded caches such as the S3 adapter's record of pointer versions,
	// and runtime and SDK state built on first use. Runs on the filesystem,
	// LocalStack, and RustFS retained under 100 KB when it was set.
	retainedAllowance = 1 << 20
	// retainedPerCommit is the heap a run may retain per acknowledged
	// commit on a store that keeps no data in memory. Once a run ends its
	// ledger and verifier are garbage, so a correct library retains nothing
	// per commit; runs retained at most 12 bytes per commit when it was set,
	// so the margin absorbs measurement noise, not a leak.
	retainedPerCommit = 64
	// goroutineSettle is how long goroutines may take to exit once a run and
	// its backend are released.
	goroutineSettle = 5 * time.Second
)

// MemoryBaseline is the live heap and the goroutines that exist before a
// run.
type MemoryBaseline struct {
	heap       uint64
	goroutines map[int]bool
}

// TakeMemoryBaseline measures the live heap and records the goroutines.
func TakeMemoryBaseline() MemoryBaseline {
	ids := make(map[int]bool)
	for id := range goroutineStacks() {
		ids[id] = true
	}
	return MemoryBaseline{heap: liveHeap(), goroutines: ids}
}

// goroutineStacks returns the stack of every goroutine, by goroutine ID,
// parsed from the runtime's dump of all stacks, in which each goroutine's
// stack begins with a line "goroutine <id> [<state>]:" and stacks are
// separated by a blank line.
func goroutineStacks() map[int]string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	stacks := make(map[int]string)
	for stack := range strings.SplitSeq(string(buf), "\n\n") {
		var id int
		if _, err := fmt.Sscanf(stack, "goroutine %d ", &id); err == nil {
			stacks[id] = stack
		}
	}
	return stacks
}

// startedSince returns the stacks of the goroutines that did not exist at
// the baseline, except those that lingering, when set, accepts.
func (b MemoryBaseline) startedSince(lingering func(stack string) bool) []string {
	var started []string
	for id, stack := range goroutineStacks() {
		if !b.goroutines[id] && (lingering == nil || !lingering(stack)) {
			started = append(started, stack)
		}
	}
	return started
}

// liveHeap returns the heap that remains after garbage collection. The
// second collection empties the caches that sync.Pool keeps for one cycle.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// RetainedHeap reports the heap that a finished run retains beyond the
// baseline, and an error when it exceeds what commits acknowledged commits
// may account for. It applies only while the store that the run wrote to is
// still reachable and keeps no data in memory.
func (b MemoryBaseline) RetainedHeap(commits int) (uint64, error) {
	now := liveHeap()
	if now <= b.heap {
		return 0, nil
	}
	retained := now - b.heap
	if limit := uint64(retainedAllowance + retainedPerCommit*commits); retained > limit {
		return retained, fmt.Errorf("heap-retained: the run retains %d bytes after %d acknowledged commits, beyond the %d bytes allowed", retained, commits, limit)
	}
	return retained, nil
}

// GoroutinesReleased waits until every goroutine started since the
// baseline has exited, except those that lingering, when set, accepts as a
// backend's own machinery, and reports the stacks of those that remain after
// goroutineSettle as an error. It compares goroutines by identity, so a
// goroutine of the baseline that exits meanwhile cannot mask a leak.
func (b MemoryBaseline) GoroutinesReleased(lingering func(stack string) bool) error {
	return b.goroutinesReleased(goroutineSettle, lingering)
}

func (b MemoryBaseline) goroutinesReleased(wait time.Duration, lingering func(stack string) bool) error {
	deadline := time.Now().Add(wait)
	for {
		started := b.startedSince(lingering)
		switch {
		case len(started) == 0:
			return nil
		case time.Now().After(deadline):
			return fmt.Errorf("goroutines-released: %d goroutines started during the run remain:\n%s", len(started), strings.Join(started, "\n\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WriteHeapProfile writes a heap profile of the live heap to dir/name.
func WriteHeapProfile(dir, name string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("chaos: heap profile directory: %w", err)
	}
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return fmt.Errorf("chaos: heap profile: %w", err)
	}
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		_ = f.Close()
		return fmt.Errorf("chaos: heap profile: %w", err)
	}
	return f.Close()
}
