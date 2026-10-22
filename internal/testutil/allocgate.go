package testutil

import (
	"runtime"
	"testing"
)

// AllocGate bounds the allocations of one hot-path operation, by count and
// by bytes. The ceilings are the figures measured when the gate was last
// set: a change that allocates more fails the gate and must justify the
// increase, and a change that allocates less lowers the ceilings to the new
// figures (docs/BENCHMARKS.md §Allocation Gates). The byte ceiling catches a
// single oversized allocation, which the count does not. Setup prepares the
// operation and returns it.
type AllocGate struct {
	Name   string
	Allocs float64
	Bytes  uint64
	Setup  func(t *testing.T) func()
}

// gateRuns is the number of measured runs of each operation.
const gateRuns = 100

// AllocGateCalls is the number of times RunAllocGates calls each operation:
// once to warm it, gateRuns+1 times in testing.AllocsPerRun (which warms it
// again), and gateRuns times to measure bytes. An operation that consumes
// prepared input, such as staged blocks, prepares this many.
const AllocGateCalls = 1 + (gateRuns + 1) + gateRuns

// RunAllocGates holds each gate's operation to its ceilings. Each operation
// runs once before it is measured, so state built lazily on first use, such
// as a known head, is not counted. The figures are deterministic for a given
// toolchain, but the race detector adds allocations of its own, so callers
// exclude race builds.
func RunAllocGates(t *testing.T, gates []AllocGate) {
	t.Helper()
	for _, g := range gates {
		t.Run(g.Name, func(t *testing.T) {
			op := g.Setup(t)
			op()
			if got := testing.AllocsPerRun(gateRuns, op); got > g.Allocs {
				t.Errorf("%s: %.0f allocations per run, ceiling %.0f", g.Name, got, g.Allocs)
			}
			if got := bytesPerRun(op); got > g.Bytes {
				t.Errorf("%s: %d bytes allocated per run, ceiling %d", g.Name, got, g.Bytes)
			}
		})
	}
}

// bytesPerRun returns the bytes that op allocates per run, averaged over
// gateRuns runs on one processor, as testing.AllocsPerRun counts objects.
func bytesPerRun(op func()) uint64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range gateRuns {
		op()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / gateRuns
}
