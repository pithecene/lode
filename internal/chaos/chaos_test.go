package chaos

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"
)

var (
	flagChaos    = flag.Bool("chaos", false, "run the chaos harness (see `task chaos`)")
	flagSeed     = flag.Uint64("chaos.seed", 0, "seed for the run's random choices; 0 picks one from the clock")
	flagDuration = flag.Duration("chaos.duration", 10*time.Second, "how long each configuration runs before the checks")
	flagWriters  = flag.Int("chaos.writers", 8, "concurrent writers, each with its own Dataset handle")
	flagReaders  = flag.Int("chaos.readers", 2, "concurrent readers")
	flagRetries  = flag.Int("chaos.retries", 5, "WithRetryCount for every handle")
	flagBackends = flag.String("chaos.backends", "memory,fs", "comma-separated backends: "+strings.Join(BackendNames(), ", "))
	flagLayouts  = flag.String("chaos.layouts", "default,hive", "comma-separated layouts: "+strings.Join(LayoutNames(), ", "))
	flagFaults   = flag.String("chaos.faults", "none", "comma-separated fault levels: "+strings.Join(FaultLevelNames(), ", "))
	flagRetain   = flag.Bool("chaos.retention", false, "run Expire and Sweep concurrently with the writers")
	flagVolume   = flag.Bool("chaos.volume", false, "also run the Volume workload once per backend and fault level")
	flagMemProf  = flag.String("chaos.memprofile", "", "directory to write a heap profile of each configuration to, after its run")
)

// TestChaos runs the harness once per backend and layout. It is skipped
// unless -chaos is passed. A failure logs the seed and the command that
// repeats the run's random choices.
func TestChaos(t *testing.T) {
	if !*flagChaos {
		t.Skip("skipping the chaos harness; use -chaos to enable")
	}
	seed := *flagSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano()) //nolint:gosec // a positive clock reading as a seed
	}
	backends, layouts, levels := splitList(*flagBackends), splitList(*flagLayouts), splitList(*flagFaults)
	faults, err := selection(backends, layouts, levels)
	if err != nil {
		t.Fatal(err)
	}
	if *flagVolume {
		layouts = append(layouts, "volume")
	}
	for _, backend := range backends {
		for _, layout := range layouts {
			for _, level := range levels {
				t.Run(backend+"/"+layout+"/"+level, func(t *testing.T) {
					runOne(t, backend, level, Config{
						Seed: seed, Duration: *flagDuration, Writers: *flagWriters, Readers: *flagReaders,
						Layout: layout, Retries: *flagRetries, Faults: faults[level], Retention: *flagRetain,
						Volume: layout == "volume",
					})
				})
			}
		}
	}
}

// selection validates the requested backends, layouts and fault levels
// before anything runs, and returns the fault settings by level. An empty
// selection would run nothing and pass.
func selection(backends, layouts, levels []string) (map[string]Faults, error) {
	for _, sel := range []struct {
		kind            string
		selected, known []string
	}{
		{"backend", backends, BackendNames()},
		{"layout", layouts, LayoutNames()},
		{"fault level", levels, FaultLevelNames()},
	} {
		if len(sel.selected) == 0 {
			return nil, fmt.Errorf("chaos: no %s selected", sel.kind)
		}
		for _, name := range sel.selected {
			if err := requireKnown(sel.kind, name, sel.known); err != nil {
				return nil, err
			}
		}
	}
	faults := make(map[string]Faults, len(levels))
	for _, level := range levels {
		faults[level] = faultLevels[level]
	}
	return faults, nil
}

func runOne(t *testing.T, backendName, level string, cfg Config) {
	t.Helper()
	runID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), cfg.Layout)
	baseline := TakeMemoryBaseline()
	backend, err := OpenBackend(t.Context(), backendName, t.TempDir(), runID)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanups run last first: the backend is released, then the heap is
	// measured, and then the goroutines are counted.
	t.Cleanup(func() {
		if err := baseline.GoroutinesReleased(backend.Lingering); err != nil {
			t.Error(err)
		}
	})
	var commits int // set once the run ends
	t.Cleanup(func() { checkMemory(t, backend, baseline, commits, backendName+"-"+cfg.Layout+"-"+level) })
	t.Cleanup(func() {
		if err := backend.Release(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("release the %s backend: %v", backendName, err)
		}
	})
	// The invariants assume CAS (CONTRACT_WRITE_API §Concurrency): without
	// it, concurrent writers may fork the history within the contract.
	if _, ok := backend.Store.(lode.ConditionalWriter); !ok {
		t.Skipf("the %s store does not implement ConditionalWriter on this platform", backendName)
	}

	report := Run(t.Context(), cfg, backend.Store)
	commits = report.Commits
	t.Logf("seed %d: %d commits, %d conflicts after retries, %d injected failures, %d reads, %d expired, %d snapshots persisted",
		report.Seed, report.Commits, report.Conflicts, report.Faults, report.Reads, report.Expired, report.Snapshots)
	for _, v := range report.Violations {
		t.Errorf("violation %s", v)
	}
	if len(report.Violations) > 0 {
		t.Logf("repeat with: go test ./internal/chaos -run 'TestChaos/%s/%s/%s' -chaos -chaos.seed=%d -chaos.backends=%s -chaos.layouts='%s' -chaos.faults=%s -chaos.retention=%t -chaos.volume=%t -chaos.writers=%d -chaos.readers=%d -chaos.retries=%d -chaos.duration=%s",
			backendName, cfg.Layout, level, cfg.Seed, backendName, *flagLayouts, level, cfg.Retention, *flagVolume, cfg.Writers, cfg.Readers, cfg.Retries, cfg.Duration)
	}
	switch {
	case report.Stalled:
		t.Logf("an abandoned commit stalled the volume; every later commit conflicted (CONTRACT_VOLUME)")
	case report.Commits == 0:
		t.Errorf("no commit succeeded; the run exercised nothing")
	}
}

// checkMemory writes the configuration's heap profile when requested and,
// for a store that keeps no data in memory, requires the finished run to
// retain no more heap than its commits allow. It runs after the backend's
// release; the store stays reachable through backend, so its own bounded
// state counts. A skipped configuration ran nothing to measure.
func checkMemory(t *testing.T, backend Backend, baseline MemoryBaseline, commits int, name string) {
	t.Helper()
	if t.Skipped() {
		return
	}
	if dir := *flagMemProf; dir != "" {
		if err := WriteHeapProfile(dir, name+".heap"); err != nil {
			t.Error(err)
		}
	}
	if backend.RetainsData {
		return
	}
	retained, err := baseline.RetainedHeap(commits)
	runtime.KeepAlive(backend.Store) // the store's own state must count
	t.Logf("the run retains %d bytes of heap after %d acknowledged commits", retained, commits)
	if err != nil {
		t.Error(err)
	}
}

func splitList(s string) []string {
	var out []string
	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
