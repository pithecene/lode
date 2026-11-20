package chaos

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/internal/testutil"

	"github.com/pithecene/lode/lode"
)

var (
	flagChaos    = flag.Bool("chaos", false, "run the chaos harness (see `task chaos`)")
	flagSeed     = flag.Uint64("chaos.seed", 0, "seed for the run's random choices; 0 picks one from the clock")
	flagDuration = flag.Duration("chaos.duration", 10*time.Second, "how long each configuration runs before the checks")
	flagWriters  = flag.Int("chaos.writers", 8, "concurrent writers, each with its own Dataset handle")
	flagReaders  = flag.Int("chaos.readers", 2, "concurrent readers")
	flagRetries  = flag.Int("chaos.retries", 5, "WithRetry for every handle")
	flagBackends = flag.String("chaos.backends", "memory,fs", "comma-separated backends: "+strings.Join(BackendNames(), ", "))
	flagLayouts  = flag.String("chaos.layouts", "default,hive,parquet,parquet-hive,blobs", "comma-separated layouts: "+strings.Join(LayoutNames(), ", "))
	flagFaults   = flag.String("chaos.faults", "none", "comma-separated fault levels: "+strings.Join(FaultLevelNames(), ", "))
	flagRetain   = flag.Bool("chaos.retention", false, "run Expire and Sweep concurrently with the writers")
	flagVolume   = flag.Bool("chaos.volume", false, "also run the Volume workload once per backend and fault level")
	flagMemProf  = flag.String("chaos.memprofile", "", "directory to write a heap profile of each configuration to, after its run")
)

// TestChaos runs the harness once per backend and layout. It is skipped
// unless -chaos is passed. A failure logs the seed and the command that
// repeats the run's random choices.
func TestChaos(t *testing.T) {
	testutil.Serial(t, "counts the process's goroutines and measures its heap")
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
	if *flagWriters < 0 || *flagReaders < 0 {
		t.Fatalf("chaos: -chaos.writers (%d) and -chaos.readers (%d) must not be negative", *flagWriters, *flagReaders)
	}
	if *flagVolume {
		layouts = append(layouts, "volume")
	}
	for _, backend := range backends {
		for _, layout := range layouts {
			for _, level := range levels {
				t.Run(backend+"/"+layout+"/"+level, func(t *testing.T) {
					tr := trial{backendName: backend, level: level, cfg: Config{
						Seed: seed, Duration: *flagDuration, Writers: *flagWriters, Readers: *flagReaders,
						Layout: layout, Retries: *flagRetries, Faults: faults[level], Retention: *flagRetain,
						Volume: layout == "volume",
					}}
					tr.run(t)
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

// trial is one configuration of TestChaos: the backend it runs on and the
// measurements taken around its run.
type trial struct {
	backendName, level string
	cfg                Config
	baseline           MemoryBaseline
	backend            Backend
	commits            int // set once the run ends
}

// name identifies the trial in heap profile file names.
func (tr *trial) name() string { return tr.backendName + "-" + tr.cfg.Layout + "-" + tr.level }

// run opens the backend, runs the harness on it, and reports the result.
// The teardown runs whatever the outcome.
func (tr *trial) run(t *testing.T) {
	t.Helper()
	runID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), tr.cfg.Layout)
	tr.baseline = TakeMemoryBaseline()
	backend, err := OpenBackend(t.Context(), tr.backendName, t.TempDir(), runID)
	if err != nil {
		t.Fatal(err)
	}
	tr.backend = backend
	t.Cleanup(func() { tr.teardown(t) })
	// The filesystem store has no file lock on platforms such as Plan 9, where
	// every commit would fail; such a backend is skipped, not reported.
	const probe = "chaos-probe/cas"
	if err := tr.backend.Store.CompareAndSwap(t.Context(), probe, "", "probe"); errors.Is(err, errors.ErrUnsupported) {
		t.Skipf("the %s store does not support CompareAndSwap here: %v", tr.backendName, err)
	}
	_ = tr.backend.Store.Delete(t.Context(), probe) // best-effort: the probe is not part of any dataset

	report := Run(t.Context(), tr.cfg, tr.backend.Store)
	tr.commits = report.Commits
	t.Logf("seed %d: %d commits, %d conflicts after retries, %d injected failures, %d reads, %d expired, %d snapshots persisted",
		report.Seed, report.Commits, report.Conflicts, report.Faults, report.Reads, report.Expired, report.Snapshots)
	for _, v := range report.Violations {
		t.Errorf("violation %s", v)
	}
	if len(report.Violations) > 0 {
		t.Logf("repeat with: go test ./internal/chaos -run 'TestChaos/%s/%s/%s' -chaos -chaos.seed=%d -chaos.backends=%s -chaos.layouts='%s' -chaos.faults=%s -chaos.retention=%t -chaos.volume=%t -chaos.writers=%d -chaos.readers=%d -chaos.retries=%d -chaos.duration=%s",
			tr.backendName, tr.cfg.Layout, tr.level, tr.cfg.Seed, tr.backendName, *flagLayouts, tr.level, tr.cfg.Retention, *flagVolume, tr.cfg.Writers, tr.cfg.Readers, tr.cfg.Retries, tr.cfg.Duration)
	}
	if report.Commits == 0 {
		t.Errorf("no commit succeeded; the run exercised nothing")
	}
}

// teardown releases the backend, then measures the heap, and then counts
// the goroutines, in that order: the heap and goroutine checks must see
// the backend released.
func (tr *trial) teardown(t *testing.T) {
	if err := tr.backend.Release(context.WithoutCancel(t.Context())); err != nil {
		t.Errorf("release the %s backend: %v", tr.backendName, err)
	}
	tr.checkMemory(t)
	if err := tr.baseline.GoroutinesReleased(tr.backend.Lingering); err != nil {
		t.Error(err)
	}
}

// checkMemory writes the configuration's heap profile when requested and,
// for a store that keeps no data in memory, requires the finished run to
// retain no more heap than its commits allow. It runs after the backend's
// release; the store stays reachable through backend, so its own bounded
// state counts. A skipped configuration ran nothing to measure.
func (tr *trial) checkMemory(t *testing.T) {
	t.Helper()
	if t.Skipped() {
		return
	}
	if dir := *flagMemProf; dir != "" {
		if err := WriteHeapProfile(dir, tr.name()+".heap"); err != nil {
			t.Error(err)
		}
	}
	if tr.backend.RetainsData {
		return
	}
	retained, err := tr.baseline.RetainedHeap(tr.commits)
	runtime.KeepAlive(tr.backend.Store) // the store's own state must count
	t.Logf("the run retains %d bytes of heap after %d acknowledged commits", retained, tr.commits)
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

// TestChaos_Workloads exercises every chaos workload (JSONL, Hive multi-partition,
// Parquet, Parquet+Hive, Blobs StreamWrite, CRC32C verification, WithAnchor
// views, Retention, Faults, and Volume) in brief in-memory runs during the
// regular test suite.
func TestChaos_Workloads(t *testing.T) {
	testutil.Serial(t, "counts the process's goroutines and measures its heap")
	if *flagChaos {
		t.Skip("skipping brief smoke runs when full -chaos suite is enabled")
	}
	for i, layout := range LayoutNames() {
		seed := uint64(100 + i)
		t.Run(layout, func(t *testing.T) {
			runSmokePair(t, Config{
				Seed:      seed,
				Duration:  150 * time.Millisecond,
				Writers:   3,
				Readers:   2,
				Layout:    layout,
				Retries:   4,
				Retention: true,
			})
		})
	}
	t.Run("volume", func(t *testing.T) {
		runSmokePair(t, Config{
			Seed:     42,
			Duration: 150 * time.Millisecond,
			Writers:  3,
			Retries:  4,
			Volume:   true,
		})
	})
}

func runSmokePair(t *testing.T, cfg Config) {
	t.Helper()
	report := Run(t.Context(), cfg, lode.NewMemory())
	for _, v := range report.Violations {
		t.Errorf("violation %s", v)
	}
	if report.Commits == 0 {
		t.Error("expected at least one commit")
	}

	cfg.Faults = faultLevels["mild"]
	faultReport := Run(t.Context(), cfg, lode.NewMemory())
	for _, v := range faultReport.Violations {
		t.Errorf("mild-fault violation %s", v)
	}
}

func TestChaos_RemoveDatasetFence(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	root := lode.Open(lode.Use(store))

	ds, err := root.Records(ctx, "fenced", lode.JSONL[lode.D](), lode.WithRetry(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ds.Write(ctx, lode.R(lode.D{"seq": 0}), nil); err != nil {
		t.Fatal(err)
	}

	const writers = 3
	var handles [writers]*lode.Records[lode.D]
	for i := range handles {
		handle, err := root.Records(ctx, "fenced", lode.JSONL[lode.D](), lode.WithRetry(4))
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = handle
	}
	done := make(chan error, writers)
	for worker, handle := range handles {
		go func() {
			done <- writeUntilRemoved(ctx, handle, worker)
		}()
	}
	removeErr := root.RemoveDataset(ctx, "fenced", lode.WithRetry(8))
	for range writers {
		if err := <-done; err != nil && !errors.Is(err, lode.ErrRemoved) && !errors.Is(err, lode.ErrHeadUncommitted) {
			t.Errorf("concurrent write: %v", err)
		}
	}
	if removeErr != nil && !errors.Is(removeErr, lode.ErrHeadUncommitted) {
		t.Fatalf("concurrent removal: %v", removeErr)
	}
	if _, err := root.Records(ctx, "fenced", lode.JSONL[lode.D]()); !errors.Is(err, lode.ErrRemoved) {
		t.Fatalf("Records after concurrent removal = %v, want ErrRemoved", err)
	}
	if err := root.RemoveDataset(ctx, "fenced", lode.WithRetry(4)); err != nil {
		t.Fatalf("final RemoveDataset = %v, want nil", err)
	}
}

func writeUntilRemoved(ctx context.Context, h *lode.Records[lode.D], worker int) error {
	for i := range 10 {
		if _, err := h.Write(ctx, lode.R(lode.D{"w": worker, "i": i}), nil); err != nil && !errors.Is(err, lode.ErrSnapshotConflict) {
			return err
		}
	}
	return nil
}
