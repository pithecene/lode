package chaos

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/pithecene/lode/lode"
)

// Config is one run of the harness.
type Config struct {
	// Seed drives every random choice of the run.
	Seed uint64
	// Duration is how long the writers and readers run before the checks.
	Duration time.Duration
	// Writers and Readers are the numbers of concurrent goroutines, each
	// with its own Dataset handle.
	Writers, Readers int
	// Layout is "default" or "hive".
	Layout string
	// Retries is the WithRetryCount each handle is opened with.
	Retries int
	// Faults are injected into every writer's and reader's store calls.
	Faults Faults
	// Retention runs Expire and Sweep concurrently with the writers.
	Retention bool
	// Volume runs Volume writers instead of the Dataset workload; Layout and
	// Retention do not apply.
	Volume bool
}

// Run drives the workload against store for cfg.Duration, then verifies
// the history it left. The report lists every violation observed during
// the run and by the final checks.
func Run(ctx context.Context, cfg Config, store lode.RangeStore) Report {
	if cfg.Volume {
		return runVolume(ctx, cfg, store)
	}
	ledger := &Ledger{}
	until := time.Now().Add(cfg.Duration)
	readCtx, stop := context.WithDeadline(ctx, until)
	defer stop()

	var wg sync.WaitGroup
	for i := range cfg.Writers {
		wg.Go(func() { writer(ctx, cfg, store, i, until, ledger) })
	}
	for i := range cfg.Readers {
		wg.Go(func() { reader(readCtx, cfg, store, i, ledger) })
	}
	if cfg.Retention {
		wg.Go(func() { newJanitor(cfg, store, ledger).run(ctx, until) })
	}
	wg.Wait()

	snapshots := verify(ctx, cfg, store, ledger)
	return ledger.report(cfg.Seed, snapshots)
}

// runVolume drives concurrent Volume writers for cfg.Duration and then
// verifies the volume's history and contents.
func runVolume(ctx context.Context, cfg Config, store lode.RangeStore) Report {
	ledger := &Ledger{}
	until := time.Now().Add(cfg.Duration)
	var wg sync.WaitGroup
	for i := range cfg.Writers {
		wg.Go(func() { volumeWriter(ctx, cfg, store, i, until, ledger) })
	}
	wg.Wait()
	snapshots := verifyVolume(ctx, cfg, store, ledger)
	return ledger.report(cfg.Seed, snapshots)
}

// requireKnown fails unless name is one of the known names of its kind.
func requireKnown(kind, name string, known []string) error {
	if slices.Contains(known, name) {
		return nil
	}
	return fmt.Errorf("chaos: unknown %s %q (known: %v)", kind, name, known)
}
