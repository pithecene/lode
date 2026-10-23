package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
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
	r := &run{cfg: cfg, store: store, ledger: &Ledger{}, until: time.Now().Add(cfg.Duration)}
	return r.drive(ctx, r.workload())
}

// run is one execution of the harness: what every actor and check of it
// shares.
type run struct {
	cfg    Config
	store  lode.RangeStore // the backend's store, without faults
	ledger *Ledger
	until  time.Time // the deadline of the workload
}

// actor is one goroutine of a workload: a writer, a reader, or the janitor.
type actor interface {
	act(ctx context.Context)
}

// workload is what a run drives and then checks. Committers run until the
// deadline, which they check between commits and never by cancelling one:
// a commit cancelled between its pointer update and its manifest write is
// abandoned, which the contract permits but which would leave the head to
// the largest-ID fallback (CONTRACT_CORE §Head Pointer). Observers only
// read, so the deadline cancels them. verify returns the number of
// snapshots persisted.
type workload struct {
	committers func(r *run) []actor
	observers  func(r *run) []actor
	verify     func(r *run, ctx context.Context) int
}

// workloads are the workloads a run can drive, by Config.Volume.
var workloads = map[bool]workload{
	false: {committers: (*run).datasetCommitters, observers: (*run).readers, verify: (*run).verifyDataset},
	true:  {committers: (*run).volumeWriters, observers: (*run).noActors, verify: (*run).verifyVolume},
}

func (r *run) workload() workload { return workloads[r.cfg.Volume] }

// drive runs the workload's actors to the deadline, waits for every one,
// and then verifies the history they left.
func (r *run) drive(ctx context.Context, w workload) Report {
	observing, stop := context.WithDeadline(ctx, r.until)
	defer stop()

	var wg sync.WaitGroup
	start := func(ctx context.Context, actors []actor) {
		for _, a := range actors {
			wg.Go(func() { a.act(ctx) })
		}
	}
	start(ctx, w.committers(r))
	start(observing, w.observers(r))
	wg.Wait()

	return r.ledger.report(r.cfg.Seed, w.verify(r, ctx))
}

// live reports whether a committer may begin another step: the run has not
// been cancelled and the deadline has not passed.
func (r *run) live(ctx context.Context) bool {
	return ctx.Err() == nil && time.Now().Before(r.until)
}

// random returns the random source of one stream of the run's choices, so
// that a seed reproduces every actor's choices.
func (r *run) random(stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(r.cfg.Seed, stream)) //nolint:gosec // reproducible test randomness, not security
}

func (r *run) noActors() []actor { return nil }

// requireKnown fails unless name is one of the known names of its kind.
func requireKnown(kind, name string, known []string) error {
	if slices.Contains(known, name) {
		return nil
	}
	return fmt.Errorf("chaos: unknown %s %q (known: %v)", kind, name, known)
}
