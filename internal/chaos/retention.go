package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/pithecene/lode/lode"
)

// sweepGrace is the grace period of the janitor's sweeps during a run. Every
// write begins after the run starts, and the janitor sweeps only before the
// deadline, so no write in progress at a sweep is older than the run, however
// long its retries take; the extra minute covers a sweep that begins just
// before the deadline. A write in progress is therefore never reclaimed
// (CONTRACT_RETENTION §Grace Period).
func (c Config) sweepGrace() time.Duration { return c.Duration + time.Minute }

// janitor runs retention concurrently with the writers until the deadline:
// expiries by instant and by ID, and sweeps. It records every snapshot that
// a report lists as expired.
func janitor(ctx context.Context, cfg Config, store lode.RangeStore, until time.Time, ledger *Ledger) {
	rng := rand.New(rand.NewPCG(cfg.Seed, 2000)) //nolint:gosec // reproducible test randomness, not security
	var ds lode.Records[any]
	for ctx.Err() == nil && time.Now().Before(until) {
		if !reopen(ctx, cfg, store, rng, &ds, "janitor", ledger) {
			return
		}
		if ds == nil {
			continue
		}
		err := retentionOps[rng.IntN(len(retentionOps))](ctx, ds, rng, cfg, ledger)
		switch {
		case err == nil, errors.Is(err, lode.ErrHeadUncommitted), ctx.Err() != nil:
		case injected(err):
			ledger.fault()
			if errors.Is(err, errKilled) {
				ds = nil
			}
		default:
			ledger.violate("retention", "%v", err)
		}
		pause(ctx, time.Duration(50+rng.IntN(200))*time.Millisecond)
	}
}

// pause waits for d or until ctx ends.
func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// retentionOp is one retention call the janitor can make.
type retentionOp func(ctx context.Context, ds lode.Records[any], rng *rand.Rand, cfg Config, ledger *Ledger) error

var retentionOps = []retentionOp{expireBefore, expireOne, sweep}

// expireBefore expires snapshots whose writes began in the first part of
// the run.
func expireBefore(ctx context.Context, ds lode.Records[any], _ *rand.Rand, cfg Config, ledger *Ledger) error {
	report, err := ds.Expire(ctx, lode.ExpireRequest{Before: time.Now().Add(-cfg.Duration / 4)})
	recordExpiry(report, ledger)
	return err
}

// expireOne expires one acknowledged snapshot by ID.
func expireOne(ctx context.Context, ds lode.Records[any], rng *rand.Rand, _ Config, ledger *Ledger) error {
	c, ok := ledger.randomCommit(rng)
	if !ok {
		return nil
	}
	report, err := ds.Expire(ctx, lode.ExpireRequest{IDs: []lode.DatasetSnapshotID{c.ID}})
	recordExpiry(report, ledger)
	return err
}

func sweep(ctx context.Context, ds lode.Records[any], _ *rand.Rand, cfg Config, _ *Ledger) error {
	_, err := ds.Sweep(ctx, lode.SweepRequest{Grace: cfg.sweepGrace()})
	return err
}

// recordExpiry notes what a report expired and checks the one promise a
// report makes on its own: the head it retained is not among the expired.
func recordExpiry(report *lode.ExpireReport, ledger *Ledger) {
	if report == nil {
		return
	}
	ledger.expire(report.Expired...)
	if report.RetainedHead != "" && slices.Contains(report.Expired, report.RetainedHead) {
		ledger.violate("head-retained", "Expire reported %s both as the retained head and as expired", report.RetainedHead)
	}
}

// expiredRead reports whether a read failed because retention removed the
// snapshot.
func expiredRead(err error) bool {
	return errors.Is(err, lode.ErrSnapshotExpired) || errors.Is(err, lode.ErrNotFound)
}

// retentionChecks run after the final sweep, when cfg.Retention is set.
var retentionChecks = []check[*verifier]{
	{"expired-snapshots-read-as-expired", (*verifier).expiredReadAsExpired},
	{"failed-reads-were-expired", (*verifier).failedReadsWereExpired},
	{"expiries-settled", (*verifier).expiriesSettled},
}

// expiredReadAsExpired requires every snapshot an Expire report listed to
// read as expired.
func (v *verifier) expiredReadAsExpired(ctx context.Context) []string {
	return v.notExpired(ctx, v.expired, "Expire reported %s as expired, but it does not read as expired")
}

// failedReadsWereExpired requires every read that failed as if its snapshot
// were expired to concern a snapshot that is.
func (v *verifier) failedReadsWereExpired(ctx context.Context) []string {
	return v.notExpired(ctx, v.failedReads, "a read of %s failed during the run, but the snapshot is not expired")
}

// notExpired describes, with format, every one of ids that does not read as
// expired.
func (v *verifier) notExpired(ctx context.Context, ids []lode.DatasetSnapshotID, format string) []string {
	var bad []string
	for _, id := range ids {
		if !v.isExpired(ctx, id) {
			bad = append(bad, fmt.Sprintf(format, id))
		}
	}
	return bad
}

// expiriesSettled requires the final sweep to have left nothing to reclaim:
// a dry run with the same grace finds no expired snapshot to settle, no
// object to delete and nothing it cannot parse.
func (v *verifier) expiriesSettled(ctx context.Context) []string {
	report, err := v.ds.Sweep(ctx, lode.SweepRequest{Grace: finalGrace, DryRun: true})
	switch {
	case err != nil:
		return []string{fmt.Sprintf("dry-run Sweep: %v", err)}
	case report.Objects > 0 || len(report.Settled) > 0 || len(report.Unparseable) > 0:
		return []string{fmt.Sprintf("after the final sweep, a dry run would still settle %v and delete %d objects, and cannot parse %v", report.Settled, report.Objects, report.Unparseable)}
	}
	return nil
}
