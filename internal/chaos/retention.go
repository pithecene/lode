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
// a report lists as expired. Like a writer, it holds one handle, which an
// injected kill discards and the next step reopens.
type janitor struct {
	*run
	rng *rand.Rand
	ds  handle[lode.Records[any]]
}

// newJanitor returns the run's janitor, seeded so that a run's retention
// calls repeat for the same seed.
func (r *run) newJanitor() *janitor {
	rng := r.random(2000)
	return &janitor{run: r, rng: rng, ds: r.datasetHandle("janitor", rng)}
}

// act makes a random retention call, then pauses, until the deadline.
func (j *janitor) act(ctx context.Context) {
	for j.live(ctx) {
		ds, next := j.ds.acquire(ctx)
		if next == halt {
			return
		}
		if next == skip {
			continue
		}
		ops := j.ops()
		j.observe(ctx, ops[j.rng.IntN(len(ops))](ctx, ds))
		pause(ctx, time.Duration(50+j.rng.IntN(200))*time.Millisecond)
	}
}

// retentionOp is one retention call the janitor can make through a handle.
type retentionOp func(ctx context.Context, ds lode.Records[any]) error

// ops are the retention calls the janitor chooses among.
func (j *janitor) ops() []retentionOp {
	return []retentionOp{j.expireBefore, j.expireOne, j.sweep}
}

// observe classifies a retention call's outcome. A head that is not yet
// committed, and the end of the run, are expected; an injected failure is
// counted, and a kill discards the handle; any other error is a violation.
func (j *janitor) observe(ctx context.Context, err error) {
	switch {
	case err == nil, errors.Is(err, lode.ErrHeadUncommitted), ctx.Err() != nil:
	case injected(err):
		j.ledger.fault()
		j.ds.drop(err)
	default:
		j.ledger.violate("retention", "%v", err)
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

// expireBefore expires snapshots whose writes began in the first part of
// the run.
func (j *janitor) expireBefore(ctx context.Context, ds lode.Records[any]) error {
	return j.expire(ctx, ds, lode.ExpireRequest{Before: time.Now().Add(-j.cfg.Duration / 4)})
}

// expireOne expires one acknowledged snapshot by ID.
func (j *janitor) expireOne(ctx context.Context, ds lode.Records[any]) error {
	c, ok := j.ledger.randomCommit(j.rng)
	if !ok {
		return nil
	}
	return j.expire(ctx, ds, lode.ExpireRequest{IDs: []lode.DatasetSnapshotID{c.ID}})
}

// sweep reclaims what expiries and abandoned writes left, with a grace
// period that spares every write in progress.
func (j *janitor) sweep(ctx context.Context, ds lode.Records[any]) error {
	_, err := ds.Sweep(ctx, lode.SweepRequest{Grace: j.cfg.sweepGrace()})
	return err
}

// expire runs one expiry and records its report, which an expiry returns
// even when it fails partway.
func (j *janitor) expire(ctx context.Context, ds lode.Records[any], req lode.ExpireRequest) error {
	report, err := ds.Expire(ctx, req)
	j.record(report)
	return err
}

// record notes what a report expired and checks the one promise a report
// makes on its own: the head it retained is not among the expired.
func (j *janitor) record(report *lode.ExpireReport) {
	if report == nil {
		return
	}
	j.ledger.expire(report.Expired...)
	if report.RetainedHead != "" && slices.Contains(report.Expired, report.RetainedHead) {
		j.ledger.violate("head-retained", "Expire reported %s both as the retained head and as expired", report.RetainedHead)
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
