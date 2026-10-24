package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/pithecene/lode/lode"
)

// Each backend runs conditionalRounds races of conditionalWriters writers.
// A service on which Lode is safe lets exactly one writer win every race;
// a service that accepts a conditional header without enforcing it
// atomically lets several win, which forks a dataset's history.
const (
	conditionalRounds  = 10
	conditionalWriters = 16
)

// conditionalRace describes one kind of race: the setup of its key, the
// write that each writer attempts, and the error that each losing writer
// must receive.
type conditionalRace struct {
	name  string
	seed  func(t *testing.T, store *Store, key string)
	write func(ctx context.Context, store *Store, key, value string) error
	loser error
}

var conditionalRaces = []conditionalRace{
	{
		// A commit advances the latest pointer with If-Match on the version
		// it read.
		name: "compare and swap",
		seed: func(t *testing.T, store *Store, key string) {
			t.Helper()
			if err := store.CompareAndSwap(t.Context(), key, "", "base"); err != nil {
				t.Fatalf("seed %s: %v", key, err)
			}
		},
		write: func(ctx context.Context, store *Store, key, value string) error {
			return store.CompareAndSwap(ctx, key, "base", value)
		},
		loser: lode.ErrSnapshotConflict,
	},
	{
		// Data files and manifests are created with If-None-Match: *.
		name: "create",
		seed: func(*testing.T, *Store, string) {},
		write: func(ctx context.Context, store *Store, key, value string) error {
			return store.Put(ctx, key, bytes.NewReader([]byte(value)))
		},
		loser: lode.ErrPathExists,
	},
}

func TestIntegration_ConditionalWrites(t *testing.T) {
	for _, backend := range s3Backends {
		for _, race := range conditionalRaces {
			t.Run(backend.name+"/"+race.name+" has one winner", func(t *testing.T) {
				base := setupTestBucket(t, backend)
				for round := range conditionalRounds {
					key := fmt.Sprintf("race/%d", round)
					race.seed(t, base, key)
					runConditionalRace(t, base, race, key)
				}
			})
		}
	}
}

// runConditionalRace starts every writer at once, each with its own Store as
// a separate process would have, and checks that exactly one wins, that every
// other writer receives the race's error, and that the object holds the
// winner's value.
func runConditionalRace(t *testing.T, base *Store, race conditionalRace, key string) {
	t.Helper()
	errs := make([]error, conditionalWriters)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range conditionalWriters {
		writer := writerStore(t, base)
		wg.Go(func() {
			<-start
			errs[i] = race.write(t.Context(), writer, key, fmt.Sprintf("writer-%d", i))
		})
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, err := range errs {
		switch {
		case err == nil && winner >= 0:
			t.Fatalf("%s: writers %d and %d both won", key, winner, i)
		case err == nil:
			winner = i
		case !errors.Is(err, race.loser):
			t.Fatalf("%s: writer %d: got %v, want %v", key, i, err, race.loser)
		}
	}
	if winner < 0 {
		t.Fatalf("%s: no writer won", key)
	}
	if got, want := readString(t, base, key), fmt.Sprintf("writer-%d", winner); got != want {
		t.Fatalf("%s holds %q, want the winner's %q", key, got, want)
	}
}

// writerStore returns a Store on the same bucket as base with no record of
// the versions that base wrote, so that each writer reads before it writes.
func writerStore(t *testing.T, base *Store) *Store {
	t.Helper()
	store, err := New(base.client, Config{Bucket: base.bucket})
	if err != nil {
		t.Fatalf("new writer store: %v", err)
	}
	return store
}

func readString(t *testing.T, store *Store, key string) string {
	t.Helper()
	rc, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return string(data)
}
