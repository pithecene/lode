package lode

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// TestRecordedComponents_Adoption covers which components a writer opened with
// WithRecordedComponents uses: the head's, published or staged, or the
// configured ones for a dataset without a head.
func TestRecordedComponents_Adoption(t *testing.T) {
	t.Parallel()
	t.Run("the head's components or the configured ones", func(t *testing.T) {
		configured := []DatasetOption{WithCompressor(Zstd()), WithChecksum(SHA256()), WithRecordedComponents()}
		cases := []struct {
			name           string
			first          []DatasetOption
			pend           bool
			wantCompressor string
			wantChecksum   string
		}{
			{"a dataset without a head uses the configured components", nil, false, "zstd", "sha256"},
			{"a published head keeps its components", []DatasetOption{}, false, "noop", ""},
			{"a staged first head keeps its components", []DatasetOption{}, true, "noop", ""},
			{"a head with other bundled components keeps them", []DatasetOption{WithCompressor(Gzip()), WithChecksum(CRC32C())}, false, "gzip", "crc32c"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store := NewMemory()
				if tc.first != nil {
					first, err := tryWrite(t, store, jsonl, R(D{"a": 1}), tc.first...)
					if err != nil {
						t.Fatal(err)
					}
					if tc.pend {
						pendFirst(t, store, first.ID)
					}
				}
				snap, err := tryWrite(t, store, jsonl, R(D{"a": 2}), configured...)
				if err != nil {
					t.Fatalf("write with the configured components: %v", err)
				}
				if got := snap.Manifest; got.Compressor != tc.wantCompressor || got.ChecksumAlgorithm != tc.wantChecksum {
					t.Fatalf("snapshot records %q and %q, want %q and %q", got.Compressor, got.ChecksumAlgorithm, tc.wantCompressor, tc.wantChecksum)
				}
				view, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl)
				if err != nil {
					t.Fatal(err)
				}
				if recs, err := view.Read(t.Context(), snap.ID); err != nil || len(recs) != 1 {
					t.Fatalf("read %d records, %v; want the snapshot's one record", len(recs), err)
				}
			})
		}
	})

	t.Run("opening reads the published head once", func(t *testing.T) {
		store := NewMemory()
		first, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(Gzip()), WithChecksum(SHA256()))
		if err != nil {
			t.Fatal(err)
		}
		spy := testutil.NewFaultStore(store)
		openOn(t, spy, jsonl, WithRecordedComponents())
		want := []string{"datasets/events/latest", "datasets/events/snapshots/" + string(first.ID) + "/manifest.json"}
		if got := spy.GetCalls(); !slices.Equal(got, want) {
			t.Fatalf("open Get calls = %v, want %v", got, want)
		}
	})

	// A head that stores no object still records its checksum, so a writer that
	// adopts it computes that checksum for the next object.
	t.Run("an object-free head keeps its checksum", func(t *testing.T) {
		store := NewMemory()
		empty, err := tryWrite(t, store, jsonl, nil, WithChecksum(SHA256()))
		if err != nil {
			t.Fatal(err)
		}
		if len(empty.Objects) != 0 || empty.ChecksumAlgorithm != "sha256" {
			t.Fatalf("empty snapshot records %q with %d objects, want sha256 and none", empty.ChecksumAlgorithm, len(empty.Objects))
		}
		next, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithChecksum(CRC32C()), WithRecordedComponents())
		if err != nil {
			t.Fatal(err)
		}
		if next.ChecksumAlgorithm != "sha256" || len(next.Objects) != 1 || next.Objects[0].Checksum == "" {
			t.Fatalf("next snapshot records %q with objects %+v, want sha256 and a digest", next.ChecksumAlgorithm, next.Objects)
		}
	})
}

// TestRecordedComponents_StreamWrite covers the Blobs stream path. A Blobs
// writer adopts the head's components too, and StreamWrite uses them
// on both stream paths: uncompressed bytes go straight to the store, and
// compressed ones through the adopted compressor. An independent view reads
// the bytes back and verifies the adopted digest.
func TestRecordedComponents_StreamWrite(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		head           []DatasetOption
		wantCompressor string
		wantChecksum   string
	}{
		{"uncompressed with CRC32C", []DatasetOption{WithChecksum(CRC32C())}, "noop", "crc32c"},
		{"zstd with SHA-256", []DatasetOption{WithCompressor(Zstd()), WithChecksum(SHA256())}, "zstd", "sha256"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := NewMemory()
			first, err := Open(Use(store)).Blobs(ctx, "files", tc.head...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Write(ctx, []byte("first"), nil); err != nil {
				t.Fatal(err)
			}
			next, err := Open(Use(store)).Blobs(ctx, "files", WithCompressor(Gzip()), WithChecksum(MD5()), WithRecordedComponents())
			if err != nil {
				t.Fatal(err)
			}
			payload := strings.Repeat("second ", 64)
			snap, err := next.StreamWrite(ctx, strings.NewReader(payload), nil)
			if err != nil {
				t.Fatalf("stream write: %v", err)
			}
			if snap.Compressor != tc.wantCompressor || snap.ChecksumAlgorithm != tc.wantChecksum || len(snap.Objects) != 1 || snap.Objects[0].Checksum == "" {
				t.Fatalf("snapshot records %q and %q with objects %+v, want %q, %q, and a digest", snap.Compressor, snap.ChecksumAlgorithm, snap.Objects, tc.wantCompressor, tc.wantChecksum)
			}
			view, err := OpenReadOnly(Use(store)).Blobs(ctx, "files")
			if err != nil {
				t.Fatal(err)
			}
			if got, err := view.Read(ctx, snap.ID); err != nil || string(got) != payload {
				t.Fatalf("read %d bytes, %v; want the streamed bytes", len(got), err)
			}
		})
	}
}

// TestRecordedComponents_CustomComponents covers a head that records a custom
// compressor or checksum: the writer needs a configured component of that name.
func TestRecordedComponents_CustomComponents(t *testing.T) {
	t.Parallel()
	// A head that records a custom component the writer does not configure
	// cannot be adopted by name, so the writer fails to open.
	t.Run("an unconfigured custom component fails the open", func(t *testing.T) {
		store := NewMemory()
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(renamedCompressor{Gzip()})); err != nil {
			t.Fatal(err)
		}
		_, err := Open(Use(store)).Records(t.Context(), "events", jsonl, WithRecordedComponents())
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("got %v, want ErrSettingsMismatch", err)
		}
	})

	t.Run("a matching custom checksum is used", func(t *testing.T) {
		store := NewMemory()
		checksum := renamedChecksum{MD5()}
		records := R(D{"a": 1})
		first, err := tryWrite(t, store, jsonl, records, WithChecksum(checksum))
		if err != nil {
			t.Fatal(err)
		}
		_, err = Open(Use(store)).Records(t.Context(), "events", jsonl, WithRecordedComponents())
		if !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("open without the custom checksum: got %v, want ErrSettingsMismatch", err)
		}
		next, err := tryWrite(t, store, jsonl, records, WithRecordedComponents(), WithChecksum(checksum))
		if err != nil {
			t.Fatalf("write with the matching custom checksum: %v", err)
		}
		if got := next.ChecksumAlgorithm; got != checksum.Name() {
			t.Errorf("checksum algorithm = %q, want %q", got, checksum.Name())
		}
		if got, want := next.Objects[0].Checksum, first.Objects[0].Checksum; got == "" || got != want {
			t.Errorf("checksum = %q, want the first snapshot's digest %q", got, want)
		}
	})

	// A head that records a custom compressor accepts a writer that configures a
	// compressor of that name: the writer uses the configured instance, and the
	// snapshot records the custom name and reads back.
	t.Run("a matching custom compressor is used", func(t *testing.T) {
		store := NewMemory()
		custom := renamedCompressor{Gzip()}
		if _, err := tryWrite(t, store, jsonl, R(D{"a": 1}), WithCompressor(custom)); err != nil {
			t.Fatal(err)
		}
		next, err := tryWrite(t, store, jsonl, R(D{"a": 2}), WithRecordedComponents(), WithCompressor(custom))
		if err != nil {
			t.Fatalf("write with the matching custom compressor: %v", err)
		}
		if next.Compressor != custom.Name() {
			t.Errorf("compressor = %q, want %q", next.Compressor, custom.Name())
		}
		view, err := OpenReadOnly(Use(store)).Records(t.Context(), "events", jsonl, WithCompressor(custom))
		if err != nil {
			t.Fatal(err)
		}
		if recs, err := view.Read(t.Context(), next.ID); err != nil || len(recs) != 1 {
			t.Fatalf("read %d records, %v; want the snapshot's one record", len(recs), err)
		}
	})

	// Adoption changes the components a write uses, not those that resolve a
	// snapshot's recorded names for reads. A history written before writers
	// enforced matching settings (v0.12.0) can hold an older snapshot with custom
	// components under a head with bundled ones: the adopting writer writes with
	// the head's, and still reads the older snapshot with the configured ones.
	t.Run("adoption keeps the configured components for reads", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()
		custom := []DatasetOption{WithCompressor(renamedCompressor{Gzip()}), WithChecksum(renamedChecksum{MD5()})}
		older, err := tryWrite(t, store, jsonl, R(D{"a": 1}), custom...)
		if err != nil {
			t.Fatal(err)
		}
		head, err := tryWrite(t, store, jsonl, R(D{"a": 2}), custom...)
		if err != nil {
			t.Fatal(err)
		}
		// The custom components wrap gzip and MD5, so the head's stored bytes
		// match the bundled names that its rewritten manifest records.
		head.Compressor, head.ChecksumAlgorithm = "gzip", "md5"
		writeManifestAt(t, store, "datasets/events/snapshots/"+string(head.ID)+"/manifest.json", head.Manifest)

		ds, err := Open(Use(store)).Records(ctx, "events", jsonl, slices.Concat(custom, []DatasetOption{WithRecordedComponents()})...)
		if err != nil {
			t.Fatal(err)
		}
		next, err := ds.Write(ctx, R(D{"a": 3}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if next.Compressor != "gzip" || next.ChecksumAlgorithm != "md5" {
			t.Errorf("write records %q and %q, want the head's gzip and md5", next.Compressor, next.ChecksumAlgorithm)
		}
		recs, err := ds.Read(ctx, older.ID)
		if err != nil || len(recs) != 1 || recs[0]["a"] != float64(1) {
			t.Fatalf("read the older custom snapshot: %v, %v; want its one record", recs, err)
		}
	})
}

// TestRecordedComponents_Boundaries covers what WithRecordedComponents leaves to
// the existing settings rules: racing first writers and the codec and layout
// checks.
func TestRecordedComponents_Boundaries(t *testing.T) {
	t.Parallel()
	// Two writers that open an empty dataset with different defaults each keep
	// their configured components. The first commit records its settings, and
	// the second fails with ErrSettingsMismatch rather than publishing bytes
	// under the winner's labels.
	t.Run("racing first writers keep their components", func(t *testing.T) {
		ctx := t.Context()
		store := NewMemory()
		open := func(opts ...DatasetOption) *Records[D] {
			return openOn(t, store, jsonl, append(opts, WithRecordedComponents())...)
		}
		winner := open(WithCompressor(Zstd()), WithChecksum(SHA256()))
		loser := open(WithCompressor(Gzip()), WithChecksum(CRC32C()))
		won, err := winner.Write(ctx, R(D{"a": 1}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := loser.Write(ctx, R(D{"a": 2}), nil); !errors.Is(err, ErrSettingsMismatch) {
			t.Fatalf("second first writer: got %v, want ErrSettingsMismatch", err)
		}
		view, err := OpenReadOnly(Use(store)).Records(ctx, "events", jsonl)
		if err != nil {
			t.Fatal(err)
		}
		head, err := view.Latest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if head.ID != won.ID || head.Compressor != "zstd" || head.ChecksumAlgorithm != "sha256" {
			t.Fatalf("head %s records %q and %q, want the winner %s with zstd and sha256", head.ID, head.Compressor, head.ChecksumAlgorithm, won.ID)
		}
	})

	// WithRecordedComponents adopts only the compressor and checksum: a writer
	// whose codec or layout differs from the head's still fails to open.
	t.Run("the codec and layout checks still apply", func(t *testing.T) {
		// Each case first reopens the head with its own settings, which must
		// succeed, then changes one setting. The layout case reorders a Hive
		// head's keys, so both writers resolve the same manifest and only the
		// partition-key check can reject the second.
		cases := []struct {
			name  string
			seed  []DatasetOption
			codec Codec[D]
			opts  []DatasetOption
		}{
			{"codec", nil, parquetOf("a", true), nil},
			{"layout", []DatasetOption{hive("a", "b")}, jsonl, []DatasetOption{hive("b", "a")}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store := NewMemory()
				if _, err := tryWrite(t, store, jsonl, R(D{"a": "x", "b": "y"}), tc.seed...); err != nil {
					t.Fatal(err)
				}
				same := slices.Concat(tc.seed, []DatasetOption{WithRecordedComponents()})
				if _, err := Open(Use(store)).Records(t.Context(), "events", jsonl, same...); err != nil {
					t.Fatalf("reopen with the head's own %s: %v", tc.name, err)
				}
				other := slices.Concat(tc.opts, []DatasetOption{WithRecordedComponents()})
				if _, err := Open(Use(store)).Records(t.Context(), "events", tc.codec, other...); !errors.Is(err, ErrSettingsMismatch) {
					t.Fatalf("open with another %s: got %v, want ErrSettingsMismatch", tc.name, err)
				}
			})
		}
	})
}

// unnamedChecksum is a checksum without a name, which no manifest can record.
type unnamedChecksum struct{ Checksum }

func (unnamedChecksum) Name() string { return "" }

// noIOStore is a Store whose every method panics, for tests that require
// argument validation before any store call.
type noIOStore struct{ Store }

// A writer or view refuses a checksum without a name before any store call,
// because no manifest could record it. RemoveDataset skips this name check,
// since removal records no checksum.
func TestWithChecksum_RequiresAName(t *testing.T) {
	t.Parallel()
	unnamed := WithChecksum(unnamedChecksum{SHA256()})
	opens := []struct {
		name string
		open func(ctx context.Context, store Store) error
	}{
		{"records writer", func(ctx context.Context, store Store) error {
			_, err := Open(Use(store)).Records(ctx, "events", jsonl, unnamed)
			return err
		}},
		{"blobs writer", func(ctx context.Context, store Store) error {
			_, err := Open(Use(store)).Blobs(ctx, "files", unnamed)
			return err
		}},
		{"records view", func(ctx context.Context, store Store) error {
			_, err := OpenReadOnly(Use(store)).Records(ctx, "events", jsonl, unnamed)
			return err
		}},
		{"blobs view", func(ctx context.Context, store Store) error {
			_, err := OpenReadOnly(Use(store)).Blobs(ctx, "files", unnamed)
			return err
		}},
	}
	for _, tc := range opens {
		t.Run(tc.name, func(t *testing.T) {
			// noIOStore embeds a nil Store, so any store call panics and fails the
			// test: the refusal must come before every kind of store I/O.
			if err := tc.open(t.Context(), noIOStore{}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
		})
	}
	t.Run("dataset removal skips the name check", func(t *testing.T) {
		if err := Open(Use(NewMemory())).RemoveDataset(t.Context(), "events", unnamed); err != nil {
			t.Fatalf("remove: %v, want success", err)
		}
	})
}
