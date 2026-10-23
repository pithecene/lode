package lode

import (
	"encoding/json"
	"errors"
	"iter"
	"slices"
	"testing"
	"time"
)

// Shared fixtures for tests of the dataset handles. Each opens a handle over
// a store the test holds, through lode.Use, so that a test can inspect or
// seed the store directly.

// recordsOn opens dataset id over store as Records[D], encoded as JSONL
// unless opts' codec says otherwise. It fails the test on error.
func recordsOn(t testing.TB, store Store, id DatasetID, opts ...DatasetOption) Records[D] {
	t.Helper()
	r, err := Open(Use(store)).Records(t.Context(), id, JSONL[D](), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// recordsWith opens dataset id over store as Records[D] with codec.
func recordsWith(t testing.TB, store Store, id DatasetID, codec Codec[D], opts ...DatasetOption) Records[D] {
	t.Helper()
	r, err := Open(Use(store)).Records(t.Context(), id, codec, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// blobsOn opens dataset id over store as Blobs.
func blobsOn(t testing.TB, store Store, id DatasetID, opts ...DatasetOption) Blobs {
	t.Helper()
	b, err := Open(Use(store)).Blobs(t.Context(), id, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// viewOn opens a view of dataset id's D records over store.
func viewOn(t testing.TB, store Store, id DatasetID, opts ...ViewOption) RecordView[D] {
	t.Helper()
	v, err := OpenReadOnly(Use(store)).Records(t.Context(), id, JSONL[D](), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// hive is the Hive layout option with keys, the common test layout.
func hive(keys ...string) DatasetOption {
	return WithLayout(Hive(keys...))
}

// seqOf yields records to StreamWrite without an error.
func seqOf[T any](records ...T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, r := range records {
			if !yield(r, nil) {
				return
			}
		}
	}
}

// failingSeq yields records, then err.
func failingSeq[T any](err error, records ...T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, r := range records {
			if !yield(r, nil) {
				return
			}
		}
		var zero T
		yield(zero, err)
	}
}

// metadataOf decodes a snapshot's metadata into a map.
func metadataOf(t testing.TB, m Metadata) D {
	t.Helper()
	var out D
	if err := m.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRecords_AnEmptyWriteRemainsReadable(t *testing.T) {
	store := NewMemory()
	snap, err := recordsOn(t, store, "events").Write(t.Context(), R(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := viewOn(t, store, "events").Read(t.Context(), snap.ID)
	if err != nil || len(got) != 0 {
		t.Fatalf("Read = %v, %v; want no records", got, err)
	}
}

func TestViews_RejectANilCompressor(t *testing.T) {
	_, err := OpenReadOnly(Use(NewMemory())).Records(t.Context(), "events", JSONL[D](), WithCompressor(nil))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestWrite_RejectsMetadataDecodedFromANonObject(t *testing.T) {
	var m Metadata
	if err := json.Unmarshal([]byte(`[1]`), &m); err != nil {
		t.Fatal(err)
	}
	_, err := recordsOn(t, NewMemory(), "events").Write(t.Context(), R(D{"a": 1}), m)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestViews_CannotWriteThroughRetention(t *testing.T) {
	store := NewMemory()
	if _, err := recordsOn(t, store, "events").Write(t.Context(), R(D{"a": 1}), nil); err != nil {
		t.Fatal(err)
	}
	// An uncommitted data object old enough that a writer's Sweep would
	// reclaim it.
	putObject(t, store, "datasets/events/snapshots/"+string(idAt(time.Now().Add(-time.Hour)))+"/data/orphan")
	before := storeKeys(t, store)
	retention, ok := viewOn(t, store, "events").(Retention)
	if !ok {
		return // a view without the methods cannot write through them
	}
	if _, err := retention.Sweep(t.Context(), SweepRequest{Grace: time.Minute}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Sweep: got %v, want ErrReadOnly", err)
	}
	if after := storeKeys(t, store); !slices.Equal(before, after) {
		t.Errorf("objects changed through a view: %v -> %v", before, after)
	}
}

// nilCodec is a codec type whose nil pointer a caller might pass.
type nilCodec struct{ Codec[D] }

func TestHandles_RejectNilComponents(t *testing.T) {
	var codec *nilCodec
	var compressor *renamedCompressor
	var checksum *renamedChecksum
	var hiveLayout *HiveLayout
	cases := map[string]func() error{
		"typed-nil codec": func() error {
			_, err := Open(Use(NewMemory())).Records(t.Context(), "events", Codec[D](codec))
			return err
		},
		"typed-nil compressor": func() error {
			_, err := Open(Use(NewMemory())).Records(t.Context(), "events", JSONL[D](), WithCompressor(compressor))
			return err
		},
		"typed-nil view compressor": func() error {
			_, err := OpenReadOnly(Use(NewMemory())).Records(t.Context(), "events", JSONL[D](), WithCompressor(compressor))
			return err
		},
		"typed-nil checksum": func() error {
			_, err := Open(Use(NewMemory())).Records(t.Context(), "events", JSONL[D](), WithChecksum(checksum))
			return err
		},
		"nil Hive layout": func() error {
			_, err := Open(Use(NewMemory())).Records(t.Context(), "events", JSONL[D](), WithLayout(hiveLayout))
			return err
		},
		"zero Hive layout": func() error {
			_, err := Open(Use(NewMemory())).Records(t.Context(), "events", JSONL[D](), WithLayout(&HiveLayout{}))
			return err
		},
		"nil By function": func() error {
			_, err := Open(Use(NewMemory())).Records(t.Context(), "events", JSONL[event](), WithLayout(Hive("day").By[event](nil)))
			return err
		},
	}
	for name, open := range cases {
		if err := open(); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: got %v, want ErrInvalidArgument", name, err)
		}
	}
}

func TestMetadata_NullEmptiesAReusedValue(t *testing.T) {
	var m Metadata
	if err := json.Unmarshal([]byte(`{"a":1}`), &m); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`null`), &m); err != nil {
		t.Fatal(err)
	}
	if got := string(m.JSON()); got != "{}" {
		t.Errorf("JSON = %s, want {}", got)
	}
}
