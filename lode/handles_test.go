package lode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
func viewOn(t testing.TB, store Store, id DatasetID) RecordView[D] {
	t.Helper()
	v, err := OpenReadOnly(Use(store)).Records(t.Context(), id, JSONL[D]())
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

// unnamedCodec is JSONL under the empty name, which denotes blobs.
type unnamedCodec struct{ Codec[D] }

func (unnamedCodec) Name() string { return "" }

func TestHandles_RejectAnUnnamedCodec(t *testing.T) {
	codec := unnamedCodec{JSONL[D]()}
	if _, err := Open(Use(NewMemory())).Records(t.Context(), "events", Codec[D](codec)); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("writer: got %v, want ErrInvalidArgument", err)
	}
	if _, err := OpenReadOnly(Use(NewMemory())).Records(t.Context(), "events", Codec[D](codec)); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("view: got %v, want ErrInvalidArgument", err)
	}
}

// mapStore is a store of a nil-able kind other than a pointer.
type mapStore map[string]string

func (mapStore) Put(context.Context, string, io.Reader) error       { return nil }
func (mapStore) Get(context.Context, string) (io.ReadCloser, error) { return nil, ErrNotFound }

func (mapStore) Exists(context.Context, string) (bool, error)                    { return false, nil }
func (mapStore) List(context.Context, string) ([]string, error)                  { return nil, nil }
func (mapStore) Delete(context.Context, string) error                            { return nil }
func (mapStore) ReadRange(context.Context, string, int64, int64) ([]byte, error) { return nil, nil }
func (mapStore) ReaderAt(context.Context, string) (io.ReaderAt, error)           { return nil, ErrNotFound }
func (mapStore) CompareAndSwap(context.Context, string, string, string) error    { return nil }

func TestRoots_RejectANilMapStore(t *testing.T) {
	var nilStore mapStore
	factory := Factory[mapStore](func() (mapStore, error) { return nilStore, nil })
	if _, err := Open(factory).Records(t.Context(), "events", JSONL[D]()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestViews_HaveNoWriterMethods(t *testing.T) {
	store := NewMemory()
	if _, err := recordsOn(t, store, "logs").Write(t.Context(), R(D{"a": 1}), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := blobsOn(t, store, "blobs").Write(t.Context(), []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	blobView, err := OpenReadOnly(Use(store)).Blobs(t.Context(), "blobs")
	if err != nil {
		t.Fatal(err)
	}
	views := map[string]any{"record view": viewOn(t, store, "logs"), "blob view": blobView}
	for name, view := range views {
		if _, ok := view.(Retention); ok {
			t.Errorf("%s implements Retention", name)
		}
		if _, ok := view.(Records[D]); ok {
			t.Errorf("%s implements Records", name)
		}
		if _, ok := view.(Blobs); ok {
			t.Errorf("%s implements Blobs", name)
		}
	}
}
