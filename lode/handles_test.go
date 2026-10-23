package lode

import (
	"iter"
	"testing"
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
