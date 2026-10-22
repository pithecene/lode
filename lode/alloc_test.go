//go:build !race

// The race detector adds allocations of its own, so these gates run only
// without it.

package lode

import (
	"bytes"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// allocGates are the hot paths of the Dataset API on the in-memory store,
// warm: the head is known, so no operation scans. Each byte ceiling is the
// measured figure rounded up to the next 64 bytes, because map growth in the
// in-memory store varies it by a few bytes.
var allocGates = []testutil.AllocGate{
	{Name: "Write of three records", Allocs: 56, Bytes: 6208, Setup: gateWrite},
	{Name: "Read of three records", Allocs: 59, Bytes: 8640, Setup: gateRead},
	{Name: "StreamWrite of one kibibyte", Allocs: 32, Bytes: 6336, Setup: gateStreamWrite},
	{Name: "StreamWriteRecords of three records", Allocs: 61, Bytes: 6400, Setup: gateStreamWriteRecords},
	{Name: "JSONL Decode of three records", Allocs: 33, Bytes: 5568, Setup: gateDecode},
}

// TestAllocationGates holds each Dataset hot path to its allocation ceiling.
func TestAllocationGates(t *testing.T) { testutil.RunAllocGates(t, allocGates) }

// gateRecords are the records every gate writes or reads.
func gateRecords() []any {
	return R(
		D{"id": "1", "value": "hello"},
		D{"id": "2", "value": "world"},
		D{"id": "3", "value": "gate"},
	)
}

func gateDataset(t *testing.T) Dataset {
	t.Helper()
	ds, err := NewDataset("gate", NewMemoryFactory(), WithCodec(NewJSONLCodec()))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func gateWrite(t *testing.T) func() {
	ds, records := gateDataset(t), gateRecords()
	return func() {
		if _, err := ds.Write(t.Context(), records, Metadata{}); err != nil {
			t.Fatal(err)
		}
	}
}

func gateRead(t *testing.T) func() {
	ds := gateDataset(t)
	snap, err := ds.Write(t.Context(), gateRecords(), Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := ds.Read(t.Context(), snap.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func gateStreamWrite(t *testing.T) func() {
	ds, err := NewDataset("gate", NewMemoryFactory())
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 1024)
	return func() {
		sw, err := ds.StreamWrite(t.Context(), Metadata{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sw.Write(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := sw.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func gateStreamWriteRecords(t *testing.T) func() {
	ds, records := gateDataset(t), gateRecords()
	return func() {
		if _, err := ds.StreamWriteRecords(t.Context(), &sliceIterator{records: records}, Metadata{}); err != nil {
			t.Fatal(err)
		}
	}
}

func gateDecode(t *testing.T) func() {
	codec := NewJSONLCodec()
	var buf bytes.Buffer
	if err := codec.Encode(&buf, gateRecords()); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	return func() {
		if _, err := codec.Decode(bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
}
