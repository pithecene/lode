//go:build !race

// The race detector adds allocations of its own, so these gates run only
// without it.

package lode

import (
	"bytes"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// allocGates are the hot paths of the dataset handles on the in-memory store,
// warm: the head is known, so no operation scans. Each byte ceiling is the
// measured figure rounded up to the next 64 bytes, because map growth in the
// in-memory store varies it by a few bytes.
var allocGates = []testutil.AllocGate{
	{Name: "Write of three records", Allocs: 53, Bytes: 6656, Setup: gateWrite},
	{Name: "Read of three records", Allocs: 60, Bytes: 10176, Setup: gateRead},
	{Name: "Blob StreamWrite of one kibibyte", Allocs: 31, Bytes: 7232, Setup: gateStreamWrite},
	{Name: "Record StreamWrite of three records", Allocs: 63, Bytes: 7296, Setup: gateStreamWriteRecords},
	{Name: "JSONL Decode of three records", Allocs: 33, Bytes: 5504, Setup: gateDecode},
}

// TestAllocationGates holds each dataset hot path to its allocation ceiling.
func TestAllocationGates(t *testing.T) { testutil.RunAllocGates(t, allocGates) }

// gateRecords are the records every gate writes or reads.
func gateRecords() []D {
	return R(
		D{"id": "1", "value": "hello"},
		D{"id": "2", "value": "world"},
		D{"id": "3", "value": "gate"},
	)
}

func gateDataset(t *testing.T) Records[D] {
	t.Helper()
	return recordsOn(t, NewMemory(), "gate")
}

func gateWrite(t *testing.T) func() {
	ds, records := gateDataset(t), gateRecords()
	return func() {
		if _, err := ds.Write(t.Context(), records, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func gateRead(t *testing.T) func() {
	ds := gateDataset(t)
	snap, err := ds.Write(t.Context(), gateRecords(), nil)
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
	ds := blobsOn(t, NewMemory(), "gate")
	payload := bytes.Repeat([]byte("x"), 1024)
	return func() {
		sw, err := ds.StreamWrite(t.Context(), nil)
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
		if _, err := ds.StreamWrite(t.Context(), seqOf(records...), nil); err != nil {
			t.Fatal(err)
		}
	}
}

func gateDecode(t *testing.T) func() {
	codec := JSONL[D]()
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
