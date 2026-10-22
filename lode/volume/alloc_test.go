//go:build !race

// The race detector adds allocations of its own, so these gates run only
// without it.

package volume

import (
	"bytes"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
	"github.com/pithecene/lode/lode"
)

const (
	gateBlockSize  = 1024
	gateBlockCount = 10
)

// allocGates are the hot paths of the Volume API on the in-memory store,
// warm: the head is known, so no operation scans. Each byte ceiling is the
// measured figure rounded up to the next 64 bytes, because map growth in the
// in-memory store varies it by a few bytes.
var allocGates = []testutil.AllocGate{
	{Name: "ReadAt within one of ten blocks", Allocs: 41, Bytes: 10816, Setup: gateReadAt},
	{Name: "Commit of one block onto a small volume", Allocs: 136, Bytes: 252032, Setup: gateCommit},
}

// TestAllocationGates holds each Volume hot path to its allocation ceiling.
func TestAllocationGates(t *testing.T) { testutil.RunAllocGates(t, allocGates) }

// gateVolume returns a volume with gateBlockCount committed blocks, room
// for as many more blocks as a gate commits, and its head.
func gateVolume(t *testing.T) (Volume, *Snapshot) {
	t.Helper()
	vol, err := New("gate", lode.NewMemoryFactory(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]BlockRef, gateBlockCount)
	for i := range refs {
		refs[i] = gateStage(t, vol, int64(i))
	}
	snap, err := vol.Commit(t.Context(), refs, lode.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	return vol, snap
}

// gateStage stages block number i.
func gateStage(t *testing.T, vol Volume, i int64) BlockRef {
	t.Helper()
	ref, err := vol.StageWriteAt(t.Context(), i*gateBlockSize, bytes.NewReader(bytes.Repeat([]byte{byte(i)}, gateBlockSize)))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func gateReadAt(t *testing.T) func() {
	vol, snap := gateVolume(t)
	return func() {
		if _, err := vol.ReadAt(t.Context(), snap.ID, 5*gateBlockSize+256, 512); err != nil {
			t.Fatal(err)
		}
	}
}

// gateCommit commits one staged block per run; staging is setup, so the
// gate measures the commit alone. Each run adds a block, so the volume grows
// from ten blocks to about two hundred during the measurement.
func gateCommit(t *testing.T) func() {
	vol, _ := gateVolume(t)
	refs := make([]BlockRef, testutil.AllocGateCalls)
	for i := range refs {
		refs[i] = gateStage(t, vol, int64(gateBlockCount+i))
	}
	next := 0
	return func() {
		if _, err := vol.Commit(t.Context(), refs[next:next+1], lode.Metadata{}); err != nil {
			t.Fatal(err)
		}
		next++
	}
}
