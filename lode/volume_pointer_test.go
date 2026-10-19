package lode

import (
	"bytes"
	"testing"
)

// A fresh Volume instance resolves its parent from the persisted pointer
// without scanning when the pointer names a loadable snapshot. The existing
// cold-start tests assert the parent linkage but not the absence of a scan,
// so negating the pointer check was unobserved.
func TestVolume_Commit_ColdStart_ValidPointer_SkipsScan(t *testing.T) {
	fs := newFaultStore(NewMemory())
	first := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 20)
	blk := stageBlock(t, first, 0, bytes.Repeat([]byte("A"), 10))
	snap1 := commitBlocks(t, first, []BlockRef{blk}, Metadata{})

	second := newTestVolume(t, "test-vol", newFaultStoreFactory(fs), 20)
	blk2 := stageBlock(t, second, 10, bytes.Repeat([]byte("B"), 10))
	before := len(fs.ListCalls())
	snap2 := commitBlocks(t, second, []BlockRef{blk2}, Metadata{})
	if got := len(fs.ListCalls()) - before; got != 0 {
		t.Fatalf("cold start with valid pointer issued %d List calls, want 0", got)
	}
	if snap2.Manifest.ParentSnapshotID != snap1.ID {
		t.Fatalf("parent = %s, want %s", snap2.Manifest.ParentSnapshotID, snap1.ID)
	}
}
