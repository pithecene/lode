package chaos

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"
)

const (
	volumeID volume.ID = "chaos-volume"
	// stripe is the share of the address space each writer owns, so
	// writers never stage overlapping blocks.
	stripe = 1 << 20
	// maxBlock bounds a block's length.
	maxBlock = 4096
)

// Block is a byte range a Volume writer committed, with the writer whose
// pattern fills it.
type Block struct {
	Offset, Length int64
	Writer         int
}

// end is the offset just past the block.
func (b Block) end() int64 { return b.Offset + b.Length }

// pattern is the block's content: a byte sequence determined by its writer
// and offset, so a read can be checked without storing the bytes.
func (b Block) pattern() []byte {
	p := make([]byte, b.Length)
	for i := range p {
		p[i] = byte((int64(b.Writer)*31 + b.Offset + int64(i)) % 251)
	}
	return p
}

// volumeLength is the volume's address space: a stripe per writer and one
// for the settling commit.
func (c Config) volumeLength() int64 { return int64(c.Writers+1) * stripe }

// settleBlock is the block of the settling commit, in the last stripe.
func (c Config) settleBlock() Block {
	return Block{Offset: int64(c.Writers) * stripe, Length: 1, Writer: -1}
}

// openVolume opens one handle on the run's volume for a goroutine, with its
// own in-memory head and, when faults are enabled, its own faulty store.
func openVolume(cfg Config, base lode.RangeStore, rng *rand.Rand) (volume.Volume, error) {
	store := cfg.storeFor(base, rng)
	factory := func() (lode.RangeStore, error) { return store, nil }
	return volume.New(volumeID, factory, cfg.volumeLength(), volume.WithRetryCount(cfg.Retries))
}

// volumeWriter stages one to three blocks at the next free offsets of its
// stripe and commits them, until the deadline. A failed commit leaves its
// blocks staged and unreferenced; the stripe moves past them. As for the
// Dataset writers, the deadline is checked between commits.
func volumeWriter(ctx context.Context, cfg Config, store lode.RangeStore, index int, until time.Time, ledger *Ledger) {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(index)+1)) //nolint:gosec // reproducible test randomness, not security
	next, end := int64(index)*stripe, int64(index+1)*stripe
	var vol volume.Volume
	for ctx.Err() == nil && time.Now().Before(until) && next < end-3*maxBlock {
		if vol == nil {
			var err error
			if vol, err = openVolume(cfg, store, rng); err != nil {
				ledger.violate("open", "volume writer %d: %v", index, err)
				return
			}
		}
		blocks := drawBlocks(rng, index, &next)
		snap, err := commitBlocks(ctx, vol, blocks, index)
		switch {
		case err == nil:
			ledger.volumeCommit(snap.ID, blocks)
		case errors.Is(err, lode.ErrSnapshotConflict):
			ledger.conflict()
		case injected(err):
			ledger.fault()
			if errors.Is(err, errKilled) {
				vol = nil
			}
		default:
			ledger.violate("volume-write", "writer %d: %v", index, err)
		}
	}
}

// drawBlocks draws one to three consecutive blocks of a writer, starting at
// next, and moves next past them.
func drawBlocks(rng *rand.Rand, writer int, next *int64) []Block {
	blocks := make([]Block, 1+rng.IntN(3))
	for i := range blocks {
		blocks[i] = Block{Offset: *next, Length: 1 + rng.Int64N(maxBlock), Writer: writer}
		*next = blocks[i].end()
	}
	return blocks
}

// commitBlocks stages blocks and commits them as one snapshot.
func commitBlocks(ctx context.Context, vol volume.Volume, blocks []Block, writer int) (*volume.Snapshot, error) {
	refs := make([]volume.BlockRef, len(blocks))
	for i, b := range blocks {
		ref, err := vol.StageWriteAt(ctx, b.Offset, bytes.NewReader(b.pattern()))
		if err != nil {
			return nil, err
		}
		refs[i] = ref
	}
	return vol.Commit(ctx, refs, volume.Metadata{"writer": writer})
}

// volumeVerifier holds the evidence a finished Volume run left: the
// persisted history, its head and what the writers were acknowledged.
type volumeVerifier struct {
	vol    volume.Volume
	chain  *chain[volume.SnapshotID]
	settle volume.SnapshotID // the settling commit
	head   *volume.Snapshot  // Latest after the settling commit
	refs   []volume.BlockRef // the head's blocks, sorted by offset
	acked  []volume.SnapshotID
	blocks []Block
}

// volumeChecks are the invariants CONTRACT_VOLUME promises for a volume
// that concurrent writers committed disjoint blocks to.
var volumeChecks = []check[*volumeVerifier]{
	{"acknowledged-commits-persist", (*volumeVerifier).acknowledgedPersist},
	{"history-shape", (*volumeVerifier).historyShape},
	{"blocks-disjoint", (*volumeVerifier).blocksDisjoint},
	{"blocks-read-back", (*volumeVerifier).blocksReadBack},
}

// verifyVolume settles the head with one clean commit and checks the
// history and contents the run left. The settling commit must succeed even
// after an abandoned commit, whose staged manifest supplies the blocks it
// merges (CONTRACT_VOLUME §Error Semantics).
func verifyVolume(ctx context.Context, cfg Config, store lode.RangeStore, ledger *Ledger) int {
	vol, err := volume.New(volumeID, func() (lode.RangeStore, error) { return store, nil }, cfg.volumeLength(), volume.WithRetryCount(cfg.Retries))
	if err != nil {
		ledger.violate("verify", "open: %v", err)
		return 0
	}
	snap, err := commitBlocks(ctx, vol, []Block{cfg.settleBlock()}, -1)
	if err != nil {
		ledger.violate("verify", "settling commit: %v", err)
		return 0
	}
	v, err := newVolumeVerifier(ctx, vol, snap.ID, ledger)
	if err != nil {
		ledger.violate("verify", "%v", err)
		return 0
	}
	runChecks(ctx, v, volumeChecks, ledger)
	return v.chain.size()
}

func newVolumeVerifier(ctx context.Context, vol volume.Volume, settle volume.SnapshotID, ledger *Ledger) (*volumeVerifier, error) {
	snaps, err := vol.Snapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("chaos: list volume snapshots: %w", err)
	}
	head, err := vol.Latest(ctx)
	if err != nil && !errors.Is(err, lode.ErrNoSnapshots) {
		return nil, fmt.Errorf("chaos: resolve the volume head: %w", err)
	}
	v := &volumeVerifier{vol: vol, chain: newChain[volume.SnapshotID](), settle: settle}
	for _, s := range snaps {
		v.chain.add(s.ID, s.Manifest.ParentSnapshotID)
	}
	v.head = head
	if v.head != nil {
		v.refs = slices.SortedFunc(slices.Values(v.head.Manifest.Blocks), byOffset)
	}
	v.acked, v.blocks = ledger.volumeHistory()
	return v, nil
}

// acknowledgedPersist requires every acknowledged commit to have a
// manifest; a volume has no retention.
func (v *volumeVerifier) acknowledgedPersist(context.Context) []string {
	var lost []string
	for _, id := range v.acked {
		if !v.chain.has(id) {
			lost = append(lost, fmt.Sprintf("acknowledged %s has no manifest", id))
		}
	}
	return lost
}

// historyShape requires Latest to return the settling commit when there is
// one, and a linear chain that the head reaches in full.
func (v *volumeVerifier) historyShape(context.Context) []string {
	if v.head == nil {
		return nil
	}
	return append(settled(v.head.ID, v.settle), v.chain.shape(v.head.ID)...)
}

// byOffset orders block references by offset.
func byOffset(a, b volume.BlockRef) int { return cmp.Compare(a.Offset, b.Offset) }

// blocksDisjoint requires the head's cumulative manifest to hold no two
// overlapping blocks.
func (v *volumeVerifier) blocksDisjoint(context.Context) []string {
	var bad []string
	refs := v.refs
	for i := 1; i < len(refs); i++ {
		if prev := refs[i-1]; prev.Offset+prev.Length > refs[i].Offset {
			bad = append(bad, fmt.Sprintf("blocks at %d and %d overlap", prev.Offset, refs[i].Offset))
		}
	}
	return bad
}

// blocksReadBack requires every acknowledged block to be in the head's
// manifest and to read back its pattern.
func (v *volumeVerifier) blocksReadBack(ctx context.Context) []string {
	if v.head == nil {
		return v.missingBlocks(v.blocks)
	}
	var bad []string
	for _, b := range v.blocks {
		if !v.committed(b) {
			bad = append(bad, fmt.Sprintf("acknowledged block [%d, +%d) of writer %d is missing from the head %s", b.Offset, b.Length, b.Writer, v.head.ID))
			continue
		}
		got, err := v.vol.ReadAt(ctx, v.head.ID, b.Offset, b.Length)
		if err != nil || !bytes.Equal(got, b.pattern()) {
			bad = append(bad, fmt.Sprintf("block [%d, +%d) of writer %d reads back wrong (%v)", b.Offset, b.Length, b.Writer, err))
		}
	}
	return bad
}

// committed reports whether the head's manifest holds block b.
func (v *volumeVerifier) committed(b Block) bool {
	i, found := slices.BinarySearchFunc(v.refs, b.Offset, func(ref volume.BlockRef, offset int64) int {
		return cmp.Compare(ref.Offset, offset)
	})
	return found && v.refs[i].Length == b.Length
}

// missingBlocks describes acknowledged blocks of a volume without a head.
func (v *volumeVerifier) missingBlocks(blocks []Block) []string {
	bad := make([]string, len(blocks))
	for i, b := range blocks {
		bad[i] = fmt.Sprintf("acknowledged block [%d, +%d) of writer %d, but the volume has no snapshot", b.Offset, b.Length, b.Writer)
	}
	return bad
}
