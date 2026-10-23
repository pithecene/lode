package chaos

import (
	"context"
	"fmt"
	"strings"

	"github.com/pithecene/lode/lode"
)

// The listings are an index of the history: on a Hive layout, a snapshot's
// partition manifests are persisted after its commit is complete, so a fault
// between them leaves a committed snapshot that only its canonical manifest
// records (CONTRACT_CORE §Head Pointer, V1_API_DECISIONS D10b). These checks
// compare the listings with the history that Snapshots enumerates.

// listingsCover requires an unfiltered ListManifests to report every
// committed snapshot, whatever its partition index holds
// (CONTRACT_READ_API §Listing Order and Cursors).
func (v *verifier) listingsCover(ctx context.Context) []string {
	listed, err := v.listed(ctx, lode.PartitionPath{})
	if err != nil {
		return []string{fmt.Sprintf("unfiltered ListManifests: %v", err)}
	}
	var missing []string
	for _, id := range v.chain.ids() {
		if !listed[id] {
			missing = append(missing, fmt.Sprintf("committed %s is missing from the unfiltered ListManifests", id))
		}
	}
	return missing
}

// indexRestored requires, after the final sweep, every committed snapshot to
// be listed under each partition that holds one of its files: Sweep restores
// an index that a fault left incomplete (CONTRACT_RETENTION §Definition,
// step 3).
func (v *verifier) indexRestored(ctx context.Context) []string {
	byPartition := make(map[string]map[lode.DatasetSnapshotID]bool)
	var bad []string
	for _, id := range v.chain.ids() {
		for _, partition := range partitionsOf(v.snapshots[id].Files) {
			listed, ok := byPartition[partition]
			if !ok {
				var err error
				if listed, err = v.listedIn(ctx, partition); err != nil {
					return append(bad, fmt.Sprintf("ListManifests(%s): %v", partition, err))
				}
				byPartition[partition] = listed
			}
			if !listed[id] {
				bad = append(bad, fmt.Sprintf("after the final sweep, committed %s is missing from ListManifests(%s)", id, partition))
			}
		}
	}
	return bad
}

// listedIn lists the snapshots under one partition, given as stored.
func (v *verifier) listedIn(ctx context.Context, partition string) (map[lode.DatasetSnapshotID]bool, error) {
	path, err := lode.ParsePartitionPath(partition)
	if err != nil {
		return nil, err
	}
	return v.listed(ctx, path)
}

// listed returns the snapshot IDs that ListManifests reports under
// partition, the zero value for the whole dataset.
func (v *verifier) listed(ctx context.Context, partition lode.PartitionPath) (map[lode.DatasetSnapshotID]bool, error) {
	refs, err := v.ds.ListManifests(ctx, partition, lode.ManifestListOptions{})
	if err != nil {
		return nil, err
	}
	ids := make(map[lode.DatasetSnapshotID]bool, len(refs))
	for _, ref := range refs {
		ids[ref.ID] = true
	}
	return ids, nil
}

// partitionsOf returns the distinct partitions of a snapshot's files, read
// from their paths under the default names: the segments between
// "partitions/" and "/segments/". A file of an unpartitioned layout has
// none.
func partitionsOf(files []lode.FileRef) []string {
	seen := make(map[string]bool)
	var partitions []string
	for _, f := range files {
		_, rest, ok := strings.Cut(f.Path, "/partitions/")
		if !ok {
			continue
		}
		partition, _, ok := strings.Cut(rest, "/segments/")
		if ok && !seen[partition] {
			seen[partition] = true
			partitions = append(partitions, partition)
		}
	}
	return partitions
}
