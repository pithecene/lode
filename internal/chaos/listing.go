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

// listingsCover requires an unfiltered Manifests iterator to report every
// committed snapshot (CONTRACT_READ_API §Listing Order and Cursors) and
// ListPartitions to report every partition present in committed snapshots.
func (v *verifier) listingsCover(ctx context.Context) []string {
	listed, err := v.listed(ctx, lode.PartitionPath{})
	if err != nil {
		return []string{fmt.Sprintf("unfiltered Manifests: %v", err)}
	}
	var bad []string
	for _, id := range v.chain.ids() {
		if _, ok := listed[id]; !ok {
			bad = append(bad, fmt.Sprintf("committed %s is missing from unfiltered Manifests", id))
		}
	}
	return append(bad, v.partitionsCover(ctx)...)
}

func (v *verifier) partitionsCover(ctx context.Context) []string {
	parts, err := v.ds.ListPartitions(ctx, lode.PartitionListOptions{})
	if err != nil {
		return []string{fmt.Sprintf("ListPartitions: %v", err)}
	}
	have := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		have[p.String()] = struct{}{}
	}
	var bad []string
	for _, id := range v.chain.ids() {
		for _, p := range partitionsOf(v.snapshots[id].Objects) {
			if _, ok := have[p]; !ok {
				bad = append(bad, fmt.Sprintf("partition %s of committed %s is missing from ListPartitions", p, id))
			}
		}
	}
	return bad
}

// indexRestored requires, after the final sweep, every committed snapshot to
// be listed under each partition that holds one of its objects: Sweep restores
// an index that a fault left incomplete (CONTRACT_RETENTION §Definition,
// step 3).
func (v *verifier) indexRestored(ctx context.Context) []string {
	byPartition := make(map[string]map[lode.SnapshotID]struct{})
	var bad []string
	for _, id := range v.chain.ids() {
		for _, partition := range partitionsOf(v.snapshots[id].Objects) {
			listed, ok := byPartition[partition]
			if !ok {
				var err error
				if listed, err = v.listedIn(ctx, partition); err != nil {
					return append(bad, fmt.Sprintf("Manifests(%s): %v", partition, err))
				}
				byPartition[partition] = listed
			}
			if _, ok := listed[id]; !ok {
				bad = append(bad, fmt.Sprintf("after the final sweep, committed %s is missing from Manifests(%s)", id, partition))
			}
		}
	}
	return bad
}

// listedIn lists the snapshots under one partition, given as stored.
func (v *verifier) listedIn(ctx context.Context, partition string) (map[lode.SnapshotID]struct{}, error) {
	path, err := lode.ParsePartitionPath(partition)
	if err != nil {
		return nil, err
	}
	return v.listed(ctx, path)
}

// listed returns the snapshot IDs that Manifests yields under partition, the
// zero value for the whole dataset.
func (v *verifier) listed(ctx context.Context, partition lode.PartitionPath) (map[lode.SnapshotID]struct{}, error) {
	ids := make(map[lode.SnapshotID]struct{})
	for lm, err := range v.ds.Manifests(ctx, partition, lode.ManifestListOptions{}) {
		if err != nil {
			return nil, err
		}
		ids[lm.Ref.ID] = struct{}{}
	}
	return ids, nil
}

// partitionsOf returns the distinct partitions of a snapshot's objects, read
// from their paths under the default names: the segments between
// "partitions/" and "/segments/". An object of an unpartitioned layout has
// none.
func partitionsOf(objects []lode.ObjectRef) []string {
	seen := make(map[string]struct{})
	var partitions []string
	for _, obj := range objects {
		_, rest, ok := strings.Cut(obj.Path, "/partitions/")
		if !ok {
			continue
		}
		partition, _, ok := strings.Cut(rest, "/segments/")
		if !ok {
			continue
		}
		if _, dup := seen[partition]; !dup {
			seen[partition] = struct{}{}
			partitions = append(partitions, partition)
		}
	}
	return partitions
}
