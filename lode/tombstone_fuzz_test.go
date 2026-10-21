package lode

import (
	"testing"
)

// FuzzValidTombstone checks that tombstone validation never panics on
// arbitrary bytes, and that whatever it accepts records the requested
// snapshot, a commit instant and a valid inventory: retention acts only on
// what this function accepts.
func FuzzValidTombstone(f *testing.F) {
	f.Add([]byte(`{"snapshot_id":"1-0123456789abcdef","requested_at":"2026-10-20T00:00:00Z","manifest_created_at":"2026-10-19T00:00:00Z","files":[]}`), "1-0123456789abcdef")
	f.Add([]byte(`{"snapshot_id":"1","files":[{"path":"","size_bytes":-1}]}`), "1")
	f.Add([]byte(``), "1")
	f.Add([]byte(`null`), "1")
	f.Fuzz(func(t *testing.T, data []byte, id string) {
		tomb, err := validTombstone("expired/x", data, DatasetSnapshotID(id))
		if err != nil {
			return
		}
		if tomb.SnapshotID != DatasetSnapshotID(id) || tomb.ManifestCreatedAt.IsZero() || tomb.Files == nil {
			t.Fatalf("accepted an incomplete tombstone %+v for %q", tomb, id)
		}
		for _, f := range tomb.Files {
			if f.Path == "" || f.SizeBytes < 0 {
				t.Fatalf("accepted an invalid inventory entry %+v", f)
			}
		}
	})
}
