package lode

import (
	"testing"
	"time"
)

func TestSnapshotInstant_OnlyGeneratedForm(t *testing.T) {
	for id, want := range map[DatasetSnapshotID]bool{
		"1792459022524750970":                   true,
		"0":                                     true,
		"1792459022524750970-0123456789abcdef":  true,
		"0-0000000000000000":                    true,
		"1792459022524750970-0123456789ABCDEF":  false,
		"1792459022524750970-0123456789abcde":   false,
		"1792459022524750970-0123456789abcdef0": false,
		"1792459022524750970-0123456789abcdeg":  false,
		"1792459022524750970-":                  false,
		"-0123456789abcdef":                     false,
		"00123-0123456789abcdef":                false,
		"00123":                                 false,
		"+123":                                  false,
		"-1":                                    false,
		"abc":                                   false,
		"":                                      false,
	} {
		if _, ok := snapshotInstant(id); ok != want {
			t.Errorf("snapshotInstant(%q) ok = %v, want %v", id, ok, want)
		}
	}
}

func TestFormatSnapshotID_RoundTripsInstant(t *testing.T) {
	at := time.Unix(0, 1792459022524750970)
	id := DatasetSnapshotID(formatSnapshotID(at, [nonceBytes]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}))
	if id != "1792459022524750970-0123456789abcdef" {
		t.Fatalf("formatSnapshotID = %q", id)
	}
	if got, ok := snapshotInstant(id); !ok || !got.Equal(at) {
		t.Fatalf("snapshotInstant(%q) = %v, %v; want %v, true", id, got, ok, at)
	}
}

// A repeated clock must not repeat an ID (CONTRACT_CORE §Snapshot ID).
func TestGenerateID_UniqueWithinOneInstant(t *testing.T) {
	const n = 1000
	seen := make(map[string]bool, n)
	for range n {
		id := generateID()
		if seen[id] {
			t.Fatalf("generateID repeated %q", id)
		}
		seen[id] = true
		if _, ok := snapshotInstant(DatasetSnapshotID(id)); !ok {
			t.Fatalf("generateID returned %q, not in the generated form", id)
		}
	}
}

// String order is instant order, and a legacy ID sorts before a new ID with
// the same instant.
func TestSnapshotID_StringOrderFollowsInstant(t *testing.T) {
	var zero, ones [nonceBytes]byte
	for i := range ones {
		ones[i] = 0xff
	}
	ordered := []string{
		"1792459022524750969",
		formatSnapshotID(time.Unix(0, 1792459022524750969), ones),
		"1792459022524750970",
		formatSnapshotID(time.Unix(0, 1792459022524750970), zero),
		formatSnapshotID(time.Unix(0, 1792459022524750971), zero),
	}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1] >= ordered[i] {
			t.Errorf("%q does not sort before %q", ordered[i-1], ordered[i])
		}
	}
}
