package chaos

import (
	"bytes"
	"fmt"
	"math/rand/v2"

	"github.com/pithecene/lode/lode"
)

// days are the partition values writers spread records over in Hive layouts.
var days = []string{"2026-10-18", "2026-10-19", "2026-10-20", "2026-10-21"}

// commitSchema is the explicit Parquet schema for Parquet chaos layouts.
var commitSchema = lode.ParquetSchema{
	Fields: []lode.ParquetField{
		{Name: "writer", Type: lode.ParquetInt64},
		{Name: "seq", Type: lode.ParquetInt64},
		{Name: "row", Type: lode.ParquetInt64},
		{Name: "day", Type: lode.ParquetString},
	},
}

// Commit is one write: what the writer put in it and, once the library
// acknowledged it, the snapshot that holds it. The commit generates its
// records or blob bytes and recognizes them when they are read back, so the
// two never drift apart.
type Commit struct {
	ID     lode.SnapshotID
	Writer int
	Seq    int
	Rows   int
	Days   []string
	Blob   bool
}

// newCommit draws the size and partition assignment of a writer's next write.
// Multi-row record commits alternate between single-partition and
// multi-partition batches so Hive layouts exercise both atomic multi-partition
// commits and single-partition expiry.
func newCommit(rng *rand.Rand, writer, seq int, blob bool) Commit {
	if blob {
		return Commit{Writer: writer, Seq: seq, Rows: 1, Blob: true}
	}
	rows := 1 + rng.IntN(5)
	start := rng.IntN(len(days))
	multi := rows > 1 && rng.IntN(2) == 0
	commitDays := make([]string, rows)
	for i := range commitDays {
		idx := start
		if multi {
			idx = (start + i) % len(days)
		}
		commitDays[i] = days[idx]
	}
	return Commit{Writer: writer, Seq: seq, Rows: rows, Days: commitDays}
}

// records are the rows the commit writes; each names its writer, sequence,
// row number, and day so a read can be traced back to its write regardless of
// partition read order.
func (c Commit) records() []lode.D {
	rows := make([]lode.D, c.Rows)
	for i := range rows {
		rows[i] = lode.D{
			"writer": int64(c.Writer),
			"seq":    int64(c.Seq),
			"row":    int64(i),
			"day":    c.Days[i],
		}
	}
	return rows
}

// blobBytes returns the deterministic payload of a blob commit.
func (c Commit) blobBytes() []byte {
	return fmt.Appendf(nil, "chaos-blob:writer=%d:seq=%d", c.Writer, c.Seq)
}

// matchesBlob reports whether data equals the commit's blob payload.
func (c Commit) matchesBlob(data []byte) bool {
	return bytes.Equal(data, c.blobBytes())
}

// matches reports whether rows are exactly the commit's records, in any
// order (a Hive layout reads partitions in path order).
func (c Commit) matches(rows []lode.D) bool {
	if len(rows) != c.Rows {
		return false
	}
	seen := make([]bool, c.Rows)
	for _, rec := range rows {
		i, ok := c.matchRow(rec)
		if !ok || seen[i] {
			return false
		}
		seen[i] = true
	}
	return true
}

func (c Commit) matchRow(rec lode.D) (int, bool) {
	writer, ok := asInt(rec["writer"])
	if !ok || writer != c.Writer {
		return 0, false
	}
	seq, ok := asInt(rec["seq"])
	if !ok || seq != c.Seq {
		return 0, false
	}
	i, ok := asInt(rec["row"])
	if !ok || i < 0 || i >= len(c.Days) || rec["day"] != c.Days[i] {
		return 0, false
	}
	return i, true
}

// asInt converts a decoded integer field from JSON Lines (float64) or Parquet
// (int64) back to int.
func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case float64:
		i := int(n)
		return i, float64(i) == n
	case int:
		return n, true
	default:
		return 0, false
	}
}
