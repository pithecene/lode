package chaos

import (
	"math/rand/v2"

	"github.com/pithecene/lode/lode"
)

// days are the partition values writers spread records over in the Hive
// layout.
var days = []string{"2026-10-18", "2026-10-19", "2026-10-20", "2026-10-21"}

// Commit is one write: what the writer put in it and, once the library
// acknowledged it, the snapshot that holds it. The commit generates its
// records and recognizes them when they are read back, so the two never
// drift apart.
type Commit struct {
	ID     lode.DatasetSnapshotID
	Writer int
	Seq    int
	Rows   int
	Day    string
}

// newCommit draws the size and partition of a writer's next write.
func newCommit(rng *rand.Rand, writer, seq int) Commit {
	return Commit{Writer: writer, Seq: seq, Rows: 1 + rng.IntN(5), Day: days[rng.IntN(len(days))]}
}

// records are the rows the commit writes; each names its writer, sequence
// and row number, so a read can be traced back to its write.
func (c Commit) records() []any {
	rows := make([]any, c.Rows)
	for i := range rows {
		rows[i] = lode.D{"writer": c.Writer, "seq": c.Seq, "row": i, "day": c.Day}
	}
	return rows
}

// matches reports whether rows are exactly the commit's records, in any
// order (a Hive layout reads partitions in path order). Rows decoded from
// JSON Lines carry numbers as float64.
func (c Commit) matches(rows []any) bool {
	if len(rows) != c.Rows {
		return false
	}
	seen := make([]bool, c.Rows)
	for _, r := range rows {
		rec, ok := r.(map[string]any)
		if !ok || rec["writer"] != float64(c.Writer) || rec["seq"] != float64(c.Seq) || rec["day"] != c.Day {
			return false
		}
		i, ok := rec["row"].(float64)
		if !ok || i < 0 || int(i) >= c.Rows || seen[int(i)] {
			return false
		}
		seen[int(i)] = true
	}
	return true
}
