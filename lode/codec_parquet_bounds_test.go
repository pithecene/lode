package lode

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

// Exact-boundary tests for the Parquet codec. The existing overflow tests use
// values far outside the range (3e9, 1e18), so the comparison operators at
// the int32 and 2^53 boundaries, and the `int` input branch, were unobserved.
//
// Known equivalent mutants left alive: `len(records) > 0` → `>=` and
// `n > 0` → `>=` guard loops that are no-ops at zero; `<` → `<=` in lessThan
// only differs for equal values, where min and max are unchanged either way.

func encodeSingle(t *testing.T, fieldType ParquetType, val any) error {
	t.Helper()
	codec, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{{Name: "val", Type: fieldType}}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	return codec.Encode(&buf, []any{map[string]any{"val": val}})
}

func assertBoundaryCases(t *testing.T, fieldType ParquetType, cases []struct {
	name    string
	val     any
	wantErr bool
},
) {
	t.Helper()
	for _, c := range cases {
		err := encodeSingle(t, fieldType, c.val)
		switch {
		case c.wantErr && !errors.Is(err, ErrSchemaViolation):
			t.Errorf("%s: got %v, want ErrSchemaViolation", c.name, err)
		case !c.wantErr && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		}
	}
}

func TestParquetCodec_Int32_ExactBounds(t *testing.T) {
	// Runtime values rather than constants so the +1/-1 cases compile on
	// every int width.
	hi, lo := int64(math.MaxInt32), int64(math.MinInt32)
	assertBoundaryCases(t, ParquetInt32, []struct {
		name    string
		val     any
		wantErr bool
	}{
		{"int at max", int(hi), false},
		{"int above max", int(hi + 1), true},
		{"int at min", int(lo), false},
		{"int below min", int(lo - 1), true},
		{"int64 at max", hi, false},
		{"int64 above max", hi + 1, true},
		{"int64 at min", lo, false},
		{"int64 below min", lo - 1, true},
		{"float64 at max", float64(hi), false},
		{"float64 above max", float64(hi) + 1, true},
		{"float64 at min", float64(lo), false},
		{"float64 below min", float64(lo) - 1, true},
		{"int32 passthrough", int32(math.MaxInt32), false},
	})
}

func TestParquetCodec_Int64_ExactSafeFloatBounds(t *testing.T) {
	safe := float64(1 << 53)
	assertBoundaryCases(t, ParquetInt64, []struct {
		name    string
		val     any
		wantErr bool
	}{
		{"float64 at +2^53", safe, false},
		{"float64 at -2^53", -safe, false},
		{"float64 above +2^53", safe * 2, true},
		{"float64 below -2^53", -safe * 2, true},
		{"float64 non-integer", 1.5, true},
		{"int passthrough", int(42), false},
	})
}

func TestParquetCodec_NewParquetCodec_TypeAtSentinel_Rejected(t *testing.T) {
	_, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{{Name: "v", Type: parquetTypeMax}}})
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("Type == parquetTypeMax: got %v, want ErrSchemaViolation", err)
	}
	if _, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{{Name: "v", Type: parquetTypeMax - 1}}}); err != nil {
		t.Fatalf("Type == parquetTypeMax-1 (last valid): unexpected error %v", err)
	}
}

// Int32 and Float32 columns never appeared in a stats test, so their
// lessThan branches were never executed.
func TestParquetCodec_FileStats_Int32AndFloat32(t *testing.T) {
	codec, err := NewParquetCodec(ParquetSchema{Fields: []ParquetField{
		{Name: "n", Type: ParquetInt32},
		{Name: "f", Type: ParquetFloat32},
	}})
	if err != nil {
		t.Fatal(err)
	}
	records := []any{
		map[string]any{"n": int32(5), "f": float32(2.5)},
		map[string]any{"n": int32(-3), "f": float32(-1.5)},
		map[string]any{"n": int32(9), "f": float32(7.25)},
	}
	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatal(err)
	}
	stats := codec.(StatisticalCodec).FileStats()
	if stats == nil || len(stats.Columns) != 2 {
		t.Fatalf("FileStats() = %+v, want 2 columns", stats)
	}
	n, f := stats.Columns[0], stats.Columns[1]
	if n.Min != int32(-3) || n.Max != int32(9) {
		t.Errorf("int32 stats: min=%v max=%v, want -3/9", n.Min, n.Max)
	}
	if f.Min != float32(-1.5) || f.Max != float32(7.25) {
		t.Errorf("float32 stats: min=%v max=%v, want -1.5/7.25", f.Min, f.Max)
	}
}
