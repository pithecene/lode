package lode

import (
	"testing"
	"time"
)

func TestToInt64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
		want int64
	}{
		{"int", int(42), 42},
		{"int32", int32(42), 42},
		{"int64", int64(42), 42},
		{"float64", float64(42.7), 42},
		{"float64 negative", float64(-3.9), -3},
		{"string fallback", "not a number", 0},
		{"nil fallback", nil, 0},
		{"bool fallback", true, 0},
		{"int32 max", int32(2147483647), 2147483647},
		{"int32 min", int32(-2147483648), -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toInt64(tt.val)
			if got != tt.want {
				t.Errorf("toInt64(%v) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
		want float64
	}{
		{"float32", float32(3.14), float64(float32(3.14))},
		{"float64", float64(3.14), 3.14},
		{"float64 negative", float64(-1.5), -1.5},
		{"string fallback", "not a number", 0},
		{"nil fallback", nil, 0},
		{"int fallback", 42, 0},
		{"float32 zero", float32(0), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toFloat64(tt.val)
			if got != tt.want {
				t.Errorf("toFloat64(%v) = %f, want %f", tt.val, got, tt.want)
			}
		})
	}
}

func TestToTimestamp(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		val  any
		want time.Time
	}{
		{"time.Time", fixed, fixed},
		{"RFC3339Nano string", "2024-06-15T12:00:00Z", fixed},
		{"RFC3339Nano with nanos", "2024-06-15T12:00:00.123456789Z", time.Date(2024, 6, 15, 12, 0, 0, 123456789, time.UTC)},
		{"invalid string", "not-a-timestamp", time.Time{}},
		{"empty string", "", time.Time{}},
		{"nil fallback", nil, time.Time{}},
		{"int fallback", 42, time.Time{}},
		{"float fallback", 3.14, time.Time{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toTimestamp(tt.val)
			if !got.Equal(tt.want) {
				t.Errorf("toTimestamp(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}
