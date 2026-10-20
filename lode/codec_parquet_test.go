package lode

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestParquetCodec_Construction(t *testing.T) {
	t.Run("name", func(t *testing.T) {
		codec, err := NewParquetCodec(ParquetSchema{})
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}
		if got := codec.Name(); got != "parquet" {
			t.Errorf("Name() = %q, want %q", got, "parquet")
		}
	})

	t.Run("with options", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
			},
		}

		// Test with different compression options
		compressions := []ParquetCompression{
			ParquetCompressionNone,
			ParquetCompressionSnappy,
			ParquetCompressionGzip,
		}

		for _, comp := range compressions {
			codec, err := NewParquetCodec(schema,
				WithParquetCompression(comp),
			)
			if err != nil {
				t.Fatalf("NewParquetCodec() error = %v", err)
			}

			records := []any{
				map[string]any{"id": int64(1)},
				map[string]any{"id": int64(2)},
			}

			var buf bytes.Buffer
			if err := codec.Encode(&buf, records); err != nil {
				t.Errorf("Encode() with compression %d error = %v", comp, err)
				continue
			}

			decoded, err := codec.Decode(&buf)
			if err != nil {
				t.Errorf("Decode() with compression %d error = %v", comp, err)
				continue
			}

			if len(decoded) != 2 {
				t.Errorf("Decode() with compression %d got %d records, want 2", comp, len(decoded))
			}
		}
	})

	t.Run("new parquet codec invalid field type", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetType(999)}, // invalid type
			},
		}
		_, err := NewParquetCodec(schema)
		if err == nil {
			t.Fatal("NewParquetCodec() expected error for invalid field type")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("new parquet codec empty field name", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "", Type: ParquetInt64}, // empty name
			},
		}
		_, err := NewParquetCodec(schema)
		if err == nil {
			t.Fatal("NewParquetCodec() expected error for empty field name")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("new parquet codec negative field type", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetType(-1)}, // negative type
			},
		}
		_, err := NewParquetCodec(schema)
		if err == nil {
			t.Fatal("NewParquetCodec() expected error for negative field type")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("new parquet codec duplicate field name", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "id", Type: ParquetString}, // duplicate name
			},
		}
		_, err := NewParquetCodec(schema)
		if err == nil {
			t.Fatal("NewParquetCodec() expected error for duplicate field name")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("NewParquetCodec() error = %v, want ErrSchemaViolation", err)
		}
	})
}

func TestParquetCodec_RoundTrip(t *testing.T) {
	t.Run("basic types", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString},
				{Name: "score", Type: ParquetFloat64},
				{Name: "active", Type: ParquetBool},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"id": int64(1), "name": "alice", "score": 95.5, "active": true},
			map[string]any{"id": int64(2), "name": "bob", "score": 87.3, "active": false},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != len(records) {
			t.Fatalf("Decode() got %d records, want %d", len(decoded), len(records))
		}

		// Verify first record
		got := decoded[0].(map[string]any)
		if got["id"] != int64(1) {
			t.Errorf("record[0].id = %v, want 1", got["id"])
		}
		if got["name"] != "alice" {
			t.Errorf("record[0].name = %v, want alice", got["name"])
		}
		if got["active"] != true {
			t.Errorf("record[0].active = %v, want true", got["active"])
		}
	})

	t.Run("all types", func(t *testing.T) {
		now := time.Now().Truncate(time.Nanosecond).UTC()
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "int32_field", Type: ParquetInt32},
				{Name: "int64_field", Type: ParquetInt64},
				{Name: "float32_field", Type: ParquetFloat32},
				{Name: "float64_field", Type: ParquetFloat64},
				{Name: "string_field", Type: ParquetString},
				{Name: "bool_field", Type: ParquetBool},
				{Name: "bytes_field", Type: ParquetBytes},
				{Name: "timestamp_field", Type: ParquetTimestamp},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{
				"int32_field":     int32(42),
				"int64_field":     int64(9999999999),
				"float32_field":   float32(3.14),
				"float64_field":   float64(2.71828),
				"string_field":    "hello",
				"bool_field":      true,
				"bytes_field":     []byte("binary data"),
				"timestamp_field": now,
			},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != 1 {
			t.Fatalf("Decode() got %d records, want 1", len(decoded))
		}

		got := decoded[0].(map[string]any)
		if got["int32_field"] != int32(42) {
			t.Errorf("int32_field = %v (%T), want 42", got["int32_field"], got["int32_field"])
		}
		if got["int64_field"] != int64(9999999999) {
			t.Errorf("int64_field = %v, want 9999999999", got["int64_field"])
		}
		if got["string_field"] != "hello" {
			t.Errorf("string_field = %v, want hello", got["string_field"])
		}
		if got["bool_field"] != true {
			t.Errorf("bool_field = %v, want true", got["bool_field"])
		}
	})

	t.Run("empty records", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, []any{}); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		// Should produce a valid parquet file
		if buf.Len() == 0 {
			t.Error("Encode() produced empty output for empty records")
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != 0 {
			t.Errorf("Decode() got %d records, want 0", len(decoded))
		}
	})
}

func TestParquetCodec_Nullable(t *testing.T) {
	t.Run("nullable fields", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString, Nullable: true},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"id": int64(1), "name": "alice"},
			map[string]any{"id": int64(2), "name": nil},
			map[string]any{"id": int64(3)}, // missing field treated as nil
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != 3 {
			t.Fatalf("Decode() got %d records, want 3", len(decoded))
		}
	})

	t.Run("nullable fields round trip verify values", func(t *testing.T) {
		// Verify that explicitly nil vs missing field both decode as nil
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "optional", Type: ParquetString, Nullable: true},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"id": int64(1), "optional": "present"},
			map[string]any{"id": int64(2), "optional": nil}, // explicit nil
			map[string]any{"id": int64(3)},                  // missing key
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != 3 {
			t.Fatalf("Decode() got %d records, want 3", len(decoded))
		}

		// Record 0: present value
		r0 := decoded[0].(map[string]any)
		if r0["optional"] != "present" {
			t.Errorf("record[0].optional = %v, want %q", r0["optional"], "present")
		}

		// Record 1: explicit nil should decode as nil
		r1 := decoded[1].(map[string]any)
		if r1["optional"] != nil {
			t.Errorf("record[1].optional = %v, want nil", r1["optional"])
		}

		// Record 2: missing key should decode as nil
		r2 := decoded[2].(map[string]any)
		if r2["optional"] != nil {
			t.Errorf("record[2].optional = %v, want nil", r2["optional"])
		}
	})

	t.Run("all types nullable round trip", func(t *testing.T) {
		// Verify all types work with nullable fields and nil values
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "int32_field", Type: ParquetInt32, Nullable: true},
				{Name: "int64_field", Type: ParquetInt64, Nullable: true},
				{Name: "float32_field", Type: ParquetFloat32, Nullable: true},
				{Name: "float64_field", Type: ParquetFloat64, Nullable: true},
				{Name: "string_field", Type: ParquetString, Nullable: true},
				{Name: "bool_field", Type: ParquetBool, Nullable: true},
				{Name: "bytes_field", Type: ParquetBytes, Nullable: true},
				{Name: "timestamp_field", Type: ParquetTimestamp, Nullable: true},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Record with all nil values
		records := []any{
			map[string]any{
				"int32_field":     nil,
				"int64_field":     nil,
				"float32_field":   nil,
				"float64_field":   nil,
				"string_field":    nil,
				"bool_field":      nil,
				"bytes_field":     nil,
				"timestamp_field": nil,
			},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != 1 {
			t.Fatalf("Decode() got %d records, want 1", len(decoded))
		}

		r := decoded[0].(map[string]any)
		for _, field := range schema.Fields {
			if r[field.Name] != nil {
				t.Errorf("%s = %v, want nil", field.Name, r[field.Name])
			}
		}
	})
}

func TestParquetCodec_EncodeErrors(t *testing.T) {
	t.Run("missing required field", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString}, // required
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"id": int64(1)}, // missing required "name"
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for missing required field")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"id": "not an int"}, // wrong type
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for type mismatch")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("not map record", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			"not a map", // invalid record type
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for non-map record")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("int32 overflow", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "val", Type: ParquetInt32},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Value exceeds int32 max
		records := []any{
			map[string]any{"val": float64(3000000000)}, // > 2147483647
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for int32 overflow")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("int32 underflow", func(t *testing.T) {
		// Test int32 underflow (negative bound)
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "val", Type: ParquetInt32},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"val": float64(-3000000000)}, // < -2147483648
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for int32 underflow")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("int64 safe range", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "val", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Value exceeds safe integer range for float64 (2^53)
		// Use 1e18 which is much larger than 2^53 (≈9e15)
		records := []any{
			map[string]any{"val": float64(1e18)}, // 1e18 >> 2^53
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for int64 safe range exceeded")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("float64 not integer", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "val", Type: ParquetInt32},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Float64 with fractional part
		records := []any{
			map[string]any{"val": float64(42.5)}, // not an integer
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for non-integer float64")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("invalid timestamp string", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "ts", Type: ParquetTimestamp},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{"ts": "not-a-valid-timestamp"},
		}

		var buf bytes.Buffer
		err = codec.Encode(&buf, records)
		if err == nil {
			t.Fatal("Encode() expected error for invalid timestamp string")
		}
		if !errors.Is(err, ErrSchemaViolation) {
			t.Errorf("Encode() error = %v, want ErrSchemaViolation", err)
		}
	})
}

func TestParquetCodec_Conversion(t *testing.T) {
	t.Run("extra fields ignored", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		records := []any{
			map[string]any{
				"id":          int64(1),
				"extra_field": "ignored", // not in schema
			},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		got := decoded[0].(map[string]any)
		if _, exists := got["extra_field"]; exists {
			t.Error("extra_field should not be in decoded record")
		}
		if got["id"] != int64(1) {
			t.Errorf("id = %v, want 1", got["id"])
		}
	})

	t.Run("JSON number conversion", func(t *testing.T) {
		// When records come from JSON, numbers are float64
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "int32_field", Type: ParquetInt32},
				{Name: "int64_field", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Simulate JSON-decoded numbers (float64)
		records := []any{
			map[string]any{
				"int32_field": float64(42),
				"int64_field": float64(9999999999),
			},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		got := decoded[0].(map[string]any)
		if got["int32_field"] != int32(42) {
			t.Errorf("int32_field = %v (%T), want int32(42)", got["int32_field"], got["int32_field"])
		}
		if got["int64_field"] != int64(9999999999) {
			t.Errorf("int64_field = %v (%T), want int64(9999999999)", got["int64_field"], got["int64_field"])
		}
	})

	t.Run("timestamp string parsing", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "ts", Type: ParquetTimestamp},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Timestamp as RFC3339 string
		records := []any{
			map[string]any{
				"ts": "2024-01-15T10:30:00Z",
			},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		got := decoded[0].(map[string]any)
		ts, ok := got["ts"].(time.Time)
		if !ok {
			t.Fatalf("ts = %T, want time.Time", got["ts"])
		}
		if ts.Year() != 2024 || ts.Month() != 1 || ts.Day() != 15 {
			t.Errorf("ts = %v, want 2024-01-15", ts)
		}
	})
}

func TestParquetCodec_Dataset(t *testing.T) {
	t.Run("large dataset", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "value", Type: ParquetString},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		// Generate 10k records
		records := make([]any, 10000)
		for i := range records {
			records[i] = map[string]any{
				"id":    int64(i),
				"value": "test value with some content to make it larger",
			}
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}

		decoded, err := codec.Decode(&buf)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if len(decoded) != 10000 {
			t.Errorf("Decode() got %d records, want 10000", len(decoded))
		}

		// Spot check
		first := decoded[0].(map[string]any)
		if first["id"] != int64(0) {
			t.Errorf("first record id = %v, want 0", first["id"])
		}
		last := decoded[9999].(map[string]any)
		if last["id"] != int64(9999) {
			t.Errorf("last record id = %v, want 9999", last["id"])
		}
	})

	t.Run("dataset write success", func(t *testing.T) {
		// Per CONTRACT_PARQUET.md: Callers MUST use Dataset.Write for Parquet encoding.

		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatalf("NewParquetCodec() error = %v", err)
		}

		ds, err := NewDataset("test-parquet-write", NewMemoryFactory(), WithCodec(codec))
		if err != nil {
			t.Fatalf("NewDataset failed: %v", err)
		}

		records := []any{
			map[string]any{"id": int64(1), "name": "alice"},
			map[string]any{"id": int64(2), "name": "bob"},
		}

		snapshot, err := ds.Write(t.Context(), records, Metadata{"source": "test"})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// Verify manifest records codec
		if snapshot.Manifest.Codec != "parquet" {
			t.Errorf("Manifest.Codec = %q, want %q", snapshot.Manifest.Codec, "parquet")
		}

		// Verify row count
		if snapshot.Manifest.RowCount != 2 {
			t.Errorf("Manifest.RowCount = %d, want 2", snapshot.Manifest.RowCount)
		}

		// Read back and verify
		readRecords, err := ds.Read(t.Context(), snapshot.ID)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if len(readRecords) != 2 {
			t.Errorf("Read returned %d records, want 2", len(readRecords))
		}
	})
}

// -----------------------------------------------------------------------------
// Schema Validation Tests
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Numeric Overflow Protection Tests
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Dataset-Level Tests
// -----------------------------------------------------------------------------

func TestParquetCodec_StreamWriteRecords_ReturnsErrCodecNotStreamable(t *testing.T) {
	// Per CONTRACT_PARQUET.md: Parquet codec does NOT implement StreamingRecordCodec.
	// StreamWriteRecords with Parquet codec MUST return ErrCodecNotStreamable.

	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	ds, err := NewDataset("test-parquet", NewMemoryFactory(), WithCodec(codec))
	if err != nil {
		t.Fatalf("NewDataset failed: %v", err)
	}

	// Create a simple iterator
	iter := &parquetTestIterator{
		records: []any{
			map[string]any{"id": int64(1)},
		},
	}

	_, err = ds.StreamWriteRecords(t.Context(), iter, Metadata{})
	if err == nil {
		t.Fatal("StreamWriteRecords should return error for Parquet codec")
	}
	if !errors.Is(err, ErrCodecNotStreamable) {
		t.Errorf("StreamWriteRecords error = %v, want ErrCodecNotStreamable", err)
	}
}

// -----------------------------------------------------------------------------
// Hardening Tests
// -----------------------------------------------------------------------------

func TestParquetCodec_Decode_InvalidFormat(t *testing.T) {
	codec, err := NewParquetCodec(ParquetSchema{
		Fields: []ParquetField{{Name: "id", Type: ParquetInt64}},
	})
	if err != nil {
		t.Fatalf("NewParquetCodec() error = %v", err)
	}

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"garbage", []byte("not parquet data at all")},
		{"truncated_magic", []byte("PAR")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := codec.Decode(bytes.NewReader(tc.data))
			if err == nil {
				t.Fatal("Decode() expected error")
			}
			if !errors.Is(err, ErrInvalidFormat) {
				t.Errorf("errors.Is(err, ErrInvalidFormat) = false, want true; err = %v", err)
			}
		})
	}
}

// parquetTestIterator is a simple RecordIterator for Parquet codec tests.
type parquetTestIterator struct {
	records []any
	index   int
	err     error
}

func (s *parquetTestIterator) Next() bool {
	if s.err != nil || s.index >= len(s.records) {
		return false
	}
	s.index++
	return true
}

func (s *parquetTestIterator) Record() any {
	if s.index == 0 || s.index > len(s.records) {
		return nil
	}
	return s.records[s.index-1]
}

func (s *parquetTestIterator) Err() error {
	return s.err
}

// -----------------------------------------------------------------------------
// FileStats tests
// -----------------------------------------------------------------------------

func TestParquetCodec_FileStats(t *testing.T) {
	t.Run("basic types", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "name", Type: ParquetString},
				{Name: "score", Type: ParquetFloat64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatal(err)
		}

		records := []any{
			map[string]any{"id": int64(10), "name": "alice", "score": 95.5},
			map[string]any{"id": int64(1), "name": "bob", "score": 87.3},
			map[string]any{"id": int64(5), "name": "carol", "score": 99.0},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatal(err)
		}

		sc, ok := codec.(StatisticalCodec)
		if !ok {
			t.Fatal("parquet codec does not implement StatisticalCodec")
		}

		stats := sc.FileStats()
		if stats == nil {
			t.Fatal("FileStats() returned nil")
		}
		if stats.RowCount != 3 {
			t.Errorf("RowCount = %d, want 3", stats.RowCount)
		}
		if len(stats.Columns) != 3 {
			t.Fatalf("len(Columns) = %d, want 3", len(stats.Columns))
		}

		// id: min=1, max=10, nulls=0
		idCol := stats.Columns[0]
		if idCol.Name != "id" {
			t.Errorf("Columns[0].Name = %q, want %q", idCol.Name, "id")
		}
		if idCol.Min != int64(1) {
			t.Errorf("id.Min = %v, want 1", idCol.Min)
		}
		if idCol.Max != int64(10) {
			t.Errorf("id.Max = %v, want 10", idCol.Max)
		}
		if idCol.NullCount != 0 {
			t.Errorf("id.NullCount = %d, want 0", idCol.NullCount)
		}

		// name: min="alice", max="carol"
		nameCol := stats.Columns[1]
		if nameCol.Min != "alice" {
			t.Errorf("name.Min = %v, want %q", nameCol.Min, "alice")
		}
		if nameCol.Max != "carol" {
			t.Errorf("name.Max = %v, want %q", nameCol.Max, "carol")
		}

		// score: min=87.3, max=99.0
		scoreCol := stats.Columns[2]
		if scoreCol.Min != 87.3 {
			t.Errorf("score.Min = %v, want 87.3", scoreCol.Min)
		}
		if scoreCol.Max != 99.0 {
			t.Errorf("score.Max = %v, want 99.0", scoreCol.Max)
		}
	})

	t.Run("single record", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "x", Type: ParquetFloat64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatal(err)
		}

		records := []any{
			map[string]any{"x": 42.0},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatal(err)
		}

		stats := codec.(StatisticalCodec).FileStats()
		col := stats.Columns[0]
		if col.Min != 42.0 {
			t.Errorf("Min = %v, want 42.0", col.Min)
		}
		if col.Max != 42.0 {
			t.Errorf("Max = %v, want 42.0", col.Max)
		}
		if stats.RowCount != 1 {
			t.Errorf("RowCount = %d, want 1", stats.RowCount)
		}
	})

	t.Run("empty records", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, []any{}); err != nil {
			t.Fatal(err)
		}

		stats := codec.(StatisticalCodec).FileStats()
		if stats == nil {
			t.Fatal("FileStats() returned nil for empty records")
		}
		if stats.RowCount != 0 {
			t.Errorf("RowCount = %d, want 0", stats.RowCount)
		}
		if len(stats.Columns) != 1 {
			t.Fatalf("len(Columns) = %d, want 1", len(stats.Columns))
		}
		if stats.Columns[0].Min != nil {
			t.Errorf("Min = %v, want nil", stats.Columns[0].Min)
		}
	})
}

func TestParquetCodec_FileStats_Nulls(t *testing.T) {
	t.Run("nullable fields", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "id", Type: ParquetInt64},
				{Name: "tag", Type: ParquetString, Nullable: true},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatal(err)
		}

		records := []any{
			map[string]any{"id": int64(1), "tag": "alpha"},
			map[string]any{"id": int64(2), "tag": nil},
			map[string]any{"id": int64(3), "tag": "gamma"},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatal(err)
		}

		stats := codec.(StatisticalCodec).FileStats()
		tagCol := stats.Columns[1]
		if tagCol.NullCount != 1 {
			t.Errorf("tag.NullCount = %d, want 1", tagCol.NullCount)
		}
		if tagCol.Min != "alpha" {
			t.Errorf("tag.Min = %v, want %q", tagCol.Min, "alpha")
		}
		if tagCol.Max != "gamma" {
			t.Errorf("tag.Max = %v, want %q", tagCol.Max, "gamma")
		}
	})

	t.Run("all nulls", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "val", Type: ParquetInt64, Nullable: true},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatal(err)
		}

		records := []any{
			map[string]any{"val": nil},
			map[string]any{"val": nil},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatal(err)
		}

		stats := codec.(StatisticalCodec).FileStats()
		col := stats.Columns[0]
		if col.NullCount != 2 {
			t.Errorf("NullCount = %d, want 2", col.NullCount)
		}
		if col.Min != nil {
			t.Errorf("Min = %v, want nil", col.Min)
		}
		if col.Max != nil {
			t.Errorf("Max = %v, want nil", col.Max)
		}
	})

	t.Run("bool and bytes no min max", func(t *testing.T) {
		schema := ParquetSchema{
			Fields: []ParquetField{
				{Name: "flag", Type: ParquetBool},
				{Name: "data", Type: ParquetBytes},
			},
		}
		codec, err := NewParquetCodec(schema)
		if err != nil {
			t.Fatal(err)
		}

		records := []any{
			map[string]any{"flag": true, "data": []byte("hello")},
			map[string]any{"flag": false, "data": []byte("world")},
		}

		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			t.Fatal(err)
		}

		stats := codec.(StatisticalCodec).FileStats()

		flagCol := stats.Columns[0]
		if flagCol.Min != nil {
			t.Errorf("flag.Min = %v, want nil", flagCol.Min)
		}
		if flagCol.Max != nil {
			t.Errorf("flag.Max = %v, want nil", flagCol.Max)
		}
		if flagCol.NullCount != 0 {
			t.Errorf("flag.NullCount = %d, want 0", flagCol.NullCount)
		}

		dataCol := stats.Columns[1]
		if dataCol.Min != nil {
			t.Errorf("data.Min = %v, want nil", dataCol.Min)
		}
		if dataCol.Max != nil {
			t.Errorf("data.Max = %v, want nil", dataCol.Max)
		}
	})
}

func TestParquetCodec_FileStats_Timestamps(t *testing.T) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "ts", Type: ParquetTimestamp},
		},
	}
	codec, err := NewParquetCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 3, 10, 8, 0, 0, 0, time.UTC)

	records := []any{
		map[string]any{"ts": t2},
		map[string]any{"ts": t1},
		map[string]any{"ts": t3},
	}

	var buf bytes.Buffer
	if err := codec.Encode(&buf, records); err != nil {
		t.Fatal(err)
	}

	stats := codec.(StatisticalCodec).FileStats()
	col := stats.Columns[0]

	minTs, ok := col.Min.(time.Time)
	if !ok {
		t.Fatalf("ts.Min is %T, want time.Time", col.Min)
	}
	if !minTs.Equal(t1) {
		t.Errorf("ts.Min = %v, want %v", minTs, t1)
	}

	maxTs, ok := col.Max.(time.Time)
	if !ok {
		t.Fatalf("ts.Max is %T, want time.Time", col.Max)
	}
	if !maxTs.Equal(t2) {
		t.Errorf("ts.Max = %v, want %v", maxTs, t2)
	}
}
