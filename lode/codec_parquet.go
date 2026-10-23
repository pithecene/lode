package lode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"time"

	"github.com/parquet-go/parquet-go"
)

// -----------------------------------------------------------------------------
// Parquet Codec Types
// -----------------------------------------------------------------------------

// ParquetType enumerates supported Parquet logical types.
type ParquetType int

// Parquet type constants for schema field definitions.
const (
	ParquetInt32 ParquetType = iota
	ParquetInt64
	ParquetFloat32
	ParquetFloat64
	ParquetString
	ParquetBool
	ParquetBytes
	ParquetTimestamp
	parquetTypeMax // sentinel for validation
)

// int32 bounds for overflow checks (stdlib has no int32 bounds constants).
const (
	minInt32     = -1 << 31  // -2147483648
	maxInt32     = 1<<31 - 1 // 2147483647
	maxSafeInt64 = 1 << 53   // max integer exactly representable in float64
)

// ParquetField defines a single field in a Parquet schema.
type ParquetField struct {
	Name     string
	Type     ParquetType
	Nullable bool
}

// ParquetSchema defines the record structure for Parquet encoding.
type ParquetSchema struct {
	Fields []ParquetField
}

// ParquetCompression specifies internal Parquet compression.
type ParquetCompression int

// Parquet compression options for internal file compression.
const (
	ParquetCompressionNone ParquetCompression = iota
	ParquetCompressionSnappy
	ParquetCompressionGzip
)

// ParquetOption configures Parquet codec behavior.
type ParquetOption func(*parquetCodec)

// WithParquetCompression sets internal Parquet compression.
func WithParquetCompression(codec ParquetCompression) ParquetOption {
	return func(c *parquetCodec) {
		c.compression = codec
	}
}

// -----------------------------------------------------------------------------
// Parquet Codec Implementation
// -----------------------------------------------------------------------------

// Error sentinels ErrSchemaViolation and ErrInvalidFormat are defined in api.go.

// parquetCodec implements Codec[D] and StatisticalCodec for Apache Parquet.
type parquetCodec struct {
	err          error // an invalid schema, reported by Root.Records
	schema       ParquetSchema
	compression  ParquetCompression
	pqSchema     *parquet.Schema
	fieldOrder   []string                // ordered field names matching schema columns
	fieldsByName map[string]ParquetField // field lookup by name
	lastStats    *FileStats              // stats from most recent Encode call
}

// validateSchema validates a ParquetSchema and builds a field lookup map.
// Returns an error if the schema contains invalid types, empty names, or duplicates.
//
// Contract: On success, the returned map contains an entry for every field in
// schema.Fields. This guarantees that all fieldOrder entries (derived from
// schema.Fields) will be present in fieldsByName.
func validateSchema(schema ParquetSchema) (map[string]ParquetField, error) {
	fieldsByName := make(map[string]ParquetField, len(schema.Fields))
	for _, field := range schema.Fields {
		if field.Type < 0 || field.Type >= parquetTypeMax {
			return nil, fmt.Errorf("parquet: %w: invalid ParquetType %d for field %q", ErrSchemaViolation, field.Type, field.Name)
		}
		if field.Name == "" {
			return nil, fmt.Errorf("parquet: %w: field name cannot be empty", ErrSchemaViolation)
		}
		if _, exists := fieldsByName[field.Name]; exists {
			return nil, fmt.Errorf("parquet: %w: duplicate field name %q", ErrSchemaViolation, field.Name)
		}
		fieldsByName[field.Name] = field
	}
	return fieldsByName, nil
}

// Parquet returns a Parquet codec for map records with the given schema.
//
// The schema defines the structure of records. All records must conform to
// it during encoding; fields not in the schema are ignored. An invalid
// schema is recorded and returned, matching ErrSchemaViolation, by
// Root.Records and ReadRoot.Records, and by Encode and Decode, so that the
// codec can be passed in a chain: root.Records(ctx, id, lode.Parquet(schema)).
//
// The Parquet codec is not a StreamingCodec, because a Parquet file requires
// a footer that references all row groups. Use Records.Write.
func Parquet(schema ParquetSchema, opts ...ParquetOption) Codec[D] {
	c, err := newParquetCodec(schema, opts...)
	if err != nil {
		return &parquetCodec{err: err}
	}
	return c
}

func newParquetCodec(schema ParquetSchema, opts ...ParquetOption) (*parquetCodec, error) {
	// The codec retains the schema; a later change to the caller's slice
	// must not change what it encodes or describes.
	schema.Fields = slices.Clone(schema.Fields)
	fieldsByName, err := validateSchema(schema)
	if err != nil {
		return nil, err
	}

	c := &parquetCodec{
		schema:       schema,
		compression:  ParquetCompressionSnappy,
		fieldsByName: fieldsByName,
	}
	for _, opt := range opts {
		opt(c)
	}
	pqSchema, err := buildParquetSchema(schema)
	if err != nil {
		return nil, err
	}
	c.pqSchema = pqSchema

	// Extract field order from the built schema
	c.fieldOrder = make([]string, len(schema.Fields))
	for i, f := range c.pqSchema.Fields() {
		c.fieldOrder[i] = f.Name()
	}

	return c, nil
}

func (c *parquetCodec) Name() string {
	return "parquet"
}

// configError is the construction error a chain defers to Root.Records.
func (c *parquetCodec) configError() error { return c.err }

// parquetTypeNames spells each ParquetType in a codec description.
var parquetTypeNames = [parquetTypeMax]string{
	ParquetInt32:     "int32",
	ParquetInt64:     "int64",
	ParquetFloat32:   "float32",
	ParquetFloat64:   "float64",
	ParquetString:    "string",
	ParquetBool:      "bool",
	ParquetBytes:     "bytes",
	ParquetTimestamp: "timestamp",
}

// describedField is one schema field in a codec description.
type describedField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// Describe returns the schema as JSON, its fields in schema order. The
// internal compression is omitted: it does not affect which records a file
// holds or how they decode.
func (c *parquetCodec) Describe() string {
	if c.err != nil {
		return ""
	}
	fields := make([]describedField, len(c.schema.Fields))
	for i, f := range c.schema.Fields {
		fields[i] = describedField{Name: f.Name, Type: parquetTypeNames[f.Type], Nullable: f.Nullable}
	}
	// Strings and bools always encode, so Marshal cannot fail here.
	data, _ := json.Marshal(struct { //nolint:errchkjson // a struct of strings and bools always encodes
		Fields []describedField `json:"fields"`
	}{fields})
	return string(data)
}

// FileStats returns statistics accumulated during the most recent Encode call.
func (c *parquetCodec) FileStats() *FileStats {
	return c.lastStats
}

func (c *parquetCodec) Encode(w io.Writer, records []D) error {
	if c.err != nil {
		return c.err
	}
	c.lastStats = nil // reset before encoding
	// Buffer to collect complete parquet file
	var buf bytes.Buffer

	// Create buffer for collecting rows
	rowBuf := parquet.NewBuffer(c.pqSchema)

	if len(records) > 0 {
		// Convert and write records to buffer
		for i, record := range records {
			row, err := c.recordToRow(record, i)
			if err != nil {
				return err
			}
			if _, err := rowBuf.WriteRows([]parquet.Row{row}); err != nil {
				return fmt.Errorf("parquet: write row %d: %w", i, err)
			}
		}
	}

	// Write buffer to parquet file
	pqWriter := parquet.NewWriter(&buf, c.pqSchema, c.getCompressionOption())
	if _, err := pqWriter.WriteRowGroup(rowBuf); err != nil {
		_ = pqWriter.Close()
		return fmt.Errorf("parquet: write row group: %w", err)
	}

	if err := pqWriter.Close(); err != nil {
		return fmt.Errorf("parquet: close writer: %w", err)
	}

	// Compute per-column statistics from the input records
	c.lastStats = computeFileStats(c.schema, records)

	// Write buffered content to output
	_, err := io.Copy(w, &buf)
	return err
}

func (c *parquetCodec) Decode(r io.Reader) ([]D, error) {
	if c.err != nil {
		return nil, c.err
	}
	// Read all content into buffer (parquet needs seeking)
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("parquet: read file: %w", err)
	}

	if len(data) == 0 {
		return nil, ErrInvalidFormat
	}

	// Open parquet file from buffer
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrInvalidFormat
		}
		return nil, fmt.Errorf("parquet: %w: %w", ErrInvalidFormat, err)
	}

	numRows := file.NumRows()
	if numRows == 0 {
		return []D{}, nil
	}

	// Read all rows
	reader := parquet.NewReader(file)
	defer closer(reader)()

	records := make([]D, 0, numRows)
	rows := make([]parquet.Row, 100)
	for {
		n, err := reader.ReadRows(rows)
		if n > 0 {
			for i := 0; i < n; i++ {
				record := c.rowToRecord(rows[i])
				records = append(records, record)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Wrap ReadRows errors with ErrInvalidFormat per contract
			return nil, fmt.Errorf("parquet: %w: read rows: %w", ErrInvalidFormat, err)
		}
	}

	return records, nil
}

func (c *parquetCodec) getCompressionOption() parquet.WriterOption {
	switch c.compression {
	case ParquetCompressionSnappy:
		return parquet.Compression(&parquet.Snappy)
	case ParquetCompressionGzip:
		return parquet.Compression(&parquet.Gzip)
	default:
		return parquet.Compression(&parquet.Uncompressed)
	}
}

// getFieldByName returns the ParquetField for a given field name.
// Schema validation guarantees presence; missing key returns zero value (programmer error).
func (c *parquetCodec) getFieldByName(name string) ParquetField {
	return c.fieldsByName[name]
}

// recordToRow converts a map record to a parquet Row.
// The row values must be in the same order as the schema fields.
func (c *parquetCodec) recordToRow(m D, index int) (parquet.Row, error) {
	// Build row in schema field order
	row := make(parquet.Row, len(c.fieldOrder))
	for i, fieldName := range c.fieldOrder {
		field := c.getFieldByName(fieldName)

		val, exists := m[fieldName]
		if !exists || val == nil {
			if !field.Nullable {
				return nil, fmt.Errorf("parquet: %w: record %d missing required field %q", ErrSchemaViolation, index, fieldName)
			}
			row[i] = parquet.NullValue().Level(0, 0, i)
			continue
		}

		pqVal, err := c.convertToParquetValue(val, field, index)
		if err != nil {
			return nil, err
		}
		defLevel := 1
		if !field.Nullable {
			defLevel = 0
		}
		row[i] = pqVal.Level(0, defLevel, i)
	}
	return row, nil
}

// rowToRecord converts a parquet Row back to a map.
func (c *parquetCodec) rowToRecord(row parquet.Row) map[string]any {
	record := make(map[string]any, len(c.fieldOrder))
	for i, fieldName := range c.fieldOrder {
		if i >= len(row) {
			continue
		}
		field := c.getFieldByName(fieldName)
		val := row[i]
		if val.IsNull() {
			record[fieldName] = nil
			continue
		}
		record[fieldName] = c.convertFromParquetValue(val, field)
	}
	return record
}

// convertToParquetValue converts a Go value to a parquet Value.
//
//nolint:gocyclo // Type switch with validation for each Parquet type is inherently complex.
func (c *parquetCodec) convertToParquetValue(val any, field ParquetField, index int) (parquet.Value, error) {
	switch field.Type {
	case ParquetInt32:
		switch v := val.(type) {
		case int:
			if v < minInt32 || v > maxInt32 {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: value %d overflows int32", ErrSchemaViolation, index, field.Name, v)
			}
			return parquet.Int32Value(int32(v)), nil
		case int32:
			return parquet.Int32Value(v), nil
		case int64:
			if v < minInt32 || v > maxInt32 {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: value %d overflows int32", ErrSchemaViolation, index, field.Name, v)
			}
			return parquet.Int32Value(int32(v)), nil
		case float64: // JSON numbers
			if math.Trunc(v) != v {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: float64 %v is not an integer", ErrSchemaViolation, index, field.Name, v)
			}
			if v < minInt32 || v > maxInt32 {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: value %v overflows int32", ErrSchemaViolation, index, field.Name, v)
			}
			return parquet.Int32Value(int32(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected int32, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetInt64:
		switch v := val.(type) {
		case int:
			return parquet.Int64Value(int64(v)), nil
		case int32:
			return parquet.Int64Value(int64(v)), nil
		case int64:
			return parquet.Int64Value(v), nil
		case float64: // JSON numbers
			if math.Trunc(v) != v {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: float64 %v is not an integer", ErrSchemaViolation, index, field.Name, v)
			}
			// float64 can represent integers up to 2^53 exactly
			if v < -maxSafeInt64 || v > maxSafeInt64 {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: value %v exceeds safe integer range for float64", ErrSchemaViolation, index, field.Name, v)
			}
			return parquet.Int64Value(int64(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected int64, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetFloat32:
		switch v := val.(type) {
		case float32:
			return parquet.FloatValue(v), nil
		case float64:
			return parquet.FloatValue(float32(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected float32, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetFloat64:
		switch v := val.(type) {
		case float32:
			return parquet.DoubleValue(float64(v)), nil
		case float64:
			return parquet.DoubleValue(v), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected float64, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetString:
		switch v := val.(type) {
		case string:
			return parquet.ByteArrayValue([]byte(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected string, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetBool:
		switch v := val.(type) {
		case bool:
			return parquet.BooleanValue(v), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected bool, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetBytes:
		switch v := val.(type) {
		case []byte:
			return parquet.ByteArrayValue(v), nil
		case string:
			return parquet.ByteArrayValue([]byte(v)), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected []byte, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	case ParquetTimestamp:
		switch v := val.(type) {
		case time.Time:
			return parquet.Int64Value(v.UnixNano()), nil
		case string:
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: invalid timestamp: %w", ErrSchemaViolation, index, field.Name, err)
			}
			return parquet.Int64Value(t.UnixNano()), nil
		default:
			return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: expected time.Time, got %T", ErrSchemaViolation, index, field.Name, val)
		}

	default:
		return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: unknown type %d", ErrSchemaViolation, index, field.Name, field.Type)
	}
}

// convertFromParquetValue converts a parquet Value back to a Go value.
func (c *parquetCodec) convertFromParquetValue(val parquet.Value, field ParquetField) any {
	switch field.Type {
	case ParquetInt32:
		return val.Int32()
	case ParquetInt64:
		return val.Int64()
	case ParquetFloat32:
		return val.Float()
	case ParquetFloat64:
		return val.Double()
	case ParquetString:
		return string(val.ByteArray())
	case ParquetBool:
		return val.Boolean()
	case ParquetBytes:
		return val.ByteArray()
	case ParquetTimestamp:
		return time.Unix(0, val.Int64()).UTC()
	default:
		return nil
	}
}

// buildParquetSchema creates a parquet-go schema from our schema definition.
func buildParquetSchema(schema ParquetSchema) (*parquet.Schema, error) {
	group := make(parquet.Group, len(schema.Fields))
	for _, field := range schema.Fields {
		node, err := buildFieldNode(field)
		if err != nil {
			return nil, err
		}
		group[field.Name] = node
	}
	return parquet.NewSchema("record", group), nil
}

func buildFieldNode(field ParquetField) (parquet.Node, error) {
	var node parquet.Node

	switch field.Type {
	case ParquetInt32:
		node = parquet.Int(32)
	case ParquetInt64:
		node = parquet.Int(64)
	case ParquetFloat32:
		node = parquet.Leaf(parquet.FloatType)
	case ParquetFloat64:
		node = parquet.Leaf(parquet.DoubleType)
	case ParquetString:
		node = parquet.String()
	case ParquetBool:
		node = parquet.Leaf(parquet.BooleanType)
	case ParquetBytes:
		node = parquet.Leaf(parquet.ByteArrayType)
	case ParquetTimestamp:
		node = parquet.Timestamp(parquet.Nanosecond)
	default:
		return nil, fmt.Errorf("parquet: %w: invalid ParquetType %d for field %q", ErrSchemaViolation, field.Type, field.Name)
	}

	if field.Nullable {
		node = parquet.Optional(node)
	}

	return node, nil
}

// -----------------------------------------------------------------------------
// Per-file statistics
// -----------------------------------------------------------------------------

// orderable maps ParquetType to whether min/max comparisons are meaningful.
var orderable = map[ParquetType]bool{
	ParquetInt32:     true,
	ParquetInt64:     true,
	ParquetFloat32:   true,
	ParquetFloat64:   true,
	ParquetString:    true,
	ParquetTimestamp: true,
	ParquetBool:      false,
	ParquetBytes:     false,
}

// computeFileStats computes per-column statistics from the input records.
func computeFileStats(schema ParquetSchema, records []D) *FileStats {
	columns := make([]ColumnStats, len(schema.Fields))
	for i, field := range schema.Fields {
		columns[i] = computeColumnStats(field, records)
	}
	return &FileStats{
		RowCount: int64(len(records)),
		Columns:  columns,
	}
}

// columnAccumulator tracks min/max/nullCount for a single column.
type columnAccumulator struct {
	field     ParquetField
	min       any
	max       any
	nullCount int64
	hasValue  bool
}

// observe records a single value for this column.
func (a *columnAccumulator) observe(val any) {
	if val == nil {
		a.nullCount++
		return
	}

	if !orderable[a.field.Type] {
		return
	}

	if !a.hasValue {
		a.min = val
		a.max = val
		a.hasValue = true
		return
	}

	if lessThan(a.field.Type, val, a.min) {
		a.min = val
	}
	if lessThan(a.field.Type, a.max, val) {
		a.max = val
	}
}

// computeColumnStats computes statistics for a single column across all records.
func computeColumnStats(field ParquetField, records []D) ColumnStats {
	acc := columnAccumulator{field: field}

	for _, m := range records {
		val, exists := m[field.Name]
		if !exists {
			acc.observe(nil)
			continue
		}
		acc.observe(val)
	}

	cs := ColumnStats{
		Name:      field.Name,
		NullCount: acc.nullCount,
	}
	if acc.hasValue {
		cs.Min = acc.min
		cs.Max = acc.max
	}
	return cs
}

// lessThan returns true if a < b for the given orderable ParquetType.
// Both a and b must be non-nil. Handles type coercion for numeric types
// that may arrive as different Go types (int, int32, int64, float64).
//
//nolint:gocyclo // Type dispatch for each orderable Parquet type.
func lessThan(ptype ParquetType, a, b any) bool {
	switch ptype {
	case ParquetInt32:
		return toInt64(a) < toInt64(b)
	case ParquetInt64:
		return toInt64(a) < toInt64(b)
	case ParquetFloat32:
		return toFloat64(a) < toFloat64(b)
	case ParquetFloat64:
		return toFloat64(a) < toFloat64(b)
	case ParquetString:
		as, _ := a.(string)
		bs, _ := b.(string)
		return as < bs
	case ParquetTimestamp:
		return toTimestamp(a).Before(toTimestamp(b))
	default:
		return false
	}
}

// toInt64 coerces numeric Go values to int64 for comparison.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// toFloat64 coerces numeric Go values to float64 for comparison.
func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

// toTimestamp coerces time values for comparison.
func toTimestamp(v any) time.Time {
	switch t := v.(type) {
	case time.Time:
		return t
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, t)
		if err != nil {
			return time.Time{}
		}
		return parsed
	default:
		return time.Time{}
	}
}
