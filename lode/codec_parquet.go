package lode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
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

type parquetConfig struct {
	compression ParquetCompression
}

// ParquetOption configures Parquet codec behavior.
type ParquetOption func(*parquetConfig)

// WithParquetCompression sets internal Parquet compression.
func WithParquetCompression(codec ParquetCompression) ParquetOption {
	return func(c *parquetConfig) {
		c.compression = codec
	}
}

// -----------------------------------------------------------------------------
// Parquet Codec Implementation
// -----------------------------------------------------------------------------

// parquetCodec implements Codec[T] and StatisticalCodec[T] for Apache Parquet.
// All state on parquetCodec is immutable after construction, so a single codec
// value is safe for concurrent Encode, EncodeWithStats, and Decode calls.
type parquetCodec[T any] struct {
	err          error // an invalid schema or record type, reported by Root.Records
	schema       ParquetSchema
	compression  ParquetCompression
	pqSchema     *parquet.Schema
	fieldOrder   []string                // ordered field names matching schema columns
	fieldIndex   map[string]int          // schema field position by name
	fieldsByName map[string]ParquetField // field lookup by name
	binder       recordBinder[T]
}

var _ StatisticalCodec[D] = (*parquetCodec[D])(nil)

// validateSchema validates a ParquetSchema and builds a field lookup map.
func validateSchema(schema ParquetSchema) (map[string]ParquetField, error) {
	if len(schema.Fields) == 0 {
		return nil, fmt.Errorf("parquet: %w: schema must have at least one field", ErrSchemaViolation)
	}
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

// Parquet returns a stateless Parquet codec for records of type T (either D /
// map[string]any or a flat Go struct) validated against the explicit schema.
//
// Struct fields match schema.Fields by `parquet` tag name, else `json` tag
// name, else exported Go field name; no schema is ever inferred from T. An
// invalid schema or incompatible struct type is recorded and returned,
// matching ErrSchemaViolation, by Root.Records and ReadRoot.Records, and by
// Encode, EncodeWithStats, and Decode:
//
//	events, err := root.Records(ctx, "events", lode.Parquet[Event](schema))
//
// The Parquet codec is not a StreamingCodec, because a Parquet file requires a
// footer that references all row groups. Use Records.Write.
func Parquet[T any](schema ParquetSchema, opts ...ParquetOption) Codec[T] {
	c, err := newParquetCodec[T](schema, opts...)
	if err != nil {
		return &parquetCodec[T]{err: err}
	}
	return c
}

func newParquetCodec[T any](schema ParquetSchema, opts ...ParquetOption) (*parquetCodec[T], error) {
	schema.Fields = slices.Clone(schema.Fields)
	fieldsByName, err := validateSchema(schema)
	if err != nil {
		return nil, err
	}

	cfg := parquetConfig{compression: ParquetCompressionSnappy}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	pqSchema, err := buildParquetSchema(schema)
	if err != nil {
		return nil, err
	}

	fieldOrder := make([]string, len(schema.Fields))
	fieldIndex := make(map[string]int, len(schema.Fields))
	for i, f := range pqSchema.Fields() {
		fieldOrder[i] = f.Name()
		fieldIndex[f.Name()] = i
	}

	binder, err := buildRecordBinder[T](schema)
	if err != nil {
		return nil, err
	}

	return &parquetCodec[T]{
		schema:       schema,
		compression:  cfg.compression,
		pqSchema:     pqSchema,
		fieldOrder:   fieldOrder,
		fieldIndex:   fieldIndex,
		fieldsByName: fieldsByName,
		binder:       binder,
	}, nil
}

func (c *parquetCodec[T]) Name() string { return "parquet" }

func (c *parquetCodec[T]) configError() error { return c.err }

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

type describedField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// Describe returns the schema as JSON, its fields in schema order. Both
// Parquet[D](schema) and Parquet[MyStruct](schema) produce the same
// description for the same schema.
func (c *parquetCodec[T]) Describe() string {
	if c.err != nil {
		return ""
	}
	fields := make([]describedField, len(c.schema.Fields))
	for i, f := range c.schema.Fields {
		fields[i] = describedField{Name: f.Name, Type: parquetTypeNames[f.Type], Nullable: f.Nullable}
	}
	data, _ := json.Marshal(struct { //nolint:errchkjson // a struct of strings and bools always encodes
		Fields []describedField `json:"fields"`
	}{fields})
	return string(data)
}

func (c *parquetCodec[T]) Encode(w io.Writer, records []T) error {
	_, err := c.EncodeWithStats(w, records)
	return err
}

// EncodeWithStats encodes records to w and returns per-object column
// statistics without mutating codec state.
func (c *parquetCodec[T]) EncodeWithStats(w io.Writer, records []T) (*ObjectStats, error) {
	if c.err != nil {
		return nil, c.err
	}

	var buf bytes.Buffer
	rowBuf := parquet.NewBuffer(c.pqSchema)

	accumulators := make([]columnAccumulator, len(c.schema.Fields))
	for i, field := range c.schema.Fields {
		accumulators[i] = columnAccumulator{field: field}
	}

	for i, record := range records {
		row, err := c.recordToRow(record, i, accumulators)
		if err != nil {
			return nil, err
		}
		if _, err := rowBuf.WriteRows([]parquet.Row{row}); err != nil {
			return nil, fmt.Errorf("parquet: write row %d: %w", i, err)
		}
	}

	pqWriter := parquet.NewWriter(&buf, c.pqSchema, c.getCompressionOption())
	if _, err := pqWriter.WriteRowGroup(rowBuf); err != nil {
		_ = pqWriter.Close()
		return nil, fmt.Errorf("parquet: write row group: %w", err)
	}
	if err := pqWriter.Close(); err != nil {
		return nil, fmt.Errorf("parquet: close writer: %w", err)
	}

	if _, err := io.Copy(w, &buf); err != nil {
		return nil, err
	}

	columns := make([]ColumnStats, len(accumulators))
	for i, acc := range accumulators {
		columns[i] = acc.finish()
	}
	return &ObjectStats{
		RowCount: int64(len(records)),
		Columns:  columns,
	}, nil
}

func (c *parquetCodec[T]) Decode(r io.Reader) ([]T, error) {
	if c.err != nil {
		return nil, c.err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("parquet: read file: %w", err)
	}
	if len(data) == 0 {
		return nil, ErrInvalidFormat
	}

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrInvalidFormat
		}
		return nil, fmt.Errorf("parquet: %w: %w", ErrInvalidFormat, err)
	}

	plan, err := c.planDecodeColumns(file.Schema())
	if err != nil {
		return nil, err
	}

	numRows := file.NumRows()
	if numRows == 0 {
		return []T{}, nil
	}

	reader := parquet.NewReader(file)
	defer closer(reader)()

	records := make([]T, 0, numRows)
	rows := make([]parquet.Row, 100)
	for {
		n, err := reader.ReadRows(rows)
		for i := range n {
			record, rowErr := c.binder.decodeRow(rows[i], plan)
			if rowErr != nil {
				return nil, rowErr
			}
			records = append(records, record)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parquet: %w: read rows: %w", ErrInvalidFormat, err)
		}
	}

	return records, nil
}

func (c *parquetCodec[T]) getCompressionOption() parquet.WriterOption {
	switch c.compression {
	case ParquetCompressionSnappy:
		return parquet.Compression(&parquet.Snappy)
	case ParquetCompressionGzip:
		return parquet.Compression(&parquet.Gzip)
	default:
		return parquet.Compression(&parquet.Uncompressed)
	}
}

func (c *parquetCodec[T]) recordToRow(record T, index int, accumulators []columnAccumulator) (parquet.Row, error) {
	row := make(parquet.Row, len(c.fieldOrder))
	for i, fieldName := range c.fieldOrder {
		field := c.fieldsByName[fieldName]
		val, exists := c.binder.extractField(record, i)
		if !exists || val == nil {
			if !field.Nullable {
				return nil, fmt.Errorf("parquet: %w: record %d missing required field %q", ErrSchemaViolation, index, fieldName)
			}
			accumulators[i].observe(nil)
			row[i] = parquet.NullValue().Level(0, 0, i)
			continue
		}

		pqVal, err := convertToParquetValue(val, field, index)
		if err != nil {
			return nil, err
		}
		accumulators[i].observe(val)
		defLevel := 1
		if !field.Nullable {
			defLevel = 0
		}
		row[i] = pqVal.Level(0, defLevel, i)
	}
	return row, nil
}

// -----------------------------------------------------------------------------
// Record Binding (D / map[string]any and flat Go structs)
// -----------------------------------------------------------------------------

type recordBinder[T any] interface {
	extractField(record T, schemaIdx int) (any, bool)
	decodeRow(row parquet.Row, plan decodePlan) (T, error)
}

func buildRecordBinder[T any](schema ParquetSchema) (recordBinder[T], error) {
	rt := reflect.TypeFor[T]()
	if rt == reflect.TypeFor[D]() {
		return fastMapBinder[T]{fields: schema.Fields}, nil
	}
	if rt.Kind() == reflect.Map && rt.Key() == reflect.TypeFor[string]() && rt.Elem() == reflect.TypeFor[any]() {
		return reflectMapBinder[T]{mapType: rt, fields: schema.Fields}, nil
	}
	if rt.Kind() == reflect.Struct {
		return buildStructBinder[T](rt, schema)
	}
	return nil, fmt.Errorf("parquet: %w: unsupported record type %s; want lode.D (map[string]any) or a struct value", ErrSchemaViolation, rt)
}

type fastMapBinder[T any] struct {
	fields []ParquetField
}

func (b fastMapBinder[T]) extractField(record T, schemaIdx int) (any, bool) {
	m := any(record).(D)
	if m == nil {
		return nil, false
	}
	val, ok := m[b.fields[schemaIdx].Name]
	return val, ok
}

func (b fastMapBinder[T]) decodeRow(row parquet.Row, plan decodePlan) (T, error) {
	var zero T
	m, err := rowToMap(row, plan, len(b.fields))
	if err != nil {
		return zero, err
	}
	return any(m).(T), nil
}

type reflectMapBinder[T any] struct {
	mapType reflect.Type
	fields  []ParquetField
}

func (b reflectMapBinder[T]) extractField(record T, schemaIdx int) (any, bool) {
	rv := reflect.ValueOf(record)
	if rv.IsNil() {
		return nil, false
	}
	v := rv.MapIndex(reflect.ValueOf(b.fields[schemaIdx].Name))
	if !v.IsValid() || v.IsNil() {
		return nil, false
	}
	return v.Interface(), true
}

func (b reflectMapBinder[T]) decodeRow(row parquet.Row, plan decodePlan) (T, error) {
	var zero T
	m, err := rowToMap(row, plan, len(b.fields))
	if err != nil {
		return zero, err
	}
	return reflect.ValueOf(m).Convert(b.mapType).Interface().(T), nil
}

func rowToMap(row parquet.Row, plan decodePlan, fieldCount int) (D, error) {
	record := make(D, fieldCount)
	for _, name := range plan.missingNullable {
		record[name] = nil
	}
	for _, val := range row {
		colIdx := val.Column()
		if colIdx < 0 || colIdx >= len(plan.columns) || !plan.columns[colIdx].matched {
			continue
		}
		col := plan.columns[colIdx]
		field := col.field
		if val.IsNull() {
			if !field.Nullable {
				return nil, fmt.Errorf("parquet: %w: null value in required column %q", ErrInvalidFormat, field.Name)
			}
			record[field.Name] = nil
			continue
		}
		record[field.Name] = convertFromParquetValue(val, col)
	}
	return record, nil
}

type structFieldAccessor struct {
	index    []int
	elemType reflect.Type
	isPtr    bool
	isBytes  bool
}

type structBinder[T any] struct {
	accessors []structFieldAccessor // indexed by schemaIdx
}

func buildStructBinder[T any](rt reflect.Type, schema ParquetSchema) (recordBinder[T], error) {
	byColName := make(map[string]reflect.StructField, rt.NumField())
	for _, sf := range reflect.VisibleFields(rt) {
		if !sf.IsExported() || isEmbeddedStructContainer(sf) {
			continue
		}
		colName, skip := structColumnName(sf)
		if skip {
			continue
		}
		if _, dup := byColName[colName]; dup {
			return nil, fmt.Errorf("parquet: %w: duplicate struct field mapping for %q on %s", ErrSchemaViolation, colName, rt)
		}
		byColName[colName] = sf
	}

	accessors := make([]structFieldAccessor, len(schema.Fields))
	for i, field := range schema.Fields {
		sf, ok := byColName[field.Name]
		if !ok {
			return nil, fmt.Errorf("parquet: %w: struct %s is missing schema field %q", ErrSchemaViolation, rt, field.Name)
		}
		if unexp, bad := unexportedEmbeddedPointer(rt, sf.Index); bad {
			return nil, fmt.Errorf("parquet: %w: field %q on %s is promoted through unexported embedded pointer %q", ErrSchemaViolation, field.Name, rt, unexp)
		}
		acc, err := validateStructField(rt, sf, field)
		if err != nil {
			return nil, err
		}
		accessors[i] = acc
	}
	return structBinder[T]{accessors: accessors}, nil
}

func unexportedEmbeddedPointer(rt reflect.Type, index []int) (string, bool) {
	cur := rt
	for _, fieldIdx := range index[:len(index)-1] {
		if cur.Kind() == reflect.Pointer {
			cur = cur.Elem()
		}
		sf := cur.Field(fieldIdx)
		if sf.Type.Kind() == reflect.Pointer && !sf.IsExported() {
			return sf.Name, true
		}
		cur = sf.Type
	}
	return "", false
}

func isEmbeddedStructContainer(sf reflect.StructField) bool {
	if !sf.Anonymous {
		return false
	}
	if sf.Type.Kind() == reflect.Struct && sf.Type != timeType {
		return true
	}
	return sf.Type.Kind() == reflect.Pointer && sf.Type.Elem().Kind() == reflect.Struct && sf.Type.Elem() != timeType
}

func structColumnName(sf reflect.StructField) (name string, skip bool) {
	if tag, ok := sf.Tag.Lookup("parquet"); ok {
		col, _, _ := strings.Cut(tag, ",")
		if col == "-" {
			return "", true
		}
		if col != "" {
			return col, false
		}
	}
	if tag, ok := sf.Tag.Lookup("json"); ok {
		col, _, _ := strings.Cut(tag, ",")
		if col == "-" {
			return "", true
		}
		if col != "" {
			return col, false
		}
	}
	return sf.Name, false
}

var (
	timeType = reflect.TypeFor[time.Time]()
	byteType = reflect.TypeFor[byte]()
)

func validateStructField(structType reflect.Type, sf reflect.StructField, field ParquetField) (structFieldAccessor, error) {
	ft := sf.Type
	isBytes := ft.Kind() == reflect.Slice && ft.Elem() == byteType

	if field.Nullable {
		if field.Type == ParquetBytes && isBytes {
			return structFieldAccessor{index: sf.Index, elemType: ft, isBytes: true}, nil
		}
		if ft.Kind() != reflect.Pointer {
			return structFieldAccessor{}, fmt.Errorf("parquet: %w: nullable field %q on %s requires a pointer type, got %s", ErrSchemaViolation, field.Name, structType, ft)
		}
		if !compatibleScalarType(ft.Elem(), field.Type) {
			return structFieldAccessor{}, fmt.Errorf("parquet: %w: field %q on %s has incompatible type %s for Parquet %s", ErrSchemaViolation, field.Name, structType, ft, parquetTypeNames[field.Type])
		}
		return structFieldAccessor{index: sf.Index, elemType: ft.Elem(), isPtr: true}, nil
	}

	if ft.Kind() == reflect.Pointer {
		return structFieldAccessor{}, fmt.Errorf("parquet: %w: non-nullable field %q on %s must not be a pointer type (%s)", ErrSchemaViolation, field.Name, structType, ft)
	}
	if !compatibleScalarType(ft, field.Type) {
		return structFieldAccessor{}, fmt.Errorf("parquet: %w: field %q on %s has incompatible type %s for Parquet %s", ErrSchemaViolation, field.Name, structType, ft, parquetTypeNames[field.Type])
	}
	return structFieldAccessor{index: sf.Index, elemType: ft, isBytes: isBytes}, nil
}

func compatibleScalarType(t reflect.Type, ptype ParquetType) bool {
	switch ptype {
	case ParquetInt32:
		return t.Kind() == reflect.Int32 || t.Kind() == reflect.Int
	case ParquetInt64:
		return t.Kind() == reflect.Int64 || t.Kind() == reflect.Int
	case ParquetFloat32:
		return t.Kind() == reflect.Float32
	case ParquetFloat64:
		return t.Kind() == reflect.Float64
	case ParquetString:
		return t.Kind() == reflect.String
	case ParquetBool:
		return t.Kind() == reflect.Bool
	case ParquetBytes:
		return t.Kind() == reflect.Slice && t.Elem() == byteType
	case ParquetTimestamp:
		return t == timeType
	default:
		return false
	}
}

func (b structBinder[T]) extractField(record T, schemaIdx int) (any, bool) {
	acc := b.accessors[schemaIdx]
	fv, ok := fieldByIndexRead(reflect.ValueOf(record), acc.index)
	if !ok {
		return nil, false
	}
	if acc.isPtr {
		if fv.IsNil() {
			return nil, false
		}
		fv = fv.Elem()
	} else if acc.isBytes && fv.IsNil() {
		return nil, false
	}
	return normalizeReflectedScalar(fv), true
}

func fieldByIndexRead(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, fieldIdx := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(fieldIdx)
	}
	return v, true
}

func fieldByIndexAlloc(v reflect.Value, index []int) reflect.Value {
	for i, fieldIdx := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(fieldIdx)
	}
	return v
}

func normalizeReflectedScalar(fv reflect.Value) any {
	if fv.Type() == timeType {
		return fv.Interface()
	}
	switch fv.Kind() {
	case reflect.Int32:
		return int32(fv.Int())
	case reflect.Int, reflect.Int64:
		return fv.Int()
	case reflect.Float32:
		return float32(fv.Float())
	case reflect.Float64:
		return fv.Float()
	case reflect.String:
		return fv.String()
	case reflect.Bool:
		return fv.Bool()
	case reflect.Slice:
		return fv.Bytes()
	default:
		return fv.Interface()
	}
}

func (b structBinder[T]) decodeRow(row parquet.Row, plan decodePlan) (T, error) {
	var out T
	rv := reflect.ValueOf(&out).Elem()
	for _, val := range row {
		colIdx := val.Column()
		if colIdx < 0 || colIdx >= len(plan.columns) || !plan.columns[colIdx].matched {
			continue
		}
		col := plan.columns[colIdx]
		if val.IsNull() {
			if !col.field.Nullable {
				return out, fmt.Errorf("parquet: %w: null value in required column %q", ErrInvalidFormat, col.field.Name)
			}
			continue
		}
		acc := b.accessors[col.schemaIdx]
		fv := fieldByIndexAlloc(rv, acc.index)
		if acc.isPtr {
			ptr := reflect.New(acc.elemType)
			if err := setScalarValue(ptr.Elem(), val, col); err != nil {
				return out, err
			}
			fv.Set(ptr)
			continue
		}
		if err := setScalarValue(fv, val, col); err != nil {
			return out, err
		}
	}
	return out, nil
}

func setScalarValue(dst reflect.Value, val parquet.Value, col decodeColumn) error {
	field := col.field
	switch field.Type {
	case ParquetInt32:
		dst.SetInt(int64(val.Int32()))
	case ParquetInt64:
		n := val.Int64()
		if dst.OverflowInt(n) {
			return fmt.Errorf("parquet: %w: column %q value %d overflows %s", ErrInvalidFormat, field.Name, n, dst.Type())
		}
		dst.SetInt(n)
	case ParquetFloat32:
		dst.SetFloat(float64(val.Float()))
	case ParquetFloat64:
		dst.SetFloat(val.Double())
	case ParquetString:
		dst.SetString(string(val.ByteArray()))
	case ParquetBool:
		dst.SetBool(val.Boolean())
	case ParquetBytes:
		raw := bytes.Clone(val.ByteArray())
		if raw == nil {
			raw = []byte{}
		}
		dst.SetBytes(raw)
	case ParquetTimestamp:
		dst.Set(reflect.ValueOf(col.timestamp(val.Int64()).UTC()))
	}
	return nil
}

// -----------------------------------------------------------------------------
// Decode Column Plan
// -----------------------------------------------------------------------------

type decodeColumn struct {
	timestamp func(int64) time.Time
	field     ParquetField
	schemaIdx int
	matched   bool
}

type decodePlan struct {
	columns         []decodeColumn
	missingNullable []string
}

var expectedParquetKind = [parquetTypeMax]parquet.Kind{
	ParquetInt32:     parquet.Int32,
	ParquetInt64:     parquet.Int64,
	ParquetFloat32:   parquet.Float,
	ParquetFloat64:   parquet.Double,
	ParquetString:    parquet.ByteArray,
	ParquetBool:      parquet.Boolean,
	ParquetBytes:     parquet.ByteArray,
	ParquetTimestamp: parquet.Int64,
}

func (c *parquetCodec[T]) planDecodeColumns(fileSchema *parquet.Schema) (decodePlan, error) {
	if fileSchema == nil {
		return decodePlan{}, fmt.Errorf("parquet: %w: missing file schema", ErrInvalidFormat)
	}
	fileFields := fileSchema.Fields()
	cols := make([]decodeColumn, len(fileSchema.Columns()))
	seen := make(map[string]bool, len(fileFields))

	for _, pf := range fileFields {
		name := pf.Name()
		if seen[name] {
			return decodePlan{}, fmt.Errorf("parquet: %w: duplicate column %q in file schema", ErrInvalidFormat, name)
		}
		seen[name] = true
		expected, ok := c.fieldsByName[name]
		if !ok {
			continue
		}
		leaf, scalar := fileSchema.Lookup(name)
		if !scalar || pf.Repeated() {
			return decodePlan{}, fmt.Errorf("parquet: %w: column %q is not a scalar leaf", ErrInvalidFormat, name)
		}
		if got, want := pf.Type().Kind(), expectedParquetKind[expected.Type]; got != want {
			return decodePlan{}, fmt.Errorf("parquet: %w: column %q has physical kind %s, want %s", ErrInvalidFormat, name, got, want)
		}
		col := decodeColumn{field: expected, schemaIdx: c.fieldIndex[name], matched: true}
		if expected.Type == ParquetTimestamp {
			unit, err := timestampDecoder(pf)
			if err != nil {
				return decodePlan{}, err
			}
			col.timestamp = unit
		}
		cols[leaf.ColumnIndex] = col
	}

	var missingNullable []string
	for _, field := range c.schema.Fields {
		if seen[field.Name] {
			continue
		}
		if !field.Nullable {
			return decodePlan{}, fmt.Errorf("parquet: %w: missing required column %q", ErrInvalidFormat, field.Name)
		}
		missingNullable = append(missingNullable, field.Name)
	}
	return decodePlan{columns: cols, missingNullable: missingNullable}, nil
}

// timestampDecoder selects the UTC interpretation once per physical column.
func timestampDecoder(field parquet.Field) (func(int64) time.Time, error) {
	logical := field.Type().LogicalType()
	if logical != nil && logical.Timestamp != nil && logical.Timestamp.IsAdjustedToUTC {
		unit := logical.Timestamp.Unit
		switch {
		case unit.Millis != nil:
			return time.UnixMilli, nil
		case unit.Micros != nil:
			return time.UnixMicro, nil
		case unit.Nanos != nil:
			return func(n int64) time.Time { return time.Unix(0, n) }, nil
		}
	}
	return nil, fmt.Errorf("parquet: %w: column %q requires a UTC timestamp with a supported unit", ErrInvalidFormat, field.Name())
}

// -----------------------------------------------------------------------------
// Value Conversion & Schema Construction
// -----------------------------------------------------------------------------

//nolint:gocyclo // Type switch with validation for each Parquet type is inherently flat dispatch.
func convertToParquetValue(val any, field ParquetField, index int) (parquet.Value, error) {
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
		case float64:
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
		case float64:
			if math.Trunc(v) != v {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d field %q: float64 %v is not an integer", ErrSchemaViolation, index, field.Name, v)
			}
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
			if v == nil && !field.Nullable {
				return parquet.Value{}, fmt.Errorf("parquet: %w: record %d missing required field %q", ErrSchemaViolation, index, field.Name)
			}
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

func convertFromParquetValue(val parquet.Value, col decodeColumn) any {
	field := col.field
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
		raw := bytes.Clone(val.ByteArray())
		if raw == nil {
			return []byte{}
		}
		return raw
	case ParquetTimestamp:
		return col.timestamp(val.Int64()).UTC()
	default:
		return nil
	}
}

type orderedGroup struct {
	parquet.Group
	fields []parquet.Field
}

func (g orderedGroup) Fields() []parquet.Field { return g.fields }

type orderedField struct {
	parquet.Node
	name string
}

func (f *orderedField) Name() string { return f.name }

func (f *orderedField) Value(base reflect.Value) reflect.Value {
	if base.Kind() == reflect.Interface {
		if base.IsNil() {
			return reflect.ValueOf(nil)
		}
		if base = base.Elem(); base.Kind() == reflect.Pointer && base.IsNil() {
			return reflect.ValueOf(nil)
		}
	}
	return base.MapIndex(reflect.ValueOf(f.name))
}

func buildParquetSchema(schema ParquetSchema) (*parquet.Schema, error) {
	group := make(parquet.Group, len(schema.Fields))
	fields := make([]parquet.Field, len(schema.Fields))
	for i, field := range schema.Fields {
		node, err := buildFieldNode(field)
		if err != nil {
			return nil, err
		}
		group[field.Name] = node
		fields[i] = &orderedField{Node: node, name: field.Name}
	}
	return parquet.NewSchema("record", orderedGroup{Group: group, fields: fields}), nil
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
// Per-object statistics
// -----------------------------------------------------------------------------

type columnAccumulator struct {
	field     ParquetField
	min       any
	max       any
	nullCount int64
	hasValue  bool
}

func (a *columnAccumulator) observe(val any) {
	if val == nil {
		a.nullCount++
		return
	}
	norm, ok := normalizeStatValue(a.field.Type, val)
	if !ok {
		return
	}
	if !a.hasValue {
		a.min = norm
		a.max = norm
		a.hasValue = true
		return
	}
	if lessThan(a.field.Type, norm, a.min) {
		a.min = norm
	}
	if lessThan(a.field.Type, a.max, norm) {
		a.max = norm
	}
}

func (a columnAccumulator) finish() ColumnStats {
	cs := ColumnStats{
		Name:      a.field.Name,
		NullCount: a.nullCount,
	}
	if a.hasValue {
		cs.Min = a.min
		cs.Max = a.max
	}
	return cs
}

func normalizeStatValue(ptype ParquetType, val any) (any, bool) {
	switch ptype {
	case ParquetInt32:
		return int32(toInt64(val)), true
	case ParquetInt64:
		return toInt64(val), true
	case ParquetFloat32:
		f := float32(toFloat64(val))
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return nil, false
		}
		return f, true
	case ParquetFloat64:
		f := toFloat64(val)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		return f, true
	case ParquetString:
		s, ok := val.(string)
		return s, ok
	case ParquetTimestamp:
		return toTimestamp(val).UTC(), true
	default:
		return nil, false
	}
}

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
