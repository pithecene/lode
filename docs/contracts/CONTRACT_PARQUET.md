# Lode Parquet Codec Contract

This document defines the required semantics of Apache Parquet codec
integration in Lode (`lode.Parquet[T]`).

---

## Goals

1. Columnar storage interoperable with standard external Parquet readers
   (Apache Arrow/PyArrow, DuckDB, Spark, Polars).
2. Explicit schema declaration (`ParquetSchema`)—never inferred from Go types
   or record payloads.
3. Support for both map records (`lode.D` / `map[string]any`) and flat typed
   Go structs under one generic constructor.
4. Stateless, concurrency-safe encoding with per-object column statistics.

---

## Non-Goals

- Query execution or predicate pushdown inside Lode.
- Automatic schema evolution or schema inference from Go struct tags alone.
- Nested types (nested structs, lists, or map columns)—schemas are flat.
- Encryption or column-level access control.

---

## Parquet Codec Interface

`Parquet[T](schema, opts...)` returns a stateless `Codec[T]` that also
implements `StatisticalCodec[T]`:

```go
type Codec[T any] interface {
    Name() string
    Describe() string
    Encode(w io.Writer, records []T) error
    Decode(r io.Reader) ([]T, error)
}

type StatisticalCodec[T any] interface {
    Codec[T]
    EncodeWithStats(w io.Writer, records []T) (*ObjectStats, error)
}
```

- All state on the returned codec value MUST be immutable after construction
  so a single codec value is safe for concurrent `Encode`, `EncodeWithStats`,
  and `Decode` calls across goroutines.
- `Parquet[T]` MUST NOT implement `StreamingCodec[T]`, because every Parquet
  file requires a footer referencing all row groups. Calling
  `Records.StreamWrite` with a Parquet codec MUST return
  `ErrCodecNotStreamable`.
- `Name()` MUST return `"parquet"`.
- `Describe()` MUST return canonical JSON listing `fields` in declared
  `schema.Fields` order (`name`, `type`, `nullable`). `Describe()` MUST NOT
  depend on `T` or internal compression: `Parquet[User](schema)` and
  `Parquet[lode.D](schema)` produce identical descriptions so readers and
  writers can freely use either representation.

---

## Schema and Record Type Binding

```go
func Parquet[T any](schema ParquetSchema, opts ...ParquetOption) Codec[T]

type ParquetSchema struct {
    Fields []ParquetField
}

type ParquetField struct {
    Name     string
    Type     ParquetType
    Nullable bool
}

type ParquetType int

const (
    ParquetInt32 ParquetType = iota
    ParquetInt64
    ParquetFloat32
    ParquetFloat64
    ParquetString
    ParquetBool
    ParquetBytes
    ParquetTimestamp
)
```

### Schema Validation at Construction

`Parquet[T]` validates `schema` and record type `T` at construction time.
When validation fails, `Parquet[T]` records an error matching
`ErrSchemaViolation`, which `Root.Records`, `ReadRoot.Records`, `Encode`,
`EncodeWithStats`, and `Decode` return:
- `schema.Fields` MUST be non-empty.
- Every `ParquetField.Name` MUST be non-empty and unique within `schema`.
- Every `ParquetField.Type` MUST be one of the eight `ParquetType` constants.
- Columns in the written Parquet file MUST preserve the exact declared order
  of `schema.Fields`.

### Supported Record Types (`T`)

`T` MUST be either a string-keyed `any` map (`lode.D` / `map[string]any`) or
a Go struct value (not a pointer to a struct, slice, or scalar; any other `T`
fails with `ErrSchemaViolation`).

#### Struct Binding Rules
- Exported fields of `T` (including promoted fields of embedded structs) map
  to column names by priority:
  1. `parquet` struct tag (`parquet:"col_name"`; `parquet:"-"` skips the
     field),
  2. `json` struct tag (`json:"col_name"`; `json:"-"` skips the field),
  3. exported Go field `Name`.
- Duplicate column mappings on `T` MUST fail construction with
  `ErrSchemaViolation`.
- Every field in `schema.Fields` MUST have a matching field on `T`
  (`ErrSchemaViolation` if missing). Additional exported fields on `T` not
  named in `schema.Fields` are ignored without error.
- Non-nullable schema fields MUST map to non-pointer Go scalar types;
  nullable schema fields MUST map to pointer Go scalar types (except
  `ParquetBytes`, where `[]byte` is valid for both nullable and non-nullable
  fields, with `nil` encoding as null when nullable and failing with
  `ErrSchemaViolation` when non-nullable).

#### Map (`lode.D`) Binding Rules
- Required (`Nullable: false`) fields MUST be present with a non-nil value on
  every record (`ErrSchemaViolation` otherwise). For `ParquetBytes`, a typed
  `[]byte(nil)` value in a map record is treated identically to `nil` (encoding
  as null with `NullCount` incremented when `Nullable: true`, and failing with
  `ErrSchemaViolation` when `Nullable: false`), matching struct `[]byte`
  fields.
- Extra map keys not present in `schema.Fields` are ignored without error.
- `Decode` returns `[]T` where each map contains every field in
  `schema.Fields` (with `nil` for null values, including nullable columns
  absent from an external Parquet file).

---

## Type Mapping and Coercion

| Parquet Type | Struct Field Type (Required / Nullable) | Map (`D`) Input Types | Decoded Go Type |
|--------------|-----------------------------------------|-----------------------|-----------------|
| `ParquetInt32` | `int32`, `int` / `*int32`, `*int` | `int`, `int32`, `int64`, `float64` | `int32` (or struct field type) |
| `ParquetInt64` | `int64`, `int` / `*int64`, `*int` | `int`, `int32`, `int64`, `float64` | `int64` (or struct field type) |
| `ParquetFloat32` | `float32` / `*float32` | `float32`, `float64` | `float32` |
| `ParquetFloat64` | `float64` / `*float64` | `float32`, `float64` | `float64` |
| `ParquetString` | `string` / `*string` | `string` | `string` |
| `ParquetBool` | `bool` / `*bool` | `bool` | `bool` |
| `ParquetBytes` | `[]byte` / `[]byte` | `[]byte`, `string` | `[]byte` |
| `ParquetTimestamp` | `time.Time` / `*time.Time` | `time.Time`, `string` (RFC 3339) | UTC `time.Time` |

Explicit coercions and overflow checks:
- Whole-number `float64` values convert to `int32` (within
  `[-2147483648, 2147483647]`) or `int64` (within `[-2^53, 2^53]`);
  fractional or out-of-range values fail with `ErrSchemaViolation`.
- `int` and `int64` inputs to `ParquetInt32` outside `int32` bounds fail with
  `ErrSchemaViolation`. On 32-bit platforms, decoding a `ParquetInt64` value
  that overflows a struct `int` field fails with `ErrInvalidFormat`.
- Projection uses physical leaf-column indices. Extra nested columns do not
  change the meaning of a requested scalar column.
- A timestamp column must carry a UTC-adjusted timestamp annotation in
  milliseconds, microseconds, or nanoseconds. Decode returns the corresponding
  UTC instant. Other annotations and bare integers yield `ErrInvalidFormat`.
- RFC 3339 / RFC 3339Nano strings convert to `ParquetTimestamp`; invalid
  timestamp strings fail with `ErrSchemaViolation`.
- Because `ParquetTimestamp` is stored physically as a signed `int64` UTC
  nanosecond count since the Unix epoch, `time.Time` values (including the Go
  zero `time.Time{}`) and RFC 3339 timestamp strings outside
  `[1677-09-21T00:12:43.145224192Z, 2262-04-11T23:47:16.854775807Z]`
  (`[time.Unix(0, math.MinInt64), time.Unix(0, math.MaxInt64)]`) MUST fail
  with `ErrSchemaViolation` before conversion or statistics collection.

---

## File Schema Validation on Decode

`Decode` validates the physical Parquet file schema before reading rows and
returns `ErrInvalidFormat` when:
- the file is empty, truncated, or corrupt,
- the file schema contains duplicate column names,
- a matched column is repeated or not a scalar leaf,
- a matched column's physical Parquet kind differs from the expected kind for
  its `ParquetType`,
- a required (`Nullable: false`) schema field is absent from the file schema,
  or
- a required (`Nullable: false`) column contains a null value in any row.

Extra columns in the file not named in `schema.Fields` are ignored, and
nullable columns absent from the file decode as null (`nil` in `D`, `nil`
pointer/`[]byte` in structs). Zero-row Parquet files decode as an empty
non-nil slice `[]T{}`.

---

## Internal Compression

```go
type ParquetOption func(*parquetConfig)

func WithParquetCompression(codec ParquetCompression) ParquetOption

type ParquetCompression int

const (
    ParquetCompressionNone ParquetCompression = iota
    ParquetCompressionSnappy
    ParquetCompressionGzip
)
```

- `ParquetCompressionSnappy` is the default internal column-chunk
  compression.
- Callers SHOULD leave Lode's dataset compressor at `Uncompressed()`
  (`"noop"`) to avoid compressing Parquet output twice.

---

## Per-Object Statistics

`EncodeWithStats` computes `*ObjectStats` from the encoded Go values without
mutating codec state:
- `RowCount` equals `int64(len(records))`.
- `Columns` contains one `ColumnStats` per field in `schema.Fields` order.
- Ordered types (`ParquetInt32`, `ParquetInt64`, `ParquetFloat32`,
  `ParquetFloat64`, `ParquetString`, `ParquetTimestamp`) record `Min` and
  `Max` normalized to the canonical decoded Go type (`int32`, `int64`,
  `float32`, `float64`, `string`, UTC `time.Time`).
- Non-finite floating-point values (`NaN`, `+Inf`, `-Inf`) are excluded from
  `Min` and `Max` so JSON manifest serialization never fails.
- Every column records `NullCount`; `ParquetBool` and `ParquetBytes` omit
  `Min` and `Max`; `DistinctCount` is `0` (omitted in JSON).

---

## Error Summary

| Condition | Sentinel |
|-----------|----------|
| Empty schema, duplicate/empty field name, out-of-range `ParquetType`, unsupported `T`, missing/incompatible struct field | `ErrSchemaViolation` |
| Missing required field, `nil` in non-nullable field, type mismatch, numeric overflow, invalid RFC 3339 timestamp on encode | `ErrSchemaViolation` |
| Empty, truncated, corrupt, or physical-schema-mismatched Parquet file, or null in required column on decode | `ErrInvalidFormat` |
| `Records.StreamWrite` called with `Parquet[T]` | `ErrCodecNotStreamable` |
