# Lode Parquet Codec — Contract

This document defines the required semantics for the integration of the
Parquet codec.
It is authoritative for any Parquet codec implementation within Lode.

---

## Goals

1. A columnar storage format for efficient analytical queries.
2. Interoperability with common external readers (tested with primitive
   types).
3. Encoding with an explicit schema (no schema is inferred from data).
4. Conformance of the codec interface to Lode's existing abstractions.

---

## Non-Goals

- Query execution or predicate pushdown.
- Automatic schema evolution or merging.
- Partitioning by row group within Lode (external readers perform it).
- Encryption or column-level access control.
- Nested types (structs, lists, maps); schemas are flat.

---

## Parquet Codec Interface

The Parquet codec MUST implement `Codec[D]` for map records
(`D = map[string]any`):

```go
type Codec[T any] interface {
    Name() string                          // Returns "parquet"
    Describe() string                      // The schema, canonical JSON
    Encode(w io.Writer, records []T) error // Batch encoding
    Decode(r io.Reader) ([]T, error)       // Batch decoding
}
```

The Parquet codec MUST NOT implement `StreamingCodec`. Because Parquet
requires a footer, genuine streaming is impossible without buffering the
entire file. `Records.StreamWrite` with a Parquet codec MUST return
`ErrCodecNotStreamable`.

### Describe

- `Describe()` MUST return the schema as canonical JSON, its fields in
  schema order with their names, types, and nullability, so that equal
  schemas yield equal descriptions (CONTRACT_LAYOUT §Codec).
- The internal compression MUST NOT be part of the description: it does not
  affect which records a file holds or how they decode.

### Name

- `Name()` MUST return `"parquet"`.
- This name is recorded in manifests.

### Encode

- `Encode(w, records)` MUST write a valid Parquet file to `w`.
- The Parquet file MUST include the footer, which requires buffering all
  data.
- Records MUST be encoded according to the configured schema.
- An empty set of records (`len(records) == 0`) MUST produce a valid Parquet
  file with zero rows.
- Encoding errors MUST be returned, never ignored.

### Decode

- `Decode(r)` MUST read a complete Parquet file from `r`.
- It returns records as `[]D`, each a `map[string]any`.
- Field types are mapped according to the table in §Type Mapping.
- `Decode` MUST support files written by this codec and by standard Parquet
  writers.

---

## Schema Requirements

Parquet requires a schema. The Lode Parquet codec MUST support explicit
configuration of the schema.

### Schema Definition

Schemas are defined when the codec is constructed:

```go
codec := lode.Parquet(lode.ParquetSchema{
    Fields: []lode.ParquetField{
        {Name: "id", Type: lode.ParquetInt64},
        {Name: "name", Type: lode.ParquetString},
        {Name: "timestamp", Type: lode.ParquetTimestamp},
    },
})
ds, err := root.Records(ctx, "events", codec) // returns a schema error
```

### Schema Construction Validation

`Parquet` validates the schema and records an error, matching
`ErrSchemaViolation`, which `Root.Records` and `ReadRoot.Records` return,
and which `Encode` and `Decode` return if called directly, for:

- invalid `ParquetType` values (out of range);
- empty field names; and
- duplicate field names (which would cause columns to be dropped without
  notice).

All schema validation errors wrap `ErrSchemaViolation`.

### Record Validation

- Records MUST contain every required (non-nullable) field defined in the
  schema.
- **Additional fields**: fields in records that are absent from the schema
  MUST be ignored without error. This rule preserves forward compatibility
  when record producers add fields.
- A missing required field MUST produce `ErrSchemaViolation`.
- A type mismatch that remains after coercion is attempted MUST produce
  `ErrSchemaViolation`.

### No Schema Inference

- The codec MUST NOT infer a schema from record data.
- An explicit schema is required for predictable, portable output.
- This rule accords with Lode's principle that it "stores facts, not
  interpretations."

---

## Type Mapping

### Supported Types

| Parquet Type         | Go Source Types (accepted)              | Decoded Go Type   |
|---------------------|----------------------------------------|-------------------|
| `ParquetInt32`      | `int`, `int32`, `int64`, `float64`     | `int32`           |
| `ParquetInt64`      | `int`, `int32`, `int64`, `float64`     | `int64`           |
| `ParquetFloat32`    | `float32`, `float64`                   | `float32`         |
| `ParquetFloat64`    | `float32`, `float64`                   | `float64`         |
| `ParquetString`     | `string`                               | `string`          |
| `ParquetBool`       | `bool`                                 | `bool`            |
| `ParquetBytes`      | `[]byte`, `string`                     | `[]byte`          |
| `ParquetTimestamp`  | `time.Time`, `string` (RFC 3339)       | `time.Time`       |

### Explicit Type Coercion

The codec performs **explicit, documented type coercions** to support
common patterns:

1. **Conversion of JSON numbers**: `float64` is accepted for integer fields,
   because `encoding/json` decodes every number as `float64`. This
   coercion allows interoperation with records decoded from JSONL.

2. **Parsing of timestamp strings**: RFC 3339 and RFC 3339 (nanosecond)
   strings are accepted for timestamp fields. An invalid timestamp string
   produces `ErrSchemaViolation`.

3. **Bytes from strings**: `string` is accepted for byte fields (converted
   with `[]byte(s)`).

These coercions are **not inference**: the schema still determines the
output type. Coercion affects only which input types are accepted.

### Numeric Overflow Protection

When coercing `float64` to integer types, the codec validates:

- **Truncation check**: `float64` values MUST be whole numbers
  (`math.Trunc(v) == v`). A non-integral value produces
  `ErrSchemaViolation`.
- **Int32 range check**: values MUST lie within `[-2147483648, 2147483647]`.
  A value outside this range produces `ErrSchemaViolation`.
- **Int64 safe-range check**: for a conversion from `float64` to `int64`,
  values MUST lie within `[-2^53, 2^53]` (the range in which `float64`
  represents integers exactly). A value outside this range produces
  `ErrSchemaViolation`.

These checks prevent the undetected corruption of data through numeric
overflow or loss of precision.

### Nullable Fields

- Fields MAY be marked as nullable in the schema.
- Nullable fields accept `nil` values and encode them as Parquet nulls.
- A non-nullable field with a `nil` or missing value MUST produce
  `ErrSchemaViolation`.

---

## Streaming Limitations

Parquet files require a footer that references the metadata of each row
group, which makes genuine streaming impossible.

### Memory Behavior

- `Encode` buffers **all** records in memory before writing the Parquet
  file.
- `Decode` reads the **entire** file into memory before returning records.
- Memory use grows linearly with the size of the data.
- For large datasets, callers MUST divide the data among several
  snapshots.

### Records.StreamWrite Behavior

The Parquet codec MUST NOT implement `StreamingCodec`:

- `Records.StreamWrite` with the Parquet codec returns
  `ErrCodecNotStreamable`.
- Callers MUST use `Records.Write` for Parquet encoding.
- The restriction is explicit: Parquet's format precludes streaming without
  buffering.

---

## Row Group Configuration

Row groups affect read performance and memory use.

### Defaults

- The size of a row group is defined by the implementation (typically the
  library's default).
- A single row group is acceptable for small datasets.

Row-group configuration is an implementation detail and is not exposed in
the public API.

---

## Compression

Parquet supports internal compression of each column chunk.

### Supported Compression Options

```go
const (
    ParquetCompressionNone ParquetCompression = iota
    ParquetCompressionSnappy  // Default; balances ratio and speed
    ParquetCompressionGzip    // Higher ratio; slower
)
```

Note: the codec offers no Zstandard compression.

### Compression Layering

**Important**: Parquet's internal compression is separate from Lode's
`Compressor`.

- When using the Parquet codec, set Lode's compressor to `"noop"`.
- Compressing twice (with Parquet and with Lode's compressor) consumes CPU
  for minimal benefit.
- The manifest records Lode's compressor (`"noop"`), not Parquet's internal
  compression.

### Manifest Recording

- Lode's `Compressor` field records the external compressor (`"noop"` is
  recommended).
- Parquet's internal compression is part of the file format and is not
  recorded in the manifest.

---

## Statistics and Metadata

Parquet files contain statistics (minimum and maximum values, null counts,
and row counts).

### Manifest Integration

The following statistics MAY be extracted and recorded in manifests:

- `RowCount`: the total number of rows in the file (from Parquet metadata).
- `MinTimestamp` and `MaxTimestamp`: when a timestamp column is designated.

### Per-File Statistics

The Parquet codec implements `StatisticalCodec` and reports per-file
statistics through `FileStats()` after each `Encode` call:

- `RowCount`: the total number of rows encoded in the file.
- `Min` and `Max` for each column of an ordered type: int32, int64, float32,
  float64, string, and timestamp.
- `NullCount` for each column: the number of null values in a nullable
  column.
- `DistinctCount`: not computed; the codec reports 0, which the manifest
  omits.
- Boolean and byte columns report only `NullCount` (no minimum or maximum).
- Statistics are computed during `Encode` from the Go record values, not
  from Parquet's internal metadata.
- Minimum and maximum values use the same Go types as the input records.
  When serialized to JSON, a `time.Time` becomes an RFC 3339 string with
  nanosecond precision; consumers must interpret values according to the
  schema.

---

## Error Semantics

### Error Sentinels

The following errors MUST be defined in `lode/api.go` so that callers can
match them:

```go
// ErrSchemaViolation indicates a record does not conform to the Parquet schema.
var ErrSchemaViolation = errSchemaViolation{}

// ErrInvalidFormat indicates the Parquet file is malformed or corrupted.
var ErrInvalidFormat = errInvalidFormat{}
```

### Encoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Missing required field       | `ErrSchemaViolation`            |
| Type mismatch after coercion | `ErrSchemaViolation`            |
| Nil value for non-nullable   | `ErrSchemaViolation`            |
| Invalid timestamp string     | `ErrSchemaViolation`            |
| Float64 with fractional part | `ErrSchemaViolation`            |
| Int32 overflow               | `ErrSchemaViolation`            |
| Int64 safe range exceeded    | `ErrSchemaViolation`            |
| Write failure                | The underlying I/O error        |

### Decoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Invalid Parquet file         | `ErrInvalidFormat`              |
| Corrupted data               | `ErrInvalidFormat`              |
| Empty file                   | `ErrInvalidFormat`              |
| Row read failure             | `ErrInvalidFormat` (wrapped)    |

Note: errors in reading individual rows are wrapped with `ErrInvalidFormat`,
because they indicate problems with the format (corrupt row data, a
truncated file, and similar defects).

---

## Construction API

### Minimal API

```go
// Parquet returns a Parquet codec for map records with the given schema.
// An invalid schema (bad types, empty names, duplicates) is recorded and
// returned when a dataset opens with the codec, so that it can be passed
// in a chain.
func Parquet(schema ParquetSchema, opts ...ParquetOption) Codec[D]

// ParquetSchema defines the record structure.
type ParquetSchema struct {
    Fields []ParquetField
}

// ParquetField defines a single field.
type ParquetField struct {
    Name     string
    Type     ParquetType
    Nullable bool
}

// ParquetType enumerates supported Parquet logical types.
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

### Options

```go
// WithParquetCompression sets internal Parquet compression.
func WithParquetCompression(codec ParquetCompression) ParquetOption

type ParquetCompression int

const (
    ParquetCompressionNone ParquetCompression = iota
    ParquetCompressionSnappy
    ParquetCompressionGzip
)
```

---

## External Reader Compatibility

### Scope

Parquet files produced by this codec are **tested with**, and **expected to
work with**:
- Apache Arrow and PyArrow
- DuckDB
- Apache Spark
- Polars

### Compatibility Constraints

- **Primitive types only**: flat schemas of primitive types (no nested
  structs, lists, or maps).
- **Standard logical types**: the codec uses standard Parquet logical types
  and no custom extensions.
- **No encryption**: files are unencrypted.

### Not Guaranteed

- Complex nested schemas
- Custom annotations of logical types
- Parquet encryption features
- Row-group statistics for partition pruning (which depend on the reader)

### Standard Compliance

- Output MUST conform to the Apache Parquet specification.
- The codec uses standard logical types, not custom extensions.

---

## Implementation Notes

### Internal Invariants

The implementation validates all schema constraints in `Parquet`.
Internal functions that process validated schemas may panic on invalid
types as a secondary safeguard. Such a panic indicates a programming error
(validation that was circumvented), not an error in user input. This
behavior is acceptable because:

1. The panic is unreachable in normal use, since validation rejects every
   invalid input.
2. It provides a clear failure if validation is inadvertently circumvented.
3. Returning errors from internal functions would add unnecessary
   complexity.

### Recommended Library

The `parquet-go` library (github.com/parquet-go/parquet-go) is recommended:
- It is implemented entirely in Go.
- It supports schema-based encoding.
- It is actively maintained.

### Alternative Libraries

- `github.com/xitongsys/parquet-go`: older and less actively maintained.
- `github.com/apache/arrow-go`: a heavier dependency with more features.

The choice of library is an implementation detail and is not governed by
this contract.

---

## Testing Requirements

### Unit Tests

- Encoding and decoding round trips for all supported types.
- Validation of schema construction (invalid types, empty names,
  duplicates).
- Record validation (missing fields, type mismatches, nullability).
- Type coercion (JSON numbers, timestamp strings).
- Protection against numeric overflow (int32 range, int64 safe range,
  truncation).
- Additional fields are ignored (forward compatibility).
- Handling of an empty set of records.
- Large batches of records (to verify row-group behavior).

### Dataset-Level Tests

- `Records.StreamWrite` returns `ErrCodecNotStreamable` for the Parquet
  codec.
- `Records.Write` produces readable Parquet files.
- The manifest records `codec: "parquet"`.

### External Reader Tests (Optional)

- Read the files with DuckDB (if available in CI).
- Read the files with PyArrow (if available in CI).

### Contract Compliance Tests

- `Name()` returns `"parquet"`.
- Manifests record the codec as `"parquet"`.
- The error sentinels are returned for the documented conditions.

---

## Scope Decisions

Three decisions bound the codec.

1. **Nested types:** the codec supports flat schemas only, without nested
   structs, lists, or maps.
2. **Schemas:** a schema is declared explicitly; the codec does not derive
   one from Go struct tags.
3. **Compression:** the codec offers Snappy, gzip, or no internal
   compression, and no Zstandard.

---

## References

- [Apache Parquet Specification](https://parquet.apache.org/docs/)
- [parquet-go Library](https://github.com/parquet-go/parquet-go)
- [CONTRACT_LAYOUT.md](CONTRACT_LAYOUT.md): recording of the codec in
  manifests
- [CONTRACT_CORE.md](CONTRACT_CORE.md): manifest requirements
- [CONTRACT_ERRORS.md](CONTRACT_ERRORS.md): error taxonomy
