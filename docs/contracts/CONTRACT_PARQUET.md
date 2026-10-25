# Lode Parquet Codec Contract

This document defines the required semantics of Parquet codec integration.
It is authoritative for any Parquet codec implementation within Lode.

---

## Goals

1. A columnar storage format for efficient analytical queries.
2. Interoperability with common external readers (tested with primitive
   types).
3. Encoding with an explicit schema (the codec infers no schema from data).
4. Conformance of the codec interface to Lode's existing abstractions.

---

## Non-Goals

- Query execution or predicate pushdown.
- Automatic schema evolution or merging.
- Partitioning by row group within Lode (external readers perform it).
- Encryption or column-level access control.
- Nested types (structs, lists, and maps). Schemas are flat.

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
- Manifests record this name.

### Encode

- `Encode(w, records)` MUST write a valid Parquet file to `w`.
- The Parquet file MUST include the footer, which requires buffering all
  data.
- The codec MUST encode records according to the configured schema.
- An empty set of records (`len(records) == 0`) MUST produce a valid Parquet
  file with zero rows.
- The codec MUST return encoding errors and never ignore them.

### Decode

- `Decode(r)` MUST read a complete Parquet file from `r`.
- It returns records as `[]D`, each a `map[string]any`.
- The codec maps field types according to the table in §Type Mapping.
- `Decode` MUST support files written by this codec and by standard Parquet
  writers.

---

## Schema Requirements

Parquet requires a schema. The Lode Parquet codec MUST support explicit
schema configuration.

### Schema Definition

The caller defines the schema when constructing the codec:

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

`Parquet` validates the schema and records an error that matches
`ErrSchemaViolation`. `Root.Records` and `ReadRoot.Records` return that
error, and so do `Encode` and `Decode` when a caller invokes them directly.
Validation fails for:

- invalid `ParquetType` values (out of range),
- empty field names, and
- duplicate field names (which would cause the codec to drop columns
  without notice).

All schema validation errors wrap `ErrSchemaViolation`.

### Record Validation

- Records MUST contain every required (non-nullable) field defined in the
  schema.
- **Additional fields**: the codec MUST ignore, without error, record
  fields that are absent from the schema. This rule preserves forward compatibility
  when record producers add fields.
- A missing required field MUST produce `ErrSchemaViolation`.
- A type mismatch that remains after attempted coercion MUST produce
  `ErrSchemaViolation`.

### No Schema Inference

- The codec MUST NOT infer a schema from record data.
- Predictable, portable output requires an explicit schema.
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

The codec performs **explicit, documented type coercions** to accept input
types that common decoders produce:

1. **Conversion of JSON numbers**: the codec accepts `float64` for integer
   fields, because `encoding/json` decodes every number as `float64`. This
   coercion allows interoperation with records decoded from JSONL.

2. **Parsing of timestamp strings**: the codec accepts RFC 3339 and
   RFC 3339 (nanosecond) strings for timestamp fields. An invalid timestamp
   string produces `ErrSchemaViolation`.

3. **Conversion of strings to bytes**: the codec accepts `string` for byte
   fields and converts it with `[]byte(s)`.

These coercions are **not inference**: the schema still determines the
output type. Coercion affects only which input types the codec accepts.

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

These checks prevent undetected data corruption through numeric overflow
or precision loss.

### Nullable Fields

- The schema MAY mark fields as nullable.
- Nullable fields accept `nil` values and encode them as Parquet nulls.
- A non-nullable field with a `nil` or missing value MUST produce
  `ErrSchemaViolation`.

---

## Streaming Limitations

Each Parquet file requires a footer that references the metadata of every
row group. This footer makes genuine streaming impossible.

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

- The implementation defines the row-group size (typically the library's
  default).
- A single row group is acceptable for small datasets.

Row-group configuration is an implementation detail, and the public API
does not expose it.

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

The codec offers no Zstandard compression.

### Compression Layering

Parquet's internal compression is separate from Lode's `Compressor`.

- A caller that uses the Parquet codec sets Lode's compressor to `"noop"`.
- Compressing twice (with Parquet and with Lode's compressor) consumes CPU
  for minimal benefit.
- The manifest records Lode's compressor (`"noop"`), not Parquet's internal
  compression.

### Manifest Recording

- Lode's `Compressor` field records the external compressor (this contract
  recommends `"noop"`).
- Parquet's internal compression is part of the file format, and the
  manifest does not record it.

---

## Statistics and Metadata

Parquet files contain statistics (minimum and maximum values, null counts,
and row counts).

### Manifest Integration

The implementation MAY extract the following statistics and record them in
manifests:

- `RowCount`: the total number of rows in the file (from Parquet metadata).
- `MinTimestamp` and `MaxTimestamp`: when the caller designates a timestamp
  column.

### Per-File Statistics

The Parquet codec implements `StatisticalCodec` and reports per-file
statistics through `FileStats()` after each `Encode` call:

- `RowCount`: the total number of rows encoded in the file.
- `Min` and `Max` for each column of an ordered type: int32, int64, float32,
  float64, string, and timestamp.
- `NullCount` for each column: the number of null values in a nullable
  column.
- `DistinctCount`: not computed. The codec reports zero, which the
  manifest omits.
- Boolean and byte columns report only `NullCount` (no minimum or maximum).
- The codec computes statistics during `Encode` from the Go record values,
  not from Parquet's internal metadata.
- Minimum and maximum values use the same Go types as the input records.
  When serialized to JSON, a `time.Time` becomes an RFC 3339 string with
  nanosecond precision, and consumers must interpret values according to the
  schema.

---

## Error Semantics

### Error Sentinels

The file `lode/api.go` MUST define the following errors so that callers can
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
| Nil value for a non-nullable field | `ErrSchemaViolation`            |
| Invalid timestamp string     | `ErrSchemaViolation`            |
| Float64 with fractional part | `ErrSchemaViolation`            |
| Int32 overflow               | `ErrSchemaViolation`            |
| Value outside the int64 safe range | `ErrSchemaViolation`            |
| Write failure                | The underlying I/O error        |

### Decoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Invalid Parquet file         | `ErrInvalidFormat`              |
| Corrupted data               | `ErrInvalidFormat`              |
| Empty file                   | `ErrInvalidFormat`              |
| Row read failure             | `ErrInvalidFormat` (wrapped)    |

The codec wraps errors in reading individual rows with `ErrInvalidFormat`,
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

The project tests this codec's Parquet files against the following
readers and expects each of them to read the files:
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

### Features Without a Compatibility Guarantee

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
(circumvented validation), not an error in user input. This
behavior is acceptable because:

1. The panic is unreachable in normal use, since validation rejects every
   invalid input.
2. It provides a clear failure if code inadvertently circumvents validation.
3. Returning errors from internal functions would add unnecessary
   complexity.

### Recommended Library

This contract recommends the `parquet-go` library
(github.com/parquet-go/parquet-go):
- It consists entirely of Go code.
- It supports schema-based encoding.
- It is actively maintained.

### Alternative Libraries

- `github.com/xitongsys/parquet-go`: older and less actively maintained.
- `github.com/apache/arrow-go`: a heavier dependency with more features.

Library selection is an implementation detail, and this contract does not
govern it.

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
- Handling of additional fields (forward compatibility).
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
- The codec returns the error sentinels for the documented conditions.

---

## Scope Decisions

Three decisions bound the codec.

1. **Nested types:** the codec supports flat schemas only, without nested
   structs, lists, or maps.
2. **Schemas:** the caller declares a schema explicitly, and the codec does
   not derive one from Go struct tags.
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
