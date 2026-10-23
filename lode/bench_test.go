package lode

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// -----------------------------------------------------------------------------
// BenchmarkDataset_Read measures read throughput for in-memory JSONL records.
// -----------------------------------------------------------------------------

func BenchmarkDataset_Read(b *testing.B) {
	ds, err := Open(Memory()).Records(b.Context(), "bench-read", JSONL[D]())
	if err != nil {
		b.Fatal(err)
	}

	records := R(
		D{"id": "1", "value": "hello"},
		D{"id": "2", "value": "world"},
		D{"id": "3", "value": "bench"},
	)
	snap, err := ds.Write(b.Context(), records, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ds.Read(b.Context(), snap.ID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkJSONLCodec_Decode measures decoding files of increasing size, so a
// fixed cost per file and a cost per record can be told apart.
// -----------------------------------------------------------------------------

func BenchmarkJSONLCodec_Decode(b *testing.B) {
	codec := JSONL[D]()
	for _, n := range []int{3, 1_000, 100_000} {
		var buf bytes.Buffer
		if err := codec.Encode(&buf, benchRecords(n)); err != nil {
			b.Fatal(err)
		}
		data := buf.Bytes()
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if _, err := codec.Decode(bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkJSONLCodec_DecodeLineLength measures decoding a file of ten
// records whose lines have the given length, since the decoder's buffer
// must grow to the longest line.
func BenchmarkJSONLCodec_DecodeLineLength(b *testing.B) {
	codec := JSONL[D]()
	for _, size := range []int{1 << 10, 16 << 10, 48 << 10, 256 << 10} {
		records := make([]D, 10)
		for i := range records {
			records[i] = D{"id": strconv.Itoa(i), "payload": strings.Repeat("x", size)}
		}
		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			b.Fatal(err)
		}
		data := buf.Bytes()
		b.Run(fmt.Sprintf("line=%dKiB", size>>10), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if _, err := codec.Decode(bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// benchRecords builds n records of a representative shape.
func benchRecords(n int) []D {
	records := make([]D, n)
	for i := range records {
		records[i] = D{"id": strconv.Itoa(i), "name": "record", "value": i, "tags": []any{"a", "b"}}
	}
	return records
}

// -----------------------------------------------------------------------------
// BenchmarkDataset_StreamWrite measures streaming write throughput with ~1KB payload.
// -----------------------------------------------------------------------------

func BenchmarkDataset_StreamWrite(b *testing.B) {
	payload := []byte(strings.Repeat("x", 1024))

	ds, err := Open(Memory()).Blobs(b.Context(), "bench-stream")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sw, err := ds.StreamWrite(b.Context(), nil)
		if err != nil {
			b.Fatal(err)
		}

		if _, err := sw.Write(payload); err != nil {
			b.Fatal(err)
		}

		if _, err := sw.Commit(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkParquetCodec_Encode measures encoding 100 records through Parquet.
// -----------------------------------------------------------------------------

func BenchmarkParquetCodec_Encode(b *testing.B) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString},
			{Name: "score", Type: ParquetFloat64},
		},
	}
	codec, err := newParquet(schema)
	if err != nil {
		b.Fatal(err)
	}

	records := make([]D, 100)
	for i := range records {
		records[i] = D{"id": int64(i), "name": "user", "score": 99.5}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := codec.Encode(&buf, records); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// BenchmarkParquetCodec_Decode measures decoding 100 Parquet records.
// -----------------------------------------------------------------------------

func BenchmarkParquetCodec_Decode(b *testing.B) {
	schema := ParquetSchema{
		Fields: []ParquetField{
			{Name: "id", Type: ParquetInt64},
			{Name: "name", Type: ParquetString},
			{Name: "score", Type: ParquetFloat64},
		},
	}
	codec, err := newParquet(schema)
	if err != nil {
		b.Fatal(err)
	}

	records := make([]D, 100)
	for i := range records {
		records[i] = D{"id": int64(i), "name": "user", "score": 99.5}
	}

	var encoded bytes.Buffer
	if err := codec.Encode(&encoded, records); err != nil {
		b.Fatal(err)
	}
	data := encoded.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Decode(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}
