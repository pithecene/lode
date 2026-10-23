package lode

import (
	"bufio"
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
)

// The decoder's buffer grows to the longest line it meets, up to
// maxScanTokenSize, so a line far longer than the scanner's initial buffer
// decodes, and a line beyond the limit fails rather than being truncated.
func TestJSONLCodec_Decode_LineLength(t *testing.T) {
	codec := JSONL[D]()

	t.Run("a line of one mebibyte decodes", func(t *testing.T) {
		value := strings.Repeat("x", 1<<20)
		records, err := codec.Decode(strings.NewReader(`{"v":"` + value + `"}` + "\n" + `{"v":"next"}` + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 {
			t.Fatalf("decoded %d records, want 2", len(records))
		}
		if got := records[0]["v"]; got != value {
			t.Errorf("first record holds %d bytes, want %d", len(got.(string)), len(value))
		}
	})

	t.Run("a line beyond the limit fails", func(t *testing.T) {
		line := `{"v":"` + strings.Repeat("x", maxScanTokenSize) + `"}` + "\n"
		if _, err := codec.Decode(strings.NewReader(line)); !errors.Is(err, bufio.ErrTooLong) {
			t.Fatalf("Decode = %v, want bufio.ErrTooLong", err)
		}
	})
}

// jsonlEvent is a typed record for the JSONL round trips.
type jsonlEvent struct {
	Source string `json:"source"`
	Count  int    `json:"count"`
}

// JSONL encodes and decodes records of any type T as encoding/json does:
// a struct keeps its field types, and a record of type any decodes as JSON's
// types.
func TestJSONLCodec_RoundTrip(t *testing.T) {
	t.Run("struct records keep their field types", func(t *testing.T) {
		codec := JSONL[jsonlEvent]()
		want := []jsonlEvent{{"a", 1}, {"b", 2}}
		var buf bytes.Buffer
		if err := codec.Encode(&buf, want); err != nil {
			t.Fatal(err)
		}
		got, err := codec.Decode(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("decoded %v, want %v", got, want)
		}
	})

	t.Run("records of type any decode as JSON's types", func(t *testing.T) {
		codec := JSONL[any]()
		var buf bytes.Buffer
		if err := codec.Encode(&buf, []any{D{"n": 1}, "text"}); err != nil {
			t.Fatal(err)
		}
		got, err := codec.Decode(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].(map[string]any)["n"] != float64(1) || got[1] != "text" {
			t.Errorf("decoded %#v, want a map with a float64 and a string", got)
		}
	})

	t.Run("the stream encoder writes what Encode writes", func(t *testing.T) {
		codec := JSONL[jsonlEvent]()
		records := []jsonlEvent{{"a", 1}, {"b", 2}}
		var batch, stream bytes.Buffer
		if err := codec.Encode(&batch, records); err != nil {
			t.Fatal(err)
		}
		enc, err := codec.(StreamingCodec[jsonlEvent]).NewStreamEncoder(&stream)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			if err := enc.WriteRecord(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		if batch.String() != stream.String() {
			t.Errorf("stream wrote %q, want %q", stream.String(), batch.String())
		}
	})
}
