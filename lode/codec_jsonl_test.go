package lode

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

// Decode reads one value per record with no line buffer in between, so a
// record far longer than the read buffer decodes, and so does a record
// longer than the ten mebibytes that earlier releases refused: Decode reads
// every file that Encode wrote.
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

	t.Run("a record longer than ten mebibytes round-trips", func(t *testing.T) {
		// Eleven mebibytes is the base64 of an eight-mebibyte blob, the
		// shape of record that the former ceiling refused after Encode
		// had written it.
		want := D{"v": strings.Repeat("x", 11<<20)}
		var buf bytes.Buffer
		if err := codec.Encode(&buf, []D{want}); err != nil {
			t.Fatal(err)
		}
		records, err := codec.Decode(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || records[0]["v"] != want["v"] {
			t.Fatalf("decoded %d records, want the one record of %d bytes", len(records), len(want["v"].(string)))
		}
	})
}

// Decode ends at the stream's end and fails on anything else: a reader's
// error, a value cut short, a line that holds no value, and a record that
// is not an object when the record type is a map.
func TestJSONLCodec_Decode_Ends(t *testing.T) {
	codec := JSONL[D]()
	errRead := errors.New("read failed")

	cases := []struct {
		name  string
		input io.Reader
		want  int   // records decoded when err is nil
		err   error // an error to match with errors.Is, or nil for any error
		fails bool
	}{
		{name: "an empty stream holds no records", input: strings.NewReader(""), want: 0},
		{name: "empty lines hold no records", input: strings.NewReader("\n\n"), want: 0},
		{name: "a last record without a newline decodes", input: strings.NewReader(`{"v":1}` + "\n" + `{"v":2}`), want: 2},
		{name: "a reader's error is returned", input: io.MultiReader(strings.NewReader(`{"v":1}`+"\n"), iotest.ErrReader(errRead)), err: errRead, fails: true},
		{name: "a value cut short fails", input: strings.NewReader(`{"v":1}` + "\n" + `{"v":`), fails: true},
		{name: "a line that holds no value fails", input: strings.NewReader(`{"v":1}` + "\nx\n"), fails: true},
		{name: "a record that is not an object fails", input: strings.NewReader(`{"v":1}` + "\n[1]\n"), fails: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records, err := codec.Decode(tc.input)
			if tc.fails {
				if err == nil {
					t.Fatalf("Decode = %v, want an error", records)
				}
				if tc.err != nil && !errors.Is(err, tc.err) {
					t.Fatalf("Decode = %v, want %v", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != tc.want {
				t.Fatalf("decoded %d records, want %d", len(records), tc.want)
			}
		})
	}
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

// pointerMarshaler encodes through its pointer receiver only, so a codec
// that encoded a record by address would persist "pointer" in place of the
// value a stream encoder persists.
type pointerMarshaler struct{ V int }

func (*pointerMarshaler) MarshalJSON() ([]byte, error) { return []byte(`"pointer"`), nil }

func TestJSONL_EncodesRecordsByValueAsStreamsDo(t *testing.T) {
	codec := JSONL[pointerMarshaler]()
	var batch bytes.Buffer
	if err := codec.Encode(&batch, []pointerMarshaler{{V: 1}}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	enc, err := codec.(StreamingCodec[pointerMarshaler]).NewStreamEncoder(&stream)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRecord(pointerMarshaler{V: 1}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if batch.String() != stream.String() {
		t.Errorf("Encode = %q, stream = %q; want one encoding", batch.String(), stream.String())
	}
}
