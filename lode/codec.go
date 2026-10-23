package lode

import (
	"bufio"
	"fmt"
	"io"

	jsoniter "github.com/json-iterator/go"
)

var jsonCodec = jsoniter.ConfigCompatibleWithStandardLibrary

const maxScanTokenSize = 10 * 1024 * 1024 // 10MB

// -----------------------------------------------------------------------------
// JSONL Codec
// -----------------------------------------------------------------------------

// jsonlCodec implements StreamingCodec[T] using JSON Lines.
type jsonlCodec[T any] struct{}

// JSONL returns a JSON Lines codec for records of type T: each record is
// one line of JSON, encoded and decoded as encoding/json does. Use D for
// map records. The codec streams, so Records.StreamWrite accepts it.
//
// It returns the Codec interface, so that Root.Records infers T from it:
// root.Records(ctx, "events", lode.JSONL[Event]()).
func JSONL[T any]() Codec[T] {
	return jsonlCodec[T]{}
}

func (jsonlCodec[T]) Name() string { return "jsonl" }

// Describe returns "": JSONL has no configuration.
func (jsonlCodec[T]) Describe() string { return "" }

func (jsonlCodec[T]) Encode(w io.Writer, records []T) error {
	enc := jsonCodec.NewEncoder(w)
	for i := range records {
		if err := enc.Encode(&records[i]); err != nil {
			return err
		}
	}
	return nil
}

func (jsonlCodec[T]) Decode(r io.Reader) ([]T, error) {
	var records []T
	scanner := bufio.NewScanner(r)
	// The scanner allocates its buffer on demand and grows it to the
	// longest line, up to maxScanTokenSize, so a small file costs a small
	// buffer rather than a fixed 64 KiB.
	scanner.Buffer(nil, maxScanTokenSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		record, err := decodeRecord[T](line)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// decodeRecord decodes one line as a record of type T. A map record (D) is
// decoded through an interface value: jsoniter's decoder for map[string]any
// allocates three more objects per record than its decoder for any, which
// yields the same map for a JSON object. The two paths are separate
// functions, so that the map path does not allocate the record variable the
// other path passes by address.
func decodeRecord[T any](line []byte) (T, error) {
	var zero T
	if _, isMap := any(zero).(map[string]any); isMap {
		return decodeMap[T](line)
	}
	return decodeValue[T](line)
}

func decodeValue[T any](line []byte) (T, error) {
	var record T
	err := jsonCodec.Unmarshal(line, &record)
	return record, err
}

func decodeMap[T any](line []byte) (T, error) {
	var zero T
	var v any
	if err := jsonCodec.Unmarshal(line, &v); err != nil {
		return zero, err
	}
	if v == nil { // JSON null decodes as a nil map
		return zero, nil
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return zero, fmt.Errorf("jsonl: record %.40s is not a JSON object", line)
	}
	return any(obj).(T), nil
}

// NewStreamEncoder makes the JSONL codec a StreamingCodec.
func (jsonlCodec[T]) NewStreamEncoder(w io.Writer) (StreamEncoder[T], error) {
	return &jsonlStreamEncoder[T]{enc: jsonCodec.NewEncoder(w)}, nil
}

// jsonlStreamEncoder implements StreamEncoder for JSONL.
type jsonlStreamEncoder[T any] struct {
	enc *jsoniter.Encoder
}

func (e *jsonlStreamEncoder[T]) WriteRecord(record T) error {
	return e.enc.Encode(record)
}

func (e *jsonlStreamEncoder[T]) Close() error {
	// JSONL has no footer/finalization needed
	return nil
}
