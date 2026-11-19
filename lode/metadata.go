package lode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/pithecene/lode/internal/validate"
)

// Metadata is a snapshot's metadata as persisted: one JSON object, retained
// as written and decoded on request (CONTRACT_CORE §Metadata Rules). Decode
// into a struct to receive its declared field types; a map receives JSON's
// types, with numbers as float64.
type Metadata struct {
	raw json.RawMessage
}

// emptyObject is the metadata of a write that supplied none.
var emptyObject = json.RawMessage("{}")

// NewMetadata encodes a caller's metadata value, which must encode as a JSON
// object: a struct, a map, or nil (including typed-nil string maps and struct
// pointers), which persists as {}.
func NewMetadata(v any) (Metadata, error) {
	if isNilMetadata(v) {
		return Metadata{raw: emptyObject}, nil
	}
	// A Metadata decoded from JSON holds whatever value it was given, so it
	// is checked as an encoded value is.
	if m, ok := v.(Metadata); ok {
		if m = m.orEmpty(); !m.Valid() {
			return Metadata{}, validate.Argument("metadata must be a JSON object, got %s", m.raw)
		}
		return m, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return Metadata{}, validate.Argument("metadata of type %T does not encode as JSON: %v", v, err)
	}
	if !isObject(data) {
		return Metadata{}, validate.Argument("metadata of type %T must encode as a JSON object, got %s", v, data)
	}
	return Metadata{raw: data}, nil
}

// isNilMetadata reports whether v is untyped nil, a nil string-keyed map, or a
// nil pointer to a struct or Metadata — the object-shaped nil values that
// coalesce to {}.
func isNilMetadata(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		return rv.IsNil() && rv.Type().Key().Kind() == reflect.String
	case reflect.Pointer:
		if !rv.IsNil() {
			return false
		}
		elem := rv.Type().Elem()
		return elem.Kind() == reflect.Struct || elem == reflect.TypeFor[Metadata]()
	default:
		return false
	}
}

// isObject reports whether data, valid JSON, is an object.
func isObject(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// orEmpty is m, or the empty object when m holds nothing.
func (m Metadata) orEmpty() Metadata {
	if len(m.raw) == 0 {
		return Metadata{raw: emptyObject}
	}
	return m
}

// Valid reports whether m holds a JSON object, as a manifest requires.
func (m Metadata) Valid() bool {
	return isObject(m.raw)
}

// Decode unmarshals the metadata into v, as encoding/json does.
func (m Metadata) Decode(v any) error {
	if err := json.Unmarshal(m.orEmpty().raw, v); err != nil {
		return fmt.Errorf("lode: decode metadata: %w", err)
	}
	return nil
}

// JSON returns a copy of the persisted JSON object.
func (m Metadata) JSON() []byte {
	return bytes.Clone(m.orEmpty().raw)
}

// MarshalJSON writes the persisted object.
func (m Metadata) MarshalJSON() ([]byte, error) {
	return m.orEmpty().raw, nil
}

// UnmarshalJSON retains a copy of data. JSON null empties m, so a reused
// value retains nothing it held before; a manifest whose metadata is absent or
// not an object fails validation (Valid).
func (m *Metadata) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		m.raw = nil
		return nil
	}
	m.raw = bytes.Clone(data)
	return nil
}
