package lode

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
)

// partitioner is the internal interface for partition key extraction.
// This is NOT part of the public API - partitioning is configured through Layout.
type partitioner interface {
	name() string
	// group returns the records of each partition key. The groups may share
	// the backing array of records, which the caller must not modify.
	group(records []any) (map[string][]any, error)
	isNoop() bool
	// keys returns the partition keys in order; nil without partitions.
	keys() []string
}

// -----------------------------------------------------------------------------
// Hive Partitioner (internal)
// -----------------------------------------------------------------------------

// hivePartitioner extracts partition keys from record fields.
type hivePartitioner struct {
	keyNames []string
}

func newHivePartitioner(keys ...string) partitioner {
	return &hivePartitioner{keyNames: slices.Clone(keys)} // the caller's slice may change later
}

func (h *hivePartitioner) name() string {
	return "hive"
}

func (h *hivePartitioner) group(records []any) (map[string][]any, error) {
	groups := make(map[string][]any)
	for _, record := range records {
		key, err := h.partitionKey(record)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], record)
	}
	return groups, nil
}

func (h *hivePartitioner) partitionKey(record any) (string, error) {
	m, ok := record.(map[string]any)
	if !ok {
		return "", fmt.Errorf("hive partitioner: record must be map[string]any, got %T", record)
	}

	parts := make([]string, 0, len(h.keyNames))
	for _, key := range h.keyNames {
		val, exists := m[key]
		if !exists {
			return "", fmt.Errorf("hive partitioner: missing key %q", key)
		}
		parts = append(parts, key+"="+escapeValue(val))
	}

	return strings.Join(parts, "/"), nil
}

func (h *hivePartitioner) isNoop() bool {
	return false
}

func (h *hivePartitioner) keys() []string { return h.keyNames }

func escapeValue(v any) string {
	var s string
	switch val := v.(type) {
	case string:
		s = val
	case time.Time:
		s = val.Format("2006-01-02")
	default:
		s = fmt.Sprintf("%v", val)
	}
	return url.PathEscape(s)
}

// -----------------------------------------------------------------------------
// NoOp Partitioner (internal)
// -----------------------------------------------------------------------------

// noopPartitioner returns empty partition keys for all records.
type noopPartitioner struct{}

func newNoopPartitioner() partitioner {
	return &noopPartitioner{}
}

func (n *noopPartitioner) name() string {
	return "noop"
}

// group returns every record under the empty key, without copying them.
// An empty batch has no group, so a write of no records persists no data
// file, as with the Hive partitioner.
func (n *noopPartitioner) group(records []any) (map[string][]any, error) {
	if len(records) == 0 {
		return map[string][]any{}, nil
	}
	return map[string][]any{"": records}, nil
}

func (n *noopPartitioner) isNoop() bool {
	return true
}

func (n *noopPartitioner) keys() []string { return nil }
