package lode

import (
	"fmt"
	"maps"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Layout (public)
// -----------------------------------------------------------------------------

// Layout chooses how a dataset is partitioned: Unpartitioned, or Hive with
// its keys. The names within the dataset's paths come from the root's path
// options, not from the layout. Pass a layout with WithLayout.
type Layout interface {
	// scheme returns the partitioning this layout persists, and any error
	// its construction recorded.
	scheme() (partitionScheme, error)
}

// Unpartitioned returns the layout without partitions, the default.
func Unpartitioned() Layout { return unpartitioned{} }

type unpartitioned struct{}

func (unpartitioned) scheme() (partitionScheme, error) { return noopScheme, nil }

// HiveLayout partitions records into key=value directories, keys in order.
// Hive returns one; By gives it a typed partition function.
type HiveLayout struct {
	keys []string
	err  error
}

// Hive returns a Hive layout with the given partition keys, in order. Without
// By, records must be maps (D), and each key names a field. An invalid key
// set is recorded and returned, matching ErrInvalidArgument, by Root.Records.
func Hive(keys ...string) *HiveLayout {
	return &HiveLayout{keys: slices.Clone(keys), err: validateHiveKeys(keys)}
}

func (h *HiveLayout) scheme() (partitionScheme, error) {
	return partitionScheme{name: "hive", keys: h.keys}, h.err
}

// By returns the Hive layout with a function that gives a record's
// partition values by key name, so that typed records are partitioned
// without reflection and a value cannot be assigned to the wrong key:
//
//	lode.Hive("source", "day").By(func(e Event) lode.Partition {
//		return lode.Partition{"source": e.Source, "day": e.Day}
//	})
//
// The function is code, not configuration, and is not persisted. A write
// whose partition names other keys than the layout's fails. Root.Records
// rejects a function written for another record type.
func (h *HiveLayout) By[T any](values func(T) Partition) Layout {
	return keyedHive[T]{HiveLayout: h, values: values}
}

// Partition is a record's partition values by key name. A value is
// formatted as a string; a time.Time formats as its date (2006-01-02). The
// formatted value is path-escaped.
type Partition map[string]any

// keyedHive is a Hive layout with a typed partition function.
type keyedHive[T any] struct {
	*HiveLayout
	values func(T) Partition
}

// validateHiveKeys requires at least one key, each one path segment without
// "=", so that a partition path names its keys unambiguously.
func validateHiveKeys(keys []string) error {
	if len(keys) == 0 {
		return validate.Argument("Hive requires at least one partition key; use Unpartitioned for unpartitioned data")
	}
	for _, key := range keys {
		if !storepath.Segment(key) || strings.Contains(key, "=") {
			return validate.Argument("Hive: partition key %q must be one path segment without \"=\", so that a partition path names its keys unambiguously", key)
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Partitioning (internal)
// -----------------------------------------------------------------------------

// partitionScheme is the partitioning a manifest records: the partitioner's
// name and its keys in order.
type partitionScheme struct {
	name string
	keys []string
}

// noopScheme is the scheme without partitions.
var noopScheme = partitionScheme{name: "noop"}

func (p partitionScheme) isNoop() bool { return p.name == noopScheme.name }

// partitionKeyFor returns the function that computes a record's partition
// path ("k1=v1/k2=v2") under the layout, for records of type T: "" without
// partitions; the typed function of By; or, for map records, a lookup by key
// name. It rejects a By function for another record type, and a Hive layout
// without By for records that are not maps.
func partitionKeyFor[T any](l Layout) (func(T) (string, error), error) {
	scheme, err := l.scheme()
	if err != nil {
		return nil, err
	}
	if scheme.isNoop() {
		return func(T) (string, error) { return "", nil }, nil
	}
	switch typed := l.(type) {
	case keyedHive[T]:
		return func(r T) (string, error) { return hivePath(scheme.keys, typed.values(r), true) }, nil
	case *HiveLayout:
		if !mapRecords[T]() {
			return nil, validate.Argument("Hive layout for records of type %s requires By, because only map records name their keys", reflect.TypeFor[T]())
		}
		return func(r T) (string, error) { return mapPartitionPath(scheme.keys, any(r)) }, nil
	default:
		return nil, validate.Argument("layout's partition function is for another record type than %s", reflect.TypeFor[T]())
	}
}

// mapRecords reports whether T can hold map[string]any records: the map
// type itself, or an interface type a map may satisfy.
func mapRecords[T any]() bool {
	t := reflect.TypeFor[T]()
	return t == reflect.TypeFor[map[string]any]() || t.Kind() == reflect.Interface
}

// mapPartitionPath reads a map record's partition values by key name.
func mapPartitionPath(keys []string, record any) (string, error) {
	m, ok := record.(map[string]any)
	if !ok {
		return "", fmt.Errorf("hive partitioner: record must be map[string]any, got %T", record)
	}
	return hivePath(keys, Partition(m), false)
}

// hivePath joins the values of keys, in order, as key=value segments. A
// typed Partition must name exactly the layout's keys (exact); a map record
// may hold other fields.
func hivePath(keys []string, values Partition, exact bool) (string, error) {
	if exact && len(values) != len(keys) {
		return "", fmt.Errorf("hive partitioner: partition names %v, want exactly the keys %v", slices.Sorted(maps.Keys(values)), keys)
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		val, exists := values[key]
		if !exists {
			return "", fmt.Errorf("hive partitioner: missing key %q", key)
		}
		parts = append(parts, key+"="+escapeValue(val))
	}
	return strings.Join(parts, "/"), nil
}

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

// groupRecords returns the records of each partition path. Without
// partitions every record is in the group "", without copying; an empty
// batch has no group, so a write of no records persists no data file.
func groupRecords[T any](records []T, keyOf func(T) (string, error), partitioned bool) (map[string][]T, error) {
	if len(records) == 0 {
		return map[string][]T{}, nil
	}
	if !partitioned {
		return map[string][]T{"": records}, nil
	}
	groups := make(map[string][]T)
	for _, record := range records {
		key, err := keyOf(record)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], record)
	}
	return groups, nil
}
