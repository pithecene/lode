package lode_test

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"
)

type tsRow struct {
	TS time.Time `parquet:"ts"`
}

type bytesRow struct {
	Payload []byte `parquet:"payload"`
}

type customAnyMap map[string]any

func TestAudit_ParquetTimestampBounds(t *testing.T) {
	t.Parallel()

	schema := lode.ParquetSchema{
		Fields: []lode.ParquetField{
			{Name: "ts", Type: lode.ParquetTimestamp, Nullable: false},
		},
	}
	mapCodec := lode.Parquet[lode.D](schema).(lode.StatisticalCodec[lode.D])
	structCodec := lode.Parquet[tsRow](schema).(lode.StatisticalCodec[tsRow])

	minTS := time.Unix(0, math.MinInt64).UTC()
	maxTS := time.Unix(0, math.MaxInt64).UTC()

	t.Run("exact min and max int64 nanosecond instants round-trip", func(t *testing.T) {
		t.Parallel()
		zone := time.FixedZone("UTC+2", 2*3600)
		records := []lode.D{
			{"ts": minTS.In(zone)},
			{"ts": maxTS.Format(time.RFC3339Nano)},
		}
		var buf bytes.Buffer
		stats, err := mapCodec.EncodeWithStats(&buf, records)
		if err != nil {
			t.Fatalf("EncodeWithStats(bounds) = %v", err)
		}
		assertTimestampBoundsStats(t, stats, minTS, maxTS)

		decoded, err := mapCodec.Decode(&buf)
		if err != nil || len(decoded) != 2 {
			t.Fatalf("Decode(bounds) = %v, %v", decoded, err)
		}
		if gotMin, ok := decoded[0]["ts"].(time.Time); !ok || !gotMin.Equal(minTS) {
			t.Fatalf("decoded[0] = %v, want %s", decoded[0]["ts"], minTS)
		}
		if gotMax, ok := decoded[1]["ts"].(time.Time); !ok || !gotMax.Equal(maxTS) {
			t.Fatalf("decoded[1] = %v, want %s", decoded[1]["ts"], maxTS)
		}
	})

	t.Run("out-of-range time.Time and RFC3339 inputs fail with ErrSchemaViolation", func(t *testing.T) {
		t.Parallel()
		badTimes := []time.Time{
			{},
			minTS.Add(-time.Nanosecond),
			maxTS.Add(time.Nanosecond),
			time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		for _, bad := range badTimes {
			assertTimestampRejected(t, mapCodec, structCodec, bad)
		}
	})
}

func assertTimestampBoundsStats(t *testing.T, stats *lode.ObjectStats, wantMin, wantMax time.Time) {
	t.Helper()
	if stats == nil || len(stats.Columns) != 1 {
		t.Fatalf("stats = %+v, want 1 column", stats)
	}
	col := stats.Columns[0]
	gotMin, okMin := col.Min.(time.Time)
	gotMax, okMax := col.Max.(time.Time)
	if !okMin || !okMax || !gotMin.Equal(wantMin) || !gotMax.Equal(wantMax) {
		t.Fatalf("stats bounds = (%v, %v), want (%s, %s)", col.Min, col.Max, wantMin, wantMax)
	}
}

func assertTimestampRejected(
	t *testing.T,
	mapCodec lode.StatisticalCodec[lode.D],
	structCodec lode.StatisticalCodec[tsRow],
	bad time.Time,
) {
	t.Helper()
	var buf bytes.Buffer
	if _, err := structCodec.EncodeWithStats(&buf, []tsRow{{TS: bad}}); !errors.Is(err, lode.ErrSchemaViolation) {
		t.Fatalf("struct Encode(%s) = %v, want ErrSchemaViolation", bad.Format(time.RFC3339Nano), err)
	}
	buf.Reset()
	if _, err := mapCodec.EncodeWithStats(&buf, []lode.D{{"ts": bad}}); !errors.Is(err, lode.ErrSchemaViolation) {
		t.Fatalf("map time.Time Encode(%s) = %v, want ErrSchemaViolation", bad.Format(time.RFC3339Nano), err)
	}
	buf.Reset()
	rfc := bad.UTC().Format(time.RFC3339Nano)
	if _, err := mapCodec.EncodeWithStats(&buf, []lode.D{{"ts": rfc}}); !errors.Is(err, lode.ErrSchemaViolation) {
		t.Fatalf("map RFC3339 Encode(%q) = %v, want ErrSchemaViolation", rfc, err)
	}
}

func TestAudit_ParquetNilByteSliceParity(t *testing.T) {
	t.Parallel()

	nullableSchema := lode.ParquetSchema{
		Fields: []lode.ParquetField{
			{Name: "payload", Type: lode.ParquetBytes, Nullable: true},
		},
	}
	mapCodec := lode.Parquet[lode.D](nullableSchema).(lode.StatisticalCodec[lode.D])
	customMapCodec := lode.Parquet[customAnyMap](nullableSchema).(lode.StatisticalCodec[customAnyMap])
	structCodec := lode.Parquet[bytesRow](nullableSchema).(lode.StatisticalCodec[bytesRow])

	t.Run("nullable nil byte slice encodes as null with NullCount=1 across struct and map binders", func(t *testing.T) {
		t.Parallel()

		var structBuf, mapBuf, customBuf bytes.Buffer
		sStats, err := structCodec.EncodeWithStats(&structBuf, []bytesRow{{Payload: nil}})
		if err != nil {
			t.Fatalf("struct EncodeWithStats = %v", err)
		}
		mStats, err := mapCodec.EncodeWithStats(&mapBuf, []lode.D{{"payload": []byte(nil)}})
		if err != nil {
			t.Fatalf("map EncodeWithStats = %v", err)
		}
		cStats, err := customMapCodec.EncodeWithStats(&customBuf, []customAnyMap{{"payload": []byte(nil)}})
		if err != nil {
			t.Fatalf("customMap EncodeWithStats = %v", err)
		}

		for name, st := range map[string]*lode.ObjectStats{"struct": sStats, "map": mStats, "customMap": cStats} {
			if st == nil || len(st.Columns) != 1 || st.Columns[0].NullCount != 1 {
				t.Fatalf("%s stats = %+v, want NullCount=1", name, st)
			}
		}

		for name, raw := range map[string][]byte{"struct": structBuf.Bytes(), "map": mapBuf.Bytes(), "customMap": customBuf.Bytes()} {
			gotMaps, err := mapCodec.Decode(bytes.NewReader(raw))
			if err != nil || len(gotMaps) != 1 || gotMaps[0]["payload"] != nil {
				t.Fatalf("%s decoded as map = %#v, %v; want payload=nil", name, gotMaps, err)
			}
			gotStructs, err := structCodec.Decode(bytes.NewReader(raw))
			if err != nil || len(gotStructs) != 1 || gotStructs[0].Payload != nil {
				t.Fatalf("%s decoded as struct = %#v, %v; want Payload=nil", name, gotStructs, err)
			}
		}
	})

	t.Run("non-nil empty byte slice remains present with NullCount=0", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		st, err := mapCodec.EncodeWithStats(&buf, []lode.D{{"payload": []byte{}}})
		if err != nil {
			t.Fatalf("EncodeWithStats([]byte{}) = %v", err)
		}
		if st.Columns[0].NullCount != 0 {
			t.Fatalf("NullCount = %d, want 0", st.Columns[0].NullCount)
		}
		got, err := mapCodec.Decode(&buf)
		if err != nil || len(got) != 1 {
			t.Fatalf("Decode = %v, %v", got, err)
		}
		b, ok := got[0]["payload"].([]byte)
		if !ok || b == nil || len(b) != 0 {
			t.Fatalf("decoded payload = %#v, want non-nil empty []byte", got[0]["payload"])
		}
	})

	t.Run("non-nullable nil byte slice fails with ErrSchemaViolation", func(t *testing.T) {
		t.Parallel()
		reqSchema := lode.ParquetSchema{
			Fields: []lode.ParquetField{
				{Name: "payload", Type: lode.ParquetBytes, Nullable: false},
			},
		}
		reqMap := lode.Parquet[lode.D](reqSchema)
		var buf bytes.Buffer
		if err := reqMap.Encode(&buf, []lode.D{{"payload": []byte(nil)}}); !errors.Is(err, lode.ErrSchemaViolation) {
			t.Fatalf("required []byte(nil) = %v, want ErrSchemaViolation", err)
		}
	})

	t.Run("nil byte slice on nullable non-bytes column still fails with ErrSchemaViolation", func(t *testing.T) {
		t.Parallel()
		nonBytesSchema := lode.ParquetSchema{
			Fields: []lode.ParquetField{
				{Name: "count", Type: lode.ParquetInt64, Nullable: true},
			},
		}
		nonBytesMap := lode.Parquet[lode.D](nonBytesSchema)
		var buf bytes.Buffer
		if err := nonBytesMap.Encode(&buf, []lode.D{{"count": []byte(nil)}}); !errors.Is(err, lode.ErrSchemaViolation) {
			t.Fatalf("nullable int64 with []byte(nil) = %v, want ErrSchemaViolation", err)
		}
	})
}
