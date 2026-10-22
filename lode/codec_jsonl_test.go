package lode

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

// The decoder's buffer grows to the longest line it meets, up to
// maxScanTokenSize, so a line far longer than the scanner's initial buffer
// decodes, and a line beyond the limit fails rather than being truncated.
func TestJSONLCodec_Decode_LineLength(t *testing.T) {
	codec := NewJSONLCodec()

	t.Run("a line of one mebibyte decodes", func(t *testing.T) {
		value := strings.Repeat("x", 1<<20)
		records, err := codec.Decode(strings.NewReader(`{"v":"` + value + `"}` + "\n" + `{"v":"next"}` + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 {
			t.Fatalf("decoded %d records, want 2", len(records))
		}
		if got := records[0].(map[string]any)["v"]; got != value {
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
