package chaos

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The memory checks must fail on the leaks they exist to catch: a goroutine
// that outlives the run, and heap that the run retains.
func TestMemoryBaseline_DetectsLeaks(t *testing.T) {
	t.Run("a goroutine that outlives the run", func(t *testing.T) {
		baseline := TakeMemoryBaseline()
		release := make(chan struct{})
		go func() { <-release }()
		err := baseline.goroutinesReleased(100*time.Millisecond, nil)
		close(release)
		if err == nil || !strings.HasPrefix(err.Error(), "goroutines-released") {
			t.Fatalf("goroutinesReleased = %v, want a goroutines-released error", err)
		}
		if err := baseline.goroutinesReleased(time.Second, nil); err != nil {
			t.Errorf("after the goroutine exits: %v", err)
		}
	})

	t.Run("a leak masked by an exiting goroutine", func(t *testing.T) {
		exit := make(chan struct{})
		exited := make(chan struct{})
		go func() { <-exit; close(exited) }()
		baseline := TakeMemoryBaseline()
		close(exit) // a goroutine of the baseline exits ...
		<-exited
		release := make(chan struct{})
		go func() { <-release }() // ... while one started since leaks
		err := baseline.goroutinesReleased(100*time.Millisecond, nil)
		close(release)
		if err == nil || !strings.HasPrefix(err.Error(), "goroutines-released") {
			t.Fatalf("goroutinesReleased = %v, want a goroutines-released error", err)
		}
	})

	for _, proto := range []struct {
		name  string
		major int
		start func(*httptest.Server)
	}{
		{"HTTP/1.1", 1, (*httptest.Server).Start},
		{"HTTP/2", 2, func(srv *httptest.Server) { srv.EnableHTTP2 = true; srv.StartTLS() }},
	} {
		t.Run("idle "+proto.name+" connections linger, a leak beside them does not", func(t *testing.T) {
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			proto.start(srv)
			defer srv.Close()
			baseline := TakeMemoryBaseline()
			client := srv.Client()
			defer client.CloseIdleConnections()
			resp, err := client.Get(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil { // the connection returns to the pool
				t.Fatal(err)
			}
			if resp.ProtoMajor != proto.major {
				t.Fatalf("the connection speaks %s, want %s", resp.Proto, proto.name)
			}
			assertLingeringAccepted(t, baseline, testServerLingering)
		})
	}

	t.Run("heap that the run retains", func(t *testing.T) {
		baseline := TakeMemoryBaseline()
		retained := make([]byte, 2*retainedAllowance)
		_, err := baseline.RetainedHeap(0)
		if err == nil || !strings.HasPrefix(err.Error(), "heap-retained") {
			t.Fatalf("RetainedHeap = %v, want a heap-retained error", err)
		}
		runtime.KeepAlive(retained)
	})
}

// testServerLingering accepts an idle client connection and the test
// server's side of it, which runs in this process; an S3 service runs in
// another.
func testServerLingering(stack string) bool {
	return idleHTTPConnection(stack) ||
		strings.Contains(stack, "net/http.(*conn).serve") ||
		strings.Contains(stack, "net/http/internal/http2.(*serverConn)")
}

// assertLingeringAccepted requires the goroutines started since baseline to
// be accepted by lingering, and a goroutine leaked beside them to be
// reported.
func assertLingeringAccepted(t *testing.T, baseline MemoryBaseline, lingering func(string) bool) {
	t.Helper()
	if err := baseline.goroutinesReleased(time.Second, lingering); err != nil {
		t.Fatalf("an idle connection's goroutines: %v", err)
	}
	release := make(chan struct{})
	go func() { <-release }()
	err := baseline.goroutinesReleased(100*time.Millisecond, lingering)
	close(release)
	if err == nil || !strings.HasPrefix(err.Error(), "goroutines-released") {
		t.Fatalf("goroutinesReleased = %v, want a goroutines-released error", err)
	}
}
