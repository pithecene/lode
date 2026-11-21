package chaos

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/testify/v2/assert"
)

// The memory checks must fail on the leaks they exist to catch: a goroutine
// that outlives the run, and heap that the run retains.
func TestMemoryBaseline_DetectsLeaks(t *testing.T) {
	t.Run("a goroutine that outlives the run", func(t *testing.T) {
		var baseline MemoryBaseline
		var err error
		assert.NoGoRoutineLeak(t, func() {
			baseline = TakeMemoryBaseline()
			release := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() { <-release })
			err = baseline.goroutinesReleased(100*time.Millisecond, nil)
			close(release)
			wg.Wait()
		})
		if err == nil || !strings.HasPrefix(err.Error(), "goroutines-released") {
			t.Fatalf("goroutinesReleased = %v, want a goroutines-released error", err)
		}
		if err := baseline.goroutinesReleased(time.Second, nil); err != nil {
			t.Errorf("after the goroutine exits: %v", err)
		}
	})

	t.Run("a leak masked by an exiting goroutine", func(t *testing.T) {
		var err error
		assert.NoGoRoutineLeak(t, func() {
			exit := make(chan struct{})
			var exited sync.WaitGroup
			exited.Go(func() { <-exit })
			baseline := TakeMemoryBaseline()
			close(exit) // a goroutine of the baseline exits ...
			exited.Wait()
			release := make(chan struct{})
			var leaked sync.WaitGroup
			leaked.Go(func() { <-release }) // ... while one started since leaks
			err = baseline.goroutinesReleased(100*time.Millisecond, nil)
			close(release)
			leaked.Wait()
		})
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
	// The leak assertion's own goroutine starts after the baseline; accept
	// it as lingering so that the error must come from the leaked goroutine.
	// The predicate runs on the leak assertion's goroutine, which the
	// assertion joins before it returns.
	assertionSeen := false
	wrapped := func(stack string) bool {
		if lingering(stack) {
			return true
		}
		if strings.Contains(stack, "github.com/go-openapi/testify/v2/internal/leak.") {
			assertionSeen = true
			return true
		}
		return false
	}
	var err error
	assert.NoGoRoutineLeak(t, func() {
		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() { <-release })
		err = baseline.goroutinesReleased(100*time.Millisecond, wrapped)
		close(release)
		wg.Wait()
	})
	if !assertionSeen {
		t.Fatal("no goroutine stack named testify's leak package; the lingering filter no longer excludes the leak assertion's goroutine")
	}
	if err == nil || !strings.HasPrefix(err.Error(), "goroutines-released") {
		t.Fatalf("goroutinesReleased = %v, want a goroutines-released error", err)
	}
}
