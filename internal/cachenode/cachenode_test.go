package cachenode

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mohith26/switchyard/internal/origin"
	"github.com/Mohith26/switchyard/internal/retry"
	"github.com/Mohith26/switchyard/internal/upstream"
)

func setup(t *testing.T, coalesce bool, swr time.Duration, oc origin.Config) (*Server, *origin.Server) {
	t.Helper()
	o := origin.New(oc)
	if err := o.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	c := New(Config{Name: "c1", Coalesce: coalesce, StaleWhileRevalidate: swr,
		Upstream: upstream.Config{Endpoints: []string{o.Addr()}, AttemptTimeout: 2 * time.Second, Retry: retry.Config{Mode: retry.None}}})
	if err := c.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); o.Close() })
	return c, o
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("X-Cache")
}

func TestMissThenHit(t *testing.T) {
	c, o := setup(t, true, 0, origin.Config{Workers: 4, Median: time.Millisecond})
	base := "http://" + c.Addr()
	if _, x := get(t, base+"/wiki/Go"); x != "MISS" {
		t.Fatalf("first request: %s", x)
	}
	if _, x := get(t, base+"/wiki/Go"); x != "HIT" {
		t.Fatalf("second request: %s", x)
	}
	if o.Received.Load() != 1 {
		t.Fatalf("origin saw %d requests", o.Received.Load())
	}
}

func TestUncacheablePassesThrough(t *testing.T) {
	c, o := setup(t, true, 0, origin.Config{Workers: 4, Median: time.Millisecond})
	for i := 0; i < 3; i++ {
		if _, x := get(t, "http://"+c.Addr()+"/api/items/1"); x != "PASS" {
			t.Fatalf("got %s", x)
		}
	}
	if o.Received.Load() != 3 {
		t.Fatal("no-store responses must never be cached")
	}
}

// The core property: 200 concurrent misses for one key produce one origin fetch.
func TestCoalescingCollapsesConcurrentMisses(t *testing.T) {
	for _, tc := range []struct {
		coalesce  bool
		maxOrigin uint64
	}{{true, 1}, {false, 150}} {
		c, o := setup(t, tc.coalesce, 0, origin.Config{Workers: 256, Median: 200 * time.Millisecond})
		var wg sync.WaitGroup
		var collapsed atomic.Int32
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				code, x := get(t, "http://"+c.Addr()+"/wiki/Hot")
				if code != 200 {
					t.Errorf("status %d", code)
				}
				if x == "COALESCED" {
					collapsed.Add(1)
				}
			}()
		}
		wg.Wait()
		got := o.Received.Load()
		if tc.coalesce && got != tc.maxOrigin {
			t.Fatalf("coalescing on: origin saw %d fetches, want 1", got)
		}
		if !tc.coalesce && got < tc.maxOrigin {
			t.Fatalf("coalescing off: origin saw only %d fetches; the test is not exercising concurrency", got)
		}
		if tc.coalesce && collapsed.Load() < 150 {
			t.Fatalf("only %d requests reported COALESCED", collapsed.Load())
		}
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	c, o := setup(t, true, 10*time.Second, origin.Config{Workers: 4, Median: 150 * time.Millisecond,
		TTLOverride: map[string]time.Duration{"/wiki/News": time.Second}})
	base := "http://" + c.Addr() + "/wiki/News"
	get(t, base)
	time.Sleep(1100 * time.Millisecond) // expire it
	t0 := time.Now()
	if _, x := get(t, base); x != "STALE" {
		t.Fatalf("want STALE, got %s", x)
	}
	if time.Since(t0) > 50*time.Millisecond {
		t.Fatal("a stale hit must not wait for the refresh")
	}
	// Keep reading while the one background refresh runs: every read is served
	// stale without waiting, and none of them may start a second refresh.
	deadline := time.Now().Add(2 * time.Second)
	var x string
	for time.Now().Before(deadline) {
		if _, x = get(t, base); x == "HIT" {
			break
		}
		if x != "STALE" {
			t.Fatalf("while refreshing: %s", x)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if x != "HIT" {
		t.Fatal("background refresh never landed")
	}
	if o.Received.Load() != 2 {
		t.Fatalf("origin saw %d fetches, want exactly 1 refresh", o.Received.Load())
	}
}

func TestDeadlineHeaderShortensFetch(t *testing.T) {
	c, _ := setup(t, false, 0, origin.Config{Workers: 1, Median: time.Second})
	req, _ := http.NewRequest("GET", "http://"+c.Addr()+"/api/slow", nil)
	req.Header.Set(DeadlineHeader, itoa(time.Now().Add(100*time.Millisecond).UnixMilli()))
	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout || time.Since(t0) > 400*time.Millisecond {
		t.Fatalf("status %d after %v; want a fast 504", resp.StatusCode, time.Since(t0))
	}
}

func itoa(n int64) string {
	b := []byte{}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
