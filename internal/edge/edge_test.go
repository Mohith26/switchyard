package edge

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mohith26/switchyard/internal/cachenode"
	"github.com/Mohith26/switchyard/internal/origin"
	"github.com/Mohith26/switchyard/internal/retry"
	"github.com/Mohith26/switchyard/internal/upstream"
)

type cluster struct {
	origin *origin.Server
	caches []*cachenode.Server
	edge   *Server
	url    string
}

func newCluster(t *testing.T, n int, cfg Config) *cluster {
	t.Helper()
	o := origin.New(origin.Config{Workers: 64, Median: time.Millisecond})
	if err := o.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	cl := &cluster{origin: o}
	for i := 0; i < n; i++ {
		c := cachenode.New(cachenode.Config{Name: "c", Coalesce: true,
			Upstream: upstream.Config{Endpoints: []string{o.Addr()}, AttemptTimeout: time.Second, Retry: retry.Config{Mode: retry.None}}})
		if err := c.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		cl.caches = append(cl.caches, c)
		cfg.CacheNodes = append(cfg.CacheNodes, c.Addr())
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	hs := &http.Server{Handler: e}
	go hs.Serve(ln)
	cl.edge, cl.url = e, "http://"+ln.Addr().String()
	t.Cleanup(func() {
		hs.Close()
		e.Close()
		for _, c := range cl.caches {
			c.Close()
		}
		o.Close()
	})
	return cl
}

func fetch(url string, hdr map[string]string) (int, http.Header, error) {
	req, _ := http.NewRequest("GET", url, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header, nil
}

func TestSameKeyAlwaysSameNode(t *testing.T) {
	cl := newCluster(t, 4, Config{Hashing: "ring"})
	for _, k := range []string{"/wiki/A", "/wiki/B", "/wiki/C"} {
		var first string
		for i := 0; i < 5; i++ {
			_, h, err := fetch(cl.url+k, nil)
			if err != nil {
				t.Fatal(err)
			}
			if first == "" {
				first = h.Get("X-Switchyard-Node")
			} else if h.Get("X-Switchyard-Node") != first {
				t.Fatalf("key %s moved between nodes with no membership change", k)
			}
		}
	}
}

func TestFailoverWhenANodeDies(t *testing.T) {
	cl := newCluster(t, 3, Config{Hashing: "ring", Failover: true, HealthInterval: Duration(50 * time.Millisecond)})
	cl.caches[1].Kill()
	for i := 0; i < 300; i++ {
		code, _, err := fetch(cl.url+"/wiki/page-"+string(rune('a'+i%26))+string(rune('a'+i/26)), nil)
		if err != nil || code != 200 {
			t.Fatalf("request %d: code=%d err=%v", i, code, err)
		}
	}
	if got := len(cl.edge.Placement()); got != 2 {
		t.Fatalf("placement has %d nodes, want the 2 live ones", got)
	}
}

func TestRateLimitPerClient(t *testing.T) {
	cl := newCluster(t, 1, Config{RateLimit: &RateLimitConfig{RPS: 1, Burst: 3, Header: "X-Client-ID"}})
	codes := map[int]int{}
	for i := 0; i < 10; i++ {
		c, _, _ := fetch(cl.url+"/api/x", map[string]string{"X-Client-ID": "noisy"})
		codes[c]++
	}
	if codes[200] != 3 || codes[429] != 7 {
		t.Fatalf("noisy client: %v", codes)
	}
	if c, _, _ := fetch(cl.url+"/api/x", map[string]string{"X-Client-ID": "quiet"}); c != 200 {
		t.Fatal("a different client must not be limited")
	}
}

func TestSheddingRejectsFastOverLimit(t *testing.T) {
	cl := newCluster(t, 1, Config{Shedding: &SheddingConfig{Initial: 2, Min: 1, Max: 2}})
	cl.origin.SetSlowdown(300) // ~300ms per request
	var wg sync.WaitGroup
	var shed, ok atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t0 := time.Now()
			c, h, _ := fetch(cl.url+"/api/x", nil)
			switch {
			case c == 503 && h.Get("X-Switchyard-Reason") == "shed":
				shed.Add(1)
				if time.Since(t0) > 100*time.Millisecond {
					t.Error("a shed request must be answered immediately")
				}
			case c == 200:
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 2 || shed.Load() != 8 {
		t.Fatalf("ok=%d shed=%d, want 2 admitted and 8 shed", ok.Load(), shed.Load())
	}
}

// Swapping the configuration while requests are in flight must not fail any.
func TestHotReloadUnderLoadDropsNothing(t *testing.T) {
	cl := newCluster(t, 4, Config{Hashing: "ring"})
	cfg := cl.edge.Config()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var fails, total atomic.Int32
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				code, _, err := fetch(cl.url+"/wiki/k"+string(rune('a'+(w*7+i)%26)), nil)
				total.Add(1)
				if err != nil || code != 200 {
					fails.Add(1)
				}
			}
		}(w)
	}
	for i := 0; i < 20; i++ {
		time.Sleep(20 * time.Millisecond)
		next := cfg
		if i%2 == 0 {
			next.CacheNodes = cfg.CacheNodes[:3]
			next.Hashing = "modn"
		}
		if err := cl.edge.Apply(next); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	wg.Wait()
	if fails.Load() != 0 {
		t.Fatalf("%d of %d requests failed across 20 reloads", fails.Load(), total.Load())
	}
	if cl.edge.Reloads.Load() != 20 {
		t.Fatalf("reloads counted: %d", cl.edge.Reloads.Load())
	}
}

func TestRejectsInvalidConfig(t *testing.T) {
	for _, c := range []Config{
		{},
		{CacheNodes: []string{"a"}, Hashing: "maglev"},
		{CacheNodes: []string{"a"}, BoundedLoad: 0.9},
		{CacheNodes: []string{"a"}, RateLimit: &RateLimitConfig{RPS: 0, Burst: 1}},
	} {
		if err := c.Validate(); err == nil {
			t.Errorf("config %+v should be rejected", c)
		}
	}
}

// BenchmarkEdgeCacheHit measures the full request path for a cache hit: client
// -> edge (placement, forwarding) -> cache node -> back, over real loopback HTTP.
func BenchmarkEdgeCacheHit(b *testing.B) {
	t := &testing.T{}
	cl := newCluster(t, 3, Config{Hashing: "ring"})
	keys := make([]string, 256)
	for i := range keys {
		keys[i] = cl.url + "/wiki/bench-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		fetch(keys[i], nil)
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024}}
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			resp, err := client.Get(keys[i&255])
			if err != nil {
				b.Error(err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			i++
		}
	})
}

// TestFrontsAGenericHTTPBackend puts the edge and a cache node in front of a
// plain HTTP server that knows nothing about Switchyard, the way you would put
// it in front of an existing service.
func TestFrontsAGenericHTTPBackend(t *testing.T) {
	var hits sync.Map
	count := func(p string) int64 {
		v, _ := hits.LoadOrStore(p, new(atomic.Int64))
		return v.(*atomic.Int64).Load()
	}
	backend := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, _ := hits.LoadOrStore(r.URL.Path, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
		switch {
		case r.URL.Path == "/healthz":
		case len(r.URL.Path) > 8 && r.URL.Path[:8] == "/static/":
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.Write([]byte("asset"))
		default:
			w.Write([]byte(`{"user":"` + r.Header.Get("Authorization") + `"}`)) // no caching headers
		}
	})}
	bl, _ := net.Listen("tcp", "127.0.0.1:0")
	go backend.Serve(bl)
	defer backend.Close()

	c := cachenode.New(cachenode.Config{Name: "c", Coalesce: true,
		Upstream: upstream.Config{Endpoints: []string{bl.Addr().String()}, AttemptTimeout: time.Second, Retry: retry.Config{Mode: retry.None}}})
	if err := c.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	e, err := New(Config{CacheNodes: []string{c.Addr()}})
	if err != nil {
		t.Fatal(err)
	}
	el, _ := net.Listen("tcp", "127.0.0.1:0")
	hs := &http.Server{Handler: e}
	go hs.Serve(el)
	defer hs.Close()
	base := "http://" + el.Addr().String()

	for i, want := range []string{"MISS", "HIT", "HIT"} {
		_, h, err := fetch(base+"/static/app.js", nil)
		if err != nil || h.Get("X-Cache") != want {
			t.Fatalf("static request %d: X-Cache=%q err=%v, want %s", i, h.Get("X-Cache"), err, want)
		}
	}
	if n := count("/static/app.js"); n != 1 {
		t.Fatalf("backend saw the cacheable asset %d times, want 1", n)
	}
	for i := 0; i < 3; i++ {
		fetch(base+"/api/me", nil)
	}
	if n := count("/api/me"); n != 3 {
		t.Fatalf("a response without Cache-Control must never be cached; backend saw %d of 3", n)
	}
	// Credentialed requests bypass the shared cache entirely.
	_, h, _ := fetch(base+"/static/app.js", map[string]string{"Authorization": "Bearer x"})
	if h.Get("X-Cache") != "PASS" || count("/static/app.js") != 2 {
		t.Fatalf("credentialed request: X-Cache=%q; must go straight to the backend", h.Get("X-Cache"))
	}
}
