// Package loadgen is an open-loop load generator. Arrivals are Poisson at a
// rate that may change over time, and every request is sent on schedule
// whether or not earlier ones have returned. That matters: a closed-loop
// generator (N workers each waiting for a reply) slows down when the system
// slows down, which hides exactly the overload behaviour the lab measures.
package loadgen

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mohith26/switchyard/internal/metrics"
)

// Stream is one traffic class.
type Stream struct {
	Class    string
	Rate     func(t time.Duration) float64 // requests/sec at time t
	Path     func(rng *rand.Rand) string
	ClientID func(rng *rand.Rand) string // optional X-Client-ID
}

// ClassStats counts outcomes for one class. Everything is recorded at
// completion time.
type ClassStats struct {
	Sent, OK, Err5xx, Shed, Limited, Timeout, ConnErr metrics.Counter
	Hit, Stale, Miss, Coalesced, Pass                 metrics.Counter
	Latency                                           metrics.Histogram // successful requests
	Inflight                                          metrics.Gauge

	genMu sync.Mutex
	gens  map[string]*metrics.Counter
}

func (c *ClassStats) gen(g string) {
	c.genMu.Lock()
	ctr, ok := c.gens[g]
	if !ok {
		ctr = &metrics.Counter{}
		c.gens[g] = ctr
	}
	c.genMu.Unlock()
	ctr.Inc()
}

// Gens returns a copy of the per-generation (per edge process) success counts.
func (c *ClassStats) Gens() map[string]uint64 {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	out := make(map[string]uint64, len(c.gens))
	for k, v := range c.gens {
		out[k] = v.Load()
	}
	return out
}

type Generator struct {
	Target  string // e.g. "http://127.0.0.1:8080"
	Streams []Stream
	Timeout time.Duration
	Seed    uint64

	client *http.Client
	stats  map[string]*ClassStats
	wg     sync.WaitGroup
	start  atomic.Int64
}

func New(target string, timeout time.Duration, seed uint64, streams ...Stream) *Generator {
	g := &Generator{Target: target, Streams: streams, Timeout: timeout, Seed: seed, stats: map[string]*ClassStats{}}
	for _, s := range streams {
		if _, ok := g.stats[s.Class]; !ok {
			g.stats[s.Class] = &ClassStats{gens: map[string]*metrics.Counter{}}
		}
	}
	g.client = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        16384,
		MaxIdleConnsPerHost: 8192,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 15 * time.Second}).DialContext,
	}}
	return g
}

// Stats returns the counters for a class.
func (g *Generator) Stats(class string) *ClassStats { return g.stats[class] }

// Classes lists traffic classes.
func (g *Generator) Classes() []string {
	var out []string
	seen := map[string]bool{}
	for _, s := range g.Streams {
		if !seen[s.Class] {
			seen[s.Class] = true
			out = append(out, s.Class)
		}
	}
	return out
}

// Elapsed is time since Run started.
func (g *Generator) Elapsed() time.Duration {
	s := g.start.Load()
	if s == 0 {
		return 0
	}
	return time.Duration(time.Now().UnixNano() - s)
}

// Run generates load for d (or until ctx ends), then waits for stragglers.
func (g *Generator) Run(ctx context.Context, d time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	g.start.Store(time.Now().UnixNano())
	var sched sync.WaitGroup
	for i, s := range g.Streams {
		sched.Add(1)
		go func(i int, s Stream) {
			defer sched.Done()
			g.schedule(ctx, s, rand.New(rand.NewPCG(g.Seed, uint64(i)+1)))
		}(i, s)
	}
	sched.Wait()
	g.wg.Wait()
}

func poisson(rng *rand.Rand, lambda float64) int {
	if lambda <= 0 {
		return 0
	}
	if lambda > 30 { // normal approximation
		n := int(math.Round(lambda + math.Sqrt(lambda)*rng.NormFloat64()))
		if n < 0 {
			return 0
		}
		return n
	}
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= rng.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

func (g *Generator) schedule(ctx context.Context, s Stream, rng *rand.Rand) {
	st := g.stats[s.Class]
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	begin := time.Now()
	last := begin
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			dt := now.Sub(last).Seconds()
			last = now
			n := poisson(rng, s.Rate(now.Sub(begin))*dt)
			for i := 0; i < n; i++ {
				path := s.Path(rng)
				cid := ""
				if s.ClientID != nil {
					cid = s.ClientID(rng)
				}
				g.wg.Add(1)
				go g.fire(st, path, cid)
			}
		}
	}
}

func (g *Generator) fire(st *ClassStats, path, cid string) {
	defer g.wg.Done()
	st.Sent.Inc()
	st.Inflight.Add(1)
	defer st.Inflight.Add(-1)
	ctx, cancel := context.WithTimeout(context.Background(), g.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.Target+path, nil)
	if err != nil {
		st.ConnErr.Inc()
		return
	}
	if cid != "" {
		req.Header.Set("X-Client-ID", cid)
	}
	t0 := time.Now()
	resp, err := g.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) || ctx.Err() != nil {
			st.Timeout.Inc()
		} else {
			st.ConnErr.Inc()
		}
		return
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil {
		if ctx.Err() != nil {
			st.Timeout.Inc()
		} else {
			st.ConnErr.Inc()
		}
		return
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		st.OK.Inc()
		st.Latency.Record(time.Since(t0))
		if gen := resp.Header.Get("X-Switchyard-Gen"); gen != "" {
			st.gen(gen)
		}
		switch resp.Header.Get("X-Cache") {
		case "HIT":
			st.Hit.Inc()
		case "STALE":
			st.Stale.Inc()
		case "MISS":
			st.Miss.Inc()
		case "COALESCED":
			st.Coalesced.Inc()
		case "PASS":
			st.Pass.Inc()
		}
	case resp.StatusCode == http.StatusTooManyRequests:
		st.Limited.Inc()
	case resp.StatusCode == http.StatusServiceUnavailable && resp.Header.Get("X-Switchyard-Reason") == "shed":
		st.Shed.Inc()
	default:
		st.Err5xx.Inc()
	}
}

// IsConnRefused is exported for callers that classify errors themselves.
func IsConnRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

// Warm requests every path once with bounded concurrency, filling the caches
// before a measured run starts.
func Warm(ctx context.Context, target string, paths []string, concurrency int) error {
	client := &http.Client{Timeout: 5 * time.Second}
	ch := make(chan string)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range ch {
				resp, err := client.Get(target + p)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	for _, p := range paths {
		select {
		case <-ctx.Done():
		case ch <- p:
			continue
		}
		break
	}
	close(ch)
	wg.Wait()
	return firstErr
}
