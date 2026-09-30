// Package edge is the front door. For every request it applies, in order:
// per-client rate limiting, adaptive-concurrency load shedding, key-based
// placement onto a cache node (consistent hashing with bounded loads, or mod-N
// as a baseline), and failover to the next node on the ring when the chosen
// node refuses the connection. Configuration can be swapped atomically while
// serving (hot reload) without dropping requests.
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mohith26/switchyard/internal/balance"
	"github.com/Mohith26/switchyard/internal/cachenode"
	"github.com/Mohith26/switchyard/internal/health"
	"github.com/Mohith26/switchyard/internal/limit"
	"github.com/Mohith26/switchyard/internal/metrics"
	"github.com/Mohith26/switchyard/internal/ring"
)

type RateLimitConfig struct {
	RPS    float64 `json:"rps"`
	Burst  int     `json:"burst"`
	Header string  `json:"header"` // client identity header; falls back to the remote IP
}

type SheddingConfig struct {
	Initial int      `json:"initial"`
	Min     int      `json:"min"`
	Max     int      `json:"max"`
	Window  Duration `json:"window"`
}

type Config struct {
	Name           string           `json:"name"`
	Listen         string           `json:"listen,omitempty"`
	CacheNodes     []string         `json:"cache_nodes"`
	Hashing        string           `json:"hashing"` // "ring" or "modn"
	VNodes         int              `json:"vnodes"`
	BoundedLoad    float64          `json:"bounded_load"` // 0 disables; e.g. 1.25
	HealthPath     string           `json:"health_path"`
	HealthInterval Duration         `json:"health_interval"`
	Failover       bool             `json:"failover"`
	RequestTimeout Duration         `json:"request_timeout"`
	RateLimit      *RateLimitConfig `json:"rate_limit,omitempty"`
	Shedding       *SheddingConfig  `json:"shedding,omitempty"`
}

// Duration is a time.Duration that reads and writes as "250ms" in JSON.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		var n int64
		if err2 := json.Unmarshal(b, &n); err2 != nil {
			return err
		}
		*d = Duration(n)
		return nil
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

func (c *Config) defaults() {
	if c.Hashing == "" {
		c.Hashing = "ring"
	}
	if c.VNodes <= 0 {
		c.VNodes = 160
	}
	if c.HealthPath == "" {
		c.HealthPath = "/healthz"
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = Duration(time.Second)
	}
}

// Validate rejects configurations that cannot serve traffic.
func (c *Config) Validate() error {
	if len(c.CacheNodes) == 0 {
		return errors.New("edge: at least one cache node is required")
	}
	if c.Hashing != "ring" && c.Hashing != "modn" && c.Hashing != "" {
		return errors.New("edge: hashing must be ring or modn")
	}
	if c.BoundedLoad != 0 && c.BoundedLoad <= 1 {
		return errors.New("edge: bounded_load must be > 1")
	}
	if c.RateLimit != nil && (c.RateLimit.RPS <= 0 || c.RateLimit.Burst < 1) {
		return errors.New("edge: rate_limit needs rps > 0 and burst >= 1")
	}
	return nil
}

// placement is an immutable snapshot of which nodes are live and how keys map
// onto them. It is rebuilt (never mutated) when membership changes.
type placement struct {
	nodes  []*balance.Endpoint // the live nodes, in picker order
	picker ring.Picker
	ring   *ring.Ring // non-nil when hashing == ring
}

// routing is everything derived from one Config.
type routing struct {
	cfg     Config
	nodes   []*balance.Endpoint // all configured nodes
	outlier *health.Outlier
	limiter *limit.KeyedBuckets
	shedder *limit.Adaptive
	place   atomic.Pointer[placement]
	cancel  context.CancelFunc
	rebuild chan struct{}
}

type Server struct {
	reg *metrics.Registry
	rt  atomic.Pointer[routing]
	hc  *http.Client

	epMu sync.Mutex
	eps  map[string]*balance.Endpoint // endpoints survive reloads, keeping their stats

	Generation string // reported in X-Switchyard-Gen (the lab uses the PID)

	Requests, RateLimited, Shed, Errors, Failovers, Reloads *metrics.Counter
	Latency                                                 *metrics.Histogram

	nodeMu   sync.Mutex
	nodeReqs map[string]*metrics.Counter

	inflight sync.WaitGroup
}

func New(cfg Config) (*Server, error) {
	reg := metrics.NewRegistry()
	s := &Server{reg: reg, eps: map[string]*balance.Endpoint{}, nodeReqs: map[string]*metrics.Counter{}}
	s.Requests = reg.Counter("switchyard_edge_requests_total", "Requests received at the edge.")
	s.RateLimited = reg.Counter("switchyard_edge_rate_limited_total", "Requests rejected by the per-client rate limit.")
	s.Shed = reg.Counter("switchyard_edge_shed_total", "Requests shed by the adaptive concurrency limit.")
	s.Errors = reg.Counter("switchyard_edge_errors_total", "Requests that failed to reach any cache node.")
	s.Failovers = reg.Counter("switchyard_edge_failovers_total", "Requests retried on the next ring node.")
	s.Reloads = reg.Counter("switchyard_edge_reloads_total", "Configuration hot reloads applied.")
	s.Latency = reg.Histogram("switchyard_edge_request_seconds", "Edge request latency.")
	reg.GaugeFunc("switchyard_edge_concurrency_limit", "Current adaptive concurrency limit (0 when disabled).", func() float64 {
		if rt := s.rt.Load(); rt != nil && rt.shedder != nil {
			return float64(rt.shedder.Limit())
		}
		return 0
	})
	s.hc = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        8192,
			MaxIdleConnsPerHost: 2048,
			IdleConnTimeout:     30 * time.Second,
			DialContext:         (&net.Dialer{Timeout: 500 * time.Millisecond, KeepAlive: 15 * time.Second}).DialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	if err := s.Apply(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) Registry() *metrics.Registry { return s.reg }

func (s *Server) endpoint(addr string) *balance.Endpoint {
	s.epMu.Lock()
	defer s.epMu.Unlock()
	e, ok := s.eps[addr]
	if !ok {
		e = balance.NewEndpoint(addr)
		s.eps[addr] = e
	}
	return e
}

// NodeRequests returns the per-node request counter for addr.
func (s *Server) NodeRequests(addr string) *metrics.Counter {
	s.nodeMu.Lock()
	defer s.nodeMu.Unlock()
	c, ok := s.nodeReqs[addr]
	if !ok {
		c = s.reg.Counter("switchyard_edge_node_requests_total", "Requests sent to each cache node.", "node", addr)
		s.nodeReqs[addr] = c
	}
	return c
}

// Apply validates cfg and swaps it in atomically. In-flight requests finish on
// the routing they started with; new requests see the new one immediately.
func (s *Server) Apply(cfg Config) error {
	cfg.defaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	rt := &routing{cfg: cfg, rebuild: make(chan struct{}, 1)}
	for _, a := range cfg.CacheNodes {
		e := s.endpoint(a)
		rt.nodes = append(rt.nodes, e)
		s.NodeRequests(a)
	}
	rt.outlier = health.NewOutlier(rt.nodes, 3, 2*time.Second, 30*time.Second, 50)
	if cfg.RateLimit != nil {
		rt.limiter = limit.NewKeyed(cfg.RateLimit.RPS, cfg.RateLimit.Burst, 4096)
	}
	if sc := cfg.Shedding; sc != nil {
		rt.shedder = limit.NewAdaptive(limit.AdaptiveConfig{Initial: sc.Initial, Min: sc.Min, Max: sc.Max, Window: time.Duration(sc.Window)})
	}
	rt.place.Store(s.buildPlacement(rt))
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	go s.maintain(ctx, rt)
	if cfg.HealthInterval > 0 {
		ch := &health.Checker{Endpoints: rt.nodes, Path: cfg.HealthPath, Interval: time.Duration(cfg.HealthInterval),
			Timeout: time.Duration(cfg.HealthInterval), UnhealthyThreshold: 2, HealthyThreshold: 2, Client: s.hc,
			OnChange: func(*balance.Endpoint, bool) { rt.poke() }}
		go ch.Run(ctx)
	}
	old := s.rt.Swap(rt)
	if old != nil {
		old.cancel()
		s.Reloads.Inc()
	}
	return nil
}

// ConcurrencyLimit is the adaptive limit, or 0 when shedding is off.
func (s *Server) ConcurrencyLimit() int {
	if rt := s.rt.Load(); rt != nil && rt.shedder != nil {
		return rt.shedder.Limit()
	}
	return 0
}

// Config returns the active configuration.
func (s *Server) Config() Config { return s.rt.Load().cfg }

func (rt *routing) poke() {
	select {
	case rt.rebuild <- struct{}{}:
	default:
	}
}

// maintain rebuilds placement when health changes, and also on a short tick so
// an outlier ejection expiring puts the node back without any event.
func (s *Server) maintain(ctx context.Context, rt *routing) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-rt.rebuild:
		case <-t.C:
		}
		cur := rt.place.Load()
		next := s.buildPlacement(rt)
		if !sameNodes(cur.nodes, next.nodes) {
			rt.place.Store(next)
		}
	}
}

func sameNodes(a, b []*balance.Endpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Server) buildPlacement(rt *routing) *placement {
	now := time.Now()
	p := &placement{}
	names := []string{}
	for _, e := range rt.nodes {
		if e.Available(now) {
			p.nodes = append(p.nodes, e)
			names = append(names, e.Addr)
		}
	}
	if len(p.nodes) == 0 { // everything looks down: route to all rather than to nothing
		p.nodes = rt.nodes
		names = append([]string(nil), rt.cfg.CacheNodes...)
	}
	if rt.cfg.Hashing == "modn" {
		p.picker = ring.NewModN(names)
	} else {
		p.ring = ring.New(names, rt.cfg.VNodes)
		p.picker = p.ring
	}
	return p
}

// Placement returns the live node addresses in picker order (for the lab).
func (s *Server) Placement() []string {
	p := s.rt.Load().place.Load()
	out := make([]string, len(p.nodes))
	for i, e := range p.nodes {
		out[i] = e.Addr
	}
	return out
}

func (s *Server) choose(rt *routing, key string, exclude *balance.Endpoint) *balance.Endpoint {
	p := rt.place.Load()
	if exclude != nil {
		if p.ring != nil {
			for _, i := range p.ring.Successors(key, 2) {
				if p.nodes[i] != exclude {
					return p.nodes[i]
				}
			}
			return nil
		}
		// mod-N has no natural successor; rebuild without the failed node.
		for _, e := range p.nodes {
			if e != exclude {
				return e
			}
		}
		return nil
	}
	if p.ring != nil && rt.cfg.BoundedLoad > 1 {
		return p.nodes[p.ring.PickBounded(key, func(i int) int64 { return p.nodes[i].Inflight() }, rt.cfg.BoundedLoad)]
	}
	return p.nodes[p.picker.Pick(key)]
}

func clientKey(r *http.Request, header string) string {
	if header != "" {
		if v := r.Header.Get(header); v != "" {
			return v
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/-/healthz":
		w.WriteHeader(http.StatusOK)
		return
	case "/-/metrics":
		s.reg.Handler().ServeHTTP(w, r)
		return
	case "/-/config":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Config())
		return
	}
	s.inflight.Add(1)
	defer s.inflight.Done()
	start := time.Now()
	s.Requests.Inc()
	rt := s.rt.Load()
	if s.Generation != "" {
		w.Header().Set("X-Switchyard-Gen", s.Generation)
	}

	if rt.limiter != nil && !rt.limiter.Allow(clientKey(r, rt.cfg.RateLimit.Header)) {
		s.RateLimited.Inc()
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-Switchyard-Reason", "rate-limited")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	var release func(time.Duration, bool)
	if rt.shedder != nil {
		rel, ok := rt.shedder.Acquire()
		if !ok {
			s.Shed.Inc()
			w.Header().Set("X-Switchyard-Reason", "shed")
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
			return
		}
		release = rel
	}
	status := s.forward(w, r, rt, start)
	lat := time.Since(start)
	s.Latency.Record(lat)
	if release != nil {
		release(lat, status >= 500)
	}
}

// forward proxies r to a cache node and returns the status sent downstream.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, rt *routing, start time.Time) int {
	deadline := start.Add(time.Duration(rt.cfg.RequestTimeout))
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	key := r.URL.Path
	node := s.choose(rt, key, nil)
	for attempt := 0; attempt < 2 && node != nil; attempt++ {
		out, err := http.NewRequestWithContext(ctx, r.Method, "http://"+node.Addr+r.URL.RequestURI(), r.Body)
		if err != nil {
			break
		}
		copyHeaders(out.Header, r.Header)
		out.Header.Set(cachenode.DeadlineHeader, strconv.FormatInt(deadline.UnixMilli(), 10))
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			out.Header.Set("X-Forwarded-For", host)
		}
		s.NodeRequests(node.Addr).Inc()
		end := node.Begin()
		t0 := time.Now()
		resp, err := s.hc.Do(out)
		if err != nil {
			end(time.Since(t0), false)
			fatal := errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
			rt.outlier.Report(node, false, fatal)
			if fatal {
				rt.poke()
			}
			if fatal && rt.cfg.Failover && r.Method == http.MethodGet && ctx.Err() == nil {
				s.Failovers.Inc()
				node = s.choose(rt, key, node)
				continue
			}
			break
		}
		// Only connection-level failures count against a cache node's health. A
		// 5xx it relays from the origin says nothing about the node itself, and
		// ejecting it would move its keys and turn one origin problem into a
		// cache-wide miss storm.
		rt.outlier.Report(node, true, false)
		for k, v := range resp.Header {
			if !hopByHop[k] {
				w.Header()[k] = v
			}
		}
		w.Header().Set("X-Switchyard-Node", node.Addr)
		w.WriteHeader(resp.StatusCode)
		_, copyErr := io.Copy(w, resp.Body)
		resp.Body.Close()
		end(time.Since(t0), resp.StatusCode < 500 && copyErr == nil)
		return resp.StatusCode
	}
	s.Errors.Inc()
	code := http.StatusBadGateway
	if ctx.Err() != nil {
		code = http.StatusGatewayTimeout
	}
	http.Error(w, "no cache node reachable", code)
	return code
}

var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
}

func copyHeaders(dst, src http.Header) {
	for k, v := range src {
		if !hopByHop[k] {
			dst[k] = v
		}
	}
}

// Wait blocks until in-flight requests finish (used by graceful shutdown).
func (s *Server) Wait() { s.inflight.Wait() }

// Close stops background goroutines.
func (s *Server) Close() {
	if rt := s.rt.Load(); rt != nil {
		rt.cancel()
	}
	s.hc.CloseIdleConnections()
}
