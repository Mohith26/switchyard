// Package cachenode is one node of the cache tier. Cacheable GETs (/wiki/...)
// are served from a local LRU; misses go to the upstream pool (the origin, or
// a shield cache in a tiered setup) through the resilient upstream client.
// With coalescing on, concurrent misses for a key share one fetch; with
// stale-while-revalidate on, an expired entry keeps being served while one
// background refresh runs. Everything else is passed through uncached.
package cachenode

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mohith26/switchyard/internal/cache"
	"github.com/Mohith26/switchyard/internal/metrics"
	"github.com/Mohith26/switchyard/internal/upstream"
)

type Config struct {
	Name                 string
	Capacity             int
	DefaultTTL           time.Duration
	StaleWhileRevalidate time.Duration
	Coalesce             bool
	FetchTimeout         time.Duration
	// CacheablePrefixes limits caching (and coalescing) to GETs under these
	// path prefixes. Default "/" means every GET is a candidate; a response is
	// still only stored if it carries Cache-Control: max-age and no no-store.
	CacheablePrefixes []string
	Upstream          upstream.Config
}

type Server struct {
	cfg   Config
	store *cache.Store
	group cache.Group
	up    *upstream.Client
	reg   *metrics.Registry

	Requests, Hits, Stale, Misses, Coalesced, Pass, Errors *metrics.Counter
	Latency                                                *metrics.Histogram

	mu    sync.Mutex
	addr  string
	srv   *http.Server
	alive bool
	ctx   context.Context
	stop  context.CancelFunc
}

// DeadlineHeader carries the caller's absolute deadline (unix milliseconds) so
// every hop spends only the time the client has left.
const DeadlineHeader = "X-Switchyard-Deadline"

func New(cfg Config) *Server {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 100000
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 60 * time.Second
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = 2 * time.Second
	}
	if len(cfg.CacheablePrefixes) == 0 {
		cfg.CacheablePrefixes = []string{"/"}
	}
	reg := metrics.NewRegistry()
	s := &Server{cfg: cfg, store: cache.NewStore(cfg.Capacity), up: upstream.New(cfg.Upstream), reg: reg}
	l := []string{"node", cfg.Name}
	s.Requests = reg.Counter("switchyard_cache_requests_total", "Requests received by the cache node.", l...)
	s.Hits = reg.Counter("switchyard_cache_hits_total", "Fresh cache hits.", l...)
	s.Stale = reg.Counter("switchyard_cache_stale_total", "Stale entries served while revalidating.", l...)
	s.Misses = reg.Counter("switchyard_cache_misses_total", "Misses that fetched from upstream.", l...)
	s.Coalesced = reg.Counter("switchyard_cache_coalesced_total", "Misses that waited on an in-flight fetch.", l...)
	s.Pass = reg.Counter("switchyard_cache_pass_total", "Uncacheable requests passed through.", l...)
	s.Errors = reg.Counter("switchyard_cache_errors_total", "Requests answered with an upstream error.", l...)
	s.Latency = reg.Histogram("switchyard_cache_request_seconds", "Cache node request latency.", l...)
	reg.GaugeFunc("switchyard_cache_entries", "Entries in the cache.", func() float64 { return float64(s.store.Len()) }, l...)
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.up.Run(s.ctx)
	return s
}

func (s *Server) Registry() *metrics.Registry { return s.reg }
func (s *Server) Upstream() *upstream.Client  { return s.up }
func (s *Server) Store() *cache.Store         { return s.store }
func (s *Server) Name() string                { return s.cfg.Name }

// Start listens on addr and serves in the background.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.Serve(ln)
	return nil
}

func (s *Server) Serve(ln net.Listener) {
	srv := &http.Server{Handler: s, ReadHeaderTimeout: 5 * time.Second}
	s.mu.Lock()
	s.addr, s.srv, s.alive = ln.Addr().String(), srv, true
	s.mu.Unlock()
	go srv.Serve(ln)
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Server) Up() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alive
}

// Kill drops the listener and every connection, like a crashed node.
func (s *Server) Kill() {
	s.mu.Lock()
	srv := s.srv
	s.alive = false
	s.mu.Unlock()
	if srv != nil {
		srv.Close()
	}
}

// Revive restarts on the same address. cold=true empties the cache first, as a
// real restarted process would come back empty.
func (s *Server) Revive(cold bool) error {
	if s.Up() {
		return nil
	}
	if cold {
		s.store.Purge()
	}
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ {
		if ln, err = net.Listen("tcp", s.Addr()); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	s.Serve(ln)
	return nil
}

func (s *Server) Close() {
	s.Kill()
	s.stop()
	s.up.CloseIdle()
}

// cacheable reports whether a request may be answered from, or coalesced into,
// a shared cached copy. Requests carrying credentials never are: a response to
// one user's Authorization or Cookie header must not be handed to another.
func (s *Server) cacheable(r *http.Request) bool {
	if r.Method != http.MethodGet || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
		return false
	}
	if strings.Contains(r.Header.Get("Cache-Control"), "no-store") {
		return false
	}
	for _, p := range s.cfg.CacheablePrefixes {
		if strings.HasPrefix(r.URL.Path, p) {
			return true
		}
	}
	return false
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		return
	case "/metrics":
		s.reg.Handler().ServeHTTP(w, r)
		return
	}
	start := time.Now()
	s.Requests.Inc()
	defer func() { s.Latency.Record(time.Since(start)) }()
	w.Header().Set("X-Cache-Node", s.cfg.Name)

	ctx, cancel := s.requestContext(r)
	defer cancel()
	fwd := http.Header{}
	for _, h := range []string{"X-Client-ID", "Authorization", "Cookie", "Accept", "Accept-Language"} {
		if v := r.Header.Get(h); v != "" {
			fwd.Set(h, v)
		}
	}

	if !s.cacheable(r) {
		s.Pass.Inc()
		resp, err := s.up.Get(ctx, r.URL.RequestURI(), fwd)
		s.writeUpstream(w, resp, err, "PASS")
		return
	}

	key := r.URL.Path
	if e, st := s.store.Get(key); st == cache.Fresh {
		s.Hits.Inc()
		writeEntry(w, e, "HIT")
		return
	} else if st == cache.Stale {
		s.Stale.Inc()
		s.group.DoAsync(key, func() (*cache.Entry, error) { return s.refresh(key, fwd) })
		writeEntry(w, e, "STALE")
		return
	}

	if !s.cfg.Coalesce {
		s.Misses.Inc()
		e, err := s.fetch(ctx, key, fwd)
		s.finish(w, e, err, "MISS")
		return
	}
	// The leader fetches on a detached context: if the request that happened
	// to arrive first is cancelled, everyone waiting on it should not fail.
	e, err, shared := s.group.Do(key, func() (*cache.Entry, error) { return s.refresh(key, fwd) })
	if shared {
		s.Coalesced.Inc()
		s.finish(w, e, err, "COALESCED")
		return
	}
	s.Misses.Inc()
	s.finish(w, e, err, "MISS")
}

func (s *Server) requestContext(r *http.Request) (context.Context, context.CancelFunc) {
	dl := time.Now().Add(s.cfg.FetchTimeout)
	if v := r.Header.Get(DeadlineHeader); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			if d := time.UnixMilli(ms); d.Before(dl) {
				dl = d
			}
		}
	}
	return context.WithDeadline(r.Context(), dl)
}

func (s *Server) refresh(key string, fwd http.Header) (*cache.Entry, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.FetchTimeout)
	defer cancel()
	return s.fetch(ctx, key, fwd)
}

// fetch gets key from upstream and stores it if the response allows caching.
func (s *Server) fetch(ctx context.Context, key string, fwd http.Header) (*cache.Entry, error) {
	resp, err := s.up.Get(ctx, key, fwd)
	if err != nil {
		return nil, err
	}
	e := &cache.Entry{Key: key, Status: resp.Status, Header: pick(resp.Header), Body: resp.Body, StoredAt: time.Now()}
	if resp.Status == http.StatusOK {
		if ttl, ok := maxAge(resp.Header.Get("Cache-Control")); ok {
			e.TTL = ttl
			e.Stale = s.cfg.StaleWhileRevalidate
			if ttl > 0 {
				s.store.Set(e)
			}
		}
	}
	return e, nil
}

func maxAge(cc string) (time.Duration, bool) {
	if strings.Contains(cc, "no-store") || strings.Contains(cc, "private") {
		return 0, false
	}
	for _, part := range strings.Split(cc, ",") {
		part = strings.TrimSpace(part)
		if v, ok := strings.CutPrefix(part, "max-age="); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return 0, false
			}
			return time.Duration(n) * time.Second, true
		}
	}
	return 0, false
}

func pick(h http.Header) http.Header {
	out := http.Header{}
	for _, k := range []string{"Content-Type", "Cache-Control", "X-Origin"} {
		if v := h.Get(k); v != "" {
			out.Set(k, v)
		}
	}
	return out
}

func writeEntry(w http.ResponseWriter, e *cache.Entry, status string) {
	for k, v := range e.Header {
		w.Header()[k] = v
	}
	w.Header().Set("X-Cache", status)
	w.WriteHeader(e.Status)
	w.Write(e.Body)
}

func (s *Server) finish(w http.ResponseWriter, e *cache.Entry, err error, status string) {
	if err != nil {
		s.writeError(w, err)
		return
	}
	if e.Status >= 500 {
		s.Errors.Inc()
	}
	writeEntry(w, e, status)
}

func (s *Server) writeUpstream(w http.ResponseWriter, resp *upstream.Response, err error, status string) {
	if err != nil {
		s.writeError(w, err)
		return
	}
	if resp.Status >= 500 {
		s.Errors.Inc()
	}
	for k, v := range pick(resp.Header) {
		w.Header()[k] = v
	}
	w.Header().Set("X-Cache", status)
	w.WriteHeader(resp.Status)
	w.Write(resp.Body)
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	s.Errors.Inc()
	code := http.StatusBadGateway
	switch {
	case errors.Is(err, upstream.ErrBreakerOpen), errors.Is(err, upstream.ErrNoEndpoint):
		code = http.StatusServiceUnavailable
		w.Header().Set("X-Switchyard-Reason", "upstream-unavailable")
	case errors.Is(err, upstream.ErrDeadline), errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	}
	http.Error(w, err.Error(), code)
}
