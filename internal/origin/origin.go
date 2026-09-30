// Package origin is a simulated origin server with a realistic capacity
// model: a fixed pool of workers pulling from a bounded FIFO queue, lognormal
// service times, and no cancellation of abandoned work (like most real
// backends, it keeps rendering a page the client has already given up on).
// That last property is what makes retry storms self-sustaining, so the lab
// can reproduce them rather than just assert them. Faults (slowdown, error
// rate, hard kill) are injected at runtime.
package origin

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mohith26/switchyard/internal/metrics"
)

type Config struct {
	Name      string
	Workers   int           // concurrent requests actually being served
	QueueCap  int           // waiting requests beyond this get a fast 503
	Median    time.Duration // median service time
	Sigma     float64       // lognormal shape; 0.25 gives a p99 of about 1.8x median
	TTL       time.Duration // Cache-Control max-age for /wiki/ pages
	BodyBytes int
	// Per-path overrides, e.g. a breaking-news page that is slower to render
	// and cached for only a second.
	CostMultiplier map[string]float64
	TTLOverride    map[string]time.Duration
}

type job struct {
	path string
	ctx  context.Context
	done chan struct{}
}

type Server struct {
	cfg   Config
	queue chan *job

	mu   sync.Mutex
	addr string
	ln   net.Listener
	srv  *http.Server
	up   bool

	slowdown  atomic.Uint64 // float64 bits
	errorRate atomic.Uint64 // float64 bits

	Received, Served, Rejected, Errors, Wasted metrics.Counter
	Busy                                       metrics.Gauge

	watchMu sync.RWMutex
	watch   map[string]*metrics.Counter

	stop chan struct{}
	wg   sync.WaitGroup
}

func New(cfg Config) *Server {
	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.QueueCap <= 0 {
		cfg.QueueCap = 1024
	}
	if cfg.Median <= 0 {
		cfg.Median = 20 * time.Millisecond
	}
	if cfg.BodyBytes <= 0 {
		cfg.BodyBytes = 2048
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 60 * time.Second
	}
	s := &Server{cfg: cfg, queue: make(chan *job, cfg.QueueCap), watch: map[string]*metrics.Counter{}, stop: make(chan struct{})}
	s.slowdown.Store(math.Float64bits(1))
	for i := 0; i < cfg.Workers; i++ {
		s.wg.Add(1)
		go s.worker(uint64(i + 1))
	}
	return s
}

// Start listens on addr ("127.0.0.1:0" for any port) and serves.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.addr = ln.Addr().String()
	s.mu.Unlock()
	s.serve(ln)
	return nil
}

// Serve uses an existing listener.
func (s *Server) Serve(ln net.Listener) {
	s.mu.Lock()
	s.addr = ln.Addr().String()
	s.mu.Unlock()
	s.serve(ln)
}

func (s *Server) serve(ln net.Listener) {
	srv := &http.Server{Handler: s, ReadHeaderTimeout: 5 * time.Second}
	s.mu.Lock()
	s.ln, s.srv, s.up = ln, srv, true
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
	return s.up
}

// Kill hard-stops the listener and every open connection, like a crashed
// process. Queued work is abandoned.
func (s *Server) Kill() {
	s.mu.Lock()
	srv := s.srv
	s.up = false
	s.mu.Unlock()
	if srv != nil {
		srv.Close()
	}
}

// Revive listens again on the same address.
func (s *Server) Revive() error {
	if s.Up() {
		return nil
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
	s.serve(ln)
	return nil
}

// Close stops serving and the worker pool.
func (s *Server) Close() {
	s.Kill()
	close(s.stop)
	s.wg.Wait()
}

func (s *Server) SetSlowdown(f float64)  { s.slowdown.Store(math.Float64bits(f)) }
func (s *Server) SetErrorRate(p float64) { s.errorRate.Store(math.Float64bits(p)) }
func (s *Server) Queued() int            { return len(s.queue) }

// Watch returns a counter of requests received for path.
func (s *Server) Watch(path string) *metrics.Counter {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	c, ok := s.watch[path]
	if !ok {
		c = &metrics.Counter{}
		s.watch[path] = c
	}
	return c
}

func (s *Server) worker(seed uint64) {
	defer s.wg.Done()
	rng := rand.New(rand.NewPCG(seed, 0x5eed))
	for {
		select {
		case <-s.stop:
			return
		case j := <-s.queue:
			s.Busy.Add(1)
			d := s.serviceTime(rng, j.path)
			time.Sleep(d)
			s.Busy.Add(-1)
			if j.ctx.Err() != nil {
				s.Wasted.Inc() // the caller left while this was queued or running
			}
			close(j.done)
		}
	}
}

func (s *Server) serviceTime(rng *rand.Rand, path string) time.Duration {
	m := float64(s.cfg.Median)
	if s.cfg.Sigma > 0 {
		m *= math.Exp(s.cfg.Sigma * rng.NormFloat64())
	}
	m *= math.Float64frombits(s.slowdown.Load())
	if c, ok := s.cfg.CostMultiplier[path]; ok {
		m *= c
	}
	return time.Duration(m)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	s.Received.Inc()
	s.watchMu.RLock()
	if c := s.watch[r.URL.Path]; c != nil {
		c.Inc()
	}
	s.watchMu.RUnlock()
	w.Header().Set("X-Origin", s.cfg.Name)
	if p := math.Float64frombits(s.errorRate.Load()); p > 0 && rand.Float64() < p {
		s.Errors.Inc()
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}
	j := &job{path: r.URL.Path, ctx: r.Context(), done: make(chan struct{})}
	select {
	case s.queue <- j:
	default:
		s.Rejected.Inc()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "origin queue full", http.StatusServiceUnavailable)
		return
	}
	select {
	case <-j.done:
	case <-r.Context().Done():
		return
	}
	s.Served.Inc()
	s.write(w, r.URL.Path)
}

func (s *Server) write(w http.ResponseWriter, path string) {
	if strings.HasPrefix(path, "/wiki/") {
		ttl := s.cfg.TTL
		if t, ok := s.cfg.TTLOverride[path]; ok {
			ttl = t
		}
		w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(int(ttl/time.Second)))
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		title := strings.TrimPrefix(path, "/wiki/")
		head := fmt.Sprintf("<!doctype html><title>%s</title><h1>%s</h1><p>rendered by %s at %d</p>",
			title, title, s.cfg.Name, time.Now().UnixMilli())
		body := make([]byte, 0, s.cfg.BodyBytes)
		body = append(body, head...)
		for len(body) < s.cfg.BodyBytes {
			body = append(body, "<p>lorem ipsum dolor sit amet</p>"...)
		}
		w.Write(body[:s.cfg.BodyBytes])
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"path":%q,"origin":%q,"ts":%d}`, path, s.cfg.Name, time.Now().UnixMilli())
}
