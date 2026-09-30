// Package balance holds the upstream Endpoint type and the load-balancing
// algorithms that choose between endpoints: round robin, least connections,
// and power of two choices over a peak-EWMA latency estimate.
package balance

import (
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Endpoint is one upstream server plus the live state every layer shares:
// in-flight count (balancers), a latency estimate (P2C), and availability
// flags written by the active health checker and the passive outlier detector.
type Endpoint struct {
	Addr string

	inflight atomic.Int64

	mu      sync.Mutex
	ewma    float64 // microseconds
	lastUpd time.Time

	activeDown   atomic.Bool  // set by the active health checker
	ejectedUntil atomic.Int64 // unix nanos; set by the outlier detector

	Requests atomic.Uint64
	Failures atomic.Uint64
}

func NewEndpoint(addr string) *Endpoint { return &Endpoint{Addr: addr} }

func (e *Endpoint) Inflight() int64 { return e.inflight.Load() }

// Begin marks a request as started and returns a func to call when it ends.
func (e *Endpoint) Begin() func(latency time.Duration, ok bool) {
	e.inflight.Add(1)
	e.Requests.Add(1)
	return func(lat time.Duration, ok bool) {
		e.inflight.Add(-1)
		if !ok {
			e.Failures.Add(1)
		}
		e.observe(lat)
	}
}

// decay is the EWMA time constant. Peak EWMA (as in Finagle and Linkerd) jumps
// straight up to a slower sample and decays back down over this window, so a
// suddenly slow endpoint is avoided at once but forgiven gradually.
const decay = 2 * time.Second

func (e *Endpoint) observe(lat time.Duration) {
	us := float64(lat.Microseconds())
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lastUpd.IsZero() || us > e.ewma {
		e.ewma = us
	} else {
		w := math.Exp(-float64(now.Sub(e.lastUpd)) / float64(decay))
		e.ewma = e.ewma*w + us*(1-w)
	}
	e.lastUpd = now
}

// Cost is the P2C score: expected latency times queue depth.
func (e *Endpoint) Cost() float64 {
	e.mu.Lock()
	lat := e.ewma
	e.mu.Unlock()
	if lat == 0 {
		lat = 1
	}
	return lat * float64(e.inflight.Load()+1)
}

func (e *Endpoint) SetActiveDown(down bool) { e.activeDown.Store(down) }
func (e *Endpoint) ActiveDown() bool        { return e.activeDown.Load() }

func (e *Endpoint) EjectUntil(t time.Time) { e.ejectedUntil.Store(t.UnixNano()) }
func (e *Endpoint) Ejected(now time.Time) bool {
	return now.UnixNano() < e.ejectedUntil.Load()
}

// Available is true when neither health signal has taken the endpoint out.
func (e *Endpoint) Available(now time.Time) bool {
	return !e.ActiveDown() && !e.Ejected(now)
}

// Balancer picks one endpoint from a non-empty candidate list.
type Balancer interface {
	Pick(candidates []*Endpoint) *Endpoint
	Name() string
}

// New returns a balancer by name: "round_robin", "least_conn" or "p2c".
func New(name string) Balancer {
	switch name {
	case "round_robin", "rr":
		return &RoundRobin{}
	case "least_conn":
		return LeastConn{}
	default:
		return P2C{}
	}
}

// RoundRobin cycles through candidates.
type RoundRobin struct{ n atomic.Uint64 }

func (r *RoundRobin) Name() string { return "round_robin" }
func (r *RoundRobin) Pick(c []*Endpoint) *Endpoint {
	return c[int(r.n.Add(1)-1)%len(c)]
}

// LeastConn picks the endpoint with the fewest in-flight requests, breaking
// ties at random so equal endpoints share load.
type LeastConn struct{}

func (LeastConn) Name() string { return "least_conn" }
func (LeastConn) Pick(c []*Endpoint) *Endpoint {
	best := c[0]
	bestN := best.Inflight()
	ties := 1
	for _, e := range c[1:] {
		n := e.Inflight()
		switch {
		case n < bestN:
			best, bestN, ties = e, n, 1
		case n == bestN:
			ties++
			if rand.IntN(ties) == 0 {
				best = e
			}
		}
	}
	return best
}

// P2C samples two distinct endpoints and keeps the cheaper one ("the power of
// two choices"): nearly the balance of scanning everything, at O(1) cost and
// without every client herding onto the same least-loaded server.
type P2C struct{}

func (P2C) Name() string { return "p2c" }
func (P2C) Pick(c []*Endpoint) *Endpoint {
	if len(c) == 1 {
		return c[0]
	}
	i := rand.IntN(len(c))
	j := rand.IntN(len(c) - 1)
	if j >= i {
		j++
	}
	if c[j].Cost() < c[i].Cost() {
		return c[j]
	}
	return c[i]
}
