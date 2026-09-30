// Package breaker is a three-state circuit breaker over a sliding window.
//
// Closed: requests flow; outcomes land in a ring of time buckets. When the
// window holds at least MinRequests and the failure ratio crosses
// FailureRatio, the breaker opens. Open: requests fail fast (no network call)
// for OpenFor. Half-open: up to Probes trial requests go through; if they all
// succeed the breaker closes, and any failure reopens it.
package breaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string { return [...]string{"closed", "open", "half_open"}[s] }

type Config struct {
	Window       time.Duration // total sliding window
	Buckets      int
	MinRequests  int
	FailureRatio float64
	OpenFor      time.Duration
	Probes       int
}

// Default returns settings tuned for sub-second recovery in the lab.
func Default() Config {
	return Config{Window: 2 * time.Second, Buckets: 10, MinRequests: 20, FailureRatio: 0.5, OpenFor: time.Second, Probes: 5}
}

type bucket struct {
	start      int64
	ok, failed int
}

type Breaker struct {
	cfg Config
	now func() time.Time

	mu        sync.Mutex
	state     State
	openedAt  time.Time
	buckets   []bucket
	probesOut int
	probesOK  int

	Opens    int // number of closed->open transitions
	Rejected int // requests refused while open
}

func New(cfg Config) *Breaker { return NewWithClock(cfg, time.Now) }

func NewWithClock(cfg Config, now func() time.Time) *Breaker {
	if cfg.Buckets < 1 {
		cfg.Buckets = 10
	}
	return &Breaker{cfg: cfg, now: now, buckets: make([]bucket, cfg.Buckets)}
}

func (b *Breaker) width() int64 { return int64(b.cfg.Window) / int64(b.cfg.Buckets) }

func (b *Breaker) cur(now time.Time) *bucket {
	w := b.width()
	slot := now.UnixNano() / w
	bk := &b.buckets[slot%int64(len(b.buckets))]
	if bk.start != slot {
		*bk = bucket{start: slot}
	}
	return bk
}

func (b *Breaker) totals(now time.Time) (ok, failed int) {
	w := b.width()
	slot := now.UnixNano() / w
	for _, bk := range b.buckets {
		if slot-bk.start < int64(len(b.buckets)) {
			ok += bk.ok
			failed += bk.failed
		}
	}
	return
}

// State reports the current state, advancing open -> half-open when due.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(b.now())
	return b.state
}

func (b *Breaker) advance(now time.Time) {
	if b.state == Open && now.Sub(b.openedAt) >= b.cfg.OpenFor {
		b.state = HalfOpen
		b.probesOut, b.probesOK = 0, 0
	}
}

// Allow asks to send one request. If it returns ok, the caller must call done
// with the outcome exactly once.
func (b *Breaker) Allow() (done func(success bool), ok bool) {
	if b == nil {
		return func(bool) {}, true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.advance(now)
	switch b.state {
	case Open:
		b.Rejected++
		return nil, false
	case HalfOpen:
		if b.probesOut >= b.cfg.Probes {
			b.Rejected++
			return nil, false
		}
		b.probesOut++
		return b.halfOpenDone, true
	}
	return b.closedDone, true
}

func (b *Breaker) closedDone(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if b.state != Closed {
		return // outcome of a request admitted before the breaker opened
	}
	bk := b.cur(now)
	if success {
		bk.ok++
		return
	}
	bk.failed++
	ok, failed := b.totals(now)
	total := ok + failed
	if total >= b.cfg.MinRequests && float64(failed)/float64(total) >= b.cfg.FailureRatio {
		b.trip(now)
	}
}

func (b *Breaker) halfOpenDone(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != HalfOpen {
		return
	}
	if !success {
		b.trip(b.now())
		return
	}
	b.probesOK++
	if b.probesOK >= b.cfg.Probes {
		b.state = Closed
		for i := range b.buckets {
			b.buckets[i] = bucket{}
		}
	}
}

func (b *Breaker) trip(now time.Time) {
	if b.state == Closed {
		b.Opens++
	}
	b.state = Open
	b.openedAt = now
}

// Stats returns counters under the lock.
func (b *Breaker) Stats() (state State, opens, rejected int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(b.now())
	return b.state, b.Opens, b.Rejected
}
