package limit

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Adaptive is a TCP-Vegas-style concurrency limit, the approach used by
// Netflix's concurrency-limits library. It never needs to be told the
// downstream capacity. It compares recent latency with the best latency seen:
//
//	queue = limit * (1 - minRTT/rtt)
//
// estimates how many requests are waiting rather than being served. If that
// estimate is small the limit grows, if it is large the limit shrinks, and any
// window with failures cuts the limit by 10%. Requests over the limit are
// rejected at once, which is what keeps admitted requests fast during overload
// (Little's law: an unbounded queue only adds latency, never throughput).
type Adaptive struct {
	min, max float64
	window   time.Duration
	probe    time.Duration
	now      func() time.Time

	inflight atomic.Int64

	mu         sync.Mutex
	limit      float64
	minRTT     time.Duration
	lastProbe  time.Time
	winStart   time.Time
	winSum     time.Duration
	winN       int
	winDropped int
	winMaxInfl int64

	Admitted atomic.Uint64
	Shed     atomic.Uint64
}

type AdaptiveConfig struct {
	Initial, Min, Max int
	Window            time.Duration // how often the limit is recomputed
	ProbeInterval     time.Duration // how often minRTT is re-measured
}

func NewAdaptive(c AdaptiveConfig) *Adaptive { return NewAdaptiveWithClock(c, time.Now) }

func NewAdaptiveWithClock(c AdaptiveConfig, now func() time.Time) *Adaptive {
	if c.Min < 1 {
		c.Min = 1
	}
	if c.Max < c.Min {
		c.Max = 1000
	}
	if c.Initial < c.Min {
		c.Initial = c.Min
	}
	if c.Window <= 0 {
		c.Window = 100 * time.Millisecond
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = 30 * time.Second
	}
	t := now()
	return &Adaptive{min: float64(c.Min), max: float64(c.Max), window: c.Window, probe: c.ProbeInterval,
		now: now, limit: float64(c.Initial), winStart: t, lastProbe: t}
}

// Limit returns the current limit.
func (a *Adaptive) Limit() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return int(a.limit)
}

func (a *Adaptive) Inflight() int64 { return a.inflight.Load() }

// Acquire admits one request or rejects it. On success, release must be called
// with the request's latency and whether it failed (timeout or 5xx).
func (a *Adaptive) Acquire() (release func(rtt time.Duration, dropped bool), ok bool) {
	a.mu.Lock()
	lim := int64(a.limit)
	a.mu.Unlock()
	n := a.inflight.Add(1)
	if n > lim {
		a.inflight.Add(-1)
		a.Shed.Add(1)
		return nil, false
	}
	a.Admitted.Add(1)
	a.mu.Lock()
	if n > a.winMaxInfl {
		a.winMaxInfl = n
	}
	a.mu.Unlock()
	return func(rtt time.Duration, dropped bool) {
		a.inflight.Add(-1)
		a.sample(rtt, dropped)
	}, true
}

func (a *Adaptive) sample(rtt time.Duration, dropped bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if dropped {
		a.winDropped++
	} else {
		a.winSum += rtt
		a.winN++
	}
	now := a.now()
	if now.Sub(a.winStart) < a.window {
		return
	}
	a.update(now)
}

func (a *Adaptive) update(now time.Time) {
	defer func() {
		a.winStart, a.winSum, a.winN, a.winDropped, a.winMaxInfl = now, 0, 0, 0, a.inflight.Load()
	}()
	if a.winDropped > 0 {
		a.limit = math.Max(a.min, a.limit*0.9)
		return
	}
	if a.winN < 5 {
		return
	}
	rtt := a.winSum / time.Duration(a.winN)
	if now.Sub(a.lastProbe) >= a.probe {
		a.minRTT = 0
		a.lastProbe = now
	}
	if a.minRTT == 0 || rtt < a.minRTT {
		a.minRTT = rtt
	}
	l := a.limit
	lg := math.Max(1, math.Log10(l))
	alpha, beta := 3*lg, 6*lg
	queue := l * (1 - float64(a.minRTT)/float64(rtt))
	switch {
	case queue <= alpha:
		// Only grow if we actually used the current limit; an idle system says
		// nothing about whether more concurrency would help.
		if float64(a.winMaxInfl) >= l*0.5 {
			l += lg
		}
	case queue >= beta:
		l -= lg
	}
	a.limit = math.Min(a.max, math.Max(a.min, l))
}
