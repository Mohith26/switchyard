// Package retry decides whether a failed upstream attempt may be retried.
//
// The naive policy retries every failure up to N times immediately. That is
// what turns a slow dependency into an outage: when the dependency slows down,
// every request becomes N+1 requests, the extra load keeps it slow, and the
// system can stay down even after the original trigger is gone (a
// "metastable" failure). The budget policy instead lets retries be at most a
// fixed fraction of recent requests (like Finagle's RetryBudget and gRPC's
// retry throttling), plus a small per-second floor so low-traffic callers can
// still retry, and spaces attempts with jittered exponential backoff.
package retry

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Mode string

const (
	None   Mode = "none"
	Naive  Mode = "naive"
	Budget Mode = "budget"
)

type Config struct {
	Mode        Mode
	MaxAttempts int           // total attempts including the first
	Ratio       float64       // budget: retries allowed per request (0.1 = 10%)
	MinPerSec   float64       // budget: floor of retries per second
	BaseBackoff time.Duration // budget: first backoff (full jitter)
	MaxBackoff  time.Duration
}

// Policy is shared by every request going to one upstream pool.
type Policy struct {
	cfg Config
	now func() time.Time

	mu       sync.Mutex
	tokens   float64 // earned by requests, spent by retries
	floor    float64 // separate refilling reserve for MinPerSec
	lastFill time.Time

	Retries   uint64
	Exhausted uint64 // retries refused by the budget
}

func New(cfg Config) *Policy { return NewWithClock(cfg, time.Now) }

func NewWithClock(cfg Config, now func() time.Time) *Policy {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return &Policy{cfg: cfg, now: now, lastFill: now(), floor: cfg.MinPerSec}
}

func (p *Policy) Mode() Mode { return p.cfg.Mode }

// maxTokens caps saved-up budget so a long quiet period cannot bank enough
// credit to allow a burst of retries later.
const maxTokens = 100

// OnRequest deposits the per-request share of retry budget. Call once per
// original (non-retry) request.
func (p *Policy) OnRequest() {
	if p == nil || p.cfg.Mode != Budget {
		return
	}
	p.mu.Lock()
	p.tokens += p.cfg.Ratio
	if p.tokens > maxTokens {
		p.tokens = maxTokens
	}
	p.mu.Unlock()
}

// Allow reports whether attempt number `attempt` (1-based, so the first retry
// is attempt 2) may proceed, and how long to wait before sending it.
func (p *Policy) Allow(attempt int) (bool, time.Duration) {
	if p == nil || p.cfg.Mode == None || attempt > p.cfg.MaxAttempts {
		return false, 0
	}
	if p.cfg.Mode == Naive {
		p.mu.Lock()
		p.Retries++
		p.mu.Unlock()
		return true, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	el := now.Sub(p.lastFill).Seconds()
	p.lastFill = now
	p.floor += el * p.cfg.MinPerSec
	if p.floor > p.cfg.MinPerSec {
		p.floor = p.cfg.MinPerSec
	}
	switch {
	case p.tokens >= 1:
		p.tokens--
	case p.floor >= 1:
		p.floor--
	default:
		p.Exhausted++
		return false, 0
	}
	p.Retries++
	return true, p.backoff(attempt)
}

func (p *Policy) backoff(attempt int) time.Duration {
	if p.cfg.BaseBackoff <= 0 {
		return 0
	}
	d := p.cfg.BaseBackoff << uint(attempt-2)
	if p.cfg.MaxBackoff > 0 && d > p.cfg.MaxBackoff {
		d = p.cfg.MaxBackoff
	}
	return time.Duration(rand.Int64N(int64(d) + 1)) // full jitter
}

// Stats returns (retries sent, retries refused by the budget).
func (p *Policy) Stats() (uint64, uint64) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Retries, p.Exhausted
}
