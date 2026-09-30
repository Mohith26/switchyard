// Package health takes failing endpoints out of rotation two ways: an active
// checker that polls a health URL, and a passive outlier detector that watches
// real traffic and ejects an endpoint after consecutive failures, modelled on
// Envoy's outlier detection (exponential ejection time, max-ejection cap).
package health

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mohith26/switchyard/internal/balance"
)

// Checker polls every endpoint's health path on an interval.
type Checker struct {
	Endpoints          []*balance.Endpoint
	Path               string
	Interval           time.Duration
	Timeout            time.Duration
	UnhealthyThreshold int // consecutive failures before marking down
	HealthyThreshold   int // consecutive successes before marking up
	Client             *http.Client
	OnChange           func(e *balance.Endpoint, up bool)
}

// Run blocks until ctx is cancelled.
func (c *Checker) Run(ctx context.Context) {
	if c.Interval <= 0 {
		return
	}
	if c.Client == nil {
		c.Client = &http.Client{}
	}
	if c.UnhealthyThreshold < 1 {
		c.UnhealthyThreshold = 2
	}
	if c.HealthyThreshold < 1 {
		c.HealthyThreshold = 2
	}
	fails := make([]int, len(c.Endpoints))
	oks := make([]int, len(c.Endpoints))
	t := time.NewTicker(c.Interval)
	defer t.Stop()
	for {
		var wg sync.WaitGroup
		res := make([]bool, len(c.Endpoints))
		for i, e := range c.Endpoints {
			wg.Add(1)
			go func(i int, e *balance.Endpoint) {
				defer wg.Done()
				res[i] = c.probe(ctx, e)
			}(i, e)
		}
		wg.Wait()
		for i, e := range c.Endpoints {
			if res[i] {
				fails[i] = 0
				oks[i]++
				if e.ActiveDown() && oks[i] >= c.HealthyThreshold {
					e.SetActiveDown(false)
					if c.OnChange != nil {
						c.OnChange(e, true)
					}
				}
			} else {
				oks[i] = 0
				fails[i]++
				if !e.ActiveDown() && fails[i] >= c.UnhealthyThreshold {
					e.SetActiveDown(true)
					if c.OnChange != nil {
						c.OnChange(e, false)
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Checker) probe(ctx context.Context, e *balance.Endpoint) bool {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+e.Addr+c.Path, nil)
	if err != nil {
		return false
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode < 500
}

// Outlier ejects endpoints that fail ConsecutiveFailures times in a row. The
// ejection lasts BaseEjection times the number of times the endpoint has been
// ejected (capped at MaxEjection), and never more than MaxEjectPercent of the
// pool is ejected at once so a pool-wide problem cannot eject everything.
type Outlier struct {
	ConsecutiveFailures int
	BaseEjection        time.Duration
	MaxEjection         time.Duration
	MaxEjectPercent     int

	mu     sync.Mutex
	state  map[*balance.Endpoint]*outlierState
	pool   []*balance.Endpoint
	Ejects atomic.Uint64
	Now    func() time.Time
}

type outlierState struct {
	consec   int
	ejectCnt int
}

// NewOutlier builds a detector over pool.
func NewOutlier(pool []*balance.Endpoint, consecutive int, base, max time.Duration, maxPct int) *Outlier {
	o := &Outlier{ConsecutiveFailures: consecutive, BaseEjection: base, MaxEjection: max, MaxEjectPercent: maxPct,
		state: map[*balance.Endpoint]*outlierState{}, pool: pool, Now: time.Now}
	for _, e := range pool {
		o.state[e] = &outlierState{}
	}
	return o
}

// Report records one request outcome. Fatal (e.g. connection refused) ejects
// immediately instead of waiting for the consecutive-failure threshold.
func (o *Outlier) Report(e *balance.Endpoint, ok, fatal bool) {
	if o == nil || o.ConsecutiveFailures <= 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	st := o.state[e]
	if st == nil {
		return
	}
	if ok {
		st.consec = 0
		return
	}
	st.consec++
	now := o.Now()
	if e.Ejected(now) {
		return
	}
	if st.consec < o.ConsecutiveFailures && !fatal {
		return
	}
	ejected := 0
	for _, p := range o.pool {
		if p.Ejected(now) {
			ejected++
		}
	}
	if o.MaxEjectPercent > 0 && (ejected+1)*100 > o.MaxEjectPercent*len(o.pool) {
		return
	}
	st.ejectCnt++
	d := o.BaseEjection * time.Duration(st.ejectCnt)
	if o.MaxEjection > 0 && d > o.MaxEjection {
		d = o.MaxEjection
	}
	e.EjectUntil(now.Add(d))
	st.consec = 0
	o.Ejects.Add(1)
}
