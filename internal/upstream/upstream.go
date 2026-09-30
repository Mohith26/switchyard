// Package upstream is the client a cache node uses to reach its origin pool.
// Each request goes through, in order: endpoint filtering (active health and
// outlier ejection), load balancing, the endpoint's circuit breaker, a
// per-attempt timeout capped by the caller's deadline, and the retry policy.
package upstream

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mohith26/switchyard/internal/balance"
	"github.com/Mohith26/switchyard/internal/breaker"
	"github.com/Mohith26/switchyard/internal/health"
	"github.com/Mohith26/switchyard/internal/retry"
)

type OutlierConfig struct {
	Consecutive int           `json:"consecutive"`
	Base        time.Duration `json:"base"`
	Max         time.Duration `json:"max"`
	MaxPercent  int           `json:"max_percent"`
}

type Config struct {
	Endpoints      []string
	Balancer       string
	AttemptTimeout time.Duration
	Retry          retry.Config
	Breaker        *breaker.Config // nil disables circuit breaking
	Outlier        OutlierConfig
	HealthPath     string
	HealthInterval time.Duration // 0 disables active checks
	MaxBodyBytes   int64
}

type Response struct {
	Status   int
	Header   http.Header
	Body     []byte
	Endpoint string
	Attempts int
}

var (
	ErrNoEndpoint  = errors.New("upstream: no available endpoint")
	ErrBreakerOpen = errors.New("upstream: circuit open")
	ErrDeadline    = errors.New("upstream: deadline exceeded")
)

type Stats struct {
	Requests, Attempts, Retries, BudgetExhausted, BreakerRejects, NoEndpoint, Failures, Ejections uint64
}

type Client struct {
	cfg      Config
	eps      []*balance.Endpoint
	bal      balance.Balancer
	breakers map[*balance.Endpoint]*breaker.Breaker
	retry    *retry.Policy
	outlier  *health.Outlier
	hc       *http.Client

	requests, attempts, breakerRejects, noEndpoint, failures atomic.Uint64
}

// New builds a client. Call Run to start active health checks.
func New(cfg Config) *Client {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 8 << 20
	}
	c := &Client{cfg: cfg, bal: balance.New(cfg.Balancer), breakers: map[*balance.Endpoint]*breaker.Breaker{}}
	for _, a := range cfg.Endpoints {
		e := balance.NewEndpoint(a)
		c.eps = append(c.eps, e)
		if cfg.Breaker != nil {
			c.breakers[e] = breaker.New(*cfg.Breaker)
		}
	}
	c.retry = retry.New(cfg.Retry)
	o := cfg.Outlier
	c.outlier = health.NewOutlier(c.eps, o.Consecutive, o.Base, o.Max, o.MaxPercent)
	c.hc = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     30 * time.Second,
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 15 * time.Second}).DialContext,
	}}
	return c
}

// Run starts the active health checker until ctx ends.
func (c *Client) Run(ctx context.Context) {
	if c.cfg.HealthInterval <= 0 {
		return
	}
	ch := &health.Checker{Endpoints: c.eps, Path: c.cfg.HealthPath, Interval: c.cfg.HealthInterval,
		Timeout: c.cfg.HealthInterval, UnhealthyThreshold: 2, HealthyThreshold: 2, Client: c.hc}
	go ch.Run(ctx)
}

func (c *Client) Endpoints() []*balance.Endpoint { return c.eps }

// Breaker returns the breaker for endpoint i (nil when disabled).
func (c *Client) Breaker(i int) *breaker.Breaker { return c.breakers[c.eps[i]] }

func (c *Client) Stats() Stats {
	r, ex := c.retry.Stats()
	return Stats{Requests: c.requests.Load(), Attempts: c.attempts.Load(), Retries: r, BudgetExhausted: ex,
		BreakerRejects: c.breakerRejects.Load(), NoEndpoint: c.noEndpoint.Load(), Failures: c.failures.Load(),
		Ejections: c.outlier.Ejects.Load()}
}

// CloseIdle drops pooled connections.
func (c *Client) CloseIdle() { c.hc.CloseIdleConnections() }

func (c *Client) candidates(now time.Time, tried map[*balance.Endpoint]bool) []*balance.Endpoint {
	var out, fallback []*balance.Endpoint
	for _, e := range c.eps {
		if !e.Available(now) {
			continue
		}
		if b := c.breakers[e]; b != nil && b.State() == breaker.Open {
			continue
		}
		if tried[e] {
			fallback = append(fallback, e)
			continue
		}
		out = append(out, e)
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func retriable(status int) bool { return status == 502 || status == 503 || status == 504 }

// Get fetches path from the pool.
func (c *Client) Get(ctx context.Context, path string, hdr http.Header) (*Response, error) {
	c.requests.Add(1)
	c.retry.OnRequest()
	tried := map[*balance.Endpoint]bool{}
	var lastErr error = ErrNoEndpoint
	var lastResp *Response
	for attempt := 1; ; attempt++ {
		if attempt > 1 {
			ok, wait := c.retry.Allow(attempt)
			if !ok {
				break
			}
			if wait > 0 {
				select {
				case <-ctx.Done():
					return nil, ErrDeadline
				case <-time.After(wait):
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, ErrDeadline
		}
		cands := c.candidates(time.Now(), tried)
		if len(cands) == 0 {
			c.noEndpoint.Add(1)
			lastErr = ErrNoEndpoint
			if c.cfg.Breaker != nil {
				lastErr = ErrBreakerOpen
			}
			continue
		}
		ep := c.bal.Pick(cands)
		tried[ep] = true
		var done func(bool)
		if b := c.breakers[ep]; b != nil {
			d, ok := b.Allow()
			if !ok {
				c.breakerRejects.Add(1)
				lastErr = ErrBreakerOpen
				continue
			}
			done = d
		}
		resp, fatal, err := c.attempt(ctx, ep, path, hdr)
		c.attempts.Add(1)
		success := err == nil && !retriable(resp.Status) && resp.Status < 500
		if done != nil {
			done(success)
		}
		c.outlier.Report(ep, success, fatal)
		if success {
			resp.Attempts = attempt
			return resp, nil
		}
		c.failures.Add(1)
		lastErr, lastResp = err, resp
		if err == nil && !retriable(resp.Status) {
			break // a non-retriable 5xx: return it as is
		}
	}
	if lastResp != nil {
		return lastResp, nil
	}
	return nil, lastErr
}

func (c *Client) attempt(ctx context.Context, ep *balance.Endpoint, path string, hdr http.Header) (*Response, bool, error) {
	timeout := c.cfg.AttemptTimeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); timeout <= 0 || rem < timeout {
			timeout = rem
		}
	}
	if timeout <= 0 {
		return nil, false, ErrDeadline
	}
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, "http://"+ep.Addr+path, nil)
	if err != nil {
		return nil, false, err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	end := ep.Begin()
	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		end(time.Since(start), false)
		return nil, isFatal(err), err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.cfg.MaxBodyBytes))
	resp.Body.Close()
	lat := time.Since(start)
	if err != nil {
		end(lat, false)
		return nil, false, err
	}
	end(lat, resp.StatusCode < 500)
	return &Response{Status: resp.StatusCode, Header: resp.Header, Body: body, Endpoint: ep.Addr}, false, nil
}

// isFatal reports errors that mean nothing is listening, which justify
// ejecting the endpoint immediately rather than after N failures.
func isFatal(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}
