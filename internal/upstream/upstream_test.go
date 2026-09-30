package upstream

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mohith26/switchyard/internal/breaker"
	"github.com/Mohith26/switchyard/internal/retry"
)

func server(h http.HandlerFunc) (*httptest.Server, string) {
	s := httptest.NewServer(h)
	return s, s.Listener.Addr().String()
}

func TestNaiveRetryRecoversFromTransient503(t *testing.T) {
	var n atomic.Int32
	s, addr := server(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) <= 2 {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte("ok"))
	})
	defer s.Close()
	c := New(Config{Endpoints: []string{addr}, AttemptTimeout: time.Second, Retry: retry.Config{Mode: retry.Naive, MaxAttempts: 3}})
	resp, err := c.Get(context.Background(), "/x", nil)
	if err != nil || resp.Status != 200 || resp.Attempts != 3 {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
}

func TestBudgetLimitsRetriesUnderSustainedFailure(t *testing.T) {
	var hits atomic.Int32
	s, addr := server(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(503) })
	defer s.Close()
	c := New(Config{Endpoints: []string{addr}, AttemptTimeout: time.Second,
		Retry: retry.Config{Mode: retry.Budget, MaxAttempts: 4, Ratio: 0.1, MinPerSec: 0}})
	for i := 0; i < 500; i++ {
		c.Get(context.Background(), "/x", nil)
	}
	if amp := float64(hits.Load()) / 500; amp > 1.11 {
		t.Fatalf("amplification %.2fx, budget should hold it near 1.1x", amp)
	}
}

func TestBreakerFailsFastWithoutTouchingTheNetwork(t *testing.T) {
	var hits atomic.Int32
	s, addr := server(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(500) })
	defer s.Close()
	bc := breaker.Config{Window: 10 * time.Second, Buckets: 10, MinRequests: 10, FailureRatio: 0.5, OpenFor: time.Minute, Probes: 1}
	c := New(Config{Endpoints: []string{addr}, AttemptTimeout: time.Second, Retry: retry.Config{Mode: retry.None}, Breaker: &bc})
	for i := 0; i < 10; i++ {
		c.Get(context.Background(), "/x", nil)
	}
	before := hits.Load()
	for i := 0; i < 100; i++ {
		_, err := c.Get(context.Background(), "/x", nil)
		if !errors.Is(err, ErrBreakerOpen) {
			t.Fatalf("want ErrBreakerOpen, got %v", err)
		}
	}
	if hits.Load() != before {
		t.Fatal("an open breaker must not send requests")
	}
}

func TestFailsOverFromADeadEndpoint(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close() // nothing listens here now
	s, live := server(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	defer s.Close()
	c := New(Config{Endpoints: []string{dead, live}, Balancer: "round_robin", AttemptTimeout: time.Second,
		Retry:   retry.Config{Mode: retry.Budget, MaxAttempts: 2, Ratio: 0.5, MinPerSec: 100},
		Outlier: OutlierConfig{Consecutive: 5, Base: time.Minute, Max: time.Minute, MaxPercent: 50}})
	for i := 0; i < 50; i++ {
		resp, err := c.Get(context.Background(), "/x", nil)
		if err != nil || resp.Status != 200 {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	if c.Stats().Ejections == 0 {
		t.Fatal("connection refused should have ejected the dead endpoint")
	}
}

func TestRespectsCallerDeadline(t *testing.T) {
	s, addr := server(func(w http.ResponseWriter, r *http.Request) { time.Sleep(500 * time.Millisecond) })
	defer s.Close()
	c := New(Config{Endpoints: []string{addr}, AttemptTimeout: 5 * time.Second, Retry: retry.Config{Mode: retry.Naive, MaxAttempts: 5}})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	c.Get(ctx, "/x", nil)
	if el := time.Since(t0); el > 300*time.Millisecond {
		t.Fatalf("took %v; retries must not outlive the caller's deadline", el)
	}
}
