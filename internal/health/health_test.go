package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mohith26/switchyard/internal/balance"
)

func pool(n int) []*balance.Endpoint {
	var out []*balance.Endpoint
	for i := 0; i < n; i++ {
		out = append(out, balance.NewEndpoint("e"+string(rune('a'+i))))
	}
	return out
}

func TestOutlierEjectsAfterConsecutiveFailures(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := pool(4)
	o := NewOutlier(p, 3, time.Second, 10*time.Second, 50)
	o.Now = func() time.Time { return now }
	o.Report(p[0], false, false)
	o.Report(p[0], false, false)
	o.Report(p[0], true, false) // a success resets the streak
	o.Report(p[0], false, false)
	o.Report(p[0], false, false)
	if p[0].Ejected(now) {
		t.Fatal("streak was broken; must not eject yet")
	}
	o.Report(p[0], false, false)
	if !p[0].Ejected(now) {
		t.Fatal("3 consecutive failures must eject")
	}
	if p[0].Ejected(now.Add(1001 * time.Millisecond)) {
		t.Fatal("first ejection lasts BaseEjection")
	}
}

func TestOutlierFatalEjectsImmediately(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := pool(3)
	o := NewOutlier(p, 5, time.Second, 10*time.Second, 50)
	o.Now = func() time.Time { return now }
	o.Report(p[1], false, true)
	if !p[1].Ejected(now) {
		t.Fatal("connection refused must eject at once")
	}
}

func TestOutlierRespectsMaxEjectPercent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := pool(4)
	o := NewOutlier(p, 1, time.Second, 10*time.Second, 50)
	o.Now = func() time.Time { return now }
	for _, e := range p {
		o.Report(e, false, false)
	}
	n := 0
	for _, e := range p {
		if e.Ejected(now) {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("%d of 4 ejected, cap is 50%%", n)
	}
}

func TestOutlierBacksOffRepeatOffenders(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := pool(2)
	o := NewOutlier(p, 1, time.Second, 10*time.Second, 100)
	o.Now = func() time.Time { return now }
	o.Report(p[0], false, false)
	now = now.Add(1100 * time.Millisecond)
	o.Report(p[0], false, false)
	if !p[0].Ejected(now.Add(1500 * time.Millisecond)) {
		t.Fatal("second ejection should last 2x BaseEjection")
	}
}

func TestActiveCheckerMarksDownAndUp(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()
	e := balance.NewEndpoint(srv.Listener.Addr().String())
	var changes atomic.Int32
	c := &Checker{Endpoints: []*balance.Endpoint{e}, Path: "/healthz", Interval: 10 * time.Millisecond,
		UnhealthyThreshold: 2, HealthyThreshold: 2, OnChange: func(*balance.Endpoint, bool) { changes.Add(1) }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	healthy.Store(false)
	waitFor(t, func() bool { return e.ActiveDown() })
	healthy.Store(true)
	waitFor(t, func() bool { return !e.ActiveDown() })
	if changes.Load() != 2 {
		t.Fatalf("want 2 transitions, got %d", changes.Load())
	}
}

func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
