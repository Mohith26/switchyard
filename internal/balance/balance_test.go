package balance

import (
	"testing"
	"time"
)

func eps(n int) []*Endpoint {
	var out []*Endpoint
	for i := 0; i < n; i++ {
		out = append(out, NewEndpoint(string(rune('a'+i))))
	}
	return out
}

func TestRoundRobinCycles(t *testing.T) {
	c := eps(3)
	rr := &RoundRobin{}
	seen := map[*Endpoint]int{}
	for i := 0; i < 300; i++ {
		seen[rr.Pick(c)]++
	}
	for _, e := range c {
		if seen[e] != 100 {
			t.Fatalf("uneven round robin: %v", seen)
		}
	}
}

func TestLeastConnAvoidsBusyEndpoint(t *testing.T) {
	c := eps(3)
	for i := 0; i < 5; i++ {
		c[0].Begin()
		c[1].Begin()
	}
	if got := (LeastConn{}).Pick(c); got != c[2] {
		t.Fatalf("picked %s, want the idle endpoint", got.Addr)
	}
}

func TestP2CAvoidsSlowEndpoint(t *testing.T) {
	c := eps(4)
	for _, e := range c {
		e.Begin()(5*time.Millisecond, true)
	}
	c[0].Begin()(500*time.Millisecond, true) // peak EWMA jumps straight up
	hits := 0
	for i := 0; i < 10000; i++ {
		if (P2C{}).Pick(c) == c[0] {
			hits++
		}
	}
	// With 4 endpoints the slow one is sampled in half of all pairs but should
	// essentially never win a comparison.
	if hits > 50 {
		t.Fatalf("slow endpoint chosen %d/10000 times", hits)
	}
}

func TestAvailability(t *testing.T) {
	e := NewEndpoint("x")
	now := time.Now()
	if !e.Available(now) {
		t.Fatal("new endpoint should be available")
	}
	e.EjectUntil(now.Add(time.Second))
	if e.Available(now) || !e.Available(now.Add(2*time.Second)) {
		t.Fatal("ejection must be time bounded")
	}
	e.SetActiveDown(true)
	if e.Available(now.Add(2 * time.Second)) {
		t.Fatal("active health check down must win")
	}
}
