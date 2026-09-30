package breaker

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest() (*Breaker, *clock) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	return NewWithClock(Config{Window: 2 * time.Second, Buckets: 10, MinRequests: 10, FailureRatio: 0.5, OpenFor: time.Second, Probes: 3}, c.now), c
}

func send(t *testing.T, b *Breaker, ok bool) bool {
	t.Helper()
	done, allowed := b.Allow()
	if allowed {
		done(ok)
	}
	return allowed
}

func TestStaysClosedBelowMinRequests(t *testing.T) {
	b, _ := newTest()
	for i := 0; i < 9; i++ {
		send(t, b, false)
	}
	if b.State() != Closed {
		t.Fatal("must not trip on fewer than MinRequests samples")
	}
}

func TestTripsOpenThenHalfOpenThenCloses(t *testing.T) {
	b, c := newTest()
	for i := 0; i < 10; i++ {
		send(t, b, false)
	}
	if b.State() != Open {
		t.Fatalf("want open, got %v", b.State())
	}
	if send(t, b, true) {
		t.Fatal("open breaker must refuse requests")
	}
	c.advance(time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("want half-open after OpenFor, got %v", b.State())
	}
	for i := 0; i < 3; i++ {
		if !send(t, b, true) {
			t.Fatal("half-open must admit probes")
		}
	}
	if b.State() != Closed {
		t.Fatalf("want closed after successful probes, got %v", b.State())
	}
}

func TestHalfOpenLimitsProbesAndReopensOnFailure(t *testing.T) {
	b, c := newTest()
	for i := 0; i < 10; i++ {
		send(t, b, false)
	}
	c.advance(time.Second)
	d1, ok1 := b.Allow()
	d2, ok2 := b.Allow()
	d3, ok3 := b.Allow()
	_, ok4 := b.Allow()
	if !ok1 || !ok2 || !ok3 || ok4 {
		t.Fatal("half-open must admit exactly Probes requests at once")
	}
	d1(true)
	d2(false)
	d3(true)
	if b.State() != Open {
		t.Fatal("a failed probe must reopen the breaker")
	}
	_, _, _ = d1, d2, d3
}

func TestOldFailuresAgeOut(t *testing.T) {
	b, c := newTest()
	for i := 0; i < 8; i++ {
		send(t, b, false)
	}
	c.advance(3 * time.Second) // beyond the window
	for i := 0; i < 8; i++ {
		send(t, b, true)
	}
	send(t, b, false)
	send(t, b, false)
	if b.State() != Closed {
		t.Fatal("failures outside the sliding window must not count")
	}
}
