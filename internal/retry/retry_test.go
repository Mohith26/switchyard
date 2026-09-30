package retry

import (
	"testing"
	"time"
)

func TestNoneNeverRetries(t *testing.T) {
	p := New(Config{Mode: None, MaxAttempts: 5})
	if ok, _ := p.Allow(2); ok {
		t.Fatal("mode none must not retry")
	}
}

func TestNaiveRetriesUpToMaxAttempts(t *testing.T) {
	p := New(Config{Mode: Naive, MaxAttempts: 4})
	for a := 2; a <= 4; a++ {
		if ok, wait := p.Allow(a); !ok || wait != 0 {
			t.Fatalf("attempt %d: naive retries immediately", a)
		}
	}
	if ok, _ := p.Allow(5); ok {
		t.Fatal("must stop at MaxAttempts")
	}
}

// Under sustained failure, a budget of 10% must keep retries near 10% of
// requests no matter how many attempts each request would like.
func TestBudgetCapsRetryRatio(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := NewWithClock(Config{Mode: Budget, MaxAttempts: 4, Ratio: 0.1, MinPerSec: 0}, func() time.Time { return now })
	retries := 0
	const requests = 10000
	for i := 0; i < requests; i++ {
		p.OnRequest()
		for a := 2; a <= 4; a++ {
			if ok, _ := p.Allow(a); ok {
				retries++
			} else {
				break
			}
		}
	}
	if got := float64(retries) / requests; got > 0.101 || got < 0.09 {
		t.Fatalf("retry ratio %.3f, want 0.10", got)
	}
	_, exhausted := p.Stats()
	if exhausted == 0 {
		t.Fatal("expected refused retries to be counted")
	}
}

func TestBudgetFloorAllowsRetriesAtLowTraffic(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := NewWithClock(Config{Mode: Budget, MaxAttempts: 3, Ratio: 0.1, MinPerSec: 2}, func() time.Time { return now })
	p.OnRequest()
	ok1, _ := p.Allow(2)
	ok2, _ := p.Allow(2)
	ok3, _ := p.Allow(2)
	if !ok1 || !ok2 || ok3 {
		t.Fatalf("floor of 2/s should allow exactly 2 retries: %v %v %v", ok1, ok2, ok3)
	}
	now = now.Add(time.Second)
	if ok, _ := p.Allow(2); !ok {
		t.Fatal("floor must refill over time")
	}
}

func TestBackoffIsJitteredAndCapped(t *testing.T) {
	p := New(Config{Mode: Budget, MaxAttempts: 10, Ratio: 1, BaseBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond})
	for i := 0; i < 200; i++ {
		p.OnRequest()
		p.OnRequest()
		_, w := p.Allow(6)
		if w < 0 || w > 40*time.Millisecond {
			t.Fatalf("backoff %v outside [0, cap]", w)
		}
	}
}
