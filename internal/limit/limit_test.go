package limit

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestTokenBucketRateAndBurst(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	k := NewKeyedWithClock(100, 20, 0, c.now)
	allowed := 0
	for i := 0; i < 100; i++ {
		if k.Allow("a") {
			allowed++
		}
	}
	if allowed != 20 {
		t.Fatalf("burst: allowed %d, want 20", allowed)
	}
	// Over 10 simulated seconds at 1ms steps, exactly rate*10 more tokens accrue.
	allowed = 0
	for i := 0; i < 10000; i++ {
		c.advance(time.Millisecond)
		if k.Allow("a") {
			allowed++
		}
	}
	if allowed < 995 || allowed > 1005 {
		t.Fatalf("steady state: allowed %d in 10s, want ~1000", allowed)
	}
}

func TestKeysAreIsolated(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	k := NewKeyedWithClock(1, 5, 0, c.now)
	for i := 0; i < 50; i++ {
		k.Allow("noisy")
	}
	for i := 0; i < 5; i++ {
		if !k.Allow("quiet") {
			t.Fatal("one key's usage must not affect another")
		}
	}
}

func TestKeyedMemoryIsBounded(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	k := NewKeyedWithClock(10, 10, 8, c.now)
	for i := 0; i < 10000; i++ {
		k.Allow("client-" + strconv.Itoa(i))
	}
	if n := k.Keys(); n > 8*shards {
		t.Fatalf("tracked %d keys, cap is %d", n, 8*shards)
	}
}

func TestAdaptiveShrinksWhenLatencyRises(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	a := NewAdaptiveWithClock(AdaptiveConfig{Initial: 100, Min: 5, Max: 1000, Window: 100 * time.Millisecond}, c.now)
	run := func(rtt time.Duration, windows int) {
		for w := 0; w < windows; w++ {
			var rels []func(time.Duration, bool)
			for i := 0; i < a.Limit(); i++ {
				if rel, ok := a.Acquire(); ok {
					rels = append(rels, rel)
				}
			}
			c.advance(100 * time.Millisecond)
			for _, rel := range rels {
				rel(rtt, false)
			}
		}
	}
	run(20*time.Millisecond, 5) // establishes minRTT = 20ms
	before := a.Limit()
	run(80*time.Millisecond, 40) // queueing: latency 4x the no-load baseline
	if after := a.Limit(); after >= before/2 {
		t.Fatalf("limit went %d -> %d under 4x latency, want a large cut", before, after)
	}
}

func TestAdaptiveGrowsWhenFastAndSaturated(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	a := NewAdaptiveWithClock(AdaptiveConfig{Initial: 10, Min: 5, Max: 1000, Window: 100 * time.Millisecond}, c.now)
	for w := 0; w < 50; w++ {
		var rels []func(time.Duration, bool)
		for i := 0; i < a.Limit(); i++ {
			if rel, ok := a.Acquire(); ok {
				rels = append(rels, rel)
			}
		}
		c.advance(100 * time.Millisecond)
		for _, rel := range rels {
			rel(20*time.Millisecond, false)
		}
	}
	if a.Limit() <= 20 {
		t.Fatalf("limit %d: should grow while latency stays at baseline", a.Limit())
	}
}

func TestAdaptiveRejectsOverLimit(t *testing.T) {
	a := NewAdaptive(AdaptiveConfig{Initial: 3, Min: 1, Max: 10})
	var rels []func(time.Duration, bool)
	for i := 0; i < 3; i++ {
		rel, ok := a.Acquire()
		if !ok {
			t.Fatal("should admit up to the limit")
		}
		rels = append(rels, rel)
	}
	if _, ok := a.Acquire(); ok {
		t.Fatal("should reject over the limit")
	}
	rels[0](time.Millisecond, false)
	if _, ok := a.Acquire(); !ok {
		t.Fatal("a release must free a slot")
	}
}

func BenchmarkKeyedAllow(b *testing.B) {
	k := NewKeyed(1e9, 1e9, 0)
	ids := make([]string, 256)
	for i := range ids {
		ids[i] = "tenant-" + strconv.Itoa(i)
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k.Allow(ids[i&255])
			i++
		}
	})
}
