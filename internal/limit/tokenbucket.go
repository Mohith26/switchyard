// Package limit contains the two admission controls at the edge: a per-client
// token bucket (fairness: one client cannot take everyone's capacity) and an
// adaptive concurrency limit (overload: shed excess work fast instead of
// queueing it until it times out).
package limit

import (
	"sync"
	"sync/atomic"
	"time"
)

const shards = 64

type bucket struct {
	tokens float64
	last   time.Time
}

type shard struct {
	mu sync.Mutex
	m  map[string]*bucket
}

// KeyedBuckets rate-limits each key (client ID) independently. State is
// sharded to keep lock contention low, and each shard is capped at MaxKeys:
// when full, buckets idle long enough to be full again are dropped, since
// forgetting a full bucket is lossless.
type KeyedBuckets struct {
	rate    float64 // tokens per second
	burst   float64
	maxKeys int
	now     func() time.Time
	shards  [shards]shard

	Allowed atomic.Uint64
	Limited atomic.Uint64
}

func NewKeyed(rate float64, burst int, maxKeysPerShard int) *KeyedBuckets {
	return NewKeyedWithClock(rate, burst, maxKeysPerShard, time.Now)
}

func NewKeyedWithClock(rate float64, burst int, maxKeysPerShard int, now func() time.Time) *KeyedBuckets {
	if maxKeysPerShard <= 0 {
		maxKeysPerShard = 4096
	}
	k := &KeyedBuckets{rate: rate, burst: float64(burst), maxKeys: maxKeysPerShard, now: now}
	for i := range k.shards {
		k.shards[i].m = map[string]*bucket{}
	}
	return k
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// Allow takes one token for key, reporting false if the bucket is empty.
func (k *KeyedBuckets) Allow(key string) bool {
	sh := &k.shards[fnv32(key)%shards]
	now := k.now()
	sh.mu.Lock()
	b, ok := sh.m[key]
	if !ok {
		if len(sh.m) >= k.maxKeys {
			k.evict(sh, now)
		}
		b = &bucket{tokens: k.burst, last: now}
		sh.m[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * k.rate
	if b.tokens > k.burst {
		b.tokens = k.burst
	}
	b.last = now
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	sh.mu.Unlock()
	if allowed {
		k.Allowed.Add(1)
	} else {
		k.Limited.Add(1)
	}
	return allowed
}

func (k *KeyedBuckets) evict(sh *shard, now time.Time) {
	refill := time.Duration(k.burst / k.rate * float64(time.Second))
	for key, b := range sh.m {
		if now.Sub(b.last) >= refill {
			delete(sh.m, key)
		}
	}
	if len(sh.m) >= k.maxKeys { // everyone is active: drop an arbitrary key
		for key := range sh.m {
			delete(sh.m, key)
			break
		}
	}
}

// Keys returns the number of tracked keys (for tests and metrics).
func (k *KeyedBuckets) Keys() int {
	n := 0
	for i := range k.shards {
		k.shards[i].mu.Lock()
		n += len(k.shards[i].m)
		k.shards[i].mu.Unlock()
	}
	return n
}
