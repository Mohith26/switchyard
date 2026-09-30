package cache

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFreshStaleMiss(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewStoreWithClock(100, func() time.Time { return now })
	s.Set(&Entry{Key: "k", Status: 200, StoredAt: now, TTL: time.Second, Stale: 2 * time.Second})
	if _, st := s.Get("k"); st != Fresh {
		t.Fatalf("want fresh, got %v", st)
	}
	now = now.Add(1500 * time.Millisecond)
	if _, st := s.Get("k"); st != Stale {
		t.Fatalf("want stale inside the SWR window, got %v", st)
	}
	now = now.Add(2 * time.Second)
	if _, st := s.Get("k"); st != Miss {
		t.Fatalf("want miss after TTL+stale, got %v", st)
	}
	if s.Len() != 0 {
		t.Fatal("expired entry should be removed on read")
	}
}

func TestLRUEviction(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewStoreWithClock(numShards*2, func() time.Time { return now }) // 2 per shard
	// Find three keys in the same shard.
	var same []string
	target := s.shard("a")
	for i := 0; len(same) < 3; i++ {
		k := "key-" + strconv.Itoa(i)
		if s.shard(k) == target {
			same = append(same, k)
		}
	}
	for _, k := range same[:2] {
		s.Set(&Entry{Key: k, StoredAt: now, TTL: time.Hour})
	}
	s.Get(same[0]) // touch: same[1] is now least recently used
	s.Set(&Entry{Key: same[2], StoredAt: now, TTL: time.Hour})
	if _, st := s.Get(same[1]); st != Miss {
		t.Fatal("least recently used entry should have been evicted")
	}
	if _, st := s.Get(same[0]); st != Fresh {
		t.Fatal("recently used entry should survive")
	}
}

func TestSingleflightCollapsesConcurrentCalls(t *testing.T) {
	var g Group
	var calls atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	const n = 500
	results := make([]*Entry, n)
	shared := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, _, sh := g.Do("k", func() (*Entry, error) {
				calls.Add(1)
				<-release
				return &Entry{Key: "k", Body: []byte("v")}, nil
			})
			results[i], shared[i] = e, sh
		}(i)
	}
	// Let every goroutine arrive, then finish the one fetch.
	for g.Collapsed.Load()+g.Leaders.Load() < n {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	if c := calls.Load(); c != 1 {
		t.Fatalf("fn ran %d times, want 1", c)
	}
	leaders := 0
	for i := range results {
		if results[i] == nil || string(results[i].Body) != "v" {
			t.Fatal("every caller must get the result")
		}
		if !shared[i] {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders, want 1", leaders)
	}
}

func TestSingleflightPropagatesErrorsAndPanics(t *testing.T) {
	var g Group
	boom := errors.New("boom")
	if _, err, _ := g.Do("k", func() (*Entry, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatal("error must propagate")
	}
	if _, err, _ := g.Do("k", func() (*Entry, error) { panic("x") }); err == nil {
		t.Fatal("a panicking fetch must become an error, not crash the node")
	}
	// The key must be usable again afterwards.
	if e, err, _ := g.Do("k", func() (*Entry, error) { return &Entry{Key: "k"}, nil }); err != nil || e == nil {
		t.Fatal("group must recover after a failed call")
	}
}

func BenchmarkStoreGetHit(b *testing.B) {
	s := NewStore(100000)
	now := time.Now()
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "/wiki/page-" + strconv.Itoa(i)
		s.Set(&Entry{Key: keys[i], StoredAt: now, TTL: time.Hour})
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(keys[i&4095])
			i++
		}
	})
}
