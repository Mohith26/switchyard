// Package cache is the storage and miss-handling core of a cache node: a
// sharded LRU with per-entry TTL and a stale-while-revalidate window, and a
// singleflight group that collapses concurrent misses for one key into a
// single upstream fetch.
package cache

import (
	"container/list"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Entry is an immutable cached response.
type Entry struct {
	Key      string
	Status   int
	Header   http.Header
	Body     []byte
	StoredAt time.Time
	TTL      time.Duration // fresh for TTL
	Stale    time.Duration // then servable-while-revalidating for Stale more
}

type State int

const (
	Miss  State = iota
	Fresh       // within TTL
	Stale       // expired, but inside the stale-while-revalidate window
)

func (e *Entry) state(now time.Time) State {
	age := now.Sub(e.StoredAt)
	switch {
	case age < e.TTL:
		return Fresh
	case age < e.TTL+e.Stale:
		return Stale
	}
	return Miss
}

type item struct {
	e   *Entry
	ele *list.Element
}

type lruShard struct {
	mu    sync.Mutex
	items map[string]*item
	order *list.List // front = most recently used
	cap   int
}

const numShards = 32

// Store is a sharded LRU. Each shard holds capacity/32 entries, so eviction is
// approximately (not globally) least-recently-used, the usual trade for not
// having one lock on every request.
type Store struct {
	shards [numShards]lruShard
	now    func() time.Time

	Evictions atomic.Uint64
}

func NewStore(capacity int) *Store { return NewStoreWithClock(capacity, time.Now) }

func NewStoreWithClock(capacity int, now func() time.Time) *Store {
	per := capacity / numShards
	if per < 1 {
		per = 1
	}
	s := &Store{now: now}
	for i := range s.shards {
		s.shards[i] = lruShard{items: map[string]*item{}, order: list.New(), cap: per}
	}
	return s
}

func (s *Store) shard(key string) *lruShard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &s.shards[h%numShards]
}

// Get returns the entry and its freshness. Fully expired entries are removed.
func (s *Store) Get(key string) (*Entry, State) {
	sh := s.shard(key)
	now := s.now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it, ok := sh.items[key]
	if !ok {
		return nil, Miss
	}
	st := it.e.state(now)
	if st == Miss {
		sh.order.Remove(it.ele)
		delete(sh.items, key)
		return nil, Miss
	}
	sh.order.MoveToFront(it.ele)
	return it.e, st
}

// Set inserts or replaces an entry, evicting the least recently used entry in
// the shard if it is full.
func (s *Store) Set(e *Entry) {
	sh := s.shard(e.Key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if it, ok := sh.items[e.Key]; ok {
		it.e = e
		sh.order.MoveToFront(it.ele)
		return
	}
	for len(sh.items) >= sh.cap {
		back := sh.order.Back()
		if back == nil {
			break
		}
		k := back.Value.(string)
		sh.order.Remove(back)
		delete(sh.items, k)
		s.Evictions.Add(1)
	}
	sh.items[e.Key] = &item{e: e, ele: sh.order.PushFront(e.Key)}
}

// Len returns the number of stored entries.
func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		n += len(s.shards[i].items)
		s.shards[i].mu.Unlock()
	}
	return n
}

// Purge removes everything (used when a node "restarts" cold in the lab).
func (s *Store) Purge() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.items = map[string]*item{}
		sh.order.Init()
		sh.mu.Unlock()
	}
}
