package cache

import (
	"sync"
	"sync/atomic"
)

type call struct {
	wg      sync.WaitGroup
	val     *Entry
	err     error
	waiters int
}

// Group collapses concurrent calls for the same key: the first caller (the
// leader) runs fn, everyone who arrives while it is running waits and shares
// the result. After a cache entry expires under heavy traffic, this is the
// difference between one origin fetch and one fetch per concurrent request.
type Group struct {
	mu sync.Mutex
	m  map[string]*call

	Leaders   atomic.Uint64 // fetches actually performed
	Collapsed atomic.Uint64 // callers who waited on someone else's fetch
}

// Do runs fn once per key at a time. shared is true for callers that did not
// run fn themselves.
func (g *Group) Do(key string, fn func() (*Entry, error)) (e *Entry, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = map[string]*call{}
	}
	if c, ok := g.m[key]; ok {
		c.waiters++
		g.mu.Unlock()
		g.Collapsed.Add(1)
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &call{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()
	g.Leaders.Add(1)

	func() {
		defer func() {
			if r := recover(); r != nil {
				c.err = errPanic
			}
		}()
		c.val, c.err = fn()
	}()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	c.wg.Done()
	return c.val, c.err, false
}

// DoAsync starts fn in the background unless a call for key is already in
// flight. It is used for stale-while-revalidate refreshes.
func (g *Group) DoAsync(key string, fn func() (*Entry, error)) bool {
	g.mu.Lock()
	if g.m == nil {
		g.m = map[string]*call{}
	}
	if _, ok := g.m[key]; ok {
		g.mu.Unlock()
		return false
	}
	g.mu.Unlock()
	go g.Do(key, fn)
	return true
}

type panicErr struct{}

func (panicErr) Error() string { return "cache: fetch panicked" }

var errPanic error = panicErr{}
