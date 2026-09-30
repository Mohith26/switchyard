// Package ring implements the key-to-node placement strategies the edge uses
// to pick a cache node: a consistent-hash ring with virtual nodes and optional
// bounded loads, and plain mod-N hashing as the baseline it is compared with.
package ring

import (
	"math"
	"sort"
	"strconv"
)

// Picker maps a key to the index of a node in Nodes().
type Picker interface {
	Pick(key string) int
	Nodes() []string
}

// Hash is 64-bit FNV-1a followed by the murmur3 finalizer. FNV alone clusters
// badly on short, similar strings such as "node-1#17"; the finalizer spreads
// them across the whole ring.
func Hash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

type point struct {
	h    uint64
	node int32
}

// Ring is an immutable consistent-hash ring. Build a new one to change
// membership; lookups never take a lock.
type Ring struct {
	nodes  []string
	points []point
}

// New builds a ring with vnodes virtual points per node.
func New(nodes []string, vnodes int) *Ring {
	if vnodes < 1 {
		vnodes = 1
	}
	r := &Ring{nodes: append([]string(nil), nodes...), points: make([]point, 0, len(nodes)*vnodes)}
	for i, n := range nodes {
		for v := 0; v < vnodes; v++ {
			r.points = append(r.points, point{h: Hash(n + "#" + strconv.Itoa(v)), node: int32(i)})
		}
	}
	sort.Slice(r.points, func(a, b int) bool { return r.points[a].h < r.points[b].h })
	return r
}

func (r *Ring) Nodes() []string { return r.nodes }

func (r *Ring) search(h uint64) int {
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].h >= h })
	if i == len(r.points) {
		i = 0
	}
	return i
}

// Pick returns the owner of key: the first virtual point clockwise of its hash.
func (r *Ring) Pick(key string) int {
	if len(r.points) == 0 {
		return -1
	}
	return int(r.points[r.search(Hash(key))].node)
}

// Successors returns up to n distinct nodes in ring order starting at the key's
// owner. The edge uses the second entry as the failover target.
func (r *Ring) Successors(key string, n int) []int {
	if len(r.points) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	out := make([]int, 0, n)
	seen := make(map[int32]bool, n)
	start := r.search(Hash(key))
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		if !seen[p.node] {
			seen[p.node] = true
			out = append(out, int(p.node))
		}
	}
	return out
}

// PickBounded implements consistent hashing with bounded loads (Mirrokni,
// Thorup and Zadimoghaddam, 2016). No node may carry more than
// ceil(factor * average) of the current load; a key whose owner is full walks
// clockwise to the next node with room. load(i) reports node i's current load
// (the edge uses in-flight requests). factor must be > 1; 1.25 is typical.
func (r *Ring) PickBounded(key string, load func(i int) int64, factor float64) int {
	if len(r.points) == 0 {
		return -1
	}
	var total int64
	for i := range r.nodes {
		total += load(i)
	}
	capacity := int64(math.Ceil(factor * float64(total+1) / float64(len(r.nodes))))
	start := r.search(Hash(key))
	seen := make([]bool, len(r.nodes))
	for i := 0; i < len(r.points); i++ {
		n := int(r.points[(start+i)%len(r.points)].node)
		if seen[n] {
			continue
		}
		seen[n] = true
		if load(n) < capacity {
			return n
		}
	}
	return int(r.points[start].node)
}

// ModN is hash(key) mod N: perfectly balanced, but changing N moves almost
// every key. It is the baseline the ring is measured against.
type ModN struct{ nodes []string }

func NewModN(nodes []string) *ModN { return &ModN{nodes: append([]string(nil), nodes...)} }

func (m *ModN) Nodes() []string { return m.nodes }

func (m *ModN) Pick(key string) int {
	if len(m.nodes) == 0 {
		return -1
	}
	return int(Hash(key) % uint64(len(m.nodes)))
}

// Moved reports the fraction of keys whose owner changes between two pickers,
// comparing owners by node name rather than index.
func Moved(before, after Picker, keys []string) float64 {
	if len(keys) == 0 {
		return 0
	}
	moved := 0
	bn, an := before.Nodes(), after.Nodes()
	for _, k := range keys {
		if bn[before.Pick(k)] != an[after.Pick(k)] {
			moved++
		}
	}
	return float64(moved) / float64(len(keys))
}
