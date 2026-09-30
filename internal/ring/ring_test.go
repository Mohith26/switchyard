package ring

import (
	"fmt"
	"math"
	"testing"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("/wiki/page-%d", i)
	}
	return out
}

func nodes(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("10.0.0.%d:9000", i+1)
	}
	return out
}

// With v virtual points per node, each node's share has a relative standard
// deviation of roughly 1/sqrt(v), so more points buy better balance.
func TestRingBalance(t *testing.T) {
	ks := keys(100000)
	for _, tc := range []struct {
		vnodes int
		maxDev float64
	}{{160, 0.22}, {1000, 0.09}} {
		r := New(nodes(5), tc.vnodes)
		counts := make([]int, 5)
		for _, k := range ks {
			counts[r.Pick(k)]++
		}
		avg := float64(len(ks)) / 5
		for i, c := range counts {
			if dev := math.Abs(float64(c)-avg) / avg; dev > tc.maxDev {
				t.Errorf("vnodes=%d: node %d holds %d keys, %.1f%% from average", tc.vnodes, i, c, dev*100)
			}
		}
	}
}

func TestRemovingANodeMovesOnlyItsKeys(t *testing.T) {
	all := nodes(5)
	after := append(append([]string(nil), all[:2]...), all[3:]...)
	ks := keys(50000)

	before, aft := New(all, 160), New(after, 160)
	moved := Moved(before, aft, ks)
	if moved < 0.15 || moved > 0.25 {
		t.Fatalf("ring moved %.1f%% of keys, want about 20%% (1/N)", moved*100)
	}
	// Every moved key must have been owned by the removed node.
	for _, k := range ks {
		b, a := before.Nodes()[before.Pick(k)], aft.Nodes()[aft.Pick(k)]
		if b != a && b != all[2] {
			t.Fatalf("key %s moved from a surviving node %s to %s", k, b, a)
		}
	}

	m := Moved(NewModN(all), NewModN(after), ks)
	if m < 0.75 || m > 0.85 {
		t.Fatalf("mod-N moved %.1f%%, want about 80%% ((N-1)/N)", m*100)
	}
}

func TestAddingANodeMovesAboutOneOverN(t *testing.T) {
	ks := keys(50000)
	moved := Moved(New(nodes(5), 160), New(nodes(6), 160), ks)
	if moved < 0.12 || moved > 0.22 {
		t.Fatalf("adding a 6th node moved %.1f%%, want about 16.7%%", moved*100)
	}
}

func TestDeterministic(t *testing.T) {
	a, b := New(nodes(4), 100), New(nodes(4), 100)
	for _, k := range keys(1000) {
		if a.Pick(k) != b.Pick(k) {
			t.Fatal("same membership must give the same placement")
		}
	}
}

func TestSuccessorsDistinct(t *testing.T) {
	r := New(nodes(5), 50)
	for _, k := range keys(500) {
		s := r.Successors(k, 3)
		if len(s) != 3 || s[0] == s[1] || s[1] == s[2] || s[0] == s[2] {
			t.Fatalf("successors not distinct: %v", s)
		}
		if s[0] != r.Pick(k) {
			t.Fatal("first successor must be the owner")
		}
	}
}

func TestBoundedLoadCapsEveryNode(t *testing.T) {
	r := New(nodes(5), 160)
	load := make([]int64, 5)
	// Every request is for the same hot key: plain consistent hashing would put
	// all of it on one node.
	for i := 0; i < 10000; i++ {
		n := r.PickBounded("/wiki/hot", func(j int) int64 { return load[j] }, 1.25)
		load[n]++
	}
	limit := int64(math.Ceil(1.25 * 10001 / 5))
	for i, l := range load {
		if l > limit {
			t.Fatalf("node %d carries %d, over the bound %d", i, l, limit)
		}
	}
}

func BenchmarkRingPick(b *testing.B) {
	r := New(nodes(16), 160)
	ks := keys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Pick(ks[i&1023])
	}
}

func BenchmarkRingPickBounded(b *testing.B) {
	r := New(nodes(16), 160)
	ks := keys(1024)
	load := func(int) int64 { return 3 }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.PickBounded(ks[i&1023], load, 1.25)
	}
}
