// Package metrics provides lock-free counters, gauges and a log-linear latency
// histogram, plus a small Prometheus text-format exporter. It has no
// dependencies outside the standard library.
package metrics

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Histogram buckets values (microseconds) on a log-linear scale: exact below
// 64us, then 32 linear sub-buckets per power of two, which bounds the relative
// error of any reported quantile to about 3%. Recording is a single atomic add.
const (
	subBits    = 5
	subCount   = 1 << subBits // 32 sub-buckets per octave
	linearMax  = 2 * subCount // values below 64 get their own bucket
	maxOctaves = 36           // covers up to ~2^41 us, far beyond any timeout
	numBuckets = linearMax + maxOctaves*subCount
)

// Histogram is safe for concurrent use.
type Histogram struct {
	counts [numBuckets]atomic.Uint64
	sum    atomic.Uint64 // microseconds
	n      atomic.Uint64
}

func bucketOf(us uint64) int {
	if us < linearMax {
		return int(us)
	}
	e := bits.Len64(us) - (subBits + 1) // shift so the value lands in [32,63]
	idx := linearMax + (e-1)*subCount + int(us>>uint(e)) - subCount
	if idx >= numBuckets {
		return numBuckets - 1
	}
	return idx
}

// bucketBounds returns the inclusive lower and exclusive upper bound of a bucket.
func bucketBounds(i int) (lo, hi uint64) {
	if i < linearMax {
		return uint64(i), uint64(i) + 1
	}
	j := i - linearMax
	e := j/subCount + 1
	m := uint64(j%subCount + subCount)
	return m << uint(e), (m + 1) << uint(e)
}

// Record adds one observation.
func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.counts[bucketOf(uint64(us))].Add(1)
	h.sum.Add(uint64(us))
	h.n.Add(1)
}

// Snapshot is an immutable copy of a histogram. Snapshots can be subtracted to
// get the distribution of a time window, which is how per-tick percentiles are
// computed without resetting the live histogram.
type Snapshot struct {
	Counts []uint64
	Sum    uint64
	N      uint64
}

// Snapshot copies the current state.
func (h *Histogram) Snapshot() Snapshot {
	s := Snapshot{Counts: make([]uint64, numBuckets)}
	for i := range h.counts {
		s.Counts[i] = h.counts[i].Load()
	}
	s.Sum = h.sum.Load()
	s.N = h.n.Load()
	return s
}

// Sub returns s minus prev (the observations recorded between the two).
func (s Snapshot) Sub(prev Snapshot) Snapshot {
	out := Snapshot{Counts: make([]uint64, len(s.Counts)), Sum: s.Sum - prev.Sum, N: s.N - prev.N}
	for i := range s.Counts {
		var p uint64
		if i < len(prev.Counts) {
			p = prev.Counts[i]
		}
		out.Counts[i] = s.Counts[i] - p
	}
	return out
}

// Merge adds o into s.
func (s *Snapshot) Merge(o Snapshot) {
	if s.Counts == nil {
		s.Counts = make([]uint64, numBuckets)
	}
	for i := range o.Counts {
		s.Counts[i] += o.Counts[i]
	}
	s.Sum += o.Sum
	s.N += o.N
}

// Quantile returns the q-quantile (0..1). It reports the midpoint of the bucket
// holding the target rank, so the error is bounded by half a bucket width.
func (s Snapshot) Quantile(q float64) time.Duration {
	if s.N == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(s.N)))
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	for i, c := range s.Counts {
		seen += c
		if seen >= rank {
			lo, hi := bucketBounds(i)
			return time.Duration((lo+hi)/2) * time.Microsecond
		}
	}
	lo, _ := bucketBounds(len(s.Counts) - 1)
	return time.Duration(lo) * time.Microsecond
}

// Mean returns the arithmetic mean.
func (s Snapshot) Mean() time.Duration {
	if s.N == 0 {
		return 0
	}
	return time.Duration(s.Sum/s.N) * time.Microsecond
}

// CountBelow returns how many observations were strictly below d. Used for the
// cumulative "le" buckets of the Prometheus exporter.
func (s Snapshot) CountBelow(d time.Duration) uint64 {
	lim := uint64(d.Microseconds())
	var n uint64
	for i, c := range s.Counts {
		_, hi := bucketBounds(i)
		if hi > lim+1 {
			break
		}
		n += c
	}
	return n
}
