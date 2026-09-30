package metrics

import (
	"bytes"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBucketBoundsContainValue(t *testing.T) {
	for _, us := range []uint64{0, 1, 63, 64, 65, 127, 128, 1000, 12345, 1 << 20, 987654321} {
		lo, hi := bucketBounds(bucketOf(us))
		if us < lo || us >= hi {
			t.Fatalf("value %d landed in bucket [%d,%d)", us, lo, hi)
		}
	}
}

func TestQuantileRelativeError(t *testing.T) {
	var h Histogram
	rng := rand.New(rand.NewPCG(1, 2))
	vals := make([]float64, 100000)
	for i := range vals {
		v := math.Exp(rng.NormFloat64()*0.8) * 20000 // lognormal around 20ms
		vals[i] = v
		h.Record(time.Duration(v) * time.Microsecond)
	}
	sort.Float64s(vals)
	s := h.Snapshot()
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := vals[int(math.Ceil(q*float64(len(vals))))-1]
		got := float64(s.Quantile(q).Microseconds())
		if rel := math.Abs(got-exact) / exact; rel > 0.035 {
			t.Errorf("q%.3f: got %.0fus want %.0fus (%.1f%% off)", q, got, exact, rel*100)
		}
	}
}

func TestSnapshotSubIsolatesWindow(t *testing.T) {
	var h Histogram
	for i := 0; i < 1000; i++ {
		h.Record(time.Millisecond)
	}
	before := h.Snapshot()
	for i := 0; i < 1000; i++ {
		h.Record(100 * time.Millisecond)
	}
	w := h.Snapshot().Sub(before)
	if w.N != 1000 {
		t.Fatalf("window count %d", w.N)
	}
	if p := w.Quantile(0.5); p < 95*time.Millisecond || p > 105*time.Millisecond {
		t.Fatalf("window p50 %v, want ~100ms", p)
	}
}

func TestConcurrentRecord(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				h.Record(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if n := h.Snapshot().N; n != 80000 {
		t.Fatalf("lost observations: %d", n)
	}
}

func TestPrometheusText(t *testing.T) {
	r := NewRegistry()
	r.Counter("reqs_total", "Requests.", "node", "a").Add(3)
	r.Gauge("inflight", "In flight.").Set(-2)
	r.GaugeFunc("entries", "Entries.", func() float64 { return 7.5 })
	h := r.Histogram("lat_seconds", "Latency.")
	h.Record(3 * time.Millisecond)
	h.Record(300 * time.Millisecond)
	var b bytes.Buffer
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"# TYPE reqs_total counter", `reqs_total{node="a"} 3`, "inflight -2", "entries 7.5",
		"# TYPE lat_seconds histogram", `lat_seconds_bucket{le="0.005"} 1`, `lat_seconds_bucket{le="0.5"} 2`,
		`lat_seconds_bucket{le="+Inf"} 2`, "lat_seconds_count 2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if r.Counter("reqs_total", "Requests.", "node", "a").Load() != 3 {
		t.Fatal("re-registering must return the same series")
	}
}

func BenchmarkHistogramRecord(b *testing.B) {
	var h Histogram
	b.RunParallel(func(pb *testing.PB) {
		d := 1500 * time.Microsecond
		for pb.Next() {
			h.Record(d)
		}
	})
}
