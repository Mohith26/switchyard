package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is a monotonically increasing uint64.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n uint64) { c.v.Add(n) }
func (c *Counter) Load() uint64 { return c.v.Load() }

// Gauge is a signed value that can go up and down.
type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(n int64) { g.v.Store(n) }
func (g *Gauge) Add(n int64) { g.v.Add(n) }
func (g *Gauge) Load() int64 { return g.v.Load() }

type kind int

const (
	kindCounter kind = iota
	kindGauge
	kindHistogram
)

type series struct {
	labels string // pre-rendered {a="b",c="d"}
	c      *Counter
	g      *Gauge
	gf     func() float64
	h      *Histogram
}

type family struct {
	name, help string
	kind       kind
	series     []*series
}

// Registry collects named metrics and renders them in the Prometheus text
// exposition format (version 0.0.4).
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

func renderLabels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	if len(kv)%2 != 0 {
		panic("metrics: labels must be key/value pairs")
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", kv[i], kv[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

func (r *Registry) fam(name, help string, k kind) *family {
	f, ok := r.families[name]
	if !ok {
		f = &family{name: name, help: help, kind: k}
		r.families[name] = f
	}
	if f.kind != k {
		panic("metrics: " + name + " registered with two different types")
	}
	return f
}

// Counter registers (or returns) a counter series.
func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.fam(name, help, kindCounter)
	l := renderLabels(labels)
	for _, s := range f.series {
		if s.labels == l {
			return s.c
		}
	}
	c := &Counter{}
	f.series = append(f.series, &series{labels: l, c: c})
	return c
}

// Gauge registers (or returns) a gauge series.
func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.fam(name, help, kindGauge)
	l := renderLabels(labels)
	for _, s := range f.series {
		if s.labels == l && s.g != nil {
			return s.g
		}
	}
	g := &Gauge{}
	f.series = append(f.series, &series{labels: l, g: g})
	return g
}

// GaugeFunc registers a gauge whose value is computed at scrape time.
func (r *Registry) GaugeFunc(name, help string, fn func() float64, labels ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.fam(name, help, kindGauge)
	f.series = append(f.series, &series{labels: renderLabels(labels), gf: fn})
}

// Histogram registers (or returns) a latency histogram series.
func (r *Registry) Histogram(name, help string, labels ...string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.fam(name, help, kindHistogram)
	l := renderLabels(labels)
	for _, s := range f.series {
		if s.labels == l {
			return s.h
		}
	}
	h := &Histogram{}
	f.series = append(f.series, &series{labels: l, h: h})
	return h
}

// Exported bucket boundaries, in seconds.
var promBuckets = []time.Duration{
	500 * time.Microsecond, time.Millisecond, 2500 * time.Microsecond, 5 * time.Millisecond,
	10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
	250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2500 * time.Millisecond, 5 * time.Second,
}

func withLabel(labels, k, v string) string {
	add := fmt.Sprintf("%s=%q", k, v)
	if labels == "" {
		return "{" + add + "}"
	}
	return labels[:len(labels)-1] + "," + add + "}"
}

// WriteText renders every metric in the Prometheus text format.
func (r *Registry) WriteText(w io.Writer) error {
	r.mu.Lock()
	names := make([]string, 0, len(r.families))
	for n := range r.families {
		names = append(names, n)
	}
	sort.Strings(names)
	fams := make([]*family, len(names))
	for i, n := range names {
		f := r.families[n]
		cp := *f
		cp.series = append([]*series(nil), f.series...)
		fams[i] = &cp
	}
	r.mu.Unlock()

	for _, f := range fams {
		typ := [...]string{"counter", "gauge", "histogram"}[f.kind]
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, typ); err != nil {
			return err
		}
		for _, s := range f.series {
			switch {
			case s.c != nil:
				fmt.Fprintf(w, "%s%s %d\n", f.name, s.labels, s.c.Load())
			case s.g != nil:
				fmt.Fprintf(w, "%s%s %d\n", f.name, s.labels, s.g.Load())
			case s.gf != nil:
				fmt.Fprintf(w, "%s%s %g\n", f.name, s.labels, s.gf())
			case s.h != nil:
				snap := s.h.Snapshot()
				for _, b := range promBuckets {
					fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, withLabel(s.labels, "le", fmt.Sprintf("%g", b.Seconds())), snap.CountBelow(b))
				}
				fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, withLabel(s.labels, "le", "+Inf"), snap.N)
				fmt.Fprintf(w, "%s_sum%s %g\n", f.name, s.labels, float64(snap.Sum)/1e6)
				fmt.Fprintf(w, "%s_count%s %d\n", f.name, s.labels, snap.N)
			}
		}
	}
	return nil
}

// Handler serves the registry at a /metrics endpoint.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_ = r.WriteText(w)
	})
}
