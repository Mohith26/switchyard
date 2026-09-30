package lab

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/Mohith26/switchyard/internal/loadgen"
	"github.com/Mohith26/switchyard/internal/metrics"
	"github.com/Mohith26/switchyard/internal/trace"
)

// Event is a fault or action applied at a fixed time in both variants.
type Event struct {
	At    time.Duration         `json:"-"`
	T     float64               `json:"t"`
	Label string                `json:"label"`
	Kind  string                `json:"kind"` // "fault", "recover", "deploy", "traffic"
	Do    func(*Topology) error `json:"-"`
}

type VariantSpec struct {
	Key      string   // "without" or "with"
	Label    string   // e.g. "mod-N hashing"
	Settings []string // human-readable config differences
	Spec     Spec
}

type Scenario struct {
	ID        string
	Title     string
	Mechanism string
	Question  string
	Explain   string
	Traffic   string
	Duration  time.Duration
	WarmKeys  int    // warm this many of the most popular pages first
	Watch     string // origin path to count separately
	Events    []Event
	Variants  [2]VariantSpec
	Streams   func(t *Topology) []loadgen.Stream
	Summarize func(r *Result, w *Windows)
	Extra     func(t *Topology) map[string]any // computed per variant after Build
}

// Variant is one recorded run.
type Variant struct {
	Key      string         `json:"key"`
	Label    string         `json:"label"`
	Settings []string       `json:"settings"`
	Ticks    []Tick         `json:"ticks"`
	Extra    map[string]any `json:"extra,omitempty"`
}

// Metric is one headline number compared across the two variants.
type Metric struct {
	Label   string  `json:"label"`
	Without float64 `json:"without"`
	With    float64 `json:"with"`
	Unit    string  `json:"unit"`
	Better  string  `json:"better"` // "lower" or "higher"
	Note    string  `json:"note,omitempty"`
}

type Result struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Mechanism string    `json:"mechanism"`
	Question  string    `json:"question"`
	Explain   string    `json:"explain"`
	Traffic   string    `json:"traffic"`
	Duration  float64   `json:"duration"`
	Recorded  string    `json:"recorded"`
	Host      string    `json:"host"`
	Live      bool      `json:"live"`
	Topology  TopoInfo  `json:"topology"`
	Events    []Event   `json:"events"`
	Variants  []Variant `json:"variants"`
	Summary   []Metric  `json:"summary"`
	Headline  string    `json:"headline"`
}

type TopoInfo struct {
	Origins       int    `json:"origins"`
	OriginWorkers int    `json:"origin_workers"`
	Caches        int    `json:"caches"`
	EdgeProcess   bool   `json:"edge_process"`
	Source        string `json:"trace_source"`
}

// Options control a run.
type Options struct {
	Bin       string  // switchyard binary, for scenarios that run the edge as a process
	TimeScale float64 // < 1 shortens everything (tests)
	RateScale float64 // < 1 lowers every traffic rate (tests, small machines)
	OnTick    func(variant string, t Tick)
	Seed      uint64
}

func (o Options) norm() Options {
	if o.TimeScale <= 0 {
		o.TimeScale = 1
	}
	if o.RateScale <= 0 {
		o.RateScale = 1
	}
	if o.Seed == 0 {
		o.Seed = 20240901
	}
	if o.Bin == "" {
		if exe, err := os.Executable(); err == nil {
			o.Bin = exe
		}
	}
	return o
}

// Windows gives summary functions access to per-tick latency histograms,
// which the JSON does not carry, so window percentiles are exact merges
// rather than averages of per-tick percentiles.
type Windows struct {
	lat   map[string][]map[string]metrics.Snapshot // variant -> tick -> class -> snapshot
	ticks map[string][]Tick
}

// Quantile of successful-request latency (ms) over [from, to) seconds, for the
// given classes (all classes when none are given).
func (w *Windows) Quantile(variant string, from, to, q float64, classes ...string) float64 {
	var s metrics.Snapshot
	for i, t := range w.ticks[variant] {
		if t.T <= from || t.T > to {
			continue
		}
		for c, snap := range w.lat[variant][i] {
			if len(classes) == 0 || contains(classes, c) {
				s.Merge(snap)
			}
		}
	}
	if s.N == 0 {
		return -1 // nothing succeeded in the window
	}
	return ms(s.Quantile(q))
}

// Done counts requests that completed in a tick, whatever the outcome.
func Done(t Tick) float64 { return float64(t.OK + t.Failed + t.Shed + t.Limited) }

// ClassDone is Done for one class.
func ClassDone(c ClassTick) float64 { return float64(c.OK + c.Failed + c.Shed + c.Limited) }

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Sum adds f(tick) over ticks in (from, to].
func (w *Windows) Sum(variant string, from, to float64, f func(Tick) float64) float64 {
	var s float64
	for _, t := range w.ticks[variant] {
		if t.T > from && t.T <= to {
			s += f(t)
		}
	}
	return s
}

// Max of f over ticks in (from, to].
func (w *Windows) Max(variant string, from, to float64, f func(Tick) float64) float64 {
	m := 0.0
	for _, t := range w.ticks[variant] {
		if t.T > from && t.T <= to {
			m = math.Max(m, f(t))
		}
	}
	return m
}

// Ticks returns a variant's series.
func (w *Windows) Ticks(variant string) []Tick { return w.ticks[variant] }

var keysOnce sync.Once
var keySampler *trace.Sampler
var keyErr error

func sampler() (*trace.Sampler, error) {
	keysOnce.Do(func() { keySampler, keyErr = trace.NewSampler(10000) })
	return keySampler, keyErr
}

// Run records both variants one after the other on identical traffic.
func Run(ctx context.Context, sc *Scenario, o Options) (*Result, error) {
	o = o.norm()
	res := newResult(sc)
	w := &Windows{lat: map[string][]map[string]metrics.Snapshot{}, ticks: map[string][]Tick{}}
	for i := range sc.Variants {
		v, lat, err := runVariant(ctx, sc, i, o)
		if err != nil {
			return nil, fmt.Errorf("%s/%s: %w", sc.ID, sc.Variants[i].Key, err)
		}
		res.Variants = append(res.Variants, v)
		w.ticks[v.Key], w.lat[v.Key] = v.Ticks, lat
		if i == 0 && !ctxSleep(ctx, 750*time.Millisecond) {
			return nil, ctx.Err()
		}
	}
	if sc.Summarize != nil {
		sc.Summarize(res, w)
	}
	return res, nil
}

// RunLive runs both variants at the same time, streaming ticks as they happen.
func RunLive(ctx context.Context, sc *Scenario, o Options) (*Result, error) {
	o = o.norm()
	res := newResult(sc)
	res.Live = true
	w := &Windows{lat: map[string][]map[string]metrics.Snapshot{}, ticks: map[string][]Tick{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	vs := make([]Variant, 2)
	errs := make([]error, 2)
	for i := range sc.Variants {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, lat, err := runVariant(ctx, sc, i, o)
			mu.Lock()
			defer mu.Unlock()
			vs[i], errs[i] = v, err
			w.ticks[v.Key], w.lat[v.Key] = v.Ticks, lat
		}(i)
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	res.Variants = vs
	if sc.Summarize != nil {
		sc.Summarize(res, w)
	}
	return res, nil
}

func newResult(sc *Scenario) *Result {
	res := &Result{ID: sc.ID, Title: sc.Title, Mechanism: sc.Mechanism, Question: sc.Question, Explain: sc.Explain,
		Traffic: sc.Traffic, Duration: sc.Duration.Seconds(), Recorded: time.Now().UTC().Format(time.RFC3339),
		Host: fmt.Sprintf("%s/%s, %d CPUs, %s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())}
	sp := sc.Variants[0].Spec
	res.Topology = TopoInfo{Origins: sp.Origins, OriginWorkers: sp.Origin.Workers, Caches: sp.Caches, EdgeProcess: sp.EdgeProcess, Source: trace.Source}
	for _, e := range sc.Events {
		e.T = e.At.Seconds()
		res.Events = append(res.Events, e)
	}
	sort.Slice(res.Events, func(i, j int) bool { return res.Events[i].T < res.Events[j].T })
	return res
}

func runVariant(ctx context.Context, sc *Scenario, idx int, o Options) (Variant, []map[string]metrics.Snapshot, error) {
	vs := sc.Variants[idx]
	v := Variant{Key: vs.Key, Label: vs.Label, Settings: vs.Settings}
	spec := vs.Spec
	spec.Bin = o.Bin
	topo, err := Build(spec)
	if err != nil {
		return v, nil, err
	}
	defer topo.Close()
	if sc.Extra != nil {
		v.Extra = sc.Extra(topo)
	}
	if sc.WarmKeys > 0 {
		s, err := sampler()
		if err != nil {
			return v, nil, err
		}
		paths := s.Paths()
		if sc.WarmKeys < len(paths) {
			paths = paths[:sc.WarmKeys]
		}
		if err := loadgen.Warm(ctx, topo.URL(), paths, 48); err != nil {
			return v, nil, fmt.Errorf("warm: %w", err)
		}
	}
	streams := sc.Streams(topo)
	for i := range streams {
		rate := streams[i].Rate
		streams[i].Rate = func(t time.Duration) float64 { return rate(t) * o.RateScale }
	}
	gen := loadgen.New(topo.URL(), time.Second, o.Seed, streams...)
	rec := &recorder{topo: topo, gen: gen}
	if sc.Watch != "" {
		for _, or := range topo.Origins {
			rec.watch = append(rec.watch, or.Watch(sc.Watch))
		}
	}
	var lat []map[string]metrics.Snapshot
	prevLat := map[string]metrics.Snapshot{}
	rec.out = func(t Tick) {
		row := map[string]metrics.Snapshot{}
		for _, c := range gen.Classes() {
			cur := gen.Stats(c).Latency.Snapshot()
			if p, ok := prevLat[c]; ok {
				row[c] = cur.Sub(p)
			} else {
				row[c] = cur
			}
			prevLat[c] = cur
		}
		lat = append(lat, row)
		if o.OnTick != nil {
			o.OnTick(vs.Key, t)
		}
	}
	for _, c := range gen.Classes() {
		prevLat[c] = gen.Stats(c).Latency.Snapshot()
	}
	dur := sc.Duration
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); rec.run(runCtx) }()
	var evErr error
	var evMu sync.Mutex
	for _, e := range sc.Events {
		wg.Add(1)
		go func(e Event) {
			defer wg.Done()
			if !ctxSleep(runCtx, e.At) || e.Do == nil {
				return
			}
			if err := e.Do(topo); err != nil {
				evMu.Lock()
				evErr = errors.Join(evErr, fmt.Errorf("event %q: %w", e.Label, err))
				evMu.Unlock()
			}
		}(e)
	}
	gen.Run(ctx, dur)
	// Keep recording until the last in-flight requests have returned.
	ctxSleep(ctx, TickInterval+50*time.Millisecond)
	cancel()
	wg.Wait()
	v.Ticks = rec.ticks
	n := int(dur/TickInterval) + 1
	if len(v.Ticks) > n {
		v.Ticks, lat = v.Ticks[:n], lat[:n]
	}
	return v, lat, evErr
}
