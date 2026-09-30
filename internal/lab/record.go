package lab

import (
	"context"
	"math"
	"time"

	"github.com/Mohith26/switchyard/internal/breaker"
	"github.com/Mohith26/switchyard/internal/loadgen"
	"github.com/Mohith26/switchyard/internal/metrics"
)

// TickInterval is the recording resolution.
const TickInterval = 100 * time.Millisecond

// ClassTick is one traffic class during one tick. Counts are per tick.
type ClassTick struct {
	Sent    uint64  `json:"sent"`
	OK      uint64  `json:"ok"`
	Failed  uint64  `json:"failed"`  // 5xx, timeouts and connection errors
	Shed    uint64  `json:"shed"`    // fast 503 from the adaptive limit
	Limited uint64  `json:"limited"` // 429 from the rate limit
	P50     float64 `json:"p50"`     // ms, successful requests
	P99     float64 `json:"p99"`
}

type NodeTick struct {
	Req     uint64 `json:"req"`
	Up      bool   `json:"up"`  // the node process is serving
	Out     bool   `json:"out"` // up, but the edge has taken it out of rotation
	Entries int    `json:"entries"`
}

type OriginTick struct {
	Req    uint64 `json:"req"`
	Queue  int    `json:"queue"`
	Busy   int64  `json:"busy"`
	Up     bool   `json:"up"`
	OpenBy int    `json:"open_by"` // cache nodes whose breaker for this origin is open
}

// Tick is everything recorded in one 100ms interval.
type Tick struct {
	T         float64              `json:"t"` // seconds since the measured run began
	Sent      uint64               `json:"sent"`
	OK        uint64               `json:"ok"`
	Failed    uint64               `json:"failed"`
	Timeout   uint64               `json:"timeout"`
	ConnErr   uint64               `json:"conn_err"`
	Shed      uint64               `json:"shed"`
	Limited   uint64               `json:"limited"`
	P50       float64              `json:"p50"`
	P99       float64              `json:"p99"`
	Hit       uint64               `json:"hit"`
	Stale     uint64               `json:"stale"`
	Miss      uint64               `json:"miss"`
	Coalesced uint64               `json:"coalesced"`
	Pass      uint64               `json:"pass"`
	OriginReq uint64               `json:"origin_req"`
	Wasted    uint64               `json:"wasted"` // origin work finished after the caller gave up
	Rejected  uint64               `json:"rejected"`
	Retries   uint64               `json:"retries"`
	Exhausted uint64               `json:"budget_exhausted"`
	FastFail  uint64               `json:"fast_fail"` // requests refused by an open breaker
	Watch     uint64               `json:"watch"`     // origin fetches of the scenario's watched path
	Limit     int                  `json:"limit"`     // adaptive concurrency limit (0 = off)
	Nodes     []NodeTick           `json:"nodes"`
	Origins   []OriginTick         `json:"origins"`
	Classes   map[string]ClassTick `json:"classes"`
	Gens      map[string]uint64    `json:"gens,omitempty"` // successful responses per edge PID
}

type classCum struct {
	sent, ok, failed, timeout, connErr, shed, limited uint64
	hit, stale, miss, coalesced, pass                 uint64
	lat                                               metrics.Snapshot
	gens                                              map[string]uint64
}

type cum struct {
	classes                                        map[string]classCum
	nodeReq                                        []uint64
	originReq                                      []uint64
	wasted, rejected, retries, exhausted, fastFail uint64
	watch                                          uint64
}

type recorder struct {
	topo  *Topology
	gen   *loadgen.Generator
	watch []*metrics.Counter
	prev  cum
	start time.Time
	ticks []Tick
	out   func(Tick)
}

func (r *recorder) snap() cum {
	c := cum{classes: map[string]classCum{}}
	for _, name := range r.gen.Classes() {
		st := r.gen.Stats(name)
		c.classes[name] = classCum{
			sent: st.Sent.Load(), ok: st.OK.Load(),
			failed:  st.Err5xx.Load() + st.Timeout.Load() + st.ConnErr.Load(),
			timeout: st.Timeout.Load(), connErr: st.ConnErr.Load(),
			shed: st.Shed.Load(), limited: st.Limited.Load(),
			hit: st.Hit.Load(), stale: st.Stale.Load(), miss: st.Miss.Load(), coalesced: st.Coalesced.Load(), pass: st.Pass.Load(),
			lat: st.Latency.Snapshot(), gens: st.Gens(),
		}
	}
	for _, n := range r.topo.Caches {
		c.nodeReq = append(c.nodeReq, n.Requests.Load())
		s := n.Upstream().Stats()
		c.retries += s.Retries
		c.exhausted += s.BudgetExhausted
		c.fastFail += s.BreakerRejects + s.NoEndpoint
	}
	for _, o := range r.topo.Origins {
		c.originReq = append(c.originReq, o.Received.Load())
		c.wasted += o.Wasted.Load()
		c.rejected += o.Rejected.Load()
	}
	for _, w := range r.watch {
		c.watch += w.Load()
	}
	return c
}

func ms(d time.Duration) float64 { return math.Round(float64(d.Microseconds())/10) / 100 }

func (r *recorder) tick(now time.Time) Tick {
	cur := r.snap()
	p := r.prev
	t := Tick{T: math.Round(now.Sub(r.start).Seconds()*10) / 10, Classes: map[string]ClassTick{}}
	var all metrics.Snapshot
	gens := map[string]uint64{}
	for name, cc := range cur.classes {
		pc := p.classes[name]
		lat := cc.lat.Sub(pc.lat)
		if pc.lat.Counts == nil {
			lat = cc.lat
		}
		ct := ClassTick{Sent: cc.sent - pc.sent, OK: cc.ok - pc.ok, Failed: cc.failed - pc.failed,
			Shed: cc.shed - pc.shed, Limited: cc.limited - pc.limited, P50: ms(lat.Quantile(0.5)), P99: ms(lat.Quantile(0.99))}
		t.Classes[name] = ct
		t.Sent += ct.Sent
		t.OK += ct.OK
		t.Failed += ct.Failed
		t.Shed += ct.Shed
		t.Limited += ct.Limited
		t.Timeout += cc.timeout - pc.timeout
		t.ConnErr += cc.connErr - pc.connErr
		t.Hit += cc.hit - pc.hit
		t.Stale += cc.stale - pc.stale
		t.Miss += cc.miss - pc.miss
		t.Coalesced += cc.coalesced - pc.coalesced
		t.Pass += cc.pass - pc.pass
		all.Merge(lat)
		for g, n := range cc.gens {
			if d := n - pc.gens[g]; d > 0 {
				gens[g] += d
			}
		}
	}
	t.P50, t.P99 = ms(all.Quantile(0.5)), ms(all.Quantile(0.99))
	if len(gens) > 0 {
		t.Gens = gens
	}
	inRotation := map[string]bool{}
	if r.topo.Edge != nil {
		for _, a := range r.topo.Edge.Placement() {
			inRotation[a] = true
		}
	}
	for i, n := range r.topo.Caches {
		var prev uint64
		if i < len(p.nodeReq) {
			prev = p.nodeReq[i]
		}
		out := r.topo.Edge != nil && n.Up() && !inRotation[n.Addr()]
		t.Nodes = append(t.Nodes, NodeTick{Req: cur.nodeReq[i] - prev, Up: n.Up(), Out: out, Entries: n.Store().Len()})
	}
	for i, o := range r.topo.Origins {
		var prev uint64
		if i < len(p.originReq) {
			prev = p.originReq[i]
		}
		open := 0
		for _, n := range r.topo.Caches {
			if b := n.Upstream().Breaker(i); b != nil && b.State() == breaker.Open {
				open++
			}
		}
		t.Origins = append(t.Origins, OriginTick{Req: cur.originReq[i] - prev, Queue: o.Queued(), Busy: o.Busy.Load(), Up: o.Up(), OpenBy: open})
		t.OriginReq += cur.originReq[i] - prev
	}
	t.Wasted = cur.wasted - p.wasted
	t.Rejected = cur.rejected - p.rejected
	t.Retries = cur.retries - p.retries
	t.Exhausted = cur.exhausted - p.exhausted
	t.FastFail = cur.fastFail - p.fastFail
	t.Watch = cur.watch - p.watch
	if r.topo.Edge != nil {
		t.Limit = r.topo.Edge.ConcurrencyLimit()
	}
	r.prev = cur
	return t
}

// run samples until ctx ends.
func (r *recorder) run(ctx context.Context) {
	r.start = time.Now()
	r.prev = r.snap()
	tk := time.NewTicker(TickInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tk.C:
			t := r.tick(now)
			r.ticks = append(r.ticks, t)
			if r.out != nil {
				r.out(t)
			}
		}
	}
}
