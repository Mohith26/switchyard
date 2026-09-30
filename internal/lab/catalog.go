package lab

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/Mohith26/switchyard/internal/breaker"
	"github.com/Mohith26/switchyard/internal/cachenode"
	"github.com/Mohith26/switchyard/internal/edge"
	"github.com/Mohith26/switchyard/internal/loadgen"
	"github.com/Mohith26/switchyard/internal/origin"
	"github.com/Mohith26/switchyard/internal/retry"
	"github.com/Mohith26/switchyard/internal/ring"
	"github.com/Mohith26/switchyard/internal/upstream"
)

// HotPage is the flash-crowd page. It is the real #1 article of the recorded
// hour: the footballer Sol Bamba died on 31 August 2024 and his page drew a
// sudden spike of traffic, which is exactly the shape this scenario replays.
const HotPage = "/wiki/Sol_Bamba"

// The capacity model every scenario shares: 3 origin replicas x 16 workers at
// a ~20ms median service time is roughly 2,300 requests/sec of real capacity.
func baseSpec() Spec {
	return Spec{
		Origins: 3,
		Origin: origin.Config{Workers: 16, QueueCap: 2048, Median: 20 * time.Millisecond, Sigma: 0.25,
			TTL: 120 * time.Second, BodyBytes: 2048},
		Caches: 5,
		Cache: cachenode.Config{Capacity: 64000, DefaultTTL: 120 * time.Second, FetchTimeout: 2 * time.Second, Coalesce: true,
			CacheablePrefixes: []string{"/wiki/"},
			Upstream: upstream.Config{Balancer: "p2c", AttemptTimeout: 300 * time.Millisecond,
				Retry:      retry.Config{Mode: retry.None, MaxAttempts: 1},
				Outlier:    upstream.OutlierConfig{Consecutive: 5, Base: time.Second, Max: 10 * time.Second, MaxPercent: 34},
				HealthPath: "/healthz", HealthInterval: 500 * time.Millisecond}},
		Edge: edge.Config{Hashing: "ring", VNodes: 160, HealthPath: "/healthz", HealthInterval: edge.Duration(250 * time.Millisecond),
			Failover: true, RequestTimeout: edge.Duration(time.Second)},
	}
}

func constant(r float64) func(time.Duration) float64 { return func(time.Duration) float64 { return r } }

// window is on from a to b.
func window(r float64, a, b time.Duration) func(time.Duration) float64 {
	return func(t time.Duration) float64 {
		if t >= a && t < b {
			return r
		}
		return 0
	}
}

func pages() func(*rand.Rand) string {
	s, err := sampler()
	if err != nil {
		panic(err)
	}
	return s.Sample
}

func apiPath(rng *rand.Rand) string { return "/api/items/" + strconv.Itoa(rng.IntN(1_000_000)) }

// Catalog returns every scenario, scaled by o.TimeScale.
func Catalog(o Options) []*Scenario {
	o = o.norm()
	sec := func(s float64) time.Duration { return time.Duration(s * o.TimeScale * float64(time.Second)) }
	return []*Scenario{
		nodeFailure(sec), flashCrowd(sec), retryStorm(sec), overload(sec), noisyNeighbor(sec), deploy(sec),
	}
}

// Find returns the scenario with id.
func Find(o Options, id string) *Scenario {
	for _, s := range Catalog(o) {
		if s.ID == id {
			return s
		}
	}
	return nil
}

func pct(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return math.Round(1000*a/b) / 10
}

func round(x float64, d int) float64 {
	p := math.Pow(10, float64(d))
	return math.Round(x*p) / p
}

// ---------------------------------------------------------------------------

func nodeFailure(sec func(float64) time.Duration) *Scenario {
	without, with := baseSpec(), baseSpec()
	without.Edge.Hashing = "modn"
	kill := sec(8)
	dur := sec(30)
	sc := &Scenario{
		ID: "node-failure", Title: "A cache node dies", Mechanism: "Consistent hashing",
		Question: "When one of five cache nodes crashes, how much of the cache is lost with it?",
		Explain: "With mod-N placement, a key's node is hash(key) mod N, so going from 5 nodes to 4 reassigns about 80% of keys and every one of them misses at once. " +
			"A consistent-hash ring with 160 virtual points per node only reassigns the dead node's own keys, about 20%.",
		Traffic:  "3,000 req/s of page views drawn from the real popularity of the top 10,000 English Wikipedia articles (1 Sep 2024, 12:00 UTC), caches warmed first",
		Duration: dur, WarmKeys: 10000,
		Events: []Event{{At: kill, Label: "cache-3 crashes", Kind: "fault", Do: func(t *Topology) error { t.Caches[2].Kill(); return nil }}},
		Variants: [2]VariantSpec{
			{Key: "without", Label: "mod-N hashing", Settings: []string{"placement: hash(key) mod live nodes", "failover + health checks on"}, Spec: without},
			{Key: "with", Label: "Consistent-hash ring", Settings: []string{"placement: ring, 160 vnodes per node", "failover + health checks on"}, Spec: with},
		},
		Streams: func(*Topology) []loadgen.Stream {
			return []loadgen.Stream{{Class: "pages", Rate: constant(3000), Path: pages()}}
		},
		Extra: func(t *Topology) map[string]any { return ringExtra(t, 2) },
	}
	sc.Summarize = func(r *Result, w *Windows) {
		k := kill.Seconds()
		end := dur.Seconds()
		base := func(v string) float64 {
			return w.Sum(v, k-4, k, func(t Tick) float64 { return float64(t.OriginReq) }) / 4
		}
		extra := func(v string) float64 {
			return w.Sum(v, k, end, func(t Tick) float64 { return float64(t.OriginReq) }) - base(v)*(end-k)
		}
		peak := func(v string) float64 {
			return w.Max(v, k, end, func(t Tick) float64 { return float64(t.OriginReq) * 10 })
		}
		hit := func(v string) float64 {
			hits := w.Sum(v, k, k+2, func(t Tick) float64 { return float64(t.Hit + t.Stale) })
			all := w.Sum(v, k, k+2, func(t Tick) float64 { return float64(t.OK) })
			return pct(hits, all)
		}
		failed := func(v string) float64 {
			return w.Sum(v, k, end, func(t Tick) float64 { return float64(t.Failed) })
		}
		moved := func(i int) float64 { return round(100*r.Variants[i].Extra["moved"].(float64), 1) }
		r.Summary = []Metric{
			{Label: "Keys reassigned by the failure", Without: moved(0), With: moved(1), Unit: "%", Better: "lower", Note: "over all 10,000 keys"},
			{Label: "Extra origin fetches after the crash", Without: extra("without"), With: extra("with"), Unit: "req", Better: "lower"},
			{Label: "Peak origin load after the crash", Without: peak("without"), With: peak("with"), Unit: "req/s", Better: "lower"},
			{Label: "Hit ratio, first 2s after the crash", Without: hit("without"), With: hit("with"), Unit: "%", Better: "higher"},
			{Label: "p99 latency, first 5s after the crash", Without: w.Quantile("without", k, k+5, 0.99), With: w.Quantile("with", k, k+5, 0.99), Unit: "ms", Better: "lower"},
			{Label: "Failed requests after the crash", Without: failed("without"), With: failed("with"), Unit: "req", Better: "lower"},
		}
		r.Headline = fmt.Sprintf("The ring reassigned %.1f%% of keys vs %.1f%% for mod-N, cutting extra origin fetches from %s to %s.",
			moved(1), moved(0), commas(math.Round(extra("without"))), commas(math.Round(extra("with"))))
	}
	return sc
}

// ringExtra records, for the dashboard's ring view, where 360 sample keys sit
// and which node owns each before and after node `dead` fails, plus the moved
// fraction over all 10,000 keys.
func ringExtra(t *Topology, dead int) map[string]any {
	names := make([]string, len(t.Caches))
	for i, c := range t.Caches {
		names[i] = c.Addr()
	}
	after := append(append([]string(nil), names[:dead]...), names[dead+1:]...)
	var before, aft ring.Picker
	if t.Spec.Edge.Hashing == "modn" {
		before, aft = ring.NewModN(names), ring.NewModN(after)
	} else {
		before, aft = ring.New(names, t.Spec.Edge.VNodes), ring.New(after, t.Spec.Edge.VNodes)
	}
	s, _ := sampler()
	keys := s.Paths()
	idx := map[string]int{}
	for i, n := range names {
		idx[n] = i
	}
	type pt struct {
		Pos    float64 `json:"pos"`
		Before int     `json:"before"`
		After  int     `json:"after"`
	}
	var pts []pt
	step := len(keys) / 360
	for i := 0; i < len(keys) && len(pts) < 360; i += step {
		k := keys[i]
		pts = append(pts, pt{Pos: round(float64(ring.Hash(k))/math.MaxUint64*360, 2),
			Before: idx[before.Nodes()[before.Pick(k)]], After: idx[aft.Nodes()[aft.Pick(k)]]})
	}
	return map[string]any{"moved": ring.Moved(before, aft, keys), "keys": pts, "dead": dead}
}

// ---------------------------------------------------------------------------

func flashCrowd(sec func(float64) time.Duration) *Scenario {
	without, with := baseSpec(), baseSpec()
	for _, s := range []*Spec{&without, &with} {
		s.Origin.CostMultiplier = map[string]float64{HotPage: 8}
		s.Origin.TTLOverride = map[string]time.Duration{HotPage: time.Second}
	}
	without.Cache.Coalesce = false
	with.Cache.Coalesce = true
	with.Cache.StaleWhileRevalidate = 5 * time.Second
	start, stop, dur := sec(8), sec(22), sec(30)
	sc := &Scenario{
		ID: "flash-crowd", Title: "One page goes viral", Mechanism: "Request coalescing + stale-while-revalidate",
		Question: "A breaking-news page gets 2,500 req/s, is slow to render and can only be cached for 1 second. What reaches the origin?",
		Explain: "Every time the page expires, every request that arrives before the next copy is ready misses. Without coalescing each of those misses is its own origin render. " +
			"With coalescing, concurrent misses wait on one fetch, and with stale-while-revalidate the expired copy keeps being served while a single background refresh runs.",
		Traffic:  "1,500 req/s of background page views plus 2,500 req/s for " + HotPage + " from 8s to 22s (the real top article of the recorded hour). The origin takes 8x longer to render it and marks it max-age=1.",
		Duration: dur, WarmKeys: 10000, Watch: HotPage,
		Events: []Event{
			{At: start, Label: "page goes viral", Kind: "traffic"},
			{At: stop, Label: "spike ends", Kind: "recover"},
		},
		Variants: [2]VariantSpec{
			{Key: "without", Label: "Every miss fetches", Settings: []string{"coalescing off", "no stale serving"}, Spec: without},
			{Key: "with", Label: "Coalesced + stale-while-revalidate", Settings: []string{"coalescing on", "serve stale up to 5s while refreshing"}, Spec: with},
		},
		Streams: func(*Topology) []loadgen.Stream {
			return []loadgen.Stream{
				{Class: "background", Rate: constant(1500), Path: pages()},
				{Class: "hot", Rate: window(2500, start, stop), Path: func(*rand.Rand) string { return HotPage }},
			}
		},
	}
	sc.Summarize = func(r *Result, w *Windows) {
		a, b := start.Seconds(), stop.Seconds()
		span := b - a
		fetches := func(v string) float64 {
			return round(w.Sum(v, a, b, func(t Tick) float64 { return float64(t.Watch) })/span, 1)
		}
		hotOK := func(v string) float64 {
			ok := w.Sum(v, a, b, func(t Tick) float64 { return float64(t.Classes["hot"].OK) })
			sent := w.Sum(v, a, b, func(t Tick) float64 { return ClassDone(t.Classes["hot"]) })
			return pct(ok, sent)
		}
		bgOK := func(v string) float64 {
			ok := w.Sum(v, a, b, func(t Tick) float64 { return float64(t.Classes["background"].OK) })
			sent := w.Sum(v, a, b, func(t Tick) float64 { return ClassDone(t.Classes["background"]) })
			return pct(ok, sent)
		}
		queue := func(v string) float64 {
			return w.Max(v, a, b, func(t Tick) float64 {
				q := 0
				for _, o := range t.Origins {
					q += o.Queue
				}
				return float64(q)
			})
		}
		r.Summary = []Metric{
			{Label: "Origin renders of the hot page", Without: fetches("without"), With: fetches("with"), Unit: "req/s", Better: "lower"},
			{Label: "Hot page p99 latency", Without: w.Quantile("without", a, b, 0.99, "hot"), With: w.Quantile("with", a, b, 0.99, "hot"), Unit: "ms", Better: "lower"},
			{Label: "Hot page requests served", Without: hotOK("without"), With: hotOK("with"), Unit: "%", Better: "higher"},
			{Label: "Background requests served", Without: bgOK("without"), With: bgOK("with"), Unit: "%", Better: "higher"},
			{Label: "Peak origin queue", Without: queue("without"), With: queue("with"), Unit: "req", Better: "lower"},
		}
		r.Headline = fmt.Sprintf("Coalescing cut origin renders of the viral page from %.0f/s to %.1f/s, and hot-page p99 from %.0fms to %.0fms.",
			fetches("without"), fetches("with"), r.Summary[1].Without, r.Summary[1].With)
	}
	return sc
}

// ---------------------------------------------------------------------------

func retryStorm(sec func(float64) time.Duration) *Scenario {
	without, with := baseSpec(), baseSpec()
	without.Cache.Upstream.Retry = retry.Config{Mode: retry.Naive, MaxAttempts: 4}
	with.Cache.Upstream.Retry = retry.Config{Mode: retry.Budget, MaxAttempts: 3, Ratio: 0.1, MinPerSec: 5,
		BaseBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
	bc := breaker.Default()
	with.Cache.Upstream.Breaker = &bc
	slow, heal, dur := sec(8), sec(14), sec(30)
	sc := &Scenario{
		ID: "retry-storm", Title: "The origin slows down", Mechanism: "Retry budget + circuit breaker",
		Question: "The origin gets 4x slower for 6 seconds. Does the system come back when the origin does?",
		Explain: "Naive clients retry every timeout 3 more times, so a slow origin suddenly receives up to 4x the traffic. The origin keeps working on requests nobody is waiting for any more, " +
			"its queue never drains, and the outage outlives its cause: a metastable failure. A retry budget caps retries at 10% of requests, and circuit breakers stop sending to an origin that is failing so its queue can drain.",
		Traffic:  "1,400 req/s of uncacheable API calls (about 60% of origin capacity). Origin service time x4 from 8s to 14s.",
		Duration: dur,
		Events: []Event{
			{At: slow, Label: "origin 4x slower", Kind: "fault", Do: func(t *Topology) error {
				for _, o := range t.Origins {
					o.SetSlowdown(4)
				}
				return nil
			}},
			{At: heal, Label: "origin back to normal", Kind: "recover", Do: func(t *Topology) error {
				for _, o := range t.Origins {
					o.SetSlowdown(1)
				}
				return nil
			}},
		},
		Variants: [2]VariantSpec{
			{Key: "without", Label: "Naive retries", Settings: []string{"up to 3 immediate retries per request", "no circuit breaker"}, Spec: without},
			{Key: "with", Label: "Retry budget + breaker", Settings: []string{"retries capped at 10% of requests, jittered backoff", "per-origin circuit breakers"}, Spec: with},
		},
		Streams: func(*Topology) []loadgen.Stream {
			return []loadgen.Stream{{Class: "api", Rate: constant(1400), Path: apiPath}}
		},
	}
	sc.Summarize = func(r *Result, w *Windows) {
		s, h, end := slow.Seconds(), heal.Seconds(), dur.Seconds()
		success := func(v string, a, b float64) float64 {
			ok := w.Sum(v, a, b, func(t Tick) float64 { return float64(t.OK) })
			return pct(ok, w.Sum(v, a, b, Done))
		}
		amp := func(v string, a, b float64) float64 {
			o := w.Sum(v, a, b, func(t Tick) float64 { return float64(t.OriginReq) })
			sent := w.Sum(v, a, b, func(t Tick) float64 { return float64(t.Sent) })
			if sent == 0 {
				return 0
			}
			return round(o/sent, 2)
		}
		wasted := func(v string) float64 { return w.Sum(v, s, end, func(t Tick) float64 { return float64(t.Wasted) }) }
		recov := func(v string) float64 { return recoveryTime(w.Ticks(v), h, 0.95) }
		r.Summary = []Metric{
			{Label: "Requests served after the origin recovered", Without: success("without", h+2, end), With: success("with", h+2, end), Unit: "%", Better: "higher", Note: "from 2s after recovery to the end"},
			{Label: "Time to recover after the origin healed", Without: recov("without"), With: recov("with"), Unit: "s", Better: "lower", Note: "-1 means it never recovered before the run ended"},
			{Label: "Load amplification while slow", Without: amp("without", s, h), With: amp("with", s, h), Unit: "x", Better: "lower", Note: "origin requests per client request"},
			{Label: "Load amplification after healing", Without: amp("without", h, end), With: amp("with", h, end), Unit: "x", Better: "lower"},
			{Label: "Requests served while slow", Without: success("without", s, h), With: success("with", s, h), Unit: "%", Better: "higher"},
			{Label: "Origin work thrown away", Without: wasted("without"), With: wasted("with"), Unit: "req", Better: "lower", Note: "finished after the caller had given up"},
		}
		rw := recov("without")
		desc := fmt.Sprintf("recovered in %.1fs", rw)
		if rw < 0 {
			desc = "never recovered"
		}
		r.Headline = fmt.Sprintf("With naive retries the system %s after the origin healed (%.1f%% served); with a retry budget and breakers it recovered in %.1fs (%.1f%% served).",
			desc, r.Summary[0].Without, recov("with"), r.Summary[0].With)
	}
	return sc
}

func latencyPhrase(p99 float64) string {
	if p99 < 0 {
		return "where every request timed out in the queue"
	}
	return fmt.Sprintf("where the p99 was %.0fms", p99)
}

// recoveryTime is seconds after `from` until the 1s success ratio reaches
// target and stays there for the rest of the run, or -1.
func recoveryTime(ticks []Tick, from, target float64) float64 {
	per := int(time.Second / TickInterval)
	ok := func(i int) bool {
		var o, s uint64
		for j := i; j < i+per && j < len(ticks); j++ {
			o += ticks[j].OK
			s += ticks[j].OK + ticks[j].Failed + ticks[j].Shed + ticks[j].Limited
		}
		return s > 0 && float64(o)/float64(s) >= target
	}
	for i, t := range ticks {
		if t.T <= from {
			continue
		}
		if i+per > len(ticks) {
			break // not enough run left to call it recovered
		}
		good := true
		for j := i; j+per <= len(ticks); j += per {
			if !ok(j) {
				good = false
				break
			}
		}
		if good {
			return round(t.T-from, 1)
		}
	}
	return -1
}

// ---------------------------------------------------------------------------

func overload(sec func(float64) time.Duration) *Scenario {
	without, with := baseSpec(), baseSpec()
	with.Edge.Shedding = &edge.SheddingConfig{Initial: 64, Min: 8, Max: 2000, Window: edge.Duration(100 * time.Millisecond)}
	rampA, rampB, holdEnd, dur := sec(5), sec(12), sec(24), sec(30)
	rate := func(t time.Duration) float64 {
		switch {
		case t < rampA:
			return 1200
		case t < rampB:
			return 1200 + 3300*float64(t-rampA)/float64(rampB-rampA)
		case t < holdEnd:
			return 4500
		default:
			return 1200
		}
	}
	sc := &Scenario{
		ID: "overload", Title: "Traffic doubles past capacity", Mechanism: "Adaptive concurrency limit",
		Question: "Offered load ramps to about 2x what the origin can serve. Is anything still served fast?",
		Explain: "Past capacity, extra requests can only wait in a queue. Once the queue wait is longer than the client's 1s timeout, the origin spends all its time on requests whose callers already left, and goodput collapses. " +
			"The edge's Vegas-style limit notices latency rising above its no-load baseline, caps in-flight requests near the real capacity, and answers the excess with an immediate 503 instead.",
		Traffic:  "Uncacheable API calls: 1,200 req/s, ramping to 4,500 req/s from 5s to 12s, held until 24s. Origin capacity is about 2,300 req/s.",
		Duration: dur,
		Events: []Event{
			{At: rampA, Label: "ramp begins", Kind: "traffic"},
			{At: rampB, Label: "2x capacity", Kind: "fault"},
			{At: holdEnd, Label: "load drops", Kind: "recover"},
		},
		Variants: [2]VariantSpec{
			{Key: "without", Label: "Queue everything", Settings: []string{"no admission control at the edge"}, Spec: without},
			{Key: "with", Label: "Adaptive concurrency limit", Settings: []string{"Vegas limit at the edge, starts at 64", "excess answered with a fast 503"}, Spec: with},
		},
		Streams: func(*Topology) []loadgen.Stream {
			return []loadgen.Stream{{Class: "api", Rate: rate, Path: apiPath}}
		},
	}
	sc.Summarize = func(r *Result, w *Windows) {
		a, b := rampB.Seconds()+1, holdEnd.Seconds()
		span := b - a
		good := func(v string) float64 {
			return math.Round(w.Sum(v, a, b, func(t Tick) float64 { return float64(t.OK) }) / span)
		}
		fastFail := func(v string) float64 {
			return w.Sum(v, a, b, func(t Tick) float64 { return float64(t.Shed) })
		}
		wasted := func(v string) float64 { return w.Sum(v, a, b, func(t Tick) float64 { return float64(t.Wasted) }) }
		r.Summary = []Metric{
			{Label: "Goodput at 2x load", Without: good("without"), With: good("with"), Unit: "req/s", Better: "higher", Note: "successful responses within the 1s timeout"},
			{Label: "p99 of successful requests at 2x load", Without: w.Quantile("without", a, b, 0.99), With: w.Quantile("with", a, b, 0.99), Unit: "ms", Better: "lower"},
			{Label: "p50 of successful requests at 2x load", Without: w.Quantile("without", a, b, 0.5), With: w.Quantile("with", a, b, 0.5), Unit: "ms", Better: "lower"},
			{Label: "Requests refused fast", Without: fastFail("without"), With: fastFail("with"), Unit: "req", Better: "", Note: "the 'with' side trades these for goodput"},
			{Label: "Origin work thrown away", Without: wasted("without"), With: wasted("with"), Unit: "req", Better: "lower"},
		}
		r.Headline = fmt.Sprintf("At 2x capacity, goodput was %s req/s with the adaptive limit (p99 %.0fms) vs %s req/s without, %s.",
			commas(good("with")), r.Summary[1].With, commas(good("without")), latencyPhrase(r.Summary[1].Without))
	}
	return sc
}

// ---------------------------------------------------------------------------

func noisyNeighbor(sec func(float64) time.Duration) *Scenario {
	without, with := baseSpec(), baseSpec()
	with.Edge.RateLimit = &edge.RateLimitConfig{RPS: 400, Burst: 200, Header: "X-Client-ID"}
	start, stop, dur := sec(8), sec(22), sec(30)
	sc := &Scenario{
		ID: "noisy-neighbor", Title: "One client floods the API", Mechanism: "Per-client rate limiting",
		Question: "Eight well-behaved clients share the API with one that suddenly sends 4,000 req/s. Who pays?",
		Explain: "Without per-client limits, capacity goes to whoever sends the most, so the flood pushes the origin past capacity and every client's requests time out together. " +
			"A token bucket per client (400 req/s, burst 200) stops the flood at the edge, before it costs the origin anything.",
		Traffic:  "8 clients x 150 req/s of API calls; client 9 sends 4,000 req/s from 8s to 22s. Origin capacity is about 2,300 req/s.",
		Duration: dur,
		Events: []Event{
			{At: start, Label: "client 9 starts flooding", Kind: "fault"},
			{At: stop, Label: "flood stops", Kind: "recover"},
		},
		Variants: [2]VariantSpec{
			{Key: "without", Label: "No per-client limit", Settings: []string{"first come, first served"}, Spec: without},
			{Key: "with", Label: "Token bucket per client", Settings: []string{"400 req/s per client, burst 200", "keyed on X-Client-ID"}, Spec: with},
		},
		Streams: func(*Topology) []loadgen.Stream {
			var ss []loadgen.Stream
			for i := 1; i <= 8; i++ {
				id := "tenant-" + strconv.Itoa(i)
				ss = append(ss, loadgen.Stream{Class: "normal", Rate: constant(150), Path: apiPath, ClientID: func(*rand.Rand) string { return id }})
			}
			ss = append(ss, loadgen.Stream{Class: "abuser", Rate: window(4000, start, stop), Path: apiPath, ClientID: func(*rand.Rand) string { return "tenant-9" }})
			return ss
		},
	}
	sc.Summarize = func(r *Result, w *Windows) {
		a, b := start.Seconds(), stop.Seconds()
		cls := func(v, c string, f func(ClassTick) uint64) float64 {
			return w.Sum(v, a, b, func(t Tick) float64 { return float64(f(t.Classes[c])) })
		}
		okPct := func(v, c string) float64 {
			return pct(cls(v, c, func(x ClassTick) uint64 { return x.OK }), cls(v, c, func(x ClassTick) uint64 { return x.OK + x.Failed + x.Shed + x.Limited }))
		}
		r.Summary = []Metric{
			{Label: "Well-behaved requests served during the flood", Without: okPct("without", "normal"), With: okPct("with", "normal"), Unit: "%", Better: "higher"},
			{Label: "Well-behaved p99 during the flood", Without: w.Quantile("without", a, b, 0.99, "normal"), With: w.Quantile("with", a, b, 0.99, "normal"), Unit: "ms", Better: "lower"},
			{Label: "Flood requests rejected at the edge", Without: pct(cls("without", "abuser", func(x ClassTick) uint64 { return x.Limited }), cls("without", "abuser", func(x ClassTick) uint64 { return x.Sent })),
				With: pct(cls("with", "abuser", func(x ClassTick) uint64 { return x.Limited }), cls("with", "abuser", func(x ClassTick) uint64 { return x.Sent })), Unit: "%", Better: ""},
			{Label: "Average load reaching the origin during the flood", Without: math.Round(w.Sum("without", a, b, func(t Tick) float64 { return float64(t.OriginReq) }) / (b - a)),
				With: math.Round(w.Sum("with", a, b, func(t Tick) float64 { return float64(t.OriginReq) }) / (b - a)), Unit: "req/s", Better: "lower", Note: "origin capacity is about 2,300 req/s"},
		}
		r.Headline = fmt.Sprintf("During the flood, well-behaved clients got %.1f%% of requests served with per-client limits vs %.1f%% without.",
			r.Summary[0].With, r.Summary[0].Without)
	}
	return sc
}

// ---------------------------------------------------------------------------

func deploy(sec func(float64) time.Duration) *Scenario {
	without, with := baseSpec(), baseSpec()
	for _, s := range []*Spec{&without, &with} {
		s.EdgeProcess = true
	}
	with.Graceful = true
	dur := sec(30)
	var evs []Event
	for i := 1; i <= 5; i++ {
		evs = append(evs, Event{At: sec(float64(5 * i)), Label: fmt.Sprintf("deploy #%d", i), Kind: "deploy", Do: func(t *Topology) error { return t.Restart() }})
	}
	sc := &Scenario{
		ID: "deploy", Title: "Five deploys under load", Mechanism: "Graceful restart (socket handoff)",
		Question: "The edge process is restarted five times under load. How many requests fail?",
		Explain: "A plain restart closes the listening socket: connections are refused until the new process binds, and requests in flight on the old one are cut off. " +
			"The graceful path hands the open listening socket to the new process, waits until it is serving, then lets the old process finish its in-flight requests before it exits. The socket never closes.",
		Traffic:  "2,500 req/s of page views through a real separate edge process (the switchyard binary), caches warmed. Deploys at 5, 10, 15, 20 and 25s.",
		Duration: dur, WarmKeys: 10000, Events: evs,
		Variants: [2]VariantSpec{
			{Key: "without", Label: "Stop, then start", Settings: []string{"SIGTERM, exit without draining", "new process binds the port again"}, Spec: without},
			{Key: "with", Label: "Socket handoff", Settings: []string{"SIGUSR2: child inherits the listening fd", "parent drains in-flight requests, then exits"}, Spec: with},
		},
		Streams: func(*Topology) []loadgen.Stream {
			return []loadgen.Stream{{Class: "pages", Rate: constant(2500), Path: pages()}}
		},
	}
	sc.Summarize = func(r *Result, w *Windows) {
		end := dur.Seconds()
		failed := func(v string) float64 { return w.Sum(v, 0, end, func(t Tick) float64 { return float64(t.Failed) }) }
		sent := func(v string) float64 { return w.Sum(v, 0, end, Done) }
		gens := func(v string) float64 {
			seen := map[string]bool{}
			for _, t := range w.Ticks(v) {
				for g := range t.Gens {
					seen[g] = true
				}
			}
			return float64(len(seen))
		}
		r.Summary = []Metric{
			{Label: "Failed requests across 5 deploys", Without: failed("without"), With: failed("with"), Unit: "req", Better: "lower"},
			{Label: "Success rate over the run", Without: round(100-100*failed("without")/math.Max(1, sent("without")), 3), With: round(100-100*failed("with")/math.Max(1, sent("with")), 3), Unit: "%", Better: "higher"},
			{Label: "p99 latency over the run", Without: w.Quantile("without", 0, end, 0.99), With: w.Quantile("with", 0, end, 0.99), Unit: "ms", Better: "lower"},
			{Label: "Edge processes that served traffic", Without: gens("without"), With: gens("with"), Unit: "", Better: ""},
		}
		r.Headline = fmt.Sprintf("Five restarts failed %.0f requests with stop-then-start and %.0f with the socket handoff.", failed("without"), failed("with"))
	}
	return sc
}
