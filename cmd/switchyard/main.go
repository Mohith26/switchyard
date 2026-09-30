// Command switchyard runs every role of the system from one binary:
//
//	switchyard edge    -config edge.json     the front door (hot reload on SIGHUP, handoff on SIGUSR2)
//	switchyard cache   -listen ... -origins  one cache node
//	switchyard origin  -listen ...           a simulated origin
//	switchyard loadgen -target ...           open-loop load against real Wikipedia popularity
//	switchyard lab     run|serve|list|export the A/B scenario lab and dashboard
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Mohith26/switchyard/internal/breaker"
	"github.com/Mohith26/switchyard/internal/cachenode"
	"github.com/Mohith26/switchyard/internal/edge"
	"github.com/Mohith26/switchyard/internal/graceful"
	"github.com/Mohith26/switchyard/internal/lab"
	"github.com/Mohith26/switchyard/internal/loadgen"
	"github.com/Mohith26/switchyard/internal/origin"
	"github.com/Mohith26/switchyard/internal/retry"
	"github.com/Mohith26/switchyard/internal/trace"
	"github.com/Mohith26/switchyard/internal/upstream"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: switchyard <edge|cache|origin|loadgen|lab> [flags]

  edge     run the edge proxy from a JSON config
  cache    run one cache node
  origin   run a simulated origin server
  loadgen  send open-loop load at a target
  lab      run the A/B scenarios, serve the dashboard, or export a static replay

Run "switchyard <command> -h" for flags.`)
	os.Exit(2)
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "edge":
		err = runEdge(os.Args[2:])
	case "cache":
		err = runCache(os.Args[2:])
	case "origin":
		err = runOrigin(os.Args[2:])
	case "loadgen":
		err = runLoadgen(os.Args[2:])
	case "lab":
		err = runLab(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "switchyard: unknown command %q\n", os.Args[1])
		usage()
	}
	if err != nil {
		log.Fatalf("switchyard %s: %v", os.Args[1], err)
	}
}

func loadEdgeConfig(path string) (edge.Config, error) {
	var c edge.Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("%s: %w", path, err)
	}
	return c, c.Validate()
}

func runEdge(args []string) error {
	fs := flag.NewFlagSet("edge", flag.ExitOnError)
	cfgPath := fs.String("config", "edge.json", "path to the JSON config (re-read on SIGHUP)")
	pidfile := fs.String("pidfile", "", "write the serving process's PID here")
	drain := fs.Bool("drain", true, "on SIGTERM, finish in-flight requests before exiting")
	drainTimeout := fs.Duration("drain-timeout", 30*time.Second, "longest a drain may take")
	drainGrace := fs.Duration("drain-grace", time.Second, "keep serving existing connections this long (with Connection: close) before closing them")
	fs.Parse(args)

	cfg, err := loadEdgeConfig(*cfgPath)
	if err != nil {
		return err
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	srv, err := edge.New(cfg)
	if err != nil {
		return err
	}
	srv.Generation = strconv.Itoa(os.Getpid())
	ln, inherited, err := graceful.Listen(cfg.Listen)
	if err != nil {
		return err
	}
	// While draining, every response carries "Connection: close", so clients
	// retire their keep-alive connections to this process after their next
	// request instead of racing a server-side close of an idle connection.
	var draining atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if draining.Load() {
			w.Header().Set("Connection", "close")
		}
		srv.ServeHTTP(w, r)
	})
	hs := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) && !draining.Load() {
			log.Fatalf("edge: serve: %v", err)
		}
	}()
	if err := graceful.Ready(); err != nil {
		return err
	}
	if *pidfile != "" {
		if err := os.WriteFile(*pidfile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
			return err
		}
	}
	log.Printf("edge: pid %d serving on %s (inherited=%v, %d cache nodes, hashing=%s)", os.Getpid(), ln.Addr(), inherited, len(cfg.CacheNodes), cfg.Hashing)

	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGINT)
	shutdown := func() error {
		// 1. Stop accepting: new connections now go only to the successor (after
		//    a handoff) or nowhere (plain shutdown).
		draining.Store(true)
		ln.Close()
		// 2. Let busy keep-alive connections finish one more request each and
		//    close cleanly on their own.
		time.Sleep(*drainGrace)
		// 3. Finish anything still in flight and close what is left.
		ctx, cancel := context.WithTimeout(context.Background(), *drainTimeout)
		defer cancel()
		err := hs.Shutdown(ctx)
		srv.Close()
		return err
	}
	for sig := range sigs {
		switch sig {
		case syscall.SIGHUP:
			next, err := loadEdgeConfig(*cfgPath)
			if err == nil {
				next.Listen = cfg.Listen // the socket cannot move without a restart
				err = srv.Apply(next)
			}
			if err != nil {
				log.Printf("edge: reload rejected, keeping the running config: %v", err)
				continue
			}
			log.Printf("edge: reloaded %s", *cfgPath)
		case syscall.SIGUSR2:
			pid, err := graceful.Handoff(ln, 10*time.Second)
			if err != nil {
				log.Printf("edge: handoff failed, still serving: %v", err)
				continue
			}
			log.Printf("edge: handed the listener to pid %d, draining", pid)
			return shutdown()
		case syscall.SIGTERM, syscall.SIGINT:
			if !*drain {
				os.Exit(0) // the naive baseline: drop everything in flight
			}
			log.Printf("edge: draining")
			return shutdown()
		}
	}
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func runCache(args []string) error {
	fs := flag.NewFlagSet("cache", flag.ExitOnError)
	listen := fs.String("listen", ":9000", "listen address")
	name := fs.String("name", "cache", "node name (used in X-Cache-Node and metrics)")
	origins := fs.String("origins", "", "comma-separated upstream addresses (origins, or a shield cache)")
	capacity := fs.Int("capacity", 100000, "max cached entries")
	coalesce := fs.Bool("coalesce", true, "collapse concurrent misses into one fetch")
	swr := fs.Duration("stale-while-revalidate", 5*time.Second, "serve expired entries this long while refreshing (0 disables)")
	bal := fs.String("balancer", "p2c", "origin load balancing: p2c, least_conn or round_robin")
	retryMode := fs.String("retry", "budget", "retry policy: none, naive or budget")
	attempts := fs.Int("attempts", 3, "max attempts per request, including the first")
	useBreaker := fs.Bool("breaker", true, "per-origin circuit breakers")
	attemptTimeout := fs.Duration("attempt-timeout", 300*time.Millisecond, "timeout per upstream attempt")
	prefixes := fs.String("cacheable", "/", "comma-separated path prefixes eligible for caching and coalescing")
	fs.Parse(args)
	cfg := cachenode.Config{Name: *name, Capacity: *capacity, Coalesce: *coalesce, StaleWhileRevalidate: *swr, CacheablePrefixes: splitList(*prefixes),
		Upstream: upstream.Config{Endpoints: splitList(*origins), Balancer: *bal, AttemptTimeout: *attemptTimeout,
			Retry: retry.Config{Mode: retry.Mode(*retryMode), MaxAttempts: *attempts, Ratio: 0.1, MinPerSec: 5,
				BaseBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond},
			Outlier:    upstream.OutlierConfig{Consecutive: 5, Base: time.Second, Max: 10 * time.Second, MaxPercent: 34},
			HealthPath: "/healthz", HealthInterval: 500 * time.Millisecond}}
	if len(cfg.Upstream.Endpoints) == 0 {
		return errors.New("-origins is required")
	}
	if *useBreaker {
		b := breaker.Default()
		cfg.Upstream.Breaker = &b
	}
	s := cachenode.New(cfg)
	if err := s.Start(*listen); err != nil {
		return err
	}
	log.Printf("cache %s: serving on %s -> %v", *name, s.Addr(), cfg.Upstream.Endpoints)
	waitForSignal()
	s.Close()
	return nil
}

func runOrigin(args []string) error {
	fs := flag.NewFlagSet("origin", flag.ExitOnError)
	listen := fs.String("listen", ":7000", "listen address")
	name := fs.String("name", "origin", "origin name")
	workers := fs.Int("workers", 16, "concurrent requests served")
	queue := fs.Int("queue", 2048, "queued requests before fast 503s")
	median := fs.Duration("median", 20*time.Millisecond, "median service time")
	ttl := fs.Duration("ttl", 120*time.Second, "Cache-Control max-age for /wiki/ pages")
	fs.Parse(args)
	s := origin.New(origin.Config{Name: *name, Workers: *workers, QueueCap: *queue, Median: *median, Sigma: 0.25, TTL: *ttl})
	if err := s.Start(*listen); err != nil {
		return err
	}
	// Runtime fault injection for manual experiments:
	//   curl -XPOST 'host/-/fault?slowdown=4'   curl -XPOST 'host/-/fault?error_rate=0.2'
	mux := http.NewServeMux()
	mux.HandleFunc("/-/fault", func(w http.ResponseWriter, r *http.Request) {
		if v := r.URL.Query().Get("slowdown"); v != "" {
			f, _ := strconv.ParseFloat(v, 64)
			s.SetSlowdown(f)
		}
		if v := r.URL.Query().Get("error_rate"); v != "" {
			f, _ := strconv.ParseFloat(v, 64)
			s.SetErrorRate(f)
		}
		fmt.Fprintln(w, "ok")
	})
	log.Printf("origin %s: serving on %s", *name, s.Addr())
	go http.ListenAndServe(adminAddr(*listen), mux)
	waitForSignal()
	s.Close()
	return nil
}

// adminAddr is the listen port + 1000, for the origin's fault endpoint.
func adminAddr(listen string) string {
	i := strings.LastIndex(listen, ":")
	p, err := strconv.Atoi(listen[i+1:])
	if err != nil {
		return ":0"
	}
	return listen[:i+1] + strconv.Itoa(p+1000)
}

func waitForSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	<-sigs
}

func runLoadgen(args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
	target := fs.String("target", "http://127.0.0.1:8080", "edge base URL")
	rate := fs.Float64("rate", 1000, "requests per second")
	dur := fs.Duration("duration", 30*time.Second, "how long to run")
	keys := fs.Int("keys", 10000, "sample from the top N Wikipedia pages")
	api := fs.Float64("api", 0, "fraction of requests that are uncacheable /api/ calls")
	fs.Parse(args)
	s, err := trace.NewSampler(*keys)
	if err != nil {
		return err
	}
	path := func(rng *rand.Rand) string {
		if *api > 0 && rng.Float64() < *api {
			return "/api/items/" + strconv.Itoa(rng.IntN(1_000_000))
		}
		return s.Sample(rng)
	}
	g := loadgen.New(*target, time.Second, uint64(time.Now().UnixNano()), loadgen.Stream{Class: "all", Rate: func(time.Duration) float64 { return *rate }, Path: path})
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		st := g.Stats("all")
		var pOK, pFail, pSent uint64
		prev := st.Latency.Snapshot()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				ok, sent := st.OK.Load(), st.Sent.Load()
				fail := st.Err5xx.Load() + st.Timeout.Load() + st.ConnErr.Load() + st.Shed.Load() + st.Limited.Load()
				cur := st.Latency.Snapshot()
				win := cur.Sub(prev)
				prev = cur
				fmt.Printf("%6.1fs  sent %6d/s  ok %6d/s  failed %5d/s  p50 %6.1fms  p99 %7.1fms  hit %.1f%%\n",
					g.Elapsed().Seconds(), sent-pSent, ok-pOK, fail-pFail,
					float64(win.Quantile(0.5).Microseconds())/1000, float64(win.Quantile(0.99).Microseconds())/1000,
					100*float64(st.Hit.Load()+st.Stale.Load())/float64(max(1, ok)))
				pOK, pFail, pSent = ok, fail, sent
			}
		}
	}()
	g.Run(ctx, *dur)
	close(done)
	return nil
}

func runLab(args []string) error {
	if len(args) == 0 {
		args = []string{"-h"}
	}
	switch args[0] {
	case "list":
		for _, s := range lab.Catalog(lab.Options{}) {
			fmt.Printf("%-15s %-40s %s\n", s.ID, s.Mechanism, s.Title)
		}
		return nil
	case "run":
		fs := flag.NewFlagSet("lab run", flag.ExitOnError)
		sc := fs.String("scenario", "all", "scenario id, or all")
		out := fs.String("out", "results", "directory for result JSON")
		ts := fs.Float64("time-scale", 1, "multiply every duration (e.g. 0.3 for a quick run)")
		rs := fs.Float64("rate-scale", 1, "multiply every traffic rate")
		fs.Parse(args[1:])
		return lab.RunAll(context.Background(), *sc, *out, lab.Options{TimeScale: *ts, RateScale: *rs}, os.Stdout)
	case "serve":
		fs := flag.NewFlagSet("lab serve", flag.ExitOnError)
		addr := fs.String("addr", "127.0.0.1:8088", "dashboard address")
		results := fs.String("results", "results", "directory of recorded results")
		fs.Parse(args[1:])
		return lab.Serve(*addr, *results)
	case "report":
		fs := flag.NewFlagSet("lab report", flag.ExitOnError)
		results := fs.String("results", "results", "directory of recorded results")
		fs.Parse(args[1:])
		return lab.Report(*results, os.Stdout)
	case "export":
		fs := flag.NewFlagSet("lab export", flag.ExitOnError)
		results := fs.String("results", "results", "directory of recorded results")
		out := fs.String("out", "docs", "output directory for the static replay site")
		single := fs.String("single", "", "instead, write one self-contained HTML file here")
		fonts := fs.String("fonts", "", "with -single: @font-face CSS to inline instead of loading web fonts")
		home := fs.String("home", "", "with -single: URL for a back link in the header")
		fs.Parse(args[1:])
		if *single != "" {
			return lab.ExportSingle(*results, *single, lab.SingleOptions{FontCSS: *fonts, Home: *home})
		}
		return lab.Export(*results, *out)
	default:
		fmt.Fprintln(os.Stderr, `usage: switchyard lab <list|run|serve|export> [flags]

  list     list scenarios
  run      record scenarios to JSON (-scenario all -out results)
  serve    dashboard with recorded replays and live runs (-addr 127.0.0.1:8088)
  export   write a static replay-only site (-out docs)
  report   print the recorded results as Markdown tables`)
		if args[0] == "-h" || args[0] == "--help" {
			return nil
		}
		return fmt.Errorf("unknown lab command %q", args[0])
	}
}
