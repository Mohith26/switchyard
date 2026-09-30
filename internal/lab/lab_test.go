package lab

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Mohith26/switchyard/internal/edge"
	"github.com/Mohith26/switchyard/internal/loadgen"
)

// buildBinary compiles cmd/switchyard once for the process-level tests.
func buildBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	bin := filepath.Join(t.TempDir(), "switchyard")
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/switchyard")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build: %v", err)
	}
	return bin
}

func TestNodeFailureScenarioSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a shortened scenario")
	}
	sc := Find(Options{TimeScale: 0.2}, "node-failure")
	sc.WarmKeys = 2000
	res, err := Run(context.Background(), sc, Options{TimeScale: 0.2, RateScale: 0.3})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Variants) != 2 || len(res.Variants[0].Ticks) < 50 {
		t.Fatalf("expected two recorded variants, got %d", len(res.Variants))
	}
	mod := res.Variants[0].Extra["moved"].(float64)
	rng := res.Variants[1].Extra["moved"].(float64)
	if mod < 0.7 || rng > 0.3 {
		t.Fatalf("moved fractions mod-N=%.2f ring=%.2f", mod, rng)
	}
	if len(res.Summary) == 0 || res.Headline == "" {
		t.Fatal("summary missing")
	}
	if _, err := json.Marshal(res); err != nil {
		t.Fatal(err)
	}
}

func pagesStream(rate float64) loadgen.Stream {
	s, _ := sampler()
	return loadgen.Stream{Class: "pages", Rate: func(time.Duration) float64 { return rate }, Path: s.Sample}
}

// Three socket handoffs under load must not fail a single request.
func TestGracefulHandoffDropsNothing(t *testing.T) {
	bin := buildBinary(t)
	spec := baseSpec()
	spec.EdgeProcess, spec.Graceful, spec.Bin = true, true, bin
	topo, err := Build(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()
	gen := loadgen.New(topo.URL(), time.Second, 7, pagesStream(800))
	done := make(chan struct{})
	go func() { gen.Run(context.Background(), 4*time.Second); close(done) }()
	pids := map[int]bool{topo.EdgePID(): true}
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		if err := topo.Restart(); err != nil {
			t.Fatal(err)
		}
		pids[topo.EdgePID()] = true
	}
	<-done
	st := gen.Stats("pages")
	failed := st.Err5xx.Load() + st.Timeout.Load() + st.ConnErr.Load()
	if failed != 0 {
		t.Fatalf("%d of %d requests failed across 3 handoffs", failed, st.Sent.Load())
	}
	if len(pids) != 4 || len(st.Gens()) < 3 {
		t.Fatalf("expected 4 edge processes to serve, saw pids=%d gens=%d", len(pids), len(st.Gens()))
	}
}

// SIGHUP re-reads the config file without restarting the process.
func TestSIGHUPHotReload(t *testing.T) {
	bin := buildBinary(t)
	spec := baseSpec()
	spec.EdgeProcess, spec.Graceful, spec.Bin = true, true, bin
	topo, err := Build(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()
	var cfg edge.Config
	b, _ := os.ReadFile(topo.proc.cfgPath)
	json.Unmarshal(b, &cfg)
	cfg.Hashing = "modn"
	cfg.RateLimit = &edge.RateLimitConfig{RPS: 50, Burst: 10}
	b, _ = json.Marshal(cfg)
	os.WriteFile(topo.proc.cfgPath, b, 0o644)
	pid := topo.EdgePID()
	syscall.Kill(pid, syscall.SIGHUP)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(topo.URL() + "/-/config")
		if err == nil {
			var got edge.Config
			json.NewDecoder(resp.Body).Decode(&got)
			resp.Body.Close()
			if got.Hashing == "modn" && got.RateLimit != nil {
				if topo.EdgePID() != pid {
					t.Fatal("reload must not restart the process")
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("config was not reloaded")
}

func TestRecoveryTime(t *testing.T) {
	var ticks []Tick
	for i := 0; i < 100; i++ {
		ok := uint64(0)
		if i >= 40 {
			ok = 10
		}
		ticks = append(ticks, Tick{T: float64(i+1) / 10, OK: ok, Failed: 10 - ok})
	}
	if got := recoveryTime(ticks, 3.0, 0.95); got < 0.9 || got > 1.2 {
		t.Fatalf("recovery %.1fs, want ~1s", got)
	}
	for i := range ticks {
		ticks[i].OK, ticks[i].Failed = 0, 10
	}
	if got := recoveryTime(ticks, 3.0, 0.95); got != -1 {
		t.Fatalf("never-recovered run reported %.1f", got)
	}
}

func TestCatalogIsComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range Catalog(Options{}) {
		if seen[s.ID] {
			t.Fatalf("duplicate id %s", s.ID)
		}
		seen[s.ID] = true
		if s.Summarize == nil || s.Streams == nil || s.Question == "" || s.Explain == "" {
			t.Fatalf("%s is missing fields", s.ID)
		}
		if s.Variants[0].Key != "without" || s.Variants[1].Key != "with" {
			t.Fatalf("%s variants must be without/with", s.ID)
		}
	}
	if len(seen) != 6 {
		t.Fatalf("want 6 scenarios, got %d", len(seen))
	}
	_ = rand.IntN
}
