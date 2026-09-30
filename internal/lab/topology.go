// Package lab builds a full Switchyard topology on real loopback sockets,
// drives it with the open-loop load generator, injects faults on a schedule,
// and records a 100ms time series of what every tier did. Each scenario runs
// twice on identical traffic, once with one mechanism off and once with it on,
// so every claim in the README is a measured A/B comparison.
package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Mohith26/switchyard/internal/cachenode"
	"github.com/Mohith26/switchyard/internal/edge"
	"github.com/Mohith26/switchyard/internal/origin"
)

// Spec describes one topology. Addresses are filled in by Build.
type Spec struct {
	Origins int
	Origin  origin.Config
	Caches  int
	Cache   cachenode.Config
	Edge    edge.Config

	// EdgeProcess runs the edge as a separate OS process (the real binary), so
	// restarts are real process restarts. Bin is the switchyard binary.
	EdgeProcess bool
	Bin         string
	Graceful    bool // restart by socket handoff instead of stop-then-start
}

type Topology struct {
	Spec     Spec
	Origins  []*origin.Server
	Caches   []*cachenode.Server
	Edge     *edge.Server // nil when EdgeProcess
	EdgeAddr string

	edgeHTTP *http.Server
	proc     *edgeProc
}

// Build starts origins, then caches pointed at them, then the edge.
func Build(spec Spec) (*Topology, error) {
	t := &Topology{Spec: spec}
	var originAddrs []string
	for i := 0; i < spec.Origins; i++ {
		oc := spec.Origin
		oc.Name = fmt.Sprintf("origin-%d", i+1)
		o := origin.New(oc)
		if err := o.Start("127.0.0.1:0"); err != nil {
			t.Close()
			return nil, err
		}
		t.Origins = append(t.Origins, o)
		originAddrs = append(originAddrs, o.Addr())
	}
	var cacheAddrs []string
	for i := 0; i < spec.Caches; i++ {
		cc := spec.Cache
		cc.Name = fmt.Sprintf("cache-%d", i+1)
		cc.Upstream.Endpoints = originAddrs
		c := cachenode.New(cc)
		if err := c.Start("127.0.0.1:0"); err != nil {
			t.Close()
			return nil, err
		}
		t.Caches = append(t.Caches, c)
		cacheAddrs = append(cacheAddrs, c.Addr())
	}
	ec := spec.Edge
	ec.CacheNodes = cacheAddrs
	if spec.EdgeProcess {
		p, err := startEdgeProc(spec.Bin, ec, spec.Graceful)
		if err != nil {
			t.Close()
			return nil, err
		}
		t.proc = p
		t.EdgeAddr = p.addr
		return t, nil
	}
	e, err := edge.New(ec)
	if err != nil {
		t.Close()
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Close()
		return nil, err
	}
	t.Edge = e
	t.EdgeAddr = ln.Addr().String()
	t.edgeHTTP = &http.Server{Handler: e, ReadHeaderTimeout: 5 * time.Second}
	go t.edgeHTTP.Serve(ln)
	return t, nil
}

// URL is the edge base URL.
func (t *Topology) URL() string { return "http://" + t.EdgeAddr }

// Close tears everything down.
func (t *Topology) Close() {
	if t.proc != nil {
		t.proc.stop()
	}
	if t.edgeHTTP != nil {
		t.edgeHTTP.Close()
	}
	if t.Edge != nil {
		t.Edge.Close()
	}
	for _, c := range t.Caches {
		c.Close()
	}
	for _, o := range t.Origins {
		o.Close()
	}
}

// Restart restarts the edge process: a socket handoff when the spec is
// graceful, otherwise stop (no drain) followed by a fresh start.
func (t *Topology) Restart() error {
	if t.proc == nil {
		return errors.New("lab: restart needs an edge process")
	}
	if t.proc.graceful {
		return t.proc.handoff()
	}
	return t.proc.hardRestart()
}

// EdgePID reports the current edge process (0 when in-process).
func (t *Topology) EdgePID() int {
	if t.proc == nil {
		return 0
	}
	return t.proc.pid
}

type edgeProc struct {
	bin, dir, cfgPath, pidPath string
	addr                       string
	graceful                   bool
	pid                        int
}

func freePort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

func startEdgeProc(bin string, cfg edge.Config, graceful bool) (*edgeProc, error) {
	if bin == "" {
		return nil, errors.New("lab: EdgeProcess needs the switchyard binary path")
	}
	addr, err := freePort()
	if err != nil {
		return nil, err
	}
	cfg.Listen = addr
	dir, err := os.MkdirTemp("", "switchyard-edge-")
	if err != nil {
		return nil, err
	}
	p := &edgeProc{bin: bin, dir: dir, cfgPath: filepath.Join(dir, "edge.json"), pidPath: filepath.Join(dir, "edge.pid"), addr: addr, graceful: graceful}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(p.cfgPath, b, 0o644); err != nil {
		return nil, err
	}
	if err := p.spawn(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *edgeProc) spawn() error {
	args := []string{"edge", "-config", p.cfgPath, "-pidfile", p.pidPath}
	if !p.graceful {
		args = append(args, "-drain=false")
	}
	cmd := exec.Command(p.bin, args...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait() // reap it whenever it exits
	p.pid = cmd.Process.Pid
	return waitListening(p.addr, 5*time.Second)
}

func waitListening(addr string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fmt.Errorf("lab: edge never listened on %s", addr)
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func waitGone(pid int, d time.Duration) {
	deadline := time.Now().Add(d)
	for alive(pid) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
}

func (p *edgeProc) hardRestart() error {
	old := p.pid
	syscall.Kill(old, syscall.SIGTERM)
	waitGone(old, 5*time.Second)
	return p.spawn()
}

func (p *edgeProc) handoff() error {
	old := p.pid
	if err := syscall.Kill(old, syscall.SIGUSR2); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(p.pidPath)
		if err == nil {
			if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n != old && alive(n) {
				p.pid = n
				return nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errors.New("lab: handoff did not produce a new edge process")
}

func (p *edgeProc) stop() {
	if p.pid != 0 && alive(p.pid) {
		syscall.Kill(p.pid, syscall.SIGKILL)
		waitGone(p.pid, 2*time.Second)
	}
	os.RemoveAll(p.dir)
}

// ctxSleep sleeps until d or ctx ends.
func ctxSleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
