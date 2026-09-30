// Package graceful implements zero-downtime restarts by passing the listening
// socket from a running process to its replacement.
//
// On SIGUSR2 the old process duplicates its listening socket's file
// descriptor and starts the new binary with that descriptor as fd 3 plus a
// pipe as fd 4. The child builds its listener from fd 3 (the socket never
// closes, so the kernel keeps queueing connections the whole time), starts
// serving, and writes "ready" to fd 4. Only then does the parent stop
// accepting and drain its in-flight requests. This is the technique nginx,
// HAProxy and Cloudflare's own tableflip library use.
package graceful

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	envListenFD = "SWITCHYARD_LISTEN_FD"
	envReadyFD  = "SWITCHYARD_READY_FD"
)

// Listen returns the inherited listener if this process was started by a
// handoff, otherwise a fresh listener on addr.
func Listen(addr string) (ln net.Listener, inherited bool, err error) {
	if os.Getenv(envListenFD) == "3" {
		f := os.NewFile(3, "inherited-listener")
		ln, err = net.FileListener(f)
		f.Close()
		os.Unsetenv(envListenFD)
		if err != nil {
			return nil, false, fmt.Errorf("graceful: inherit listener: %w", err)
		}
		return ln, true, nil
	}
	ln, err = net.Listen("tcp", addr)
	return ln, false, err
}

// Ready tells the parent (if any) that this process is serving.
func Ready() error {
	if os.Getenv(envReadyFD) != "4" {
		return nil
	}
	os.Unsetenv(envReadyFD)
	f := os.NewFile(4, "ready-pipe")
	defer f.Close()
	_, err := f.Write([]byte("ready\n"))
	return err
}

// Handoff starts a replacement process that inherits ln and waits until it
// reports ready. On success the caller should stop accepting and drain.
func Handoff(ln net.Listener, timeout time.Duration) (int, error) {
	tl, ok := ln.(*net.TCPListener)
	if !ok {
		return 0, errors.New("graceful: listener is not TCP")
	}
	lf, err := tl.File() // a dup; closing it does not close ln
	if err != nil {
		return 0, err
	}
	defer lf.Close()
	r, w, err := os.Pipe()
	if err != nil {
		return 0, err
	}
	defer r.Close()
	exe, err := os.Executable()
	if err != nil {
		w.Close()
		return 0, err
	}
	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, envListenFD+"=") && !strings.HasPrefix(kv, envReadyFD+"=") {
			env = append(env, kv)
		}
	}
	cmd.Env = append(env, envListenFD+"=3", envReadyFD+"=4")
	cmd.ExtraFiles = []*os.File{lf, w}
	if err := cmd.Start(); err != nil {
		w.Close()
		return 0, err
	}
	w.Close() // the child holds its own copy
	done := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(r).ReadString('\n')
		if err == nil && strings.TrimSpace(line) != "ready" {
			err = fmt.Errorf("graceful: unexpected readiness message %q", line)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			cmd.Process.Kill()
			return 0, fmt.Errorf("graceful: child never became ready: %w", err)
		}
	case <-time.After(timeout):
		cmd.Process.Kill()
		return 0, errors.New("graceful: child readiness timed out")
	}
	pid := cmd.Process.Pid
	cmd.Process.Release()
	return pid, nil
}
