package live_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
)

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// A blocked exec does not stop the actor from handling messages.
func TestBlockedExecDoesNotStallActor(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	release := make(chan struct{})
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		if _, ok := m.(start); ok {
			actor.Perform(o, read{"slow"}, func(d readDone) msg { return got(d) })
			o.TellSelf(note{"next"})
		}
	}), func(ctx *live.ExecCtx, x io) error {
		<-release
		live.Answer(ctx, x.(read), readDone{"done"})
		return nil
	})
	done := runHost(t, h)
	rec.waitFor(t, "note next") // the exec is still blocked
	close(release)
	rec.waitFor(t, "got done")
	_ = done()
}

// No more than ExecWorkers execs run at once; the others queue, then run.
func TestExecWorkersBound(t *testing.T) {
	const workers, total = 3, 8
	h := live.NewHost(live.HostConfig{Clock: live.RealClock(), StopGrace: 200 * ms, ExecWorkers: workers})
	release := make(chan struct{})
	var running, peak, finished atomic.Int32
	live.Spawn(h, props("a", newRecorder(), func(m msg, o *outbox) {
		if _, ok := m.(start); ok {
			for range total {
				o.Do(read{"x"})
			}
		}
	}), func(ctx *live.ExecCtx, x io) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		<-release
		running.Add(-1)
		finished.Add(1)
		return nil
	})
	done := runHost(t, h)
	waitUntil(t, "workers busy and the rest queued", func() bool {
		q, b := h.ExecStats()
		return b == workers && q == total-workers
	})
	time.Sleep(20 * time.Millisecond) // a fourth exec would have started by now
	if p := peak.Load(); p != workers {
		t.Fatalf("peak concurrent execs = %d, want %d", p, workers)
	}
	close(release)
	waitUntil(t, "every exec to finish", func() bool { return finished.Load() == total })
	if p := peak.Load(); p > workers {
		t.Fatalf("peak concurrent execs = %d, want at most %d", p, workers)
	}
	_ = done()
}

// Stopping the Host cancels the context of a running exec once StopGrace has
// passed: the exec gets the grace to finish first.
func TestHostStopCancelsExecContext(t *testing.T) {
	h := newHost() // StopGrace 200 ms
	started := make(chan struct{})
	cancelled := make(chan struct{})
	live.Spawn(h, props("a", newRecorder(), func(m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.Do(read{"x"})
		}
	}), func(ctx *live.ExecCtx, x io) error {
		close(started)
		<-ctx.Context().Done()
		close(cancelled)
		return nil
	})
	done := runHost(t, h)
	<-started
	select {
	case <-cancelled:
		t.Fatal("the context ended before the Host stopped")
	default:
	}
	stopped := time.Now()
	_ = done()
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the exec context was not cancelled")
	}
	if d := time.Since(stopped); d < 150*time.Millisecond {
		t.Fatalf("the context was cancelled after %v, before the grace", d)
	}
}

// IO recorded just before a Trip is carried out, running and queued alike.
func TestHostStopDrainsExecQueue(t *testing.T) {
	const total = 6
	h := live.NewHost(live.HostConfig{Clock: live.RealClock(), StopGrace: 2 * actor.Second, ExecWorkers: 2})
	var ran atomic.Int32
	var cancelled atomic.Bool
	live.Spawn(h, props("a", newRecorder(), func(m msg, o *outbox) {
		if _, ok := m.(start); ok {
			for range total {
				o.Do(read{"x"})
			}
			o.Stop(actor.Trip, "x")
		}
	}), func(ctx *live.ExecCtx, x io) error {
		time.Sleep(10 * time.Millisecond)
		if ctx.Context().Err() != nil {
			cancelled.Store(true)
		}
		ran.Add(1)
		return nil
	})
	err := h.Run(context.Background())
	if !errors.Is(err, live.ErrTripped) {
		t.Fatalf("Run error = %v, want ErrTripped", err)
	}
	if n := ran.Load(); n != total {
		t.Fatalf("%d of %d IO ran", n, total)
	}
	if cancelled.Load() {
		t.Fatal("an exec saw its context cancelled during the drain")
	}
}

// A panic in exec ends Run with an error.
func TestExecPanicEndsRun(t *testing.T) {
	h := newHost()
	live.Spawn(h, props("a", newRecorder(), func(m msg, o *outbox) {
		switch m.(type) {
		case start:
			o.Do(read{"x"})
		case stop:
			o.Stop(actor.Exit, "")
		}
	}), func(*live.ExecCtx, io) error { panic("boom") })
	errc := make(chan error, 1)
	go func() { errc <- h.Run(context.Background()) }()
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), "exec") {
			t.Fatalf("Run error = %v, want the exec panic", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not end after an exec panic")
	}
}
