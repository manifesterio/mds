package live_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
)

// An outside Ask whose context ends while the Host stops must return, not
// wait for an answer that never comes. The actor cancels the context and
// stops the Host in one Receive, so Ask often finds both ready.
func TestOutsideAskReturnsWhenCancelRacesStop(t *testing.T) {
	for i := range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		h := newHost()
		a := live.Spawn(h, props("a", newRecorder(), func(m msg, o *outbox) {
			if _, ok := m.(asked); ok {
				cancel()
				o.Stop(actor.Trip, "stop with the ask open")
			}
		}), nil)
		runErr := make(chan error, 1)
		go func() { runErr <- h.Run(context.Background()) }()
		askErr := make(chan error, 1)
		go func() {
			_, err := live.Ask(ctx, h, a, func(to actor.ReplyTo[answer]) msg { return asked{Q: "q", ReplyTo: to} })
			askErr <- err
		}()
		select {
		case err := <-askErr:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, live.ErrHostStopped) {
				t.Fatalf("round %d: Ask: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: Ask did not return after cancel and stop", i)
		}
		if err := <-runErr; !errors.Is(err, live.ErrTripped) {
			t.Fatalf("round %d: Run: %v", i, err)
		}
	}
}

// A Watch recorded while its outside asker gives up must still fire. The
// window where the asker ends between registration and the liveness check
// is too narrow to hit reliably; this covers the path where the watch
// finds the asker gone and fires itself.
func TestWatchFiresWhenAskerEndsMeanwhile(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	cancels := make(map[string]context.CancelFunc)
	ready := make(chan struct{})
	a := live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		if m, ok := m.(asked); ok {
			<-ready
			cancels[m.Q]()
			o.Watch(m.ReplyTo, gone{m.Q})
		}
	}), nil)
	done := runHost(t, h)
	defer func() { _ = done() }()
	for i := range 300 {
		q := strconv.Itoa(i)
		ctx, cancel := context.WithCancel(context.Background())
		cancels[q] = cancel
		errc := make(chan error, 1)
		go func() {
			_, err := live.Ask(ctx, h, a, func(to actor.ReplyTo[answer]) msg { return asked{Q: q, ReplyTo: to} })
			errc <- err
		}()
		ready <- struct{}{}
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Fatalf("round %d: Ask: %v", i, err)
		}
		rec.waitFor(t, "gone "+q)
	}
}
