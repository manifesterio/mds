package live_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/dst/trace"
)

// ---- the test protocol ----

type msg interface {
	actor.Payload
	isMsg()
}

type (
	start struct{}
	stop  struct{}
	note  struct{ S string }
	asked struct {
		Q       string
		ReplyTo actor.ReplyTo[answer]
	}
	got  struct{ V string }
	gone struct{ Q string }
)

func (start) isMsg() {}
func (stop) isMsg()  {}
func (note) isMsg()  {}
func (asked) isMsg() {}
func (got) isMsg()   {}
func (gone) isMsg()  {}

func (start) Fold(h *trace.Hash)   { h.Uint64(1) }
func (stop) Fold(h *trace.Hash)    { h.Uint64(2) }
func (m note) Fold(h *trace.Hash)  { h.Uint64(3); h.String(m.S) }
func (m asked) Fold(h *trace.Hash) { h.Uint64(4); h.String(m.Q); m.ReplyTo.Fold(h) }
func (m got) Fold(h *trace.Hash)   { h.Uint64(5); h.String(m.V) }
func (m gone) Fold(h *trace.Hash)  { h.Uint64(6); h.String(m.Q) }

type answer struct{ V string }

func (m answer) Fold(h *trace.Hash) { h.Uint64(7); h.String(m.V) }

type io interface {
	actor.Payload
	isIO()
}

type read struct{ Key string }

func (read) isIO()                       {}
func (m read) Fold(h *trace.Hash)        { h.Uint64(20); h.String(m.Key) }
func (m read) AsRequest() (io, readDone) { return m, readDone{} }

type readDone struct{ V string }

func (m readDone) Fold(h *trace.Hash) { h.Uint64(21); h.String(m.V) }

type outbox = actor.Outbox[msg, io]

// ---- a scripted actor that reports what it received ----

type recorder struct {
	mu   sync.Mutex
	seen []string
}

func newRecorder() *recorder { return &recorder{} }

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.seen = append(r.seen, s)
	r.mu.Unlock()
}

func (r *recorder) lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

// waitFor waits until want is received, or fails after 2 s.
func (r *recorder) waitFor(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !slices.Contains(r.lines(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("did not receive %q; got %v", want, r.lines())
		}
		time.Sleep(time.Millisecond)
	}
}

type scripted struct {
	rec *recorder
	fn  func(m msg, o *outbox)
}

func (a *scripted) Receive(_ actor.Now, m msg, o *outbox) {
	a.rec.add(text(m))
	if a.fn != nil {
		a.fn(m, o)
	}
}

func (*scripted) StateHash() uint64 { return 0 }

func text(m msg) string {
	switch m := m.(type) {
	case start:
		return "start"
	case stop:
		return "stop"
	case note:
		return "note " + m.S
	case asked:
		return "asked " + m.Q
	case got:
		return "got " + m.V
	case gone:
		return "gone " + m.Q
	}
	return "?"
}

func props(name string, rec *recorder, fn func(m msg, o *outbox)) actor.Props[msg, io] {
	return actor.Props[msg, io]{
		Name:    name,
		Boot:    func(actor.Incarnation) msg { return start{} },
		StopMsg: func(actor.Mono) msg { return stop{} },
		New:     func(actor.Env) actor.Actor[msg, io] { return &scripted{rec: rec, fn: fn} },
	}
}

const ms = actor.Millisecond

// runHost runs h in the background and returns a function that stops it
// and returns Run's error.
func runHost(t *testing.T, h *live.Host) func() error {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- h.Run(ctx) }()
	return func() error {
		stop()
		select {
		case err := <-errc:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return")
			return nil
		}
	}
}

func newHost() *live.Host {
	return live.NewHost(live.HostConfig{Clock: live.RealClock(), StopGrace: 200 * ms})
}

// ---- effects in record order ----

func TestEffectsAppliedInOrder(t *testing.T) {
	h := newHost()
	recB := newRecorder()
	var b actor.EntityRef[msg]
	sent := make(chan struct{})
	live.Spawn(h, props("a", newRecorder(), func(m msg, o *outbox) {
		if _, ok := m.(start); ok {
			for _, s := range []string{"1", "2", "3", "4", "5"} {
				b.Tell(o, note{s})
			}
			close(sent)
		}
	}), nil)
	b = live.Spawn(h, props("b", recB, nil), nil)
	done := runHost(t, h)
	<-sent
	recB.waitFor(t, "note 5")
	_ = done()
	var notes []string
	for _, l := range recB.lines() {
		if l != "start" {
			notes = append(notes, l)
		}
	}
	if !slices.Equal(notes, []string{"note 1", "note 2", "note 3", "note 4", "note 5"}) {
		t.Errorf("b: %v", notes)
	}
}

// ---- cancel and restart drop a fired timer ----

func TestCancelDropsFiredTimer(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		switch m := m.(type) {
		case start:
			o.TellSelf(note{"cancel"})
			o.StartTimer("k", 0, note{"k-fired"})
			o.TellSelf(note{"restart"})
			o.StartTimer("j", 0, note{"j-old"})
			o.TellSelf(note{"end"})
		case note:
			switch m.S {
			case "cancel":
				o.CancelTimer("k")
			case "restart":
				o.StartTimer("j", ms, note{"j-new"})
			}
		}
	}), nil)
	done := runHost(t, h)
	rec.waitFor(t, "note j-new")
	_ = done()
	for _, l := range rec.lines() {
		if l == "note k-fired" || l == "note j-old" {
			t.Fatalf("a cancelled or replaced timer was received: %v", rec.lines())
		}
	}
}

// ---- answers reach only the incarnation and pending set that asked ----

func TestPerformAnswerScoped(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	var answerLater func()
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		switch m := m.(type) {
		case start:
			actor.Perform(o, read{"forgotten"}, func(d readDone) msg { return got(d) })
			o.TellSelf(note{"forget"})
		case note:
			if m.S == "forget" {
				o.ForgetPending()
				actor.Perform(o, read{"kept"}, func(d readDone) msg { return got(d) })
			}
		}
	}), func(ctx *live.ExecCtx, x io) error {
		r := x.(read)
		if r.Key == "forgotten" {
			answerLater = func() { live.Answer(ctx, r, readDone{"forgotten"}) }
			return nil
		}
		go func() {
			answerLater() // the forgotten answer arrives first
			live.Answer(ctx, r, readDone{"kept"})
		}()
		return nil
	})
	done := runHost(t, h)
	rec.waitFor(t, "got kept")
	_ = done()
	if slices.Contains(rec.lines(), "got forgotten") {
		t.Fatalf("a forgotten Perform answer was received: %v", rec.lines())
	}
}

// ---- ForgetPending drops timers ----

func TestForgetPendingDropsAnswers(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		switch m := m.(type) {
		case start:
			o.StartTimer("t", 20*ms, note{"timer"})
			o.After(0, note{"after"})
			o.TellSelf(note{"forget"})
		case note:
			if m.S == "forget" {
				o.ForgetPending()
				o.After(40*ms, note{"end"})
			}
		}
	}), nil)
	done := runHost(t, h)
	rec.waitFor(t, "note end")
	_ = done()
	if slices.Contains(rec.lines(), "note timer") {
		t.Fatalf("a forgotten timer fired: %v", rec.lines())
	}
}

// ---- Watch fires when an outside Ask's context ends; never after a Reply ----

func TestOutsideAskCancelFiresWatch(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	a := live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		if m, ok := m.(asked); ok {
			o.Watch(m.ReplyTo, gone{m.Q})
			if m.Q == "answered" {
				actor.Reply(o, m.ReplyTo, answer{"yes"})
			}
		}
	}), nil)
	done := runHost(t, h)
	defer func() { _ = done() }()

	ans, err := live.Ask(context.Background(), h, a,
		func(to actor.ReplyTo[answer]) msg { return asked{Q: "answered", ReplyTo: to} })
	if err != nil || ans.V != "yes" {
		t.Fatalf("answered: %v %v", ans, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		rec.waitFor(t, "asked abandoned")
		cancel()
	}()
	_, err = live.Ask(ctx, h, a, func(to actor.ReplyTo[answer]) msg { return asked{Q: "abandoned", ReplyTo: to} })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned: %v", err)
	}
	rec.waitFor(t, "gone abandoned")
	time.Sleep(20 * time.Millisecond)
	n := 0
	for _, l := range rec.lines() {
		if l == "gone answered" {
			t.Fatalf("gone after a Reply: %v", rec.lines())
		}
		if l == "gone abandoned" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("gone delivered %d times", n)
	}
}

// ---- Stop ends the Host's run with its error ----

func TestStopDropsEverything(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.StartTimer("t", 10*ms, note{"never"})
			o.Stop(actor.Trip, "bad state")
		}
	}), nil)
	other := newRecorder()
	live.Spawn(h, props("b", other, nil), nil)
	err := h.Run(context.Background())
	if !errors.Is(err, live.ErrTripped) {
		t.Fatalf("Run: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if slices.Contains(rec.lines(), "note never") {
		t.Fatalf("a timer of a stopped actor fired: %v", rec.lines())
	}
	if !slices.Contains(other.lines(), "stop") {
		t.Errorf("the other actor was not asked to stop: %v", other.lines())
	}
}

// ---- Sub wraps and scopes ----

type compMsg struct{ S string }

func (m compMsg) Fold(h *trace.Hash) { h.Uint64(30); h.String(m.S) }

func TestSubWrapsMessages(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		comp := actor.Sub(o, "comp", func(c compMsg) msg { return note{"comp:" + c.S} })
		switch m := m.(type) {
		case start:
			comp.StartTimer("t", 10*ms, compMsg{"timer"})
			o.StartTimer("t", 30*ms, note{"parent"})
			comp.TellSelf(compMsg{"now"})
		case note:
			if m.S == "comp:now" {
				comp.ForgetPending()
			}
		}
	}), nil)
	done := runHost(t, h)
	rec.waitFor(t, "note parent")
	_ = done()
	if slices.Contains(rec.lines(), "note comp:timer") {
		t.Fatalf("the component's forgotten timer fired: %v", rec.lines())
	}
}

// ---- shutdown, stop grace, OnLoop ----

func TestShutdownStopsGracefully(t *testing.T) {
	shutdown := make(chan struct{})
	h := live.NewHost(live.HostConfig{Clock: live.RealClock(), Shutdown: shutdown, StopGrace: time2d(time.Second)})
	rec := newRecorder()
	live.Spawn(h, props("a", rec, func(m msg, o *outbox) {
		if _, ok := m.(stop); ok {
			o.Stop(actor.Exit, "")
		}
	}), nil)
	errc := make(chan error, 1)
	go func() { errc <- h.Run(context.Background()) }()
	rec.waitFor(t, "start")
	close(shutdown)
	if err := <-errc; err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
}

func TestStopGraceKills(t *testing.T) {
	shutdown := make(chan struct{})
	h := live.NewHost(live.HostConfig{Clock: live.RealClock(), Shutdown: shutdown, StopGrace: 50 * ms})
	rec := newRecorder()
	live.Spawn(h, props("a", rec, nil), nil) // ignores stop
	errc := make(chan error, 1)
	go func() { errc <- h.Run(context.Background()) }()
	rec.waitFor(t, "start")
	close(shutdown)
	if err := <-errc; !errors.Is(err, live.ErrKilled) {
		t.Fatalf("want ErrKilled, got %v", err)
	}
}

func TestOnLoopAndTellAfterStop(t *testing.T) {
	h := newHost()
	rec := newRecorder()
	a := live.Spawn(h, props("a", rec, nil), nil)
	done := runHost(t, h)
	rec.waitFor(t, "start")
	ran := false
	if err := live.OnLoop(context.Background(), h, a, func() { ran = true }); err != nil || !ran {
		t.Fatalf("OnLoop: %v %v", err, ran)
	}
	if err := live.Tell(h, a, msg(note{"x"})); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, "note x")
	_ = done()
	if err := live.Tell(h, a, msg(note{"y"})); !errors.Is(err, live.ErrHostStopped) {
		t.Fatalf("Tell after stop: %v", err)
	}
	if _, err := live.Ask(context.Background(), h, a,
		func(to actor.ReplyTo[answer]) msg { return asked{ReplyTo: to} }); !errors.Is(err, live.ErrHostStopped) {
		t.Fatalf("Ask after stop: %v", err)
	}
}

func time2d(d time.Duration) actor.Duration { return actor.Duration(d) }
