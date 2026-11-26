package session

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/actor/actortest"
)

// record runs fn with a fresh outbox and returns the effects it recorded.
func record(t *testing.T, fn func(o *Outbox)) []actor.Effect {
	t.Helper()
	n := uint64(0)
	return actortest.Run(t, &n, "session", fn)
}

// kinds lists the kinds of effects.
func kinds(effects []actor.Effect) []actor.EffectKind {
	var out []actor.EffectKind
	for _, e := range effects {
		out = append(out, e.Kind)
	}
	return out
}

func TestRetryDelay(t *testing.T) {
	t.Parallel()
	if RetryWaitTime(1) != 100*actor.Millisecond {
		t.Fatalf("RetryDelay(1) = %d", RetryWaitTime(1))
	}
	if RetryWaitTime(99) != RetryWaitTime(4) {
		t.Fatal("RetryDelay does not cap at 4")
	}
}

func TestDialFailAndReady(t *testing.T) {
	t.Parallel()
	s := &Session{ID: 0, Brokers: []string{"b0", "b1"}}
	effects := record(t, s.Dial)
	if len(effects) != 1 || effects[0].Kind != actor.EffectPerform || s.State != Connecting {
		t.Fatalf("dial = %v state %d", kinds(effects), s.State)
	}
	if c, ok := effects[0].Msg.(core.Connect); !ok || c.Addr != "b0" {
		t.Fatalf("dial = %#v", effects[0].Msg)
	}
	var ev Event
	effects = record(t, func(o *Outbox) { ev = s.Receive(Dialed{Done: core.ConnectDone{Err: core.ErrUnavailable}}, o) })
	if s.State != WaitingToRetry || ev.Kind != Nothing || len(effects) != 1 || effects[0].Kind != actor.EffectStartTimer {
		t.Fatalf("dial fail state=%d effects=%v", s.State, kinds(effects))
	}
	effects = record(t, func(o *Outbox) { s.Receive(WakeDue{}, o) })
	if c, ok := effects[0].Msg.(core.Connect); !ok || c.Addr != "b1" {
		t.Fatalf("redial = %v", kinds(effects))
	}
	record(t, func(o *Outbox) { ev = s.Receive(Dialed{Done: core.ConnectDone{Conn: 7}}, o) })
	if s.State != Connected || s.Conn != 7 || ev.Kind != Ready {
		t.Fatalf("ready state=%d conn=%d event=%v", s.State, s.Conn, ev)
	}
	record(t, func(o *Outbox) { ev = s.Receive(Net{Event: core.ConnClosed{Conn: 6}}, o) })
	if s.State != Connected || ev.Kind != Nothing {
		t.Fatal("a close of an older connection was taken")
	}
	effects = record(t, func(o *Outbox) { ev = s.Receive(Net{Event: core.ConnClosed{Conn: 7}}, o) })
	if s.State != WaitingToRetry || ev.Kind != Lost || ev.Why != "conn_closed" {
		t.Fatalf("closed state=%d event=%v", s.State, ev)
	}
	if k := kinds(effects); len(k) != 2 || k[0] != actor.EffectForgetPending || k[1] != actor.EffectStartTimer {
		t.Fatalf("lost effects = %v", k)
	}
}

func TestRequestTimeout(t *testing.T) {
	t.Parallel()
	s := &Session{ID: 1, Brokers: []string{"b0"}, State: Connected, Conn: 3}
	effects := record(t, func(o *Outbox) { s.Request(o, []byte("x"), actor.Second) })
	if k := kinds(effects); len(k) != 2 || k[0] != actor.EffectDo || k[1] != actor.EffectStartTimer || !s.Busy {
		t.Fatalf("Request effects = %v", k)
	}
	var ev Event
	effects = record(t, func(o *Outbox) { ev = s.Receive(RequestDue{}, o) })
	if s.State != WaitingToRetry || ev.Kind != Lost || ev.Why != "timeout" || s.Busy {
		t.Fatalf("timeout state=%d event=%v", s.State, ev)
	}
	if c, ok := effects[0].Msg.(core.CloseConn); !ok || c.Conn != 3 || !c.Abort {
		t.Fatalf("timeout effects = %v", kinds(effects))
	}
	// A timeout with no request outstanding does nothing.
	s.State, s.Conn = Connected, 4
	if effects := record(t, func(o *Outbox) { ev = s.Receive(RequestDue{}, o) }); len(effects) != 0 || ev.Kind != Nothing {
		t.Fatalf("idle timeout = %v", kinds(effects))
	}
}

func TestIdleAndBytes(t *testing.T) {
	t.Parallel()
	s := &Session{ID: 2, Brokers: []string{"b0"}, State: Connected, Conn: 9}
	var ev Event
	record(t, func(o *Outbox) { ev = s.Receive(WakeDue{}, o) })
	if ev.Kind != Idle {
		t.Fatalf("idle = %v", ev)
	}
	record(t, func(o *Outbox) { ev = s.Receive(Net{Event: core.BytesReceived{Conn: 9, Data: []byte("ab")}}, o) })
	if ev.Kind != Bytes || string(ev.Data) != "ab" {
		t.Fatalf("bytes = %v", ev)
	}
	s.Busy = true
	record(t, func(o *Outbox) { ev = s.Receive(WakeDue{}, o) })
	if ev.Kind != Nothing {
		t.Fatal("a wake while busy was idle")
	}
}
