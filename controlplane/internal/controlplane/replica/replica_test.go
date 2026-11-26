package replica_test

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/actor/actortest"
)

// harness runs a Replica one Receive at a time and keeps what a runtime
// would keep for it: the armed timers and the Performs not yet answered,
// with the forget epochs of their scopes. A timer or answer that the
// replica forgot (ForgetPending) or replaced (StartTimer, CancelTimer) can
// not be delivered.
type harness struct {
	t      *testing.T
	m      *replica.Replica
	env    *actortest.Env
	book   *actortest.Book
	number uint64 // last pending number handed out
}

func newHarness(t *testing.T, config replica.Config) *harness {
	t.Helper()
	env := actortest.NewEnv(actortest.Config{Self: actor.Incarnation{Name: "replica"}, Counter: true})
	return &harness{
		t: t, m: replica.New(config)(env), env: env,
		book: &actortest.Book{
			Timers: map[actor.TimerKey]actortest.Record{},
			Asked:  map[uint64]actortest.Record{},
			Forgot: map[string]uint64{},
		},
	}
}

// run lends the replica a fresh outbox, calls f with it, and returns the
// recorded effects after keeping track of timers and Performs.
func (h *harness) run(f func(o *actor.Outbox[runtime.EventData, runtime.IO])) []actor.Effect {
	h.t.Helper()
	effects := actortest.Run(h.t, &h.number, actor.Binding{}, f)
	actortest.Drain(h.t, effects, h.book, actortest.Handler{})
	return effects
}

// recvAt gives msg to the replica at time at.
func (h *harness) recvAt(at actor.Mono, msg runtime.EventData) []actor.Effect {
	h.t.Helper()
	return h.run(func(o *actor.Outbox[runtime.EventData, runtime.IO]) {
		h.m.Receive(actor.Now{Mono: at}, msg, o)
	})
}

// recv gives msg to the replica at time zero.
func (h *harness) recv(msg runtime.EventData) []actor.Effect { return h.recvAt(0, msg) }

// offer runs Offer for a command whose caller is ask number n.
func (h *harness) offer(n uint64, op replica.ClientOperation) []actor.Effect {
	h.t.Helper()
	op.ReplyTo = caller(n)
	return h.run(func(o *actor.Outbox[runtime.EventData, runtime.IO]) { h.m.Offer(op, o) })
}

// answerAt answers the Perform e with done at time at, as the executor
// would. The Perform must still be waited for.
func (h *harness) answerAt(at actor.Mono, e actor.Effect, done actor.Payload) []actor.Effect {
	h.t.Helper()
	if !h.book.Waited(e) {
		h.t.Fatalf("answer to %T: the replica does not wait for it any more", e.Msg)
	}
	delete(h.book.Asked, e.Number)
	return h.recvAt(at, deliver(h.t, e, done))
}

func (h *harness) answer(e actor.Effect, done actor.Payload) []actor.Effect {
	h.t.Helper()
	return h.answerAt(0, e, done)
}

// waited reports whether the replica still waits for the answer of e.
func (h *harness) waited(e actor.Effect) bool { return h.book.Waited(e) }

// armed returns the timer under the stored key, if it would still fire.
func (h *harness) armed(key actor.TimerKey) (actor.Effect, bool) { return h.book.Armed(key) }

// fireAt delivers the timer under the stored key at time at. It must be armed.
func (h *harness) fireAt(at actor.Mono, key actor.TimerKey) []actor.Effect {
	h.t.Helper()
	e, ok := h.book.TakeTimer(key)
	if !ok {
		h.t.Fatalf("timer %q is not armed", key)
	}
	msg, ok := e.Msg.(runtime.EventData)
	if !ok {
		h.t.Fatalf("timer %q carries %T", key, e.Msg)
	}
	return h.recvAt(at, msg)
}

// Stored keys of the replica's timers.
const (
	keyCatchUp    actor.TimerKey = "work/catchup"
	keyRole       actor.TimerKey = "role"
	keyCheckpoint actor.TimerKey = "checkpoint"
)

// deliver turns an answer into the replica's message with the Perform's adapt.
func deliver(t *testing.T, e actor.Effect, done actor.Payload) runtime.EventData {
	t.Helper()
	if e.Kind != actor.EffectPerform {
		t.Fatalf("effect %d is not a Perform", e.Kind)
	}
	return actortest.Adapt[runtime.EventData](t, e, done)
}

// caller is the reply handle of ask number n of the test.
func caller(n uint64) actor.ReplyTo[runtime.ControlPlaneDone] {
	return actor.NewReplyTo[runtime.ControlPlaneDone](actor.ReplyID{
		Asker: actor.Asker{Kind: actor.SimOutsideAsker, Name: "test"}, Number: n,
	})
}

// reply is one Reply effect: the ask it answers and the answer.
type reply struct {
	n    uint64
	done runtime.ControlPlaneDone
}

// replies returns the Reply effects in record order.
func replies(t *testing.T, effects []actor.Effect) []reply {
	t.Helper()
	var out []reply
	for _, e := range effects {
		if e.Kind != actor.EffectReply {
			continue
		}
		d, ok := e.Msg.(runtime.ControlPlaneDone)
		if !ok {
			t.Fatalf("reply carries %T", e.Msg)
		}
		out = append(out, reply{n: e.Reply.Number, done: d})
	}
	return out
}

// performs returns the Perform and Do effects in record order.
func performs(effects []actor.Effect) []actor.Effect {
	var out []actor.Effect
	for _, e := range effects {
		if e.Kind == actor.EffectPerform || e.Kind == actor.EffectDo {
			out = append(out, e)
		}
	}
	return out
}

// find returns the first Perform or Do of a T.
func find[T runtime.IO](effects []actor.Effect) (T, actor.Effect, bool) {
	for _, e := range performs(effects) {
		if v, ok := e.Msg.(T); ok {
			return v, e, true
		}
	}
	var zero T
	return zero, actor.Effect{}, false
}

// must returns the first Perform or Do of a T, or fails.
func must[T runtime.IO](t *testing.T, effects []actor.Effect) (T, actor.Effect) {
	t.Helper()
	v, e, ok := find[T](effects)
	if !ok {
		var zero T
		t.Fatalf("no %T in %v", zero, describe(effects))
	}
	return v, e
}

// only returns the one Perform or Do of effects, which must be a T.
func only[T runtime.IO](t *testing.T, effects []actor.Effect) (T, actor.Effect) {
	t.Helper()
	ps := performs(effects)
	if len(ps) != 1 {
		var zero T
		t.Fatalf("IO = %v, want one %T", describe(effects), zero)
	}
	v, ok := ps[0].Msg.(T)
	if !ok {
		t.Fatalf("IO = %T, want %T", ps[0].Msg, v)
	}
	return v, ps[0]
}

// describe lists effects for failure messages.
func describe(effects []actor.Effect) []string {
	out := make([]string, 0, len(effects))
	for _, e := range effects {
		s := e.Scope + ":"
		switch e.Kind {
		case actor.EffectStartTimer, actor.EffectCancelTimer:
			s += "timer " + string(e.StoredKey())
		case actor.EffectStop:
			s += "stop " + e.Cause
		case actor.EffectReply:
			s += "reply"
		case actor.EffectForgetPending:
			s += "forget"
		default:
			s += fmt.Sprintf("%T", e.Msg)
		}
		out = append(out, s)
	}
	return out
}
