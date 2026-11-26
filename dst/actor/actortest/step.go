package actortest

import (
	"strings"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
)

// TB is the testing.TB (or rapid.T) surface the harness needs.
type TB interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// Run builds the outbox of one Receive of the actor called name, calls recv,
// and returns the closed effects. Ask and Call numbers count up from *next.
// The outbox is the actor's own: Self is named but not wired, and the
// asks it makes come from an actor asker of that name. Misuse fails t.
func Run[In, IO actor.Payload](t TB, next *uint64, name string, recv func(*actor.Outbox[In, IO])) []actor.Effect {
	t.Helper()
	o := core.NewOutbox[In, IO](core.Binding{
		Self:   core.EntityID{Name: name},
		Asker:  core.Asker{Kind: core.ActorAsker, Name: name},
		Next:   func() uint64 { *next++; return *next },
		Misuse: func(what string) { t.Fatalf("outbox misuse: %s", what) },
	})
	recv(o)
	return core.Close(o)
}

// Ref is a handle for the actor called name, for tests that wire an actor to
// a peer that is not running. Each call makes a handle of its own runtime, so
// two handles of one name are not equal.
func Ref[P actor.Payload](name string) actor.EntityRef[P] {
	return core.NewEntityRef[P](core.EntityID{Runtime: core.NewOwner(), Slot: 1, Name: name})
}

// ReplyTo is the handle of ask number n made by the outside caller called
// name. Two handles with the same name and n are equal.
func ReplyTo[R actor.Payload](name string, n uint64) actor.ReplyTo[R] {
	return core.NewReplyTo[R](core.ReplyID{Asker: core.Asker{Kind: core.SimOutsideAsker, Name: name}, Number: n})
}

// Record is one kept effect with the tick it was recorded at.
type Record struct {
	Effect actor.Effect
	Tick   uint64
}

// Book tracks armed timers and unanswered Performs across Receives, and
// drops them after a ForgetPending of their scope (or a parent scope).
type Book struct {
	Timers map[actor.TimerKey]Record
	Asked  map[uint64]Record
	Forgot map[string]uint64 // scope -> tick of last ForgetPending
	Tick   uint64
}

// Handler is the product side of Drain. Nil callbacks are ignored unless Strict.
type Handler struct {
	Strict     bool // Fatalf on an unhandled non-Fact effect
	AfterStart func(actor.Effect)
	Perform    func(actor.Effect)
	Do         func(actor.Effect)
	Stop       func(actor.Effect)
}

// Drain walks effects: Book takes timers, forgets, tracked Performs and the
// Stop-must-be-last check; Handler gets the rest. Every effect bumps Book.Tick
// when a Book is set, matching forget-scope timing across mixed effect kinds.
func Drain(t TB, effects []actor.Effect, book *Book, h Handler) {
	t.Helper()
	for i, e := range effects {
		if book != nil {
			book.Tick++
			if handled, after := book.apply(t, effects, e, i); handled {
				if after != nil {
					after(h)
				}
				continue
			}
		}
		dispatch(t, h, effects, e, i)
	}
}

func dispatch(t TB, h Handler, effects []actor.Effect, e actor.Effect, i int) {
	t.Helper()
	switch e.Kind {
	case actor.EffectPerform:
		call(t, h.Strict, h.Perform, e, "Perform")
	case actor.EffectDo:
		call(t, h.Strict, h.Do, e, "Do")
	case actor.EffectFact:
		// never behavioural; ignore
	case actor.EffectStop:
		if i != len(effects)-1 {
			t.Fatalf("Stop is not the last effect: %v", effects)
		}
		if h.Stop != nil {
			h.Stop(e)
		}
	default:
		if h.Strict {
			t.Fatalf("unexpected effect %d", e.Kind)
		}
	}
}

func call(t TB, strict bool, f func(actor.Effect), e actor.Effect, kind string) {
	t.Helper()
	if f != nil {
		f(e)
	} else if strict {
		t.Fatalf("unexpected %s of %T", kind, e.Msg)
	}
}

func (b *Book) apply(t TB, effects []actor.Effect, e actor.Effect, i int) (bool, func(Handler)) {
	t.Helper()
	switch e.Kind {
	case actor.EffectForgetPending:
		if b.Forgot == nil {
			b.Forgot = map[string]uint64{}
		}
		b.Forgot[e.Scope] = b.Tick
		return true, nil
	case actor.EffectStartTimer:
		if b.Timers == nil {
			b.Timers = map[actor.TimerKey]Record{}
		}
		b.Timers[e.StoredKey()] = Record{Effect: e, Tick: b.Tick}
		return true, func(h Handler) {
			if h.AfterStart != nil {
				h.AfterStart(e)
			}
		}
	case actor.EffectCancelTimer:
		delete(b.Timers, e.StoredKey())
		return true, nil
	case actor.EffectPerform:
		if b.Asked == nil {
			return false, nil
		}
		b.Asked[e.Number] = Record{Effect: e, Tick: b.Tick}
		return true, nil
	case actor.EffectStop:
		if i != len(effects)-1 {
			t.Fatalf("Stop is not the last effect: %v", effects)
		}
		return true, func(h Handler) {
			if h.Stop != nil {
				h.Stop(e)
			}
		}
	}
	return false, nil
}

func within(scope, forgetScope string) bool {
	return forgetScope == "" || scope == forgetScope || strings.HasPrefix(scope, forgetScope+"/")
}

func (b *Book) forgotten(r Record) bool {
	for scope, at := range b.Forgot { //determinismcheck:ok boolean OR; order does not matter
		if at > r.Tick && within(r.Effect.Scope, scope) {
			return true
		}
	}
	return false
}

// Waited reports whether the Perform e is still unanswered and not forgotten.
func (b *Book) Waited(e actor.Effect) bool {
	r, ok := b.Asked[e.Number]
	return ok && !b.forgotten(r)
}

// Armed returns the timer under key if it would still fire.
func (b *Book) Armed(key actor.TimerKey) (actor.Effect, bool) {
	r, ok := b.Timers[key]
	if !ok || b.forgotten(r) {
		return actor.Effect{}, false
	}
	return r.Effect, true
}

// TakeTimer removes and returns the armed timer under key.
func (b *Book) TakeTimer(key actor.TimerKey) (actor.Effect, bool) {
	e, ok := b.Armed(key)
	if ok {
		delete(b.Timers, key)
	}
	return e, ok
}

// Adapt turns an answer into an In with the effect's Adapt, or fails.
func Adapt[In actor.Payload](t TB, e actor.Effect, answer actor.Payload) In {
	t.Helper()
	msg, ok := e.Adapt(answer).(In)
	if !ok {
		var zero In
		t.Fatalf("adapt of %T did not make %T", e.Msg, zero)
	}
	return msg
}
