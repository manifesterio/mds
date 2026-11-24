package actor

// This file is the runtime side of the package, used by dst/sim and
// dst/runtime/live. Product code never calls it. It is also the only place
// where message types are erased to Payload and asserted back.

import "github.com/manifesterio/mds/dst/core/trace"

// EffectKind says what an Effect does. The numbers are the frozen trace-hash
// tags of ADR section 5.
type EffectKind uint16

// The effects an Outbox records.
const (
	EffectTellSelf      EffectKind = 2001
	EffectAfter         EffectKind = 2002
	EffectStartTimer    EffectKind = 2003
	EffectCancelTimer   EffectKind = 2004
	EffectDo            EffectKind = 2005
	EffectStop          EffectKind = 2006
	EffectFact          EffectKind = 2007
	EffectForgetPending EffectKind = 2008
	EffectTell          EffectKind = 2009
	EffectTellService   EffectKind = 2010
	EffectAsk           EffectKind = 2011
	EffectCall          EffectKind = 2012
	EffectReply         EffectKind = 2013
	EffectPerform       EffectKind = 2014
	EffectWatch         EffectKind = 2015

	// tagSubPrefix comes before an effect recorded through a Sub.
	tagSubPrefix = 2016
)

// Effect is one recorded effect, with its message types erased. Which fields
// are set depends on Kind.
type Effect struct {
	Kind  EffectKind
	Scope string // the Sub path, "" for the actor itself

	Msg     Payload   // the message, request, answer, IO value or gone message
	D       Duration  // After, StartTimer
	Key     TimerKey  // StartTimer, CancelTimer: the key as recorded (see StoredKey)
	To      EntityID  // Tell, Ask
	Service ServiceID // TellService, Call
	Reply   ReplyID   // Ask (the new ReplyTo), Reply, Watch
	Number  uint64    // Ask, Call, Perform: the pending number
	How     End       // Stop
	Cause   string    // Stop
	Fact    Fact      // Fact

	// Adapt turns the answer of an Ask, Call or Perform into a message of
	// the actor's own protocol (already wrapped by every Sub on the way). It
	// panics if the answer has the wrong type: a runtime bug.
	Adapt func(answer Payload) Payload
}

// StoredKey is the key of a timer as the runtime keeps it: scoped by the
// Sub path, so two components never share a key.
func (e Effect) StoredKey() TimerKey {
	if e.Scope == "" {
		return e.Key
	}
	return TimerKey(e.Scope + "/" + string(e.Key))
}

// Fold writes the effect into the trace hash (ADR section 5). The epoch of a
// ForgetPending is runtime state: the runtime folds it after this.
func (e Effect) Fold(h *trace.Hash) {
	if e.Scope != "" {
		h.Uint64(tagSubPrefix)
		h.String(e.Scope)
	}
	h.Uint64(uint64(e.Kind))
	switch e.Kind {
	case EffectAfter:
		h.Int64(int64(e.D))
	case EffectStartTimer:
		h.String(string(e.StoredKey()))
		h.Int64(int64(e.D))
	case EffectCancelTimer:
		h.String(string(e.StoredKey()))
	case EffectStop:
		h.Uint64(uint64(e.How))
		h.String(e.Cause)
	case EffectFact:
		e.Fact.Fold(h)
	case EffectForgetPending:
		h.String(e.Scope)
	case EffectTell:
		EntityRef[Payload]{id: e.To}.Fold(h)
	case EffectTellService:
		ServiceRef[Payload, Payload]{id: e.Service}.Fold(h)
	case EffectAsk:
		EntityRef[Payload]{id: e.To}.Fold(h)
		e.Reply.Fold(h)
	case EffectCall:
		ServiceRef[Payload, Payload]{id: e.Service}.Fold(h)
		h.Uint64(e.Number)
	case EffectReply, EffectWatch:
		e.Reply.Fold(h)
	case EffectPerform:
		h.Uint64(e.Number)
	case EffectTellSelf, EffectDo:
	}
	if e.Msg != nil {
		e.Msg.Fold(h)
	}
}

// Binding is what a runtime tells an Outbox about the incarnation that is
// receiving.
type Binding struct {
	Self  EntityID
	Asker Asker
	// Next returns the next ask/pending number of the incarnation (from 1).
	Next func() uint64
	// Misuse reports a protocol failure: an effect recorded after the
	// Receive, or through a Sub where it is not allowed.
	Misuse func(what string)
}

// recording is the effect list shared by an Outbox and its Subs.
type recording struct {
	binding Binding
	effects []Effect
	closed  bool
}

func (r *recording) add(e Effect) {
	if r.closed {
		r.misuse("an Outbox was used after its Receive")
		return
	}
	r.effects = append(r.effects, e)
}

func (r *recording) next() uint64 { return r.binding.Next() }

func (r *recording) misuse(what string) {
	if r.binding.Misuse != nil {
		r.binding.Misuse(what)
	}
}

// NewOutbox makes the Outbox of one Receive.
func NewOutbox[In, IO Payload](b Binding) *Outbox[In, IO] {
	return &Outbox[In, IO]{rec: &recording{binding: b}, up: identity}
}

// Close ends the Outbox's Receive and returns its effects in record order.
// Every later use of it, or of its Subs, is reported as misuse.
func Close[In, IO Payload](o *Outbox[In, IO]) []Effect {
	o.rec.closed = true
	effects := o.rec.effects
	o.rec.effects = nil
	return effects
}

func identity(msg Payload) Payload { return msg }

// lift erases f, a function into the outbox's protocol (an answer's adapt,
// or a Sub's wrap), and puts its result through the outbox's own wraps. The
// assertion cannot fail: the runtime matched the answer to its request, and
// only a Sub's own methods pass its message type.
func lift[In, IO, X Payload](o *Outbox[In, IO], f func(X) In) func(Payload) Payload {
	return func(v Payload) Payload { return o.up(f(v.(X))) } //nolint:forcetypeassert // see above
}

// NewEntityRef makes the handle of an actor. Runtimes only.
func NewEntityRef[P Payload](id EntityID) EntityRef[P] { return EntityRef[P]{id: id} }

// NewReplyTo makes the handle of one ask. Runtimes only (sim.Ask, live.Ask).
func NewReplyTo[R Payload](id ReplyID) ReplyTo[R] { return ReplyTo[R]{id: id} }

// NewServiceRef makes the handle of a service. Runtimes only.
func NewServiceRef[Req, Rep Payload](id ServiceID) ServiceRef[Req, Rep] {
	return ServiceRef[Req, Rep]{id: id}
}

// As asserts an erased message back to its type; ok is false on a mismatch
// (a runtime routing bug, or a foreign ref). Runtimes only.
func As[P Payload](msg Payload) (P, bool) {
	p, ok := msg.(P)
	return p, ok
}
