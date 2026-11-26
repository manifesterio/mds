package core

import "github.com/manifesterio/mds/dst/trace"

// EffectKind says what an Effect does. The numbers are the frozen trace-hash
// tags of the effects.
type EffectKind uint16

// The effects an Outbox records. 2010 and 2012 (the retired service effects)
// are reserved: never reuse them.
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
	EffectAsk           EffectKind = 2011
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

	Msg    Payload  // the message, request, answer, IO value or gone message
	D      Duration // After, StartTimer
	Key    TimerKey // StartTimer, CancelTimer: the key as recorded (see StoredKey)
	To     EntityID // Tell, Ask
	Reply  ReplyID  // Ask (the new ReplyTo), Reply, Watch
	Number uint64   // Ask, Perform: the pending number
	How    End      // Stop
	Cause  string   // Stop
	Fact   Fact     // Fact

	// Adapt turns the answer of an Ask or Perform into a message of
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

// Fold writes the effect into the trace hash. The epoch of a
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
	case EffectAsk:
		EntityRef[Payload]{id: e.To}.Fold(h)
		e.Reply.Fold(h)
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
