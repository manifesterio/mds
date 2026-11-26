package actor

import "github.com/manifesterio/mds/dst/internal/core"

// Payload is every message, request, answer and IO value. Fold writes the
// value into the trace hash: its frozen tag first, then its fields. dstgen
// writes Fold from //dst:cases and //dst:payload.
type Payload = core.Payload

// NoIO is the IO type of an actor that does no product IO. It has no cases,
// so nothing can be passed to Do or Perform on an Outbox[In, NoIO].
type NoIO = core.NoIO

// Request ties one case of an IO sum to its answer type, so Perform and the
// executors' Answer are checked at compile time. dstgen writes AsRequest
// from //dst:answer; it returns the value itself as an IO and the zero
// answer. Product code never calls it.
type Request[IO, Rep Payload] = core.Request[IO, Rep]

// TimerKey names a timer of one actor. Products build keys from their own
// data, e.g. TimerKey("session/" + group + "/" + member).
type TimerKey = core.TimerKey

// EntityRef is the stable handle of an actor whose protocol is P. It
// survives restarts: a message sent to it goes to whatever incarnation is up
// when it arrives, and is dropped if none is. The zero EntityRef, or one of
// another runtime, is a protocol failure when used. Only runtimes make one.
type EntityRef[P Payload] = core.EntityRef[P]

// EntityID is the identity behind an EntityRef. Read it, never make one.
type EntityID = core.EntityID

// Entity is any EntityRef, for lifecycle calls that do not need the message
// type (sim.System.Crash, Pause, ...).
type Entity = core.Entity

// ReplyTo is the handle an Ask puts into its request. It names one ask of
// one asker incarnation and dies with it. It is comparable.
type ReplyTo[R Payload] = core.ReplyTo[R]

// ReplyID is the identity behind a ReplyTo. It is comparable, so products
// may use it as a map key.
type ReplyID = core.ReplyID

// Asked is anything that names one ask: a ReplyTo of any answer type.
type Asked = core.Asked

// Outbox records the effects of one Receive, in order. The runtime carries
// them out after Receive returns, in that order. An Outbox is lent for
// one Receive only: using it later is a protocol failure.
//
// An Outbox never performs anything itself; it only appends to a list.
type Outbox[In, IO Payload] = core.Outbox[In, IO]

// EffectKind says what an Effect does. The numbers are the frozen trace-hash
// tags of the effects.
type EffectKind = core.EffectKind

// Effect is one recorded effect, with its message types erased. Tests and
// simulator hooks read it.
type Effect = core.Effect

// The effects an Outbox records.
const (
	EffectTellSelf      = core.EffectTellSelf
	EffectAfter         = core.EffectAfter
	EffectStartTimer    = core.EffectStartTimer
	EffectCancelTimer   = core.EffectCancelTimer
	EffectDo            = core.EffectDo
	EffectStop          = core.EffectStop
	EffectFact          = core.EffectFact
	EffectForgetPending = core.EffectForgetPending
	EffectTell          = core.EffectTell
	EffectAsk           = core.EffectAsk
	EffectReply         = core.EffectReply
	EffectPerform       = core.EffectPerform
	EffectWatch         = core.EffectWatch
)
