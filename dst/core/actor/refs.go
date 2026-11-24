package actor

import "github.com/manifesterio/mds/dst/core/trace"

// Payload is every message, request, answer and IO value. Fold writes the
// value into the trace hash: its frozen tag first, then its fields. dstgen
// writes Fold from //dst:cases and //dst:payload.
type Payload interface{ Fold(h *trace.Hash) }

// NoIO is the IO type of an actor that does no product IO. It has no cases,
// so nothing can be passed to Do or Perform on an Outbox[In, NoIO].
type NoIO interface {
	Payload
	noIO()
}

// Request ties one case of an IO sum to its answer type, so Perform and the
// executors' Answer are checked at compile time. dstgen writes AsRequest
// from //dst:answer; it returns the value itself as an IO and the zero
// answer. Product code never calls it.
type Request[IO, Rep Payload] interface {
	Payload
	AsRequest() (IO, Rep)
}

// Trace-hash tags of refs (ADR section 5). They are frozen.
const (
	tagEntityRef  = 2101
	tagReplyTo    = 2102
	tagServiceRef = 2103
)

// Owner marks the runtime that made a handle, so a handle of another System
// or Host is caught. Only runtimes make one (NewOwner); it is never folded.
type Owner struct{ _ byte }

// NewOwner makes the mark of one runtime instance.
func NewOwner() *Owner { return &Owner{} }

// EntityID is the identity behind an EntityRef: the runtime that made it,
// the actor's slot in that runtime, and its name. Only runtimes make one.
type EntityID struct {
	Runtime *Owner // nil only in the zero EntityID
	Slot    uint32
	Name    string
}

// Entity is any EntityRef, for lifecycle calls that do not need the message
// type (sim.System.Crash, Pause, ...).
type Entity interface{ EntityID() EntityID }

// EntityRef is the stable handle of an actor whose protocol is P. It
// survives restarts: a message sent to it goes to whatever incarnation is up
// when it arrives, and is dropped if none is. The zero EntityRef, or one of
// another runtime, is a protocol failure when used.
type EntityRef[P Payload] struct{ id EntityID }

// EntityID returns the identity of the actor.
func (r EntityRef[P]) EntityID() EntityID { return r.id }

// Name returns the actor's Props.Name.
func (r EntityRef[P]) Name() string { return r.id.Name }

// IsZero reports whether r is the zero EntityRef.
func (r EntityRef[P]) IsZero() bool { return r.id.Runtime == nil }

// Fold writes tag 2101 and the actor's name.
func (r EntityRef[P]) Fold(h *trace.Hash) {
	h.Uint64(tagEntityRef)
	h.String(r.id.Name)
}

// AskerKind says what kind of asker a ReplyTo belongs to.
type AskerKind uint8

// The kinds of askers. The numbers are folded into the trace hash.
const (
	NoAsker          AskerKind = iota // the zero ReplyTo
	ActorAsker                        // an actor incarnation (Ask)
	SimOutsideAsker                   // host code of a simulation (sim.Ask)
	LiveOutsideAsker                  // a caller outside the actors (live.Ask)
)

// Asker names who asked: one actor incarnation, or one outside caller.
type Asker struct {
	Kind    AskerKind
	Runtime *Owner
	Slot    uint32
	Name    string
	Boot    ID
}

// ReplyID is the identity behind a ReplyTo: the asker and the ask number.
// It is comparable, so runtimes and products may use it as a map key.
type ReplyID struct {
	Asker  Asker
	Number uint64
}

// ReplyTo is the handle an Ask puts into its request. It names one ask of
// one asker incarnation and dies with it (G4). Only runtimes make one. It is
// comparable.
type ReplyTo[R Payload] struct{ id ReplyID }

// ID returns the identity of the ask.
func (r ReplyTo[R]) ID() ReplyID { return r.id }

// IsZero reports whether r is the zero ReplyTo.
func (r ReplyTo[R]) IsZero() bool { return r.id.Asker.Kind == NoAsker }

// Fold writes tag 2102, the asker kind, name and boot, and the ask number.
func (r ReplyTo[R]) Fold(h *trace.Hash) { r.id.Fold(h) }

// Fold writes tag 2102, the asker kind, name and boot, and the ask number.
func (r ReplyID) Fold(h *trace.Hash) {
	h.Uint64(tagReplyTo)
	h.Uint64(uint64(r.Asker.Kind))
	h.String(r.Asker.Name)
	h.Bytes(r.Asker.Boot[:])
	h.Uint64(r.Number)
}

// ServiceID is the identity behind a ServiceRef. Only runtimes make one.
type ServiceID struct {
	Runtime *Owner
	Slot    uint32
	Name    string
}

// ServiceRef is the handle of a simulated outside service that takes Req
// and answers Rep. The type lives here so that actors can hold it.
type ServiceRef[Req, Rep Payload] struct{ id ServiceID }

// ID returns the identity of the service.
func (r ServiceRef[Req, Rep]) ID() ServiceID { return r.id }

// Name returns the service's name.
func (r ServiceRef[Req, Rep]) Name() string { return r.id.Name }

// IsZero reports whether r is the zero ServiceRef.
func (r ServiceRef[Req, Rep]) IsZero() bool { return r.id.Runtime == nil }

// Fold writes tag 2103 and the service's name.
func (r ServiceRef[Req, Rep]) Fold(h *trace.Hash) {
	h.Uint64(tagServiceRef)
	h.String(r.id.Name)
}

// TimerKey names a timer of one actor. Products build keys from their own
// data, e.g. TimerKey("session/" + group + "/" + member).
type TimerKey string
