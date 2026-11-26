package actor

import (
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
)

// Mono is node-local monotonic time in nanoseconds. In a simulation it
// includes the fixed clock skew of the node.
type Mono int64

// Wall is UTC in nanoseconds since the Unix epoch.
type Wall int64

// Duration is a length of time in nanoseconds.
type Duration int64

// Units of Duration.
const (
	Nanosecond  Duration = 1
	Microsecond          = 1000 * Nanosecond
	Millisecond          = 1000 * Microsecond
	Second               = 1000 * Millisecond
)

// Now is the time an actor sees for one Receive.
type Now struct {
	Mono Mono // use for deadlines and leases
	Wall Wall // UTC
}

// ID is a 16-byte id drawn from the seeded random stream (Rand.NewID).
type ID [16]byte

// Incarnation names one boot of one actor: its Props.Name and a boot number
// that is new for every boot. It is an identity label for logs, facts and
// storage keys; it is never used to send.
type Incarnation struct {
	Name string
	Boot ID
}

// Actor is one state machine with one input protocol In and one product IO
// sum IO. Receive is the only place its state changes; it never blocks and
// never does IO. Everything it wants done is recorded on outbox and carried
// out by the runtime after Receive returns.
//
// StateHash is a hash of the whole state; the simulator folds it into the
// trace hash after every Receive.
type Actor[In, IO Payload] interface {
	Receive(now Now, msg In, outbox *Outbox[In, IO])
	StateHash() uint64
}

// Props says how to run one actor. It is the same value in sim and live.
type Props[In, IO Payload] struct {
	// Name is the identity label of the actor (logs, trace, facts), unique
	// in one runtime. It is not used for wiring.
	Name string
	// New builds the actor on every boot; nothing survives a crash.
	// Required.
	New func(env Env) Actor[In, IO]
	// Boot, when set, makes the first message of every boot.
	Boot func(self Incarnation) In
	// StopMsg, when set, makes the graceful-stop request, delivered with
	// Control priority. Nil: a graceful stop is not allowed.
	StopMsg func(deadline Mono) In
	// Priority picks the inbox of a message. Control is received before
	// Data. Nil: everything is Data.
	Priority func(msg In) Priority
	// Restart decides when the next incarnation boots after one ended. Nil,
	// or ok=false: the actor stays down. The live runtime never restarts in
	// process.
	Restart RestartPolicy
}

// Priority picks one of the two inboxes of an actor.
type Priority uint8

// The two priorities.
const (
	Control Priority = iota
	Data
)

// End says how an incarnation ended.
type End uint8

// The ways an incarnation can end.
const (
	Crash End = iota // killed from outside (nemesis, attrition, stop deadline, panic)
	Trip             // the actor asked for a restart: its memory is not trusted
	Exit             // the actor finished a graceful stop
)

// String returns the plain name of the end.
func (e End) String() string {
	switch e {
	case Crash:
		return "crash"
	case Trip:
		return "trip"
	case Exit:
		return "exit"
	}
	return "unknown"
}

// RestartPolicy returns how long after an End the next boot starts, or
// ok=false for no restart. It draws only from r.
type RestartPolicy func(how End, r Rand) (after Duration, ok bool)

// Env is the only way randomness, buggify and assertions reach an actor.
// Time is not here: it comes with each Receive.
type Env interface {
	Rand
	Faults
	Assertions
	// Self is the incarnation this Env was made for.
	Self() Incarnation
}

// Rand is the seeded random stream of the run, as an actor sees it.
type Rand interface {
	Uint64() uint64
	// Intn returns a value in [0, n). It returns 0, and draws nothing, when n <= 0.
	Intn(n int) int
	// Between returns a value in [lo, hi], both ends included. It returns lo,
	// and draws nothing, when hi <= lo.
	Between(lo, hi int64) int64
	// Float64 returns a value in [0, 1).
	Float64() float64
	// Chance is true with probability p. It draws nothing when p <= 0.
	Chance(p float64) bool
	// Duration is Between for durations.
	Duration(lo, hi Duration) Duration
	// NewID draws two Uint64 values and puts them in the id, little-endian.
	NewID() ID
}

// Faults is buggify: faults the code injects on purpose at declared sites.
type Faults interface {
	// Maybe switches the site on for the run on first use (probability 0.5),
	// then fires with probability 0.05 when it is on.
	Maybe(site fault.Site) bool
	// MaybeP is Maybe with fire probability p.
	MaybeP(site fault.Site, p float64) bool
	// Fault fires with probability p. It never fires outside the fault phase.
	Fault(site fault.Site, p float64) bool
}

// Assertions record properties. A failed safety property is reported and the
// run keeps going.
type Assertions interface {
	Always(cond bool, p assert.Always, details ...assert.Pair)
	AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, details ...assert.Pair)
	Sometimes(cond bool, p assert.Sometimes)
	Reachable(p assert.Reachable)
	Unreachable(p assert.Unreachable, details ...assert.Pair)
}
