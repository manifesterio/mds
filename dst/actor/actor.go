package actor

import "github.com/manifesterio/mds/dst/internal/core"

// Mono is node-local monotonic time in nanoseconds. In a simulation it
// includes the fixed clock skew of the node.
type Mono int64

// Wall is UTC in nanoseconds since the Unix epoch.
type Wall int64

// Duration is a length of time in nanoseconds.
type Duration = core.Duration

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
type ID = core.ID

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
type End = core.End

// The ways an incarnation can end.
const (
	Crash = core.Crash // killed from outside (nemesis, attrition, stop deadline, panic)
	Trip  = core.Trip  // the actor asked for a restart: its memory is not trusted
	Exit  = core.Exit  // the actor finished a graceful stop
)

// RestartPolicy returns how long after an End the next boot starts, or
// ok=false for no restart. It draws only from r.
type RestartPolicy func(how End, r *Rand) (after Duration, ok bool)

// Env is the only way randomness, buggify and assertions reach an actor.
// Time is not here: it comes with each Receive. Env is a small value: pass
// it by value. Runtimes build it with NewEnv.
type Env struct {
	*Rand
	Faults
	Assertions
	self Incarnation
}

// NewEnv makes the Env of incarnation self over the run's random stream, fault
// sites and assertions. The runtime calls it; products only receive an Env.
func NewEnv(self Incarnation, r *Rand, f Faults, a Assertions) Env {
	return Env{Rand: r, Faults: f, Assertions: a, self: self}
}

// Self is the incarnation this Env was made for.
func (e Env) Self() Incarnation { return e.self }

// Faults is buggify: faults the code injects on purpose at declared sites.
type Faults interface {
	// Maybe switches the site on for the run on first use (probability 0.5),
	// then fires with probability 0.05 when it is on.
	Maybe(site Site) bool
	// MaybeP is Maybe with fire probability p.
	MaybeP(site Site, p float64) bool
	// Fault fires with probability p. It never fires outside the fault phase.
	Fault(site Site, p float64) bool
}

// Assertions record properties. A failed safety property is reported and the
// run keeps going.
type Assertions interface {
	Always(cond bool, p Always, details ...Pair)
	AlwaysOrUnreachable(cond bool, p AlwaysOrUnreachable, details ...Pair)
	Sometimes(cond bool, p Sometimes)
	Reachable(p Reachable)
	Unreachable(p Unreachable, details ...Pair)
}
