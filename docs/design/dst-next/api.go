//go:build ignore

// DESIGN SKETCH, NOT COMPILED. The frozen dst API (P1, ADR.md). Three packages
// are shown in one file: actor (dst/core/actor), sim (dst/sim) and live
// (dst/runtime/live). Bodies are omitted; doc comments are the contract.
// Section numbers (G1..G10, O1..O5) refer to ADR.md.
package actor

// ===================== package actor (dst/core/actor) =====================
// What product code imports. Deterministic, no goroutines, safe in production.

// ---- time and identity ----

// Mono is node-local monotonic time in nanoseconds. In a simulation it
// includes the fixed clock skew of the node. Was machine.NodeTime.
type Mono int64

// Wall is UTC in nanoseconds since the Unix epoch. Was machine.RealTime.
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
// that is new for every boot. Was machine.NodeInstance. It is an identity
// label for logs, facts and storage keys; it is never used to send.
type Incarnation struct {
	Name string
	Boot ID
}

// ---- messages ----

// Payload is every message, request, answer and IO value. Fold writes the
// value into the trace hash: its frozen tag first, then its fields. dstgen
// writes Fold (ADR section 6); hand-written Fold stays legal with
// //dst:fold manual.
type Payload interface{ Fold(h *trace.Hash) }

// NoIO is the IO type of an actor that does no product IO. It has no cases,
// so Do and Perform cannot be called on an Outbox[In, NoIO].
type NoIO interface {
	Payload
	noIO()
}

// Request ties one case of an IO sum to its answer type, so Perform and the
// executors' Answer are checked at compile time (O1). dstgen writes the
// method from a //dst:answer annotation; product code never calls it.
// AsRequest returns the value itself as an IO and the zero answer.
type Request[IO, Rep Payload] interface {
	Payload
	AsRequest() (IO, Rep)
}

// ---- the actor ----

// Actor is one state machine with one input protocol In and one product IO
// sum IO. Receive is the only place its state changes; it never blocks and
// never does IO. Everything it wants done is recorded on outbox and carried
// out by the runtime after Receive returns (G1).
//
// StateHash is a hash of the whole state. The sim runtime folds it into the
// trace hash after every Receive.
type Actor[In, IO Payload] interface {
	Receive(now Now, msg In, outbox *Outbox[In, IO])
	StateHash() uint64
}

// Props says how to run one actor. It is the same value in sim and live.
type Props[In, IO Payload] struct {
	// Name is the identity label of the actor (logs, trace, facts). Unique in
	// one System or Host. Not used for wiring.
	Name string
	// New builds the actor. It is called on every boot; nothing survives a
	// crash, so it never returns an old actor. Required.
	New func(env Env) Actor[In, IO]
	// Boot, when set, is the first message of every boot. It is received
	// before anything else of that incarnation.
	Boot func(self Incarnation) In
	// StopMsg, when set, makes the graceful-stop request. It is delivered
	// with Control priority. Nil: a graceful stop is not allowed (sim:
	// protocol failure; live: the actor is killed at once).
	StopMsg func(deadline Mono) In
	// Priority picks the inbox of a message. Control is always received
	// before Data. Nil: everything is Data.
	Priority func(msg In) Priority
	// Restart decides when the next incarnation boots after one ended. Nil,
	// or ok=false: the actor stays down until a host boots it (sim). Live
	// does not restart in process (ADR section 3.7).
	Restart RestartPolicy
}

// Priority picks one of the two inboxes of an actor. Was machine.Class.
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
	Intn(n int) int             // [0, n); 0 and no draw when n <= 0
	Between(lo, hi int64) int64 // [lo, hi]; lo and no draw when hi <= lo
	Chance(p float64) bool      // no draw when p <= 0
	Duration(lo, hi Duration) Duration
	NewID() ID
}

// Faults is buggify (unchanged from machine.Faults).
type Faults interface {
	Maybe(site fault.Site) bool
	MaybeP(site fault.Site, p float64) bool
	Fault(site fault.Site, p float64) bool
}

// Assertions record properties (unchanged from machine.Assertions).
type Assertions interface {
	Always(cond bool, p assert.Always, details ...assert.Pair)
	AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, details ...assert.Pair)
	Sometimes(cond bool, p assert.Sometimes)
	Reachable(p assert.Reachable)
	Unreachable(p assert.Unreachable, details ...assert.Pair)
}

// Fact, Field and the FieldValue cases (Int64Value, Uint64Value, StringValue,
// Bool, Bytes) move unchanged from package machine, with their frozen tags
// 401..405 and 501.
type Fact struct {
	Name   string
	Fields []Field
}

// ---- handles: plain typed values, foldable, safe in messages and state ----

// EntityID is the opaque identity behind an EntityRef: which runtime made
// it, which slot, and the actor's Name. Products never build one.
type EntityID struct{ /* runtime id, slot, name */ }

// Entity is any EntityRef, for lifecycle calls that do not need the message
// type (sim.System.Crash, Pause, ...).
type Entity interface{ EntityID() EntityID }

// EntityRef[P] is the stable handle of an actor whose protocol is P. It
// survives restarts: a message sent to it goes to whatever incarnation is up
// when it arrives, and is dropped if none is. Only Spawn makes one. The zero
// EntityRef, or one of another runtime, is a protocol failure when used.
type EntityRef[P Payload] struct{ id EntityID }

func (r EntityRef[P]) EntityID() EntityID
func (r EntityRef[P]) Name() string
func (r EntityRef[P]) IsZero() bool
func (r EntityRef[P]) Fold(h *trace.Hash) // ADR section 5: tag 1101, name

// ReplyTo[R] is the handle an Ask puts into its request. It names one ask of
// one asker incarnation (an actor, or an outside caller of sim.Ask or
// live.Ask). It dies with that incarnation: Reply to a dead ReplyTo is
// dropped (G4). Only the runtime makes one. Comparable, so products may use
// it as a map key.
type ReplyTo[R Payload] struct{ /* asker kind, slot, boot, ask number */ }

func (r ReplyTo[R]) IsZero() bool
func (r ReplyTo[R]) Fold(h *trace.Hash) // tag 1102, asker name, boot, ask number

// ServiceRef[Req, Rep] is the handle of a simulated outside service (sim
// only, but the type lives here so actors can hold it).
type ServiceRef[Req, Rep Payload] struct{ /* runtime id, slot, name */ }

func (r ServiceRef[Req, Rep]) Name() string
func (r ServiceRef[Req, Rep]) Fold(h *trace.Hash) // tag 1103, name

// TimerKey names a timer of one actor. Products build keys from their own
// data, e.g. TimerKey("session/" + group + "/" + member), so an actor needs
// no timer-id counter and no owners map.
type TimerKey string

// ---- Outbox: the effects of one Receive ----

// Outbox records the effects of one Receive, in order. The runtime carries
// them out after Receive returns, in that order (G1). An Outbox is lent for
// one Receive only: using it later is a protocol failure.
type Outbox[In, IO Payload] struct{ /* recorded effects */ }

// Self is my own stable handle, to give to others.
func (o *Outbox[In, IO]) Self() EntityRef[In]

// TellSelf puts msg in my inbox now, behind messages already there. Same as
// After(0, msg). It dies with this incarnation.
func (o *Outbox[In, IO]) TellSelf(msg In)

// After delivers msg to this incarnation after d of node time. One-shot,
// nothing to cancel; dropped by Stop and ForgetPending. d < 0 counts as 0.
func (o *Outbox[In, IO]) After(d Duration, msg In)

// StartTimer delivers msg after d, under key. A timer with the same key is
// replaced, including one that fired but is not yet received (G3).
func (o *Outbox[In, IO]) StartTimer(key TimerKey, d Duration, msg In)

// CancelTimer drops the timer key, including one that fired but is not yet
// received (G3). An unknown key is a no-op.
func (o *Outbox[In, IO]) CancelTimer(key TimerKey)

// Do hands io to the executor. Its results, if any, come back as my own
// messages that the executor sends (broker sockets: BytesReceived). Use
// Perform when the IO has one typed answer.
func (o *Outbox[In, IO]) Do(io IO)

// Stop ends this incarnation: how is Trip or Exit. Everything it owned is
// dropped (G7). It must be the last effect of the Receive.
func (o *Outbox[In, IO]) Stop(how End, cause string)

// Fact appends f to the timeline (sim) or the trace log (live). It never
// changes behaviour.
func (o *Outbox[In, IO]) Fact(f Fact)

// ForgetPending drops every timer and every Ask, Call and Perform answer
// this actor is still waiting for, without ending the incarnation (G5).
// Answers that arrive later are dropped. Watches stay. Through a Sub, it
// forgets only that component's pending work (O2).
func (o *Outbox[In, IO]) ForgetPending()

// ---- sends: package functions, because Go methods cannot add type parameters ----

// Tell sends msg to an actor. No answer is expected.
func Tell[In, IO, P Payload](o *Outbox[In, IO], to EntityRef[P], msg P)

// TellService sends req to a service. Any answer the service gives is dropped.
func TellService[In, IO, Req, Rep Payload](o *Outbox[In, IO], to ServiceRef[Req, Rep], req Req)

// Ask sends an actor a request that carries a fresh ReplyTo. makeReq puts it
// in the request. The answer comes back as one of my messages, built by
// adapt, so I keep no table of pending asks. There is no timeout: use After.
func Ask[In, IO, Req, Rep Payload](o *Outbox[In, IO], to EntityRef[Req],
	makeReq func(ReplyTo[Rep]) Req, adapt func(Rep) In)

// Call is Ask for a service: the service gets its reply handle from Handle,
// so the request carries no ReplyTo.
func Call[In, IO, Req, Rep Payload](o *Outbox[In, IO], to ServiceRef[Req, Rep], req Req, adapt func(Rep) In)

// Reply answers an Ask I received. A second Reply to the same ReplyTo, or a
// Reply to an asker that is gone, is dropped (G4). It removes any Watch on
// that ReplyTo (G6).
func Reply[In, IO, R Payload](o *Outbox[In, IO], to ReplyTo[R], answer R)

// Perform hands req to the executor and turns its one answer into my
// message with adapt. Req must be a case of IO that declares its answer with
// //dst:answer (O1); a wrong adapt type does not compile. The answer reaches
// only this incarnation and only if not forgotten (G10).
func Perform[In, IO, Rep Payload, Req Request[IO, Rep]](o *Outbox[In, IO], req Req, adapt func(Rep) In)

// Watch delivers gone to me once if the asker behind who ends before I
// Reply to it: its incarnation ended (sim, live), its sim.Ask deadline
// passed, or its live.Ask context ended (G6). Akka: DeathWatch.
func Watch[In, IO, R Payload](o *Outbox[In, IO], who ReplyTo[R], gone In)

// Sub lends a component its own Outbox with protocol C. What the component
// schedules for itself (timers, answers) comes back wrapped by wrap into my
// message type. name scopes its timer keys and its ForgetPending (O2). Sub
// is called inside one Receive and dies with it, like the Outbox.
func Sub[In, IO, C Payload](o *Outbox[In, IO], name string, wrap func(C) In) *Outbox[C, IO]

// ===================== package sim (dst/sim) =====================
// Test only. Replaces kernel.World. The scheduler is sim/sched.

// System is one simulated run: the event queue, actors, services, phases,
// faults, timeline and trace hash.
type System struct{ /* ... */ }

// Config is kernel.Config unchanged: Seed, WallStart, End, MaxSteps,
// ClockSkewMax, Attrition, Log, Tail, HaltOnFirst, VerifyHashes.
type Config struct{ /* ... */ }

func NewSystem(cfg Config) *System

// Running. Start boots every actor (workloads last); RunUntil steps until
// stop is true, the queue drains, End passes or the run halts; Finish
// returns the Result. Run is Start, RunUntil(nil), Finish.
func (s *System) Start()
func (s *System) RunUntil(stop func() bool) Stopped
func (s *System) Finish() Result
func (s *System) Run() Result

// Lifecycle, from host code (nemesis, scenario). Same meaning as kernel.World.
func (s *System) Up(e actor.Entity) bool
func (s *System) Current(e actor.Entity) (actor.Incarnation, bool)
func (s *System) Down(e actor.Entity) (actor.End, bool)
func (s *System) Boot(e actor.Entity)
func (s *System) Crash(e actor.Entity, cause string)
func (s *System) Stop(e actor.Entity, grace actor.Duration)
func (s *System) Pause(e actor.Entity)
func (s *System) Resume(e actor.Entity)

// Host tasks, phases, rules, facts, env: carried over unchanged from
// kernel.World (At, After, Cancel, SchedulePhases, SetPhase, Phase, AddRule,
// Fail, Fact, Note, Timeline, Env, Rand, Faults, Assert, Now, Wall, Steps).

// Spawn adds an actor and returns its stable handle. Start (or Boot) boots it.
func Spawn[In, IO actor.Payload](s *System, p actor.Props[In, IO], opts ...SpawnOption[In, IO]) actor.EntityRef[In]

// SpawnOption configures what only a simulation needs.
type SpawnOption[In, IO actor.Payload] interface{ apply(*spawn[In, IO]) }

func WithExecutor[In, IO actor.Payload](x Executor[IO]) SpawnOption[In, IO] // required unless IO is NoIO
func Workload[In, IO actor.Payload]() SpawnOption[In, IO]                   // was NodeSpec.Driver
func OnPhase[In, IO actor.Payload](f func(Phase) (In, bool)) SpawnOption[In, IO]
func Cost[In, IO actor.Payload](c CostParams) SpawnOption[In, IO]
func Attrition[In, IO actor.Payload]() SpawnOption[In, IO]
func AfterReceive[In, IO actor.Payload](f func(self actor.Incarnation, msg In)) SpawnOption[In, IO]

// Executor simulates product IO. One executor per IO sum, shared by every
// actor of that sum. It runs right after the Receive that recorded the IO,
// once per Do or Perform, in record order. It must not block.
type Executor[IO actor.Payload] func(ctx *ExecCtx, io IO)

// ExecCtx is lent to one executor call.
type ExecCtx struct{ /* system, issuing incarnation, pending answer */ }

func (c *ExecCtx) System() *System
func (c *ExecCtx) Issuer() actor.Incarnation

// Answer answers the Perform that issued req, after d. Typed by the same
// Request constraint as Perform. The answer to a request sent with Do is
// dropped (nobody waits); answering twice is a protocol failure.
func Answer[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, req Req, d actor.Duration, answer Rep)

// Forward hands the Perform that issued req to a service whose protocol is
// the request and answer types themselves; the service's reply becomes the
// Perform answer.
func Forward[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, req Req, to actor.ServiceRef[Req, Rep])

// Service simulates one outside system (object store, log, control plane
// over gRPC). Draw every fault and delay in Handle, in issue order.
type Service[Req, Rep actor.Payload] interface {
	Handle(ctx *ServiceCtx, req Req, reply Responder[Rep])
	StateHash() uint64
}

// Optional service extras, found by type assertion.
type CrashObserver interface{ OnCrash(ctx *ServiceCtx, who actor.Incarnation) }
type RecoveryObserver interface{ EnterRecovery() }

func AddService[Req, Rep actor.Payload](s *System, name string, svc Service[Req, Rep]) actor.ServiceRef[Req, Rep]

// ServiceCtx is lent to Handle, OnCrash and Later steps.
type ServiceCtx struct{ /* ... */ }

// ServiceCtx is an actor.Rand, actor.Faults and actor.Assertions of the run.
func (c *ServiceCtx) Now() actor.Mono
func (c *ServiceCtx) Later(d actor.Duration, fn func(ctx *ServiceCtx))
func (c *ServiceCtx) Fault(kind string, fields ...actor.Field)

// Responder answers one request. It holds the caller's ReplyTo and adapter;
// the service never sees them.
type Responder[R actor.Payload] struct{ /* ... */ }

// After answers after d. Dropped if the caller is gone, has forgotten the
// call, or did not want an answer (TellService). A second answer is a
// protocol failure.
func (r Responder[R]) After(d actor.Duration, answer R)

// Wanted reports whether anyone waits for this answer.
func (r Responder[R]) Wanted() bool

// Tell sends msg to an actor from host code or an executor, after d. It is
// how a sim executor delivers unsolicited IO results (O5).
func Tell[P actor.Payload](s *System, to actor.EntityRef[P], d actor.Duration, msg P)

// Ask asks an actor from host code (a scenario step, a simulated gRPC
// client). done runs as a host task with the answer, or with ok=false once
// within passes; then the asker counts as gone and Watches fire (G6).
func Ask[Req, Rep actor.Payload](s *System, to actor.EntityRef[Req], makeReq func(actor.ReplyTo[Rep]) Req,
	within actor.Duration, done func(answer Rep, ok bool))

// ===================== package live (dst/runtime/live) =====================
// Production. One Host per process; it may hold several actors (controlplane
// runs the replica and rebalance in one process).

// Host runs actors: one loop goroutine per actor, real timers.
type Host struct{ /* ... */ }

// HostConfig: Clock, Env per actor (a factory), Shutdown channel, StopGrace.
type HostConfig struct{ /* ... */ }

// The ways Run can end (were live.ErrStoppedForRestart, ErrStopped, ErrKilled).
var (
	ErrTripped     error // an actor ended with Trip; the text has the cause
	ErrStopped     error // the context ended, or a graceful stop finished
	ErrKilled      error // a graceful stop missed its deadline
	ErrHostStopped error // returned by Ask, Tell and OnLoop after Run ended
)

func NewHost(cfg HostConfig) *Host

// Spawn adds an actor before Run. exec runs product IO on the actor's
// goroutine; it must not block (start a goroutine and Answer or Tell later).
// An error from exec ends the Host.
func Spawn[In, IO actor.Payload](h *Host, p actor.Props[In, IO], exec func(ctx *ExecCtx, io IO) error,
	opts ...SpawnOption[In]) actor.EntityRef[In]

// AfterReceive runs on the actor's goroutine after every Receive and its
// effects (host metrics: controlplane epoch, role).
func AfterReceive[In actor.Payload](f func()) SpawnOption[In]

// Run boots every actor and blocks. When any actor's incarnation ends, the
// Host stops the others (StopMsg within StopGrace, else killed) and Run
// returns the first end's error.
func (h *Host) Run(ctx context.Context) error

// ExecCtx is lent to one exec call; Answer may be called from any goroutine.
type ExecCtx struct{ /* ... */ }

func Answer[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, req Req, answer Rep)

// Tell sends msg to an actor from outside (a socket reader goroutine, a
// hook). Safe from any goroutine.
func Tell[P actor.Payload](h *Host, to actor.EntityRef[P], msg P) error

// Ask asks an actor from outside: the gRPC handler. It blocks until the
// answer (nil error), ctx ends (ctx.Err(); the asker is gone and the actor's
// Watch fires) or the Host stops (ErrHostStopped).
func Ask[Req, Rep actor.Payload](ctx context.Context, h *Host, to actor.EntityRef[Req],
	makeReq func(actor.ReplyTo[Rep]) Req) (Rep, error)

// OnLoop runs fn on the actor's goroutine between two Receives and waits for
// it. For reads of process state that must not race with Receive
// (controlplane GetStatus, BrokerHeartbeat leases). fn must not block.
func OnLoop[P actor.Payload](ctx context.Context, h *Host, on actor.EntityRef[P], fn func()) error
