//go:build ignore

// DESIGN SKETCH, NOT COMPILED. The proposed dst API, revised after sketching the
// real machines (controlplane replica + rebalance + gRPC server, broker + its sim
// clients). Every addition below names the machine that needs it.
package actor

// ===================== package actor (dst/core/actor) =====================
// What product code imports. Safe in production.

type Payload interface{ Fold(h *trace.Hash) } // every message; dstgen writes Fold
type NoIO interface{ Payload; noIO() }         // IO type of an actor with no product IO

type Actor[In, IO Payload] interface {
	Receive(now Now, msg In, out *Out[In, IO])
	StateHash() uint64
}

type Props[In, IO Payload] struct {
	Name     string                      // identity label (logs, trace), not wiring
	New      func(env Env) Actor[In, IO] // called on every boot; nothing survives a crash
	Boot     In                          // optional first message of every boot
	StopMsg  func(deadline Mono) In      // optional graceful-stop request
	Priority func(In) Priority           // optional: Control before Data
	Restart  RestartPolicy               // optional supervision: when to boot again
}

// ---- handles: plain typed values, foldable, safe in messages and state ----

type EntityRef[P Payload] struct{ /* node id */ }                  // stable; survives restarts
type ReplyTo[R Payload] struct{ /* incarnation id + ask number */ } // dies with the asker
type ServiceRef[Req, Rep Payload] struct{ /* service id */ }

// TimerKey names a timer of one actor. Keys are plain values the product builds
// from its own data, e.g. TimerKey("session/" + group + "/" + member), so an actor
// with one timer per member needs no TimerID counter and no owners map (rebalance).
type TimerKey string

// ---- Out: the effects of one Receive, carried out by the runtime afterwards ----

type Out[In, IO Payload] struct{ /* recorded effects */ }

func (o *Out[In, IO]) TellSelf(msg In)                             // next in my inbox (rebalance drainLater)
func (o *Out[In, IO]) After(d Duration, msg In)                    // one-shot, nothing to cancel
func (o *Out[In, IO]) StartTimer(key TimerKey, d Duration, msg In) // replaces a timer with the same key
func (o *Out[In, IO]) CancelTimer(key TimerKey)                    // also drops it if it fired but is not yet received
func (o *Out[In, IO]) Do(io IO)                                    // IO whose results arrive as my own messages (broker sockets)
func (o *Out[In, IO]) Stop(how End, cause string)                  // end this incarnation; runtime drops everything it owned
func (o *Out[In, IO]) Fact(f Fact)                                 // for oracles; never changes behavior

// ForgetPending drops every timer, ask, call and perform answer this actor is
// still waiting for, without ending the incarnation. It is the in-machine
// "start over": controlplane replica's injected soft crash, and rebalance losing
// its term (a broken write). Answers that arrive later are dropped by the runtime.
func (o *Out[In, IO]) ForgetPending()

// ---- sends: package functions, because Go methods cannot add type parameters ----

func Tell[In, IO, P Payload](out *Out[In, IO], to EntityRef[P], msg P)
func TellService[In, IO, Req, Rep Payload](out *Out[In, IO], to ServiceRef[Req, Rep], req Req)
func Ask[In, IO, Req, Rep Payload](out *Out[In, IO], to EntityRef[Req],
	makeReq func(ReplyTo[Rep]) Req, adapt func(Rep) In)
func Call[In, IO, Req, Rep Payload](out *Out[In, IO], to ServiceRef[Req, Rep], req Req, adapt func(Rep) In)
func Reply[In, IO, R Payload](out *Out[In, IO], to ReplyTo[R], answer R)

// Perform is product IO with a typed answer: the runtime matches the answer to
// the request, so the actor keeps no request-ID table (replica claimReq,
// rebalance reqs map[RequestID]any). Do stays for IO whose results are
// unsolicited events (bytes received on a socket).
func Perform[In, IO, Req, Rep Payload](out *Out[In, IO], req Req, adapt func(Rep) In) // Req must be a case of IO

// Watch tells me when an asker is gone: its incarnation ended, or (outside
// callers) its context was cancelled. Rebalance needs it for parked JoinGroup
// calls: today the gRPC layer sends CallAbandoned by hand. Akka: DeathWatch.
func Watch[In, IO, R Payload](out *Out[In, IO], who ReplyTo[R], gone In)

// Sub lends a component its own typed Out: what the component schedules for
// itself (timers, answers) comes back wrapped into my message type. It lets one
// actor host reusable parts with their own protocols: broker sim's session loop
// (dial, back off, reconnect), and the flow helper.
func Sub[In, IO, C Payload](out *Out[In, IO], wrap func(C) In) *Out[C, IO]

type Env interface {
	Rand       // Uint64, Intn, Between, Chance, Duration(lo, hi), NewID
	Faults     // Maybe, MaybeP, Fault
	Assertions // Always, Sometimes, Reachable, Unreachable, ...
}
type Now struct{ Mono Mono; Wall Wall }

// ===================== package sim (dst/sim) =====================
// Test only.

type System struct{ /* scheduler, topology, lifecycle, faults, trace */ }

func NewSystem(cfg Config) *System
func (s *System) Run() Result

// Spawn adds an actor. Product IO goes to the executor; an actor with NoIO passes none.
func Spawn[In, IO Payload](s *System, p actor.Props[In, IO], opts ...SpawnOption[In, IO]) actor.EntityRef[In]

// SpawnOption: WithExecutor(Executor[IO]), Workload() (was Driver bool),
// OnPhase(func(Phase) (In, bool)), Cost(CostParams), Attrition(), AfterReceive(hook).
type SpawnOption[In, IO Payload] interface{ apply(*spawn[In, IO]) }

// Executor simulates product IO: usually by calling a simulated service, whose
// reply becomes the Perform answer. One executor per IO sum, reused by every actor.
type Executor[IO Payload] func(ctx *ExecCtx, io IO)

type Service[Req, Rep actor.Payload] interface { // a simulated outside system
	Handle(ctx *ServiceCtx, req Req, reply Responder[Rep])
	StateHash() uint64
} // optional extras: OnCrash, EnterRecovery

func AddService[Req, Rep actor.Payload](s *System, name string, svc Service[Req, Rep]) actor.ServiceRef[Req, Rep]

type Responder[R actor.Payload] struct{ /* the caller's ReplyTo + adapter */ }

func (r Responder[R]) After(d actor.Duration, answer R) // dropped if the caller is gone

// Ask from the test itself (a scenario step, not an actor), e.g. to drive gRPC-like calls.
func Ask[Req, Rep actor.Payload](s *System, to actor.EntityRef[Req], makeReq func(actor.ReplyTo[Rep]) Req,
	within actor.Duration) (Rep, bool)

// ===================== package live (dst/runtime/live) =====================
// Production. One Host per process; it may hold several actors (controlplane
// runs the replica and rebalance in one process and they talk to each other).

type Host struct{ /* one loop goroutine per actor, inboxes, real timers */ }

func NewHost(cfg HostConfig) *Host // clock, env, shutdown
func Spawn[In, IO actor.Payload](h *Host, p actor.Props[In, IO], exec func(ctx *ExecCtx, io IO) error) actor.EntityRef[In]
func (h *Host) Run(ctx context.Context) error

// Ask from outside the actors: the gRPC handler. It blocks until the answer, the
// context ends (then the actor's Watch fires) or the host stops. This replaces
// controlplane's hand-written call()/answer()/calls map/CallAbandoned.
func Ask[Req, Rep actor.Payload](ctx context.Context, h *Host, to actor.EntityRef[Req],
	makeReq func(actor.ReplyTo[Rep]) Req) (Rep, error)

// AfterReceive hooks stay for host metrics (controlplane health: epoch, role).
