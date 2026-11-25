// Package sim runs actors in a deterministic simulation: one seed, one
// random stream, one event queue, virtual time. It carries out the effects
// actors record on their Outbox (package actor), simulates outside systems
// as Services and product IO with Executors, and folds every step into a
// trace hash, so one seed always gives one hash.
//
// Test code only: production runs the same actors on dst/runtime/live.
//
// Glossary:
//
//	System       one run: the queue, the actors, the services, phases, rules.
//	Spawn        adds an actor; options say what only a simulation needs
//	             (WithExecutor, Workload, OnPhase, Cost, Attrition,
//	             AfterReceive, OnEnd).
//	Executor     runs product IO for an actor; answers a Perform with Answer
//	             or Forward, sends unsolicited results with Tell.
//	Service      a simulated outside system; Handle(ctx, req, reply) answers
//	             through its Responder after a delay.
//	Tell, Ask    sends from host code (a scenario step, a simulated client).
//	AskDelayed   Ask with delay-before and optional reply backhaul delay.
//	Bootstrap    NewSystem+Start with Env and Rand for host-driven setups.
//	Phase        faults, heal, drain: when faults may fire.
//	Lane         a FIFO group of the queue: messages from one sender to one
//	             actor at one time keep their order; others are seeded.
//	Trace hash   every Receive, effect, queue entry and state hash, folded;
//	             golden tests pin it.
package sim
