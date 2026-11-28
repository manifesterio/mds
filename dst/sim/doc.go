// Package sim runs actors in a deterministic simulation: one seed, one
// random stream, one event queue, virtual time. It carries out the effects
// actors record on their Outbox (package actor), simulates product IO with
// Executors (outside systems are simulated behind them), and folds every
// step into a trace hash, so one seed always gives one hash.
//
// sim is mechanism only. Policy (phases, attrition, CPU cost models, fault
// coverage) lives in dst/sim/kit, built on the exported API here.
//
// Test code only: production runs the same actors on dst/live.
//
// Glossary:
//
//	System       one run: the queue, the actors, rules.
//	Spawn        adds an actor; options say what only a simulation needs
//	             (WithExecutor, Workload, Cost, AfterReceive, OnEnd).
//	Executor     runs product IO for an actor; answers a Perform with Answer
//	             or holds its Reply (Hold) to answer later; sends unsolicited
//	             results with Tell.
//	Track        folds outside state (a fake behind an executor) into the
//	             trace hash; Touch marks it changed.
//	Tell, Ask    sends from host code (a scenario step, a simulated client).
//	OnStep       a hook after every queue pop: how policy such as attrition
//	             acts on the run.
//	Faults       the buggify switch (SetFaults, FaultsOn); the sim_faults fact
//	             records each change.
//	Stop, Crash  end an actor gracefully (with a deadline) or at once. A
//	             workload may do neither.
//	Lane         a FIFO group of the queue: messages from one sender to one
//	             actor at one time keep their order; others are seeded.
//	Trace hash   every Receive, effect, queue entry and state hash, folded;
//	             golden tests pin it.
package sim
