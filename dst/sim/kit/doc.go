// Package kit is simulation policy on top of the dst/sim kernel: what a
// product simulator usually wants but the kernel does not need. It uses only
// the exported API of sim, so a product can write its own policy the same way.
//
// Contents:
//
//	Phase, Phases     the fault regime of a run: faults, heal, drain.
//	SchedulePhases    queues the phase changes and sets the end of the run.
//	SetPhase          changes the phase now: a sim_phase fact, buggify on or
//	                  off, then the caller's hooks.
//	PhaseStart        when a phase began, read from the timeline.
//	Attrition         kills and stops actors at random during PhaseFaults,
//	                  within a budget (sim.System.OnStep, Crash and Stop).
//	FaultCoverage     Sometimes handles for a hard crash and a graceful stop,
//	                  checked by FaultCoverageRule.
//	CostParams        a CPU model for sim.Cost; FastWithRareSlow, DrawBetween
//	                  and MaybeAdd are its draws.
//	DeriveRand        an independent stream for host setup before a System
//	                  exists.
//	RecordFault       the sim_fault fact of an injected host-side fault.
//
// Kit must not import dst/live, dst/campaign, dst/spectest or dst/examples.
package kit
