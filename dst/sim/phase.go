package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
)

// Phase is the fault regime of the run.
type Phase uint8

// The phases, in run order. Numeric values are frozen (0, 1, 2): they are folded
// into the trace hash (phaseBody.Fold and nodeHost.phaseStep), so changing them
// breaks seed determinism and golden pins.
const (
	PhaseFaults Phase = 0 // faults on
	PhaseHeal   Phase = 1 // faults off, nodes alive; liveness is checked
	PhaseDrain  Phase = 2 // draining remaining in-flight messages
)

// String returns a short name for debugging. It is not folded into the trace hash.
func (p Phase) String() string {
	switch p {
	case PhaseFaults:
		return "faults"
	case PhaseHeal:
		return "heal"
	case PhaseDrain:
		return "drain"
	default:
		return fmt.Sprintf("Phase(%d)", p)
	}
}

// Phases is the duration schedule: Faults, then Heal, then Drain.
type Phases struct {
	Faults actor.Duration
	Heal   actor.Duration
	Drain  actor.Duration
}

// phaseBody is the body of a phase change entry.
type phaseBody struct{ to Phase }

// Fold writes tag 902 and the new phase.
func (b phaseBody) Fold(h *trace.Hash) {
	h.Uint64(tagPhase)
	h.Uint64(uint64(b.to))
}

// SchedulePhases queues the two phase changes on HostLane (PhaseHeal after
// Faults, PhaseDrain after Faults+Heal) and sets Config.End to
// Faults+Heal+Drain. Prefer this when the product uses the three-phase
// fault/heal/drain schedule with Config.End. Products that drive quiet and
// final tails themselves (custom heal, convergence waits) should call
// SetPhase and QuietElapsed instead.
func SchedulePhases(sys *System, p Phases) {
	at := Time(p.Faults)
	sys.queuePhase(at, PhaseHeal)
	at += Time(p.Heal)
	sys.queuePhase(at, PhaseDrain)
	sys.cfg.End = Time(p.Faults + p.Heal + p.Drain)
}

// QuietElapsed reports that a quiet wait should end: max reached, or min
// reached while ready. Use with RunUntil for a product quiet/convergence tail
// that does not use SchedulePhases.
func QuietElapsed(waited, min, max actor.Duration, ready bool) bool {
	return waited >= max || (waited >= min && ready)
}

func (sys *System) queuePhase(at Time, to Phase) {
	e := &entry{at: at, lane: HostLane, kind: kindPhase, node: -1, body: phaseBody{to}}
	e.run = func() {
		sys.counters[countKernelSteps]++
		sys.SetPhase(to)
	}
	sys.q.add(e)
}

// SetPhase changes the phase. It appends the "sim_phase" fact, makes buggify
// Fault quiet outside PhaseFaults, calls EnterRecovery on every service when
// PhaseHeal starts, then gives each live driver NodeSpec.OnPhase(p), in
// node order, as one Step that takes no node time and skips the inbox.
func (sys *System) SetPhase(p Phase) {
	sys.phase = p
	sys.logMsg(fmt.Sprintf("phase %s", p), nil)
	sys.systemFact("sim_phase", actor.Field{Key: "phase", Value: actor.Uint64Value(p)})
	sys.faults.SetQuiet(p != PhaseFaults)
	if p == PhaseHeal {
		for _, s := range sys.services {
			s.base().invalidateHash()
			if !sys.guard("EnterRecovery "+s.base().name, s.enterRecovery) {
				sys.halted = true
			}
		}
	}
	for _, s := range sys.actors {
		if sys.halted {
			return
		}
		if s.base().workload {
			s.phaseStep(p)
		}
	}
}
