package kit

import (
	"fmt"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
)

// factPhase is the fact SetPhase appends.
const factPhase = "sim_phase"

// Phase is the fault regime of the run.
type Phase uint8

// The phases, in run order. The numbers are written to the sim_phase fact,
// which is folded into the trace hash, so they are frozen.
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

// SchedulePhases queues the two phase changes as host tasks (PhaseHeal after
// Faults, PhaseDrain after Faults+Heal) and sets the end of the run to
// Faults+Heal+Drain. Each change is SetPhase with hooks. Products that drive
// their own quiet and final tails call SetPhase themselves.
func SchedulePhases(sys *sim.System, p Phases, hooks ...func(Phase)) {
	at := sim.Time(p.Faults)
	sys.At(at, func() { SetPhase(sys, PhaseHeal, hooks...) })
	at += sim.Time(p.Heal)
	sys.At(at, func() { SetPhase(sys, PhaseDrain, hooks...) })
	sys.SetEnd(sim.Time(p.Faults + p.Heal + p.Drain))
}

// SetPhase announces phase p: it appends the "sim_phase" fact, turns buggify
// Fault sites on for PhaseFaults and off otherwise (sim.System.SetFaults), then
// runs each hook in order.
func SetPhase(sys *sim.System, p Phase, hooks ...func(Phase)) {
	sys.Note(fmt.Sprintf("phase %s", p))
	sys.Fact(actor.Fact{Name: factPhase, Fields: []actor.Field{{Key: "phase", Value: actor.Uint64Value(p)}}})
	sys.SetFaults(p == PhaseFaults)
	for _, h := range hooks {
		h(p)
	}
}

// PhaseStart reports when SetPhase announced phase p, from the sim_phase facts.
func PhaseStart(tl *sim.Timeline, p Phase) (sim.Time, bool) {
	for _, tf := range tl.Snapshot(factPhase) {
		if ph, ok := tf.Fact.Field("phase").(actor.Uint64Value); ok && Phase(ph) == p {
			return tf.At, true
		}
	}
	return 0, false
}
