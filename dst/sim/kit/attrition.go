package kit

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
)

const defaultStopGrace = 5 * actor.Second

// AttritionPlan makes Attrition kill actors during PhaseFaults, at random,
// within a budget.
type AttritionPlan struct {
	MaxDead        int            // actors down or stopping; 0 means attrition is off
	CrashProb      float64        // rolled after each queue pop
	CrashWeight    float64        // hard crash ...
	GracefulWeight float64        // ... or graceful Stop, by weight
	StopGrace      actor.Duration // 0 means 5 s
}

// Attrition rolls CrashProb after every queue pop while faults are on. A hit
// picks one live actor of refs (not stopping) and, if fewer than MaxDead of
// refs are down or stopping, crashes it (cause "attrition") or asks it to stop
// by weight. Workloads must not be in refs. It does nothing when MaxDead is
// zero or refs is empty.
func Attrition(sys *sim.System, plan AttritionPlan, refs ...actor.Entity) {
	if plan.MaxDead <= 0 || len(refs) == 0 {
		return
	}
	sys.OnStep(func() {
		r := sys.Env().Rand
		if !sys.FaultsOn() || !r.Chance(plan.CrashProb) {
			return
		}
		var live []actor.Entity
		down := 0
		for _, e := range refs {
			if !sys.Up(e) || sys.Stopping(e) {
				down++
			} else {
				live = append(live, e)
			}
		}
		if down >= plan.MaxDead || len(live) == 0 {
			return
		}
		e := live[r.Intn(len(live))]
		if total := plan.CrashWeight + plan.GracefulWeight; total > 0 && r.Float64()*total < plan.GracefulWeight {
			grace := plan.StopGrace
			if grace <= 0 {
				grace = defaultStopGrace
			}
			sys.Stop(e, grace)
			return
		}
		sys.Crash(e, "attrition")
	})
}

// FaultCoverage names the Sometimes handles that FaultCoverageRule records.
type FaultCoverage struct {
	HardCrash    actor.Sometimes
	GracefulStop actor.Sometimes
}

// FaultCoverageRule reports a hard crash (sim_crashed cause=attrition) and a
// graceful stop (sim_exited) from the kernel facts.
func FaultCoverageRule(names FaultCoverage) sim.Rule {
	return &faultCoverage{names: names}
}

type faultCoverage struct{ names FaultCoverage }

func (*faultCoverage) Name() string     { return "FaultCoverage" }
func (*faultCoverage) Observe(sim.View) {}

func (f *faultCoverage) AtEnd(v sim.View) {
	tl := v.Timeline()
	hard := false
	for _, tf := range tl.Snapshot("sim_crashed") {
		if s, ok := tf.Fact.Field("cause").(actor.StringValue); ok && string(s) == "attrition" {
			hard = true
			break
		}
	}
	v.Assert().Sometimes(hard, f.names.HardCrash)
	v.Assert().Sometimes(len(tl.Snapshot("sim_exited")) > 0, f.names.GracefulStop)
}
