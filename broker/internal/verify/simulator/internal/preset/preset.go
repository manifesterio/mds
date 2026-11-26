// Package preset holds the default schedule and fault knobs, the swarm and the
// fault coverage rule of the scenario.
package preset

import (
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/faulty"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim/kit"
)

// Preset is the fault defaults and the phase schedule shared by the scenarios.
type Preset struct {
	Settings     cluster.Settings
	Phases       kit.Phases
	QuiesceEvery actor.Duration
}

func ms(n int64) actor.Duration { return actor.Duration(n) * actor.Millisecond }

func slowTail(fmin, fmax, smin, smax int64, p float64) kit.FastWithRareSlow {
	return kit.FastWithRareSlow{
		FastMin: ms(fmin), FastMax: ms(fmax),
		SlowMin: ms(smin), SlowMax: ms(smax), SlowProb: p,
	}
}

// Phases is Faults 30s, Heal 10s, Drain 5s.
func Phases() kit.Phases {
	return kit.Phases{Faults: 30 * actor.Second, Heal: 10 * actor.Second, Drain: 5 * actor.Second}
}

// New returns a Preset. MaxCommitItems stays zero.
func New() Preset {
	return Preset{
		Settings: cluster.Settings{
			ObjectStore: fakeservices.ObjectStoreFaults{
				Faults: faulty.Faults{
					ApplyLatency:   slowTail(1, 5, 50, 200, 0.05),
					RespondLatency: slowTail(1, 3, 20, 100, 0.05),
					Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.02,
					Outage:        faulty.Outage{Prob: 0.002, Dur: [2]actor.Duration{ms(200), 2 * actor.Second}},
					CallerTimeout: ms(500),
				},
				GetCorruptProb: 0.03,
			},
			ControlPlane: fakeservices.ControlPlaneFaults{
				Faults: faulty.Faults{
					ApplyLatency:   slowTail(1, 5, 50, 200, 0.05),
					RespondLatency: slowTail(1, 3, 20, 100, 0.05),
					Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.03,
					Outage:        faulty.Outage{Prob: 0.002, Dur: [2]actor.Duration{ms(200), 2 * actor.Second}},
					CallerTimeout: ms(500),
				},
			},
			NetDelay:      [2]actor.Duration{200 * actor.Microsecond, 2 * actor.Millisecond},
			ConnResetProb: 0.002,
			ClockSkewMax:  ms(100),
			Attrition:     kit.AttritionPlan{MaxDead: 1, CrashProb: 0.0005, CrashWeight: 2, GracefulWeight: 1, StopGrace: 5 * actor.Second},
			RecoveryDelay: [2]actor.Duration{ms(200), ms(1500)},
			Cost:          kit.CostParams{PerEvent: 50 * actor.Microsecond, PauseProb: 0.001, Pause: slowTail(1, 5, 20, 100, 0.1)},
		},
		Phases:       Phases(),
		QuiesceEvery: actor.Second,
	}
}

// ProgressGraceTime is the Heal offset used by the liveness rules.
const ProgressGraceTime = 6 * actor.Second

// The fault coverage assertions of the scenario.
var (
	KafkaFaultCoverage = kit.FaultCoverage{ //determinismcheck:ok catalog handles, set once at start
		HardCrash:    actor.NewSometimes("kafka: fault fired: hard crash"),
		GracefulStop: actor.NewSometimes("kafka: fault fired: graceful stop"),
	}
)

// Swarm draws the shared fault knobs from r.
func Swarm(r *actor.Rand, s *cluster.Settings) {
	menu := [...]float64{0.5, 1, 2, 4}
	knob := func(p *float64) {
		if r.Chance(0.4) {
			*p = 0
			return
		}
		*p = min(*p*menu[r.Intn(len(menu))], 0.4)
	}
	o, cp := &s.ObjectStore, &s.ControlPlane
	knob(&o.Unavailable503)
	knob(&o.Timeout)
	knob(&o.LostResponse)
	knob(&o.GetCorruptProb)
	knob(&o.Outage.Prob)
	knob(&cp.Unavailable503)
	knob(&cp.Timeout)
	knob(&cp.LostResponse)
	knob(&cp.Outage.Prob)
	knob(&s.ConnResetProb)
	knob(&s.Attrition.CrashProb)
	if r.Chance(0.3) {
		o.ApplyLatency.SlowProb, cp.ApplyLatency.SlowProb = 0.3, 0.3
	}
}
