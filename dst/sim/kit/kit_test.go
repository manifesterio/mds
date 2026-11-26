package kit_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/sim/kit"
	"github.com/manifesterio/mds/dst/trace"
)

const ms = actor.Millisecond

type ping struct{}

func (ping) Fold(h *trace.Hash) { h.Uint64(1) }

type stopReq struct{}

func (stopReq) Fold(h *trace.Hash) { h.Uint64(2) }

type node struct{}

func (node) Receive(_ actor.Now, m actor.Payload, o *actor.Outbox[actor.Payload, actor.NoIO]) {
	if _, ok := m.(stopReq); ok {
		o.Stop(actor.Exit, "asked")
		return
	}
	o.After(ms, ping{})
}
func (node) StateHash() uint64 { return 0 }

func spawnNode(sys *sim.System, name string) actor.EntityRef[actor.Payload] {
	return sim.Spawn(sys, actor.Props[actor.Payload, actor.NoIO]{
		Name:    name,
		Boot:    func(actor.Incarnation) actor.Payload { return ping{} },
		StopMsg: func(actor.Mono) actor.Payload { return stopReq{} },
		New:     func(actor.Env) actor.Actor[actor.Payload, actor.NoIO] { return node{} },
		Restart: func(actor.End, *actor.Rand) (actor.Duration, bool) { return 5 * ms, true },
	})
}

func TestSchedulePhasesSetsFaultsFactsEndAndRunsHooks(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 1})
	var seen []kit.Phase
	var faults []bool
	kit.SchedulePhases(sys, kit.Phases{Faults: 5 * ms, Heal: 5 * ms, Drain: 5 * ms}, func(p kit.Phase) {
		seen = append(seen, p)
		faults = append(faults, sys.FaultsOn())
	})
	res := sys.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if !slices.Equal(seen, []kit.Phase{kit.PhaseHeal, kit.PhaseDrain}) || !slices.Equal(faults, []bool{false, false}) {
		t.Fatalf("hooks saw %v faults %v", seen, faults)
	}
	if at, ok := kit.PhaseStart(sys.Timeline(), kit.PhaseHeal); !ok || at != sim.Time(5*ms) {
		t.Fatalf("heal at %d,%v", at, ok)
	}
	if at, ok := kit.PhaseStart(sys.Timeline(), kit.PhaseDrain); !ok || at != sim.Time(10*ms) {
		t.Fatalf("drain at %d,%v", at, ok)
	}
	if _, ok := kit.PhaseStart(sys.Timeline(), kit.PhaseFaults); ok {
		t.Fatal("PhaseFaults is never announced")
	}
}

func TestSetPhaseTurnsFaultsBackOn(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 2})
	kit.SetPhase(sys, kit.PhaseHeal)
	if sys.FaultsOn() {
		t.Fatal("faults on in heal")
	}
	kit.SetPhase(sys, kit.PhaseFaults)
	if !sys.FaultsOn() {
		t.Fatal("faults off in the fault phase")
	}
	if kit.PhaseHeal.String() != "heal" || kit.Phase(9).String() != "Phase(9)" {
		t.Fatal("Phase.String")
	}
}

func TestAttritionKillsOnlyWhileFaultsAreOn(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 3, End: sim.Time(2000 * ms)})
	a, b := spawnNode(sys, "a"), spawnNode(sys, "b")
	kit.Attrition(sys, kit.AttritionPlan{MaxDead: 1, CrashProb: 0.2, CrashWeight: 1, GracefulWeight: 1}, a, b)
	sys.At(sim.Time(1000*ms), func() { kit.SetPhase(sys, kit.PhaseHeal) })
	res := sys.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if res.Crashes+res.Exits == 0 {
		t.Fatal("attrition never fired")
	}
	for _, name := range []string{"sim_crashed", "sim_exited"} {
		for _, tf := range sys.Timeline().Snapshot(name) {
			if tf.At > sim.Time(1000*ms+5000*ms) {
				t.Fatalf("%s after heal: %d", name, tf.At)
			}
		}
	}
}

func TestAttritionOffWithoutBudget(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 4, End: sim.Time(500 * ms)})
	a := spawnNode(sys, "a")
	kit.Attrition(sys, kit.AttritionPlan{MaxDead: 0, CrashProb: 1}, a)
	res := sys.Run()
	if res.Crashes+res.Exits != 0 {
		t.Fatalf("crashes=%d exits=%d", res.Crashes, res.Exits)
	}
}

func TestFaultCoverageRuleReadsKernelFacts(t *testing.T) {
	t.Parallel()
	names := kit.FaultCoverage{
		HardCrash:    actor.NewSometimes("kit test: hard crash"),
		GracefulStop: actor.NewSometimes("kit test: graceful stop"),
	}
	sys := sim.NewSystem(sim.Config{Seed: 5, End: sim.Time(100 * ms)})
	a, b := spawnNode(sys, "a"), spawnNode(sys, "b")
	sys.AddRule(kit.FaultCoverageRule(names))
	sys.At(sim.Time(10*ms), func() { sys.Crash(a, "attrition") })
	sys.At(sim.Time(20*ms), func() { sys.Stop(b, 50*ms) })
	res := sys.Run()
	for _, n := range []string{"kit test: hard crash", "kit test: graceful stop"} {
		if c := res.Coverage[n]; c.Passes == 0 {
			t.Errorf("%s: %+v", n, c)
		}
	}
}

func TestCostAndDraws(t *testing.T) {
	t.Parallel()
	r := kit.DeriveRand(1, "x")
	if kit.DeriveRand(1, "x").Uint64() != r.Uint64() || kit.DeriveRand(1, "y").Uint64() == kit.DeriveRand(1, "x").Uint64() {
		t.Fatal("DeriveRand")
	}
	if d := (kit.CostParams{PerEvent: ms}).Sample(r); d != ms {
		t.Fatalf("no pause: %d", d)
	}
	if d := kit.DrawBetween(r, 2*ms, 2*ms); d != 2*ms {
		t.Fatalf("DrawBetween: %d", d)
	}
	if d := kit.MaybeAdd(r, 0, ms, 2*ms); d != 0 {
		t.Fatalf("MaybeAdd p=0: %d", d)
	}
	slow := kit.FastWithRareSlow{FastMin: ms, FastMax: ms, SlowMin: 9 * ms, SlowMax: 9 * ms, SlowProb: 1}
	if d := (kit.CostParams{PerEvent: ms, PauseProb: 1, Pause: slow}).Sample(r); d != 10*ms {
		t.Fatalf("pause: %d", d)
	}
}

func TestRecordFaultAppendsTheFact(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 6})
	kit.RecordFault(sys, "boom", actor.Field{Key: "service", Value: actor.StringValue("s")})
	facts := sys.Timeline().Snapshot("sim_fault")
	if len(facts) != 1 || facts[0].Fact.Field("kind") != actor.StringValue("boom") {
		t.Fatalf("facts: %+v", facts)
	}
}
