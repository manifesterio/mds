package sim_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
)

func TestOnStepRunsAfterEveryPop(t *testing.T) {
	t.Parallel()
	w := newWorld(30)
	w.spawn("a", nil)
	var steps []uint64
	w.sys.OnStep(func() { steps = append(steps, w.sys.Steps()) })
	w.sys.After(ms, func() {})
	mustOK(t, w.sys.Run())
	if len(steps) == 0 || uint64(len(steps)) != w.sys.Steps() || steps[0] != 1 {
		t.Fatalf("OnStep calls %v for %d steps", steps, w.sys.Steps())
	}
}

func TestSetFaultsAppendsFactAndFlipsFaultsOn(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 31})
	if !sys.FaultsOn() {
		t.Fatal("a System starts with faults on")
	}
	sys.SetFaults(false)
	if sys.FaultsOn() {
		t.Fatal("faults still on")
	}
	facts := sys.Timeline().Snapshot("sim_faults")
	if len(facts) != 1 || facts[0].Fact.Field("on") != actor.Bool(false) {
		t.Fatalf("sim_faults facts: %+v", facts)
	}
}

func TestSetEndStopsTheRun(t *testing.T) {
	t.Parallel()
	sys := sim.NewSystem(sim.Config{Seed: 32})
	var ran []int
	sys.At(sim.Time(ms), func() { ran = append(ran, 1) })
	sys.At(sim.Time(3*ms), func() { ran = append(ran, 3) })
	sys.SetEnd(sim.Time(2 * ms))
	mustOK(t, sys.Run())
	if !slices.Equal(ran, []int{1}) {
		t.Fatalf("ran %v", ran)
	}
}

func TestStopAsksThenCrashesAtTheDeadline(t *testing.T) {
	t.Parallel()
	w := newWorld(33)
	props := w.props("a", nil)
	props.StopMsg = func(actor.Mono) msg { return note{"stop"} }
	a := sim.Spawn(w.sys, props)
	var stopping bool
	w.sys.After(ms, func() {
		w.sys.Stop(a, 10*ms)
		stopping = w.sys.Stopping(a)
	})
	mustOK(t, w.sys.Run())
	if !stopping || w.sys.Stopping(a) || w.sys.Up(a) {
		t.Fatalf("stopping=%v then stopping=%v up=%v", stopping, w.sys.Stopping(a), w.sys.Up(a))
	}
	if !slices.Contains(w.lines("a"), "note{S:stop}") {
		t.Fatalf("a: %v", w.lines("a"))
	}
}

func TestStopRefusesAWorkload(t *testing.T) {
	t.Parallel()
	w := newWorld(34)
	a := w.spawn("a", nil, sim.Workload())
	w.sys.After(ms, func() { w.sys.Stop(a, ms) })
	if r := w.sys.Run(); !hasFailure(r, "workload_ended") {
		t.Fatalf("failures: %+v", r.Failures)
	}
}

func TestCostKeepsTheActorBusy(t *testing.T) {
	t.Parallel()
	w := newWorld(35)
	a := w.spawn("a", nil, sim.Cost(func(*actor.Rand) actor.Duration { return 10 * ms }))
	sim.Tell(w.sys, a, 0, msg(note{"x"}))
	sim.Tell(w.sys, a, 0, msg(note{"y"}))
	mustOK(t, w.sys.Run())
	if len(w.log) != 3 {
		t.Fatalf("log %v", w.log)
	}
}
