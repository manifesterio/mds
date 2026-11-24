package preset

import (
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/rng"
	"github.com/manifesterio/mds/dst/sim"
)

// stubAssert keeps the names of the Sometimes properties that held.
type stubAssert struct{ sometimes []string }

func (a *stubAssert) Always(bool, assert.Always, ...assert.Pair)                           {}
func (a *stubAssert) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (a *stubAssert) Sometimes(cond bool, p assert.Sometimes) {
	if cond {
		a.sometimes = append(a.sometimes, p.Name())
	}
}
func (a *stubAssert) Reachable(assert.Reachable)                     {}
func (a *stubAssert) Unreachable(assert.Unreachable, ...assert.Pair) {}

type stubView struct {
	tl *sim.Timeline
	as actor.Assertions
}

func (v stubView) Step() uint64             { return 0 }
func (v stubView) Now() sim.Time            { return 0 }
func (v stubView) Phase() sim.Phase         { return sim.PhaseFaults }
func (v stubView) Timeline() *sim.Timeline  { return v.tl }
func (v stubView) Assert() actor.Assertions { return v.as }

func TestNewAndPhases(t *testing.T) {
	t.Parallel()
	p := New()
	if p.Phases != Phases() {
		t.Fatalf("New phases = %+v, want %+v", p.Phases, Phases())
	}
	if p.QuiesceEvery != actor.Second {
		t.Fatalf("New defaults QuiesceEvery=%d", p.QuiesceEvery)
	}
	if ProgressGraceTime != 6*actor.Second {
		t.Fatalf("ProgressGraceTime = %d", ProgressGraceTime)
	}
}

func TestSwarm(t *testing.T) {
	t.Parallel()
	p := New()
	s := p.Settings
	Swarm(rng.New(1), &s)
	Swarm(rng.New(2), &s)
}

func TestPhaseStart(t *testing.T) {
	t.Parallel()
	tl := sim.NewTimeline()
	tl.Append(actor.Incarnation{}, 10, actor.Fact{
		Name:   "sim_phase",
		Fields: []actor.Field{{Key: "phase", Value: actor.Uint64Value(uint64(sim.PhaseHeal))}},
	})
	v := stubView{tl: tl, as: &stubAssert{}}
	at, ok := PhaseStart(v, sim.PhaseHeal)
	if !ok || at != 10 {
		t.Fatalf("PhaseStart Heal = %d, %v", at, ok)
	}
	if _, ok := PhaseStart(v, sim.PhaseDrain); ok {
		t.Fatal("PhaseStart Drain unexpectedly found")
	}
}

func TestFaultCoverage(t *testing.T) {
	t.Parallel()
	rule := FaultCoverageCheck(KafkaFaultCoverage)
	if rule.Name() != "FaultCoverage" {
		t.Fatalf("Name = %q", rule.Name())
	}
	rule.Observe(stubView{tl: sim.NewTimeline(), as: &stubAssert{}})

	tl := sim.NewTimeline()
	tl.Append(actor.Incarnation{}, 1, actor.Fact{
		Name:   "sim_crashed",
		Fields: []actor.Field{{Key: "cause", Value: actor.StringValue("attrition")}},
	})
	tl.Append(actor.Incarnation{}, 2, actor.Fact{Name: "sim_exited"})
	as := &stubAssert{}
	rule.AtEnd(stubView{tl: tl, as: as})
	if len(as.sometimes) != 2 {
		t.Fatalf("Sometimes props = %v", as.sometimes)
	}
}
