package sim

import (
	"github.com/manifesterio/mds/dst/actor"
)

// Rule checks one property of a run. Make a fresh Rule for every run, so it
// needs no Reset. A rule reports a broken property through View.Assert.
type Rule interface {
	Name() string
	// Observe runs after every queue pop.
	Observe(v View)
	// AtEnd runs once, after the run (in Finish).
	AtEnd(v View)
}

// View is what a rule sees. It is an interface so that tests can stub it.
type View interface {
	Step() uint64
	Now() Time
	Phase() Phase
	Timeline() *Timeline
	Assert() actor.Assertions
}

// view is the View of a System.
type view struct{ sys *System }

func (v view) Step() uint64             { return v.sys.steps }
func (v view) Now() Time                { return v.sys.q.now }
func (v view) Phase() Phase             { return v.sys.phase }
func (v view) Timeline() *Timeline      { return v.sys.tl }
func (v view) Assert() actor.Assertions { return env{sys: v.sys} }

// FaultCoverage names the Sometimes handles that FaultCoverageRule records.
type FaultCoverage struct {
	HardCrash    actor.Sometimes
	GracefulStop actor.Sometimes
}

// FaultCoverageRule reports a hard crash (sim_crashed cause=attrition) and a
// graceful stop (sim_exited) from System facts.
func FaultCoverageRule(names FaultCoverage) Rule {
	return &faultCoverage{names: names}
}

type faultCoverage struct{ names FaultCoverage }

func (*faultCoverage) Name() string { return "FaultCoverage" }
func (*faultCoverage) Observe(View) {}

func (f *faultCoverage) AtEnd(v View) {
	tl := v.Timeline()
	v.Assert().Sometimes(tl.hasFact(factCrashed, "cause", "attrition"), f.names.HardCrash)
	v.Assert().Sometimes(tl.hasFact(factExited, "", ""), f.names.GracefulStop)
}
