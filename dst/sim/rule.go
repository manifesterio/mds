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
	Timeline() *Timeline
	Assert() actor.Assertions
}
