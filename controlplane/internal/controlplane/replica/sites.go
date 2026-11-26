package replica

import (
	"github.com/manifesterio/mds/dst/actor"
)

// The buggify sites of the replica. Both are Fault sites: they fire only in the
// fault phase, with the probability that the config gives (StallProb, CrashProb).
// Each name is declared once here and does not change.
var (
	// siteStall: the active replica stops working for a while.
	siteStall = actor.NewFaultSite("logcp_stall") //determinismcheck:ok catalog handle, set once at start
	// siteCrash: the active replica crashes by itself.
	siteCrash = actor.NewFaultSite("logcp_crash") //determinismcheck:ok catalog handle, set once at start
)
