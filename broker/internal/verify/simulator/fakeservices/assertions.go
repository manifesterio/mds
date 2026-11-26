package fakeservices

import (
	"github.com/manifesterio/mds/broker/internal/verify/simulator/faulty"
	"github.com/manifesterio/mds/dst/core/assert"
)

// The assertions of the simulated services. Each one is declared once here, as a
// value of its own kind (Reachable, Sometimes), and the code uses the value. The
// declaration puts the name in the catalog of dst/core/assert. A name is never built at
// run time, and it must not change: reports and replays use it.
var (
	objectAsserts       = faulty.NamedAsserts("obj")               //determinismcheck:ok catalog handles, set once at start
	controlPlaneAsserts = faulty.NamedAsserts("cp")                //determinismcheck:ok catalog handles, set once at start
	objectGetCorrupted  = assert.NewReachable("obj get corrupted") //determinismcheck:ok catalog handle, set once at start
)
