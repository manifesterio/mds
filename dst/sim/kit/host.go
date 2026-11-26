package kit

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
	"github.com/manifesterio/mds/dst/sim"
)

// DeriveRand returns an independent stream for (seed, label), for host setup
// that runs before a System exists (a swarm over the knobs of a seed). It
// never draws from the run stream of seed.
func DeriveRand(seed uint64, label string) *actor.Rand {
	return actor.NewRand(rng.New(seed).Derive(label))
}

// RecordFault records an injected host-side fault: a "sim_fault" fact with
// field kind, then the extra fields (pass a "service" field when the fault
// has one).
func RecordFault(sys *sim.System, kind string, fields ...actor.Field) {
	all := append([]actor.Field{
		{Key: "kind", Value: actor.StringValue(kind)},
	}, fields...)
	sys.Fact(actor.Fact{Name: "sim_fault", Fields: all})
}
