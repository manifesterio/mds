package sim

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
)

// DeriveRand returns an independent stream for (seed, label), for host setup
// that runs before a System exists (a swarm over the knobs of a seed). It
// never draws from the run stream of seed.
func DeriveRand(seed uint64, label string) *actor.Rand {
	return actor.NewRand(rng.New(seed).Derive(label))
}
