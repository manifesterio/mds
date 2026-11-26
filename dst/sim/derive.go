package sim

import (
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
)

// DeriveRand returns an independent stream for (seed, label), for host setup
// that runs before a System exists (a swarm over the knobs of a seed). It
// never draws from the run stream of seed.
func DeriveRand(seed uint64, label string) actor.Rand {
	return derived{rng.New(seed).Derive(label)}
}

// derived is an actor.Rand over a stream of its own.
type derived struct{ *rng.Rand }

func (d derived) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(d.Between(int64(lo), int64(hi)))
}

// NewID draws two Uint64 values and puts them in the id, little-endian.
func (d derived) NewID() actor.ID {
	var id actor.ID
	d.Bytes(id[:])
	return id
}

var _ actor.Rand = derived{}
