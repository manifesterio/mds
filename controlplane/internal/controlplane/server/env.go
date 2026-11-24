package server

import (
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
)

// productionEnvironment is the replica's Env in production: a counter for
// random numbers (the replica draws only for fault injection, which is off)
// and no buggify.
type productionEnvironment struct {
	self actor.Incarnation
	n    uint64
}

func (e *productionEnvironment) Self() actor.Incarnation { return e.self }

func (e *productionEnvironment) Uint64() uint64 {
	e.n++
	return e.n
}

func (e *productionEnvironment) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}

func (e *productionEnvironment) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(e.Intn(int(hi-lo+1)))
}

func (e *productionEnvironment) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(e.Between(int64(lo), int64(hi)))
}

func (e *productionEnvironment) Chance(p float64) bool {
	return p > 0 && float64(e.Uint64()%1000) < p*1000
}

func (e *productionEnvironment) NewID() actor.ID {
	var id actor.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}

func (*productionEnvironment) Maybe(fault.Site) bool                      { return false }
func (*productionEnvironment) MaybeP(fault.Site, float64) bool            { return false }
func (*productionEnvironment) Fault(fault.Site, float64) bool             { return false }
func (*productionEnvironment) Always(bool, assert.Always, ...assert.Pair) {}
func (*productionEnvironment) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {
}
func (*productionEnvironment) Sometimes(bool, assert.Sometimes)               {}
func (*productionEnvironment) Reachable(assert.Reachable)                     {}
func (*productionEnvironment) Unreachable(assert.Unreachable, ...assert.Pair) {}

var _ actor.Env = (*productionEnvironment)(nil)
