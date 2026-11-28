package kit

import (
	"github.com/manifesterio/mds/dst/actor"
)

// CostParams is the CPU model of an actor: how long it is busy after one
// Receive. Use it as sim.Cost(p.Sample). Zero params cost nothing and draw
// nothing.
type CostParams struct {
	PerEvent  actor.Duration   // the base cost of any Receive
	PauseProb float64          // stands in for GC pauses and CPU contention
	Pause     FastWithRareSlow // the extra cost of a pause
}

// Sample draws the cost of one Receive: PerEvent, plus a Pause sample with
// probability PauseProb.
func (p CostParams) Sample(r *actor.Rand) actor.Duration {
	d := p.PerEvent
	if r.Chance(p.PauseProb) {
		d += p.Pause.Sample(r)
	}
	return d
}

// FastWithRareSlow is a delay that is mostly fast, with a rare slow tail.
// Sample picks either the fast range or the slow range (replacement), not a
// base delay plus an add-on. A delay that is "base + rare add-on" uses
// DrawBetween + MaybeAdd instead (different hashes).
type FastWithRareSlow struct {
	FastMin, FastMax actor.Duration
	SlowMin, SlowMax actor.Duration
	SlowProb         float64
}

// Sample draws one delay: a coin for the slow tail, then a value in the chosen
// range, both ends included. A range with Max <= Min gives Min and draws
// nothing.
func (t FastWithRareSlow) Sample(r *actor.Rand) actor.Duration {
	lo, hi := t.FastMin, t.FastMax
	if r.Chance(t.SlowProb) {
		lo, hi = t.SlowMin, t.SlowMax
	}
	return DrawBetween(r, lo, hi)
}

// DrawBetween returns a duration in [lo, hi], both ends included. Max <= Min
// gives Min and draws nothing.
func DrawBetween(r *actor.Rand, lo, hi actor.Duration) actor.Duration {
	return actor.Duration(r.Between(int64(lo), int64(hi)))
}

// MaybeAdd returns extra in [lo, hi] with probability p, else 0.
func MaybeAdd(r *actor.Rand, p float64, lo, hi actor.Duration) actor.Duration {
	if !r.Chance(p) {
		return 0
	}
	return DrawBetween(r, lo, hi)
}
