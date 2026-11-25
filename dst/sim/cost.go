package sim

import (
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/rng"
)

// CostParams is the CPU model of a node: how long the node is busy after one
// Step. While a node is busy, its inputs wait in its inbox. Zero params cost
// nothing and draw nothing, so every input is one Step at once.
type CostParams struct {
	PerEvent  actor.Duration   // the base cost of any Step
	PauseProb float64          // stands in for GC pauses and CPU contention
	Pause     FastWithRareSlow // the extra cost of a pause
}

// Sample draws the cost of one Step: PerEvent, plus a Pause sample with
// probability PauseProb.
func (p CostParams) Sample(r *rng.Rand) actor.Duration {
	d := p.PerEvent
	if r.Chance(p.PauseProb) {
		d += p.Pause.Sample(r)
	}
	return d
}

// FastWithRareSlow is a delay that is mostly fast, with a rare slow tail.
// Sample picks either the fast range or the slow range (replacement), not a
// base delay plus an add-on.
//
// Latency patterns in sim hosts:
//
//   - Service / CostParams: use FastWithRareSlow (and CostParams.Pause) so a
//     rare draw replaces the fast range. Broker presets and faulty.Faults use
//     this on AddService paths.
//   - Host After / AskDelayed RTT: draw a base delay with Rand.Between, then
//     optionally add a slow extra under a fault coin (base + add-on). Controlplane
//     netDelay uses that shape; do not force it onto FastWithRareSlow without
//     changing hashes.
type FastWithRareSlow struct {
	FastMin, FastMax actor.Duration
	SlowMin, SlowMax actor.Duration
	SlowProb         float64
}

// Sample draws one delay: a coin for the slow tail, then a value in the chosen
// range, both ends included. A range with Max <= Min gives Min and draws
// nothing.
func (t FastWithRareSlow) Sample(r *rng.Rand) actor.Duration {
	lo, hi := t.FastMin, t.FastMax
	if r.Chance(t.SlowProb) {
		lo, hi = t.SlowMin, t.SlowMax
	}
	return DrawBetween(r, lo, hi)
}

// DrawBetween returns a duration in [lo, hi], both ends included. Max <= Min
// gives Min and draws nothing. Shared by host RTT draws that are not
// FastWithRareSlow.
func DrawBetween(r *rng.Rand, lo, hi actor.Duration) actor.Duration {
	return actor.Duration(r.Between(int64(lo), int64(hi)))
}

// MaybeAdd returns extra in [lo, hi] with probability p, else 0. Use with
// DrawBetween for the host "fast base + rare slow add-on" pattern.
func MaybeAdd(r *rng.Rand, p float64, lo, hi actor.Duration) actor.Duration {
	if !r.Chance(p) {
		return 0
	}
	return DrawBetween(r, lo, hi)
}
