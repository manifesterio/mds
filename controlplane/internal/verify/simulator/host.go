package simulator

import (
	"github.com/manifesterio/mds/dst/actor"
	dstsim "github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/sim/kit"
)

// quietElapsed reports that a quiet wait should end: max reached, or min
// reached while ready. It is the stop condition of the quiet and final tails,
// which this simulator drives itself with RunUntil.
func quietElapsed(waited, min, max actor.Duration, ready bool) bool {
	return waited >= max || (waited >= min && ready)
}

// everyWhile schedules fn about every mean (jittered in [mean/2, mean*3/2]
// from the run stream) until still returns false. The first call is after one
// gap; mean <= 0 does nothing. Which fault fn applies stays with the caller.
func everyWhile(sys *dstsim.System, mean actor.Duration, still func() bool, fn func()) {
	if mean <= 0 {
		return
	}
	gap := func() actor.Duration { return kit.DrawBetween(sys.Env().Rand, mean/2, mean*3/2) }
	var tick func()
	tick = func() {
		if still != nil && !still() {
			return
		}
		fn()
		sys.After(gap(), tick)
	}
	sys.After(gap(), tick)
}

// askDelayed asks to after delayBefore (Ask runs at once when delayBefore <=
// 0). within is Ask's timeout measured from the Ask. On a timely answer,
// backhaul runs at Reply time (capture product state there) and returns the
// delay before done(answer, true); a non-positive delay calls done at once. On
// Ask timeout, done(zero, false) runs with no backhaul.
func askDelayed[Req, Rep actor.Payload](
	sys *dstsim.System,
	to actor.EntityRef[Req],
	delayBefore actor.Duration,
	makeReq func(actor.ReplyTo[Rep]) Req,
	within actor.Duration,
	backhaul func(answer Rep) actor.Duration,
	done func(answer Rep, ok bool),
) {
	start := func() {
		dstsim.Ask(sys, to, makeReq, within, func(rep Rep, ok bool) {
			if !ok {
				done(rep, false)
				return
			}
			d := actor.Duration(0)
			if backhaul != nil {
				d = backhaul(rep)
			}
			if d <= 0 {
				done(rep, true)
				return
			}
			sys.After(d, func() { done(rep, true) })
		})
	}
	if delayBefore <= 0 {
		start()
		return
	}
	sys.After(delayBefore, start)
}
