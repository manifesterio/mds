package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
)

// AttritionPlan makes the System kill actors spawned with the Attrition option
// during PhaseFaults, at random, within a budget.
type AttritionPlan struct {
	MaxDead        int            // actors down or stopping; 0 means attrition is off
	CrashProb      float64        // rolled after each Receive and each service step
	CrashWeight    float64        // hard crash ...
	GracefulWeight float64        // ... or graceful Stop, by weight
	StopGrace      actor.Duration // 0 means 5 s
}

// lookup resolves an entity handle of this System. A nil, zero or foreign
// one is a protocol failure.
func (sys *System) lookup(e actor.Entity) (slot, bool) {
	if e == nil {
		sys.Fail(FailProtocol, "zero address", "a nil Entity was used")
		return nil, false
	}
	return sys.actorOf(e.EntityID())
}

// live returns the actor if it is up.
func (sys *System) live(e actor.Entity) (slot, bool) {
	s, ok := sys.lookup(e)
	if !ok || !s.base().up {
		return nil, false
	}
	return s, true
}

// Up reports whether an incarnation of the actor is up.
func (sys *System) Up(e actor.Entity) bool {
	_, ok := sys.live(e)
	return ok
}

// Current returns the incarnation of the actor that is up.
func (sys *System) Current(e actor.Entity) (actor.Incarnation, bool) {
	s, ok := sys.live(e)
	if !ok {
		return actor.Incarnation{}, false
	}
	return s.base().self(), true
}

// Boot starts a new incarnation of an actor that is down.
func (sys *System) Boot(e actor.Entity) {
	if s, ok := sys.lookup(e); ok {
		s.boot()
	}
}

// Crash kills the incarnation that is up. A workload may not crash.
func (sys *System) Crash(e actor.Entity, cause string) {
	s, ok := sys.live(e)
	if !ok || sys.refuseWorkload(s, "crash") {
		return
	}
	sys.end(s, actor.Crash, cause)
}

// Pause holds the actor's inbox: it receives nothing until Resume.
func (sys *System) Pause(e actor.Entity) {
	if s, ok := sys.live(e); ok {
		s.base().paused = true
		sys.logMsg("pause "+s.base().name, nil)
	}
}

// Resume undoes Pause.
func (sys *System) Resume(e actor.Entity) {
	s, ok := sys.live(e)
	if !ok || !s.base().paused {
		return
	}
	b := s.base()
	b.paused = false
	sys.logMsg("resume "+b.name, nil)
	if c, d := s.inboxLens(); c+d > 0 {
		sys.ensureWake(b, sys.q.now)
	}
}

// refuseWorkload fails the run when a workload would end.
func (sys *System) refuseWorkload(s slot, what string) bool {
	if !s.base().workload {
		return false
	}
	sys.Fail(FailProtocol, "workload_ended", fmt.Sprintf("%s: a workload may not %s", s.base().name, what))
	sys.halted = true
	return true
}

// end ends the incarnation that is up: its queued entries, timers,
// awaited answers and watches are dropped, its asks die and watches on
// them fire. Then the restart policy decides the next boot.
func (sys *System) end(s slot, how actor.End, cause string) {
	b := s.base()
	if !b.up {
		return
	}
	who := b.self()
	idx, inc := b.idx, b.inc
	b.up, b.stopping, b.paused, b.lastEnd = false, false, false, how
	sys.q.dropIncarnation(idx, inc)
	name := sys.countEnd(how)
	sys.logMsg(fmt.Sprintf("%s %s (%s)", name, b.name, cause), nil)
	sys.systemFact(name, actor.Field{Key: "node", Value: actor.StringValue(b.name)},
		actor.Field{Key: "cause", Value: actor.StringValue(cause)})
	sys.watcherEnded(idx, inc)
	sys.askerEnded(func(r actor.ReplyID) bool {
		return r.Asker.Kind == core.ActorAsker && int(r.Asker.Slot) == idx && r.Asker.Boot == inc
	})
	for _, sv := range sys.services {
		sv.onCrash(who)
	}
	s.drop()
	s.ended(who, how, cause)
	if b.restart == nil {
		return
	}
	var after actor.Duration
	var again bool
	if !sys.guard("Restart "+b.name, func() { after, again = b.restart(how, env{sys: sys}) }) {
		sys.halted = true
		return
	}
	if again {
		sys.queueBoot(s, sys.q.now+Time(max(after, 0)))
	}
}

func (sys *System) countEnd(how actor.End) string {
	switch how {
	case actor.Trip:
		sys.trips++
		return "sim_tripped"
	case actor.Exit:
		sys.exits++
		return "sim_exited"
	case actor.Crash:
	}
	sys.crashes++
	return "sim_crashed"
}

func (sys *System) queueBoot(s slot, at Time) {
	b := s.base()
	e := &entry{at: at, lane: hostLane, kind: kindRestart, node: b.idx}
	e.run = func() {
		s.boot()
	}
	sys.q.add(e)
}

func (sys *System) stop(s slot, grace actor.Duration) {
	b := s.base()
	deadline := sys.q.now + Time(max(grace, 0))
	if !s.stopRequest(actor.Mono(deadline) + actor.Mono(b.skew)) {
		sys.Fail(FailProtocol, "stop_not_allowed", b.name+": the actor has no StopMsg")
		return
	}
	b.stopping = true
	inc := b.inc
	e := &entry{at: deadline, lane: hostLane, kind: kindCrash, node: b.idx, inc: inc}
	e.run = func() {
		if !b.alive(inc) {
			return
		}
		sys.end(s, actor.Crash, "stop deadline")
	}
	sys.q.add(e)
	sys.systemFact("sim_stop_requested", actor.Field{Key: "node", Value: actor.StringValue(b.name)})
	s.pump()
}

func (sys *System) attrition() {
	a := sys.cfg.Attrition
	if sys.phase != PhaseFaults || !sys.rand.Chance(a.CrashProb) {
		return
	}
	var live []slot
	down := 0
	for _, s := range sys.actors {
		b := s.base()
		switch {
		case b.workload || !b.attrition:
		case !b.up || b.stopping:
			down++
		default:
			live = append(live, s)
		}
	}
	if down >= a.MaxDead || len(live) == 0 {
		return
	}
	s := live[sys.rand.Intn(len(live))]
	if total := a.CrashWeight + a.GracefulWeight; total > 0 && sys.rand.Float64()*total < a.GracefulWeight {
		grace := a.StopGrace
		if grace <= 0 {
			grace = defaultStopGrace
		}
		sys.stop(s, grace)
		return
	}
	sys.end(s, actor.Crash, "attrition")
}
