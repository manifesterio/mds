package sim

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
)

// actorOf resolves an EntityID of this System. A zero or foreign one is a
// protocol failure.
func (sys *System) actorOf(id actor.EntityID) (slot, bool) {
	switch {
	case id.Runtime == nil:
		sys.Fail(FailProtocol, "zero address", "a zero EntityRef was used")
		return nil, false
	case id.Runtime != sys.owner || int(id.Slot) >= len(sys.actors):
		sys.Fail(FailProtocol, "foreign address", "an EntityRef of another System was used")
		return nil, false
	}
	return sys.actors[id.Slot], true
}

// tell delivers msg to whatever incarnation of `to` is up when it arrives,
// after d, on the lane of (source, to).
func (sys *System) tell(source string, to actor.EntityID, d actor.Duration, msg actor.Payload) {
	s, ok := sys.actorOf(to)
	if !ok {
		return
	}
	b := s.base()
	e := &entry{at: sys.q.now + Time(max(d, 0)), lane: sys.actorLane(source, b.idx), kind: kindDeliver, node: b.idx, body: msg}
	e.run = func() { s.arrive(msg, actor.ID{}, false, 0) }
	sys.q.add(e)
}

// reply delivers answer to the asker behind r, now, if it is still waiting.
func (sys *System) reply(source string, r actor.ReplyID, answer actor.Payload) {
	switch r.Asker.Kind {
	case core.ActorAsker:
		sys.replyToActor(source, r, answer)
	case core.SimOutsideAsker:
		sys.replyOutside(r, answer)
	default:
		sys.Fail(FailProtocol, "zero reply", "Reply to a zero or foreign ReplyTo")
	}
}

func (sys *System) replyToActor(source string, r actor.ReplyID, answer actor.Payload) {
	if r.Asker.Runtime != sys.owner || int(r.Asker.Slot) >= len(sys.actors) {
		sys.Fail(FailProtocol, "foreign address", "a ReplyTo of another System was used")
		return
	}
	s := sys.actors[r.Asker.Slot]
	b := s.base()
	if !b.alive(r.Asker.Boot) {
		sys.dropped(dropDeadAsker, b.name)
		return
	}
	inc, n := r.Asker.Boot, r.Number
	e := &entry{at: sys.q.now, lane: sys.actorLane(source, b.idx), kind: kindDeliver, node: b.idx, inc: inc, body: answer}
	e.run = func() { s.answerArrives(answer, inc, n) }
	sys.q.add(e)
}

// ---- watches ----

// watch is one Watch of one incarnation.
type watch struct {
	idx   int
	inc   actor.ID
	reply actor.ReplyID
	gone  actor.Payload
}

// watch registers a Watch. If the asker is already gone, gone is delivered
// at once.
func (sys *System) watch(idx int, inc actor.ID, r actor.ReplyID, gone actor.Payload) {
	w := watch{idx: idx, inc: inc, reply: r, gone: gone}
	if !sys.askerAlive(r) {
		sys.fireWatch(w)
		return
	}
	sys.watches = append(sys.watches, w)
}

// askerAlive reports whether the asker behind r can still get an answer.
func (sys *System) askerAlive(r actor.ReplyID) bool {
	switch r.Asker.Kind {
	case core.ActorAsker:
		return r.Asker.Runtime == sys.owner && int(r.Asker.Slot) < len(sys.actors) &&
			sys.actors[r.Asker.Slot].base().alive(r.Asker.Boot)
	case core.SimOutsideAsker:
		_, ok := sys.outside[r.Number]
		return ok
	}
	return false
}

// watchAnswered removes the watches of incarnation (idx, inc) on r: a Reply
// was applied, so gone never comes.
func (sys *System) watchAnswered(idx int, inc actor.ID, r actor.ReplyID) {
	sys.watches = removeWatches(sys.watches, func(w watch) bool {
		return w.idx == idx && w.inc == inc && w.reply == r
	})
}

// askerEnded fires every watch on asks of the asker that ended.
func (sys *System) askerEnded(ended func(r actor.ReplyID) bool) {
	var fire []watch
	sys.watches = removeWatches(sys.watches, func(w watch) bool {
		if ended(w.reply) {
			fire = append(fire, w)
			return true
		}
		return false
	})
	for _, w := range fire {
		sys.fireWatch(w)
	}
}

// watcherEnded drops the watches an ended incarnation held.
func (sys *System) watcherEnded(idx int, inc actor.ID) {
	sys.watches = removeWatches(sys.watches, func(w watch) bool { return w.idx == idx && w.inc == inc })
}

func (sys *System) fireWatch(w watch) {
	s := sys.actors[w.idx]
	inc, gone := w.inc, w.gone
	e := &entry{at: sys.q.now, lane: sys.actorLane("watch", w.idx), kind: kindDeliver, node: w.idx, inc: inc, body: gone}
	e.run = func() { s.arrive(gone, inc, true, 0) }
	sys.q.add(e)
}

func removeWatches(ws []watch, drop func(watch) bool) []watch {
	out := ws[:0]
	for _, w := range ws {
		if !drop(w) {
			out = append(out, w)
		}
	}
	return out
}

// watchCount is the number of watches incarnation (idx, inc) holds.
func (sys *System) watchCount(idx int, inc actor.ID) int {
	n := 0
	for _, w := range sys.watches {
		if w.idx == idx && w.inc == inc {
			n++
		}
	}
	return n
}
