package sim

import (
	"github.com/manifesterio/mds/dst/actor"
)

// responderTarget is who waits for an answer: one awaited number of one
// incarnation. The zero target waits for nothing (a Do).
type responderTarget struct {
	set    bool
	idx    int
	inc    actor.ID
	number uint64
}

// Reply answers one IO of one issuer. It holds the issuer's identity and
// number; whoever answers never sees them. Make one with Hold.
type Reply[R actor.Payload] struct {
	sys      *System
	source   string
	to       responderTarget
	answered *bool
}

// Wanted reports whether anyone still waits for this answer: the IO was a
// Perform, and the issuer's incarnation is still up.
func (r Reply[R]) Wanted() bool {
	return r.to.set && r.sys.actors[r.to.idx].base().alive(r.to.inc)
}

// After answers after d. The answer is dropped if the IO was a Do, or if the
// issuer is gone or has forgotten the Perform by then. A second answer is a
// protocol failure.
func (r Reply[R]) After(d actor.Duration, answer R) {
	sys := r.sys
	if sys == nil {
		return
	}
	if *r.answered {
		sys.Fail(FailProtocol, "answered_twice", r.source+": a request was answered twice")
		return
	}
	*r.answered = true
	if !r.to.set {
		return
	}
	sys.foldExecAnswer(r.to.number, d, answer)
	sys.answerAt(r.source, r.to, Time(max(d, 0)), answer)
}

// answerAt queues answer for target after d.
func (sys *System) answerAt(source string, to responderTarget, d Time, answer actor.Payload) {
	s := sys.actors[to.idx]
	inc, n := to.inc, to.number
	e := &entry{at: sys.q.now + d, lane: sys.actorLane(source, to.idx), kind: kindDeliver, node: to.idx, inc: inc, body: answer}
	e.run = func() { s.answerArrives(answer, inc, n) }
	sys.q.add(e)
}

// Tracked is outside state (a simulated service, a fake) whose hash is
// folded into the trace hash after every step. The hash is cached until
// Touch.
type Tracked struct {
	name string
	hash func() uint64
	hashCache
}

// Track folds hash() into the trace hash after each step. Call Touch after
// every change of the state it covers; it is recomputed only then (with
// Config.VerifyHashes, every step, and a cached value that differs fails the
// seed). A panic in hash halts the run. A nil hash or a name that is empty or
// used twice is a protocol failure, and the returned Tracked does nothing.
func (sys *System) Track(name string, hash func() uint64) *Tracked {
	t := &Tracked{name: name, hash: hash}
	switch {
	case hash == nil:
		sys.Fail(FailProtocol, "track", "a nil hash was tracked")
		return t
	case name == "" || sys.trackNames[name]:
		sys.Fail(FailProtocol, "track", "tracked name "+name+" is empty or used twice")
		return t
	}
	sys.trackNames[name] = true
	sys.tracked = append(sys.tracked, t)
	return t
}

// Touch marks the tracked state as changed.
func (t *Tracked) Touch() { t.invalidateHash() }
