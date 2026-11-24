package sim

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
)

// Tag numbers of the trace hash for system bodies. They are frozen.
const (
	tagWake    = 901
	tagPhase   = 902
	tagOnPhase = 903
	tagDropped = 2018
)

// owned is one timer or awaited answer of an incarnation. It is checked when
// the message is taken from the inbox, so a cancelled, replaced or forgotten
// one is dropped even if it already arrived (G3, G5).
type owned struct {
	scope string
	key   actor.TimerKey // keyed timers only: the stored key
	timer bool           // a timer (else an answer)
	adapt func(actor.Payload) actor.Payload
}

// actorBase is the part of an actor host that does not depend on its types.
// The struct outlives incarnations; the fields marked "per incarnation" are
// reset at each boot.
type actorBase struct {
	sys       *System
	idx       int
	name      string
	skew      actor.Duration
	workload  bool
	attrition bool
	restart   actor.RestartPolicy

	up      bool
	broken  bool // the factory failed: never booted again
	lastEnd actor.End
	boots   int

	// per incarnation
	inc         actor.ID
	stopping    bool
	paused      bool
	received    uint64
	busy        Time
	wakePending bool
	numbers     uint64                    // timer, ask and pending numbers
	owned       map[uint64]owned          // by number
	keyed       map[actor.TimerKey]uint64 // stored key -> number of the live timer
	forgets     uint64                    // ForgetPending calls, folded

	hashCache
}

func (b *actorBase) self() actor.Incarnation { return actor.Incarnation{Name: b.name, Boot: b.inc} }

func (b *actorBase) entityID() actor.EntityID {
	return actor.EntityID{Runtime: b.sys.owner, Slot: uint32(b.idx), Name: b.name}
}

func (b *actorBase) asker() actor.Asker {
	return actor.Asker{Kind: actor.ActorAsker, Runtime: b.sys.owner, Slot: uint32(b.idx), Name: b.name, Boot: b.inc}
}

// alive reports whether incarnation inc of this actor is up.
func (b *actorBase) alive(inc actor.ID) bool { return b.up && b.inc == inc }

// local is the node clock: virtual time plus the actor's skew.
func (b *actorBase) local() actor.Mono {
	return actor.Mono(b.sys.q.now) + actor.Mono(b.skew)
}

func (b *actorBase) nextNumber() uint64 {
	b.numbers++
	return b.numbers
}

func (b *actorBase) resetIncarnation() {
	b.stopping, b.paused, b.wakePending = false, false, false
	b.received, b.busy, b.numbers, b.forgets = 0, 0, 0, 0
	b.owned = map[uint64]owned{}
	b.keyed = map[actor.TimerKey]uint64{}
}

// counts returns the number of live timers and awaited answers.
func (b *actorBase) counts() (int, int) {
	timers, answers := 0, 0
	for _, o := range b.owned { //determinismcheck:ok only counted
		if o.timer {
			timers++
		} else {
			answers++
		}
	}
	return timers, answers
}

// take reports whether an inbox item that carries owned number n may be
// received, and forgets n when it may.
func (b *actorBase) take(n uint64) bool {
	if n == 0 {
		return true
	}
	o, ok := b.owned[n]
	if !ok {
		return b.sys.plantKeepFiredTimers
	}
	delete(b.owned, n)
	if o.key != "" && b.keyed[o.key] == n {
		delete(b.keyed, o.key)
	}
	return true
}

// cancelKey drops the live timer of a stored key, pending or fired (G3).
func (b *actorBase) cancelKey(key actor.TimerKey) {
	if n, ok := b.keyed[key]; ok {
		delete(b.owned, n)
		delete(b.keyed, key)
		b.sys.counters[countTimersCancelled]++
	}
}

// forget drops every timer and awaited answer in scope or below it (G5).
func (b *actorBase) forget(scope string) {
	b.forgets++
	for n, o := range b.owned { //determinismcheck:ok deletes only; the order does not matter
		if inScope(o.scope, scope) {
			delete(b.owned, n)
			if o.key != "" {
				delete(b.keyed, o.key)
			}
		}
	}
}

// inScope reports whether a Sub path is scope or below it.
func inScope(path, scope string) bool {
	return scope == "" || path == scope || strings.HasPrefix(path, scope+"/")
}

// actorLane is the lane of messages from source to actor idx: one lane per
// (source, target) pair, so they keep their order at one time (ADR 3.4).
func (sys *System) actorLane(source string, idx int) Lane {
	k := laneKey2{source: source, target: idx}
	if l, ok := sys.pairLanes[k]; ok {
		return l
	}
	sys.nextPairLane++
	l := pairLaneBase + sys.nextPairLane
	sys.pairLanes[k] = l
	return l
}

type laneKey2 struct {
	source string
	target int
}

// pairLaneBase puts pair lanes above any lane host code allocates.
const pairLaneBase Lane = 1 << 61

func actorSource(idx int) string { return fmt.Sprintf("actor/%d", idx) }

// wakeBody is the body of a wake-up entry: the actor should try to Receive.
type wakeBody struct{}

// Fold writes tag 901.
func (wakeBody) Fold(h *trace.Hash) { h.Uint64(tagWake) }

// ensureWake queues one wake-up of the actor at time at, unless one is pending.
func (sys *System) ensureWake(b *actorBase, at Time) {
	if b.wakePending {
		return
	}
	b.wakePending = true
	inc := b.inc
	e := &entry{at: at, lane: nodeLane(b.idx, 1), kind: kindDeliver, node: b.idx, inc: inc, body: wakeBody{}}
	e.run = func() {
		if !b.alive(inc) {
			sys.counters[countDroppedStale]++
			return
		}
		b.wakePending = false
		sys.counters[countWakes]++
		sys.logMsg("wake "+b.name, nil)
		sys.actors[b.idx].pump()
	}
	sys.q.add(e)
}

// nodeLane is a lane of the system for actor idx: k=1 wake-ups.
func nodeLane(idx int, k uint64) Lane {
	return HostLane + 1 + Lane(idx)<<8 + Lane(k)
}

// dropped counts a delivery that was dropped and folds why.
func (sys *System) dropped(reason uint64, target string) {
	sys.counters[countDroppedStale]++
	sys.hash.Uint64(tagDropped)
	sys.hash.Uint64(reason)
	sys.hash.String(target)
}

// Reasons a delivery is dropped, folded with tag 2018.
const (
	dropDeadTarget  = 1 // the incarnation it was for has ended
	dropNotAwaited  = 2 // an answer nobody waits for (forgotten, answered twice)
	dropStaleTimer  = 3 // a cancelled, replaced or forgotten timer
	dropDeadAsker   = 4 // a Reply to an asker that is gone
	dropUnwanted    = 5 // a service answer to a TellService
	dropAnsweredDo  = 6 // an executor answer to a Do
	dropOutsideGone = 7 // an answer to a sim.Ask whose deadline passed
)
