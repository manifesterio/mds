package sim

import (
	"github.com/manifesterio/mds/dst/core/actor"
)

// apply carries out the effects of one Receive in record order. Each is
// folded into the trace hash before it takes effect. Stop must be last: an
// effect after it is a protocol failure and is not applied.
func (h *host[In, IO]) apply(effects []actor.Effect) {
	sys := h.sys
	inc := h.inc
	for i, e := range effects {
		if !h.alive(inc) || sys.halted {
			return
		}
		e.Fold(&sys.hash)
		if e.Kind == actor.EffectStop {
			if i != len(effects)-1 {
				sys.Fail(FailProtocol, "effect_after_stop", h.name+": an effect was recorded after Stop")
			}
			if sys.refuseWorkload(h, "stop") {
				return
			}
			sys.end(h, e.How, e.Cause)
			return
		}
		h.applyOne(e)
	}
}

func (h *host[In, IO]) applyOne(e actor.Effect) {
	switch e.Kind {
	case actor.EffectTellSelf, actor.EffectAfter, actor.EffectStartTimer:
		h.startTimer(e)
	case actor.EffectCancelTimer:
		h.cancelKey(e.StoredKey())
	case actor.EffectForgetPending:
		h.forget(e.Scope)
		h.sys.hash.Uint64(h.forgets)
	case actor.EffectFact:
		h.sys.appendFact(h.self(), e.Fact)
	case actor.EffectTell:
		h.sys.tell(actorSource(h.idx), e.To, 0, e.Msg)
	case actor.EffectTellService:
		h.sys.callService(e.Service, e.Msg, responderTarget{})
	case actor.EffectAsk:
		h.await(e)
		h.sys.tell(actorSource(h.idx), e.To, 0, e.Msg)
	case actor.EffectCall:
		h.await(e)
		h.sys.callService(e.Service, e.Msg, h.target(e.Number))
	case actor.EffectReply:
		h.sys.reply(actorSource(h.idx), e.Reply, e.Msg)
		h.sys.watchAnswered(h.idx, h.inc, e.Reply)
	case actor.EffectPerform:
		h.await(e)
		h.execute(e.Msg, e.Number)
	case actor.EffectDo:
		h.execute(e.Msg, 0)
	case actor.EffectWatch:
		h.sys.watch(h.idx, h.inc, e.Reply, e.Msg)
	case actor.EffectStop:
	}
}

// startTimer arms a self message: TellSelf and After are one-shot, StartTimer
// replaces the live timer of its key.
func (h *host[In, IO]) startTimer(e actor.Effect) {
	sys := h.sys
	n := h.nextNumber()
	o := owned{scope: e.Scope, timer: true}
	if e.Kind == actor.EffectStartTimer {
		key := e.StoredKey()
		h.cancelKey(key)
		o.key = key
		h.keyed[key] = n
	}
	h.owned[n] = o
	h.deliverAt(sys.q.now+Time(e.D), sys.actorLane(actorSource(h.idx), h.idx), e.Msg, n)
}

// await records that an answer with number e.Number is awaited.
func (h *host[In, IO]) await(e actor.Effect) {
	h.owned[e.Number] = owned{scope: e.Scope, adapt: e.Adapt}
}

// target is where a service or executor answer for number n goes.
func (h *host[In, IO]) target(n uint64) responderTarget {
	return responderTarget{set: true, idx: h.idx, inc: h.inc, number: n}
}

// deliverAt queues msg for this incarnation at time at on lane, carrying
// owned number n.
func (h *host[In, IO]) deliverAt(at Time, lane Lane, msg actor.Payload, n uint64) EntryID {
	inc := h.inc
	e := &entry{at: at, lane: lane, kind: kindDeliver, node: h.idx, inc: inc, body: msg}
	e.run = func() { h.arrive(msg, inc, true, n) }
	return h.sys.q.add(e)
}

// execute hands io to the executor. n is the Perform's number, 0 for Do.
func (h *host[In, IO]) execute(io actor.Payload, n uint64) {
	sys := h.sys
	if h.exec == nil {
		sys.Fail(FailProtocol, "no_executor", h.name+": Do or Perform without WithExecutor")
		return
	}
	v, ok := actor.As[IO](io)
	if !ok {
		sys.Fail(FailProtocol, "wrong_io_type", h.name+": the IO value is not of the actor's IO type")
		return
	}
	ctx := &ExecCtx{sys: sys, issuer: h.target(n), name: h.name, self: h.self()}
	if !sys.guard("executor "+h.name, func() { h.exec(ctx, v) }) {
		sys.halted = true
	}
}

// answerArrives handles an answer (from a service, an executor or a Reply)
// for number n of incarnation inc: it is adapted and queued if still
// awaited, else dropped.
func (h *host[In, IO]) answerArrives(answer actor.Payload, inc actor.ID, n uint64) {
	sys := h.sys
	if !h.alive(inc) {
		sys.dropped(dropDeadTarget, h.name)
		return
	}
	o, ok := h.owned[n]
	if !ok || o.timer {
		sys.dropped(dropNotAwaited, h.name)
		return
	}
	var msg actor.Payload
	if !sys.guard("adapt "+h.name, func() { msg = o.adapt(answer) }) {
		sys.halted = true
		return
	}
	h.arrive(msg, inc, true, n)
}
