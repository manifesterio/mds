package sim

import (
	"github.com/manifesterio/mds/dst/core/actor"
)

// ExecCtx is lent to one executor call: one Do or Perform of one actor.
type ExecCtx struct {
	sys      *System
	issuer   responderTarget // number 0 for a Do
	name     string
	self     actor.Incarnation
	answered bool
}

// System is the run, for executors that Tell actors or use services.
func (c *ExecCtx) System() *System { return c.sys }

// Issuer is the incarnation that recorded the IO.
func (c *ExecCtx) Issuer() actor.Incarnation { return c.self }

// claim reports whether the IO may be answered now: the answer of a Do is
// dropped, and a second answer is a protocol failure.
func (c *ExecCtx) claim() bool {
	if c.answered {
		c.sys.Fail(FailProtocol, "answered_twice", c.name+": the executor answered one IO twice")
		return false
	}
	c.answered = true
	if c.issuer.number == 0 {
		c.sys.dropped(dropAnsweredDo, c.name)
		return false
	}
	return true
}

// Answer answers the Perform that issued req, after d. It is typed by the
// same Request constraint as Perform. The answer of a Do is dropped; a
// second answer is a protocol failure.
func Answer[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, _ Req, d actor.Duration, answer Rep) {
	if !c.claim() {
		return
	}
	c.sys.foldExecAnswer(c.issuer.number, d, answer)
	c.sys.answerAt("exec/"+c.name, c.issuer, Time(max(d, 0)), answer)
}

// Forward hands the Perform that issued req to a service whose protocol is
// the request and answer types themselves; the service's answer becomes the
// Perform's answer.
func Forward[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, req Req, to actor.ServiceRef[Req, Rep]) {
	target := c.issuer
	if !c.claim() {
		target = responderTarget{}
	}
	c.sys.callService(to.ID(), req, target)
}

// foldExecAnswer folds a scheduled executor answer (tag 2017).
func (sys *System) foldExecAnswer(n uint64, d actor.Duration, answer actor.Payload) {
	sys.hash.Uint64(tagExecAnswer)
	sys.hash.Uint64(n)
	sys.hash.Int64(int64(d))
	answer.Fold(&sys.hash)
}

const tagExecAnswer = 2017

// ---- host code: scenario steps ----

// Tell sends msg to an actor from host code or an executor, after d. It is
// how an executor delivers unsolicited IO results.
func Tell[P actor.Payload](sys *System, to actor.EntityRef[P], d actor.Duration, msg P) {
	sys.tell("host", to.EntityID(), d, msg)
}

// outsideAsk is one sim.Ask that has not finished.
type outsideAsk struct {
	done     func(answer actor.Payload, ok bool)
	deadline EntryID
}

// Ask asks an actor from host code (a scenario step, a simulated gRPC
// client). done runs with the answer as soon as the Reply is applied, or with
// ok=false (as a host task) once within passes; then the asker counts as gone and Watches on it fire.
func Ask[Req, Rep actor.Payload](sys *System, to actor.EntityRef[Req], makeReq func(actor.ReplyTo[Rep]) Req,
	within actor.Duration, done func(answer Rep, ok bool)) {
	sys.outsideAsks++
	n := sys.outsideAsks
	id := actor.ReplyID{Asker: actor.Asker{Kind: actor.SimOutsideAsker, Runtime: sys.owner}, Number: n}
	req := makeReq(actor.NewReplyTo[Rep](id))
	ask := &outsideAsk{done: func(answer actor.Payload, ok bool) {
		var rep Rep
		if ok {
			rep, ok = actor.As[Rep](answer)
		}
		done(rep, ok)
	}}
	sys.outside[n] = ask
	ask.deadline = sys.After(within, func() {
		if _, waiting := sys.outside[n]; !waiting {
			return
		}
		delete(sys.outside, n)
		sys.askerEnded(func(r actor.ReplyID) bool { return r == id })
		ask.done(nil, false)
	})
	sys.tell("host", to.EntityID(), 0, req)
}

// AskDelayed asks to after delayBefore (Ask runs at once when delayBefore <= 0).
// within is Ask's timeout measured from the Ask. On a timely answer, backhaul
// runs at Reply time (capture product state there) and returns the delay
// before done(answer, true); a non-positive delay calls done at once. On Ask
// timeout, done(zero, false) runs with no backhaul. Leave product-level
// parallel timeouts to the caller.
func AskDelayed[Req, Rep actor.Payload](
	sys *System,
	to actor.EntityRef[Req],
	delayBefore actor.Duration,
	makeReq func(actor.ReplyTo[Rep]) Req,
	within actor.Duration,
	backhaul func(answer Rep) actor.Duration,
	done func(answer Rep, ok bool),
) {
	start := func() {
		Ask(sys, to, makeReq, within, func(rep Rep, ok bool) {
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

// replyOutside answers a sim.Ask: done runs at once, while the Reply is
// applied, so it sees the replying actor as the Reply left it.
func (sys *System) replyOutside(r actor.ReplyID, answer actor.Payload) {
	if r.Asker.Runtime != sys.owner {
		sys.Fail(FailProtocol, "foreign address", "a ReplyTo of another System was used")
		return
	}
	ask, ok := sys.outside[r.Number]
	if !ok {
		sys.dropped(dropOutsideGone, "host")
		return
	}
	delete(sys.outside, r.Number)
	sys.Cancel(ask.deadline)
	if !sys.guard("sim.Ask done", func() { ask.done(answer, true) }) {
		sys.halted = true
	}
}
