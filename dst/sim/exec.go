package sim

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
)

// ExecCtx is lent to one executor call: one Do or Perform of one actor.
type ExecCtx struct {
	sys      *System
	issuer   responderTarget // number 0 for a Do
	name     string
	self     actor.Incarnation
	answered bool
}

// Issuer is the incarnation that recorded the IO.
func (c *ExecCtx) Issuer() actor.Incarnation { return c.self }

// Hold claims the answer of the IO that issued req and returns its Reply, to
// answer now or later (from another step of host code). It is typed by the
// same Request constraint as Perform. The answer of a Do is dropped (and
// counted); a second claim is a protocol failure.
func Hold[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, _ Req) Reply[Rep] {
	r := Reply[Rep]{sys: c.sys, source: "exec/" + c.name, to: c.issuer, answered: new(bool)}
	switch {
	case c.answered:
		c.sys.Fail(FailProtocol, "answered_twice", c.name+": the executor answered one IO twice")
		*r.answered = true
		r.to = responderTarget{}
	case c.issuer.number == 0:
		c.sys.dropped(dropAnsweredDo, c.name)
		r.to = responderTarget{}
	}
	c.answered = true
	return r
}

// Answer answers the Perform that issued req, after d: Hold(c, req).After(d,
// answer). The answer of a Do is dropped; a second answer is a protocol
// failure.
func Answer[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, req Req, d actor.Duration, answer Rep) {
	Hold(c, req).After(d, answer)
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
	deadline entryID
}

// Ask asks an actor from host code (a scenario step, a simulated gRPC
// client). done runs with the answer as soon as the Reply is applied, or with
// ok=false (as a host task) once within passes; then the asker counts as gone and Watches on it fire.
func Ask[Req, Rep actor.Payload](sys *System, to actor.EntityRef[Req], makeReq func(actor.ReplyTo[Rep]) Req,
	within actor.Duration, done func(answer Rep, ok bool)) {
	sys.outsideAsks++
	n := sys.outsideAsks
	id := actor.ReplyID{Asker: core.Asker{Kind: core.SimOutsideAsker, Runtime: sys.owner}, Number: n}
	req := makeReq(core.NewReplyTo[Rep](id))
	ask := &outsideAsk{done: func(answer actor.Payload, ok bool) {
		var rep Rep
		if ok {
			rep, ok = core.As[Rep](answer)
		}
		done(rep, ok)
	}}
	sys.outside[n] = ask
	ask.deadline = sys.after(within, func() {
		if _, waiting := sys.outside[n]; !waiting {
			return
		}
		delete(sys.outside, n)
		sys.askerEnded(func(r actor.ReplyID) bool { return r == id })
		ask.done(nil, false)
	})
	sys.tell("host", to.EntityID(), 0, req)
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
	sys.cancel(ask.deadline)
	if !sys.guard("sim.Ask done", func() { ask.done(answer, true) }) {
		sys.halted = true
	}
}
