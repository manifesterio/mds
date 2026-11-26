package actor

import "github.com/manifesterio/mds/dst/internal/core"

// Ask sends an actor a request that carries a fresh ReplyTo; makeReq puts it
// in the request. The answer comes back as one of my messages, built by
// adapt, so I keep no table of pending asks. There is no timeout: use After.
func Ask[In, IO, Req, Rep Payload](o *Outbox[In, IO], to EntityRef[Req],
	makeReq func(ReplyTo[Rep]) Req, adapt func(Rep) In) {
	n := core.Next(o)
	replyTo := core.NewReplyTo[Rep](ReplyID{Asker: core.AskerOf(o), Number: n})
	core.Record(o, core.Effect{Kind: core.EffectAsk, To: to.EntityID(), Reply: replyTo.ID(), Number: n,
		Msg: makeReq(replyTo), Adapt: core.Lift(o, adapt)})
}

// Call is Ask for a service: the service gets its reply handle from Handle,
// so the request carries no ReplyTo. Bind the request first:
// actor.Call(o, store.Request(get{Key: k}), adapt).
func Call[In, IO, Rep Payload](o *Outbox[In, IO], call ServiceRequest[Rep], adapt func(Rep) In) {
	service, req := core.ServiceCall(call)
	core.Record(o, core.Effect{Kind: core.EffectCall, Service: service, Number: core.Next(o), Msg: req, Adapt: core.Lift(o, adapt)})
}

// Reply answers an Ask I received. A second Reply to the same ReplyTo, or a
// Reply to an asker that is gone, is dropped. It removes any Watch on
// that ReplyTo.
func Reply[In, IO, R Payload](o *Outbox[In, IO], to ReplyTo[R], answer R) {
	core.Record(o, core.Effect{Kind: core.EffectReply, Reply: to.ID(), Msg: answer})
}

// Perform hands req to the executor and turns its one answer into my message
// with adapt. Req must be a case of IO that declares its answer with
// //dst:answer; a wrong adapt type does not compile. The answer reaches only
// this incarnation and only if not forgotten.
func Perform[In, IO, Rep Payload, Req Request[IO, Rep]](o *Outbox[In, IO], req Req, adapt func(Rep) In) {
	io, _ := req.AsRequest()
	core.Record(o, core.Effect{Kind: core.EffectPerform, Number: core.Next(o), Msg: io, Adapt: core.Lift(o, adapt)})
}

// Sub lends a component its own Outbox with protocol C. What the component
// schedules for itself (timers, answers) comes back wrapped by wrap into my
// message type. name scopes its timer keys and its ForgetPending. Sub is
// called inside one Receive and dies with it, like the Outbox.
func Sub[In, IO, C Payload](o *Outbox[In, IO], name string, wrap func(C) In) *Outbox[C, IO] {
	return core.Sub(o, name, wrap)
}
