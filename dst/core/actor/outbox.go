package actor

// Outbox records the effects of one Receive, in order. The runtime carries
// them out after Receive returns, in that order (G1). An Outbox is lent for
// one Receive only: using it later is a protocol failure.
//
// An Outbox never performs anything itself; it only appends to a list.
type Outbox[In, IO Payload] struct {
	rec   *recording
	scope string // "" for the actor itself, "session" or "session/retry" through Sub
	// up turns a message of this outbox's protocol into one of the actor's
	// own protocol (the identity for the actor, the wraps of Sub otherwise).
	up func(msg Payload) Payload
}

// Self is my own stable handle, to give to others. Through a Sub it is the
// zero EntityRef: a component has no address of its own.
func (o *Outbox[In, IO]) Self() EntityRef[In] {
	if o.scope != "" {
		return EntityRef[In]{}
	}
	return EntityRef[In]{id: o.rec.binding.Self}
}

// TellSelf puts msg in my inbox now, behind messages already there. It is
// After(0, msg); it dies with this incarnation.
func (o *Outbox[In, IO]) TellSelf(msg In) {
	o.record(Effect{Kind: EffectTellSelf, Msg: o.up(msg)})
}

// After delivers msg to this incarnation after d of node time. One-shot,
// nothing to cancel; dropped by Stop and ForgetPending. d < 0 counts as 0.
func (o *Outbox[In, IO]) After(d Duration, msg In) {
	o.record(Effect{Kind: EffectAfter, D: max(d, 0), Msg: o.up(msg)})
}

// StartTimer delivers msg after d, under key. A timer with the same key is
// replaced, including one that fired but is not yet received (G3).
func (o *Outbox[In, IO]) StartTimer(key TimerKey, d Duration, msg In) {
	o.record(Effect{Kind: EffectStartTimer, Key: key, D: max(d, 0), Msg: o.up(msg)})
}

// CancelTimer drops the timer key, including one that fired but is not yet
// received (G3). An unknown key is a no-op.
func (o *Outbox[In, IO]) CancelTimer(key TimerKey) {
	o.record(Effect{Kind: EffectCancelTimer, Key: key})
}

// Do hands io to the executor. Its results, if any, come back as my own
// messages that the executor sends. Use Perform when the IO has one typed
// answer.
func (o *Outbox[In, IO]) Do(io IO) {
	o.record(Effect{Kind: EffectDo, Msg: io})
}

// Stop ends this incarnation; how is Trip or Exit. Everything it owned is
// dropped (G7). It must be the last effect of the Receive, and it cannot be
// recorded through a Sub.
func (o *Outbox[In, IO]) Stop(how End, cause string) {
	if o.scope != "" {
		o.rec.misuse("Stop through the Sub " + o.scope + ": a component cannot end its actor")
		return
	}
	o.record(Effect{Kind: EffectStop, How: how, Cause: cause})
}

// Fact appends f to the timeline (sim) or the trace log (live). It never
// changes behaviour.
func (o *Outbox[In, IO]) Fact(f Fact) {
	o.record(Effect{Kind: EffectFact, Fact: f})
}

// ForgetPending drops every timer and every Ask, Call and Perform answer
// this actor (or, through a Sub, this component) is still waiting for,
// without ending the incarnation (G5). Watches stay.
func (o *Outbox[In, IO]) ForgetPending() {
	o.record(Effect{Kind: EffectForgetPending})
}

func (o *Outbox[In, IO]) record(e Effect) {
	e.Scope = o.scope
	o.rec.add(e)
}

// Sender is an Outbox (of any protocol), seen by the send methods of the
// handles. Only *Outbox implements it.
type Sender interface{ send(e Effect) }

func (o *Outbox[In, IO]) send(e Effect) { o.record(e) }

// Tell sends msg to the actor. No answer is expected. It is a method, not
// a function, so that msg may be any case of P: Go checks it against the
// handle's type instead of inferring P from it.
func (r EntityRef[P]) Tell(o Sender, msg P) {
	o.send(Effect{Kind: EffectTell, To: r.id, Msg: msg})
}

// Tell sends req to the service. Any answer the service gives is dropped.
func (r ServiceRef[Req, Rep]) Tell(o Sender, req Req) {
	o.send(Effect{Kind: EffectTellService, Service: r.id, Msg: req})
}

// ServiceRequest is a request bound to the service that takes it, ready for
// Call. Make one with ServiceRef.Request.
type ServiceRequest[Rep Payload] struct {
	service ServiceID
	req     Payload
}

// Request binds req to the service, for Call.
func (r ServiceRef[Req, Rep]) Request(req Req) ServiceRequest[Rep] {
	return ServiceRequest[Rep]{service: r.id, req: req}
}

// Ask sends an actor a request that carries a fresh ReplyTo; makeReq puts it
// in the request. The answer comes back as one of my messages, built by
// adapt, so I keep no table of pending asks. There is no timeout: use After.
func Ask[In, IO, Req, Rep Payload](o *Outbox[In, IO], to EntityRef[Req],
	makeReq func(ReplyTo[Rep]) Req, adapt func(Rep) In) {
	n := o.rec.next()
	replyTo := ReplyTo[Rep]{id: ReplyID{Asker: o.rec.binding.Asker, Number: n}}
	o.record(Effect{Kind: EffectAsk, To: to.id, Reply: replyTo.id, Number: n,
		Msg: makeReq(replyTo), Adapt: lift(o, adapt)})
}

// Call is Ask for a service: the service gets its reply handle from Handle,
// so the request carries no ReplyTo. Bind the request first:
// actor.Call(o, store.Request(get{Key: k}), adapt).
func Call[In, IO, Rep Payload](o *Outbox[In, IO], call ServiceRequest[Rep], adapt func(Rep) In) {
	o.record(Effect{Kind: EffectCall, Service: call.service, Number: o.rec.next(), Msg: call.req, Adapt: lift(o, adapt)})
}

// Reply answers an Ask I received. A second Reply to the same ReplyTo, or a
// Reply to an asker that is gone, is dropped (G4). It removes any Watch on
// that ReplyTo (G6).
func Reply[In, IO, R Payload](o *Outbox[In, IO], to ReplyTo[R], answer R) {
	o.record(Effect{Kind: EffectReply, Reply: to.id, Msg: answer})
}

// Perform hands req to the executor and turns its one answer into my message
// with adapt. Req must be a case of IO that declares its answer with
// //dst:answer; a wrong adapt type does not compile. The answer reaches only
// this incarnation and only if not forgotten (G10).
func Perform[In, IO, Rep Payload, Req Request[IO, Rep]](o *Outbox[In, IO], req Req, adapt func(Rep) In) {
	io, _ := req.AsRequest()
	o.record(Effect{Kind: EffectPerform, Number: o.rec.next(), Msg: io, Adapt: lift(o, adapt)})
}

// Asked is anything that names one ask: a ReplyTo of any answer type.
type Asked interface{ ID() ReplyID }

// Watch delivers gone to me once if the asker behind who ends before I Reply
// to it (G6).
func (o *Outbox[In, IO]) Watch(who Asked, gone In) {
	o.record(Effect{Kind: EffectWatch, Reply: who.ID(), Msg: o.up(gone)})
}

// Sub lends a component its own Outbox with protocol C. What the component
// schedules for itself (timers, answers) comes back wrapped by wrap into my
// message type. name scopes its timer keys and its ForgetPending. Sub is
// called inside one Receive and dies with it, like the Outbox.
func Sub[In, IO, C Payload](o *Outbox[In, IO], name string, wrap func(C) In) *Outbox[C, IO] {
	scope := name
	if o.scope != "" {
		scope = o.scope + "/" + name
	}
	return &Outbox[C, IO]{rec: o.rec, scope: scope, up: lift(o, wrap)}
}
