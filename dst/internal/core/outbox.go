package core

// Outbox records the effects of one Receive, in order. The runtime carries
// them out after Receive returns, in that order. An Outbox is lent for
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
// replaced, including one that fired but is not yet received.
func (o *Outbox[In, IO]) StartTimer(key TimerKey, d Duration, msg In) {
	o.record(Effect{Kind: EffectStartTimer, Key: key, D: max(d, 0), Msg: o.up(msg)})
}

// CancelTimer drops the timer key, including one that fired but is not yet
// received. An unknown key is a no-op.
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
// dropped. It must be the last effect of the Receive, and it cannot be
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
// without ending the incarnation. Watches stay.
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

// Asked is anything that names one ask: a ReplyTo of any answer type.
type Asked interface{ ID() ReplyID }

// Watch delivers gone to me once if the asker behind who ends before I Reply
// to it.
func (o *Outbox[In, IO]) Watch(who Asked, gone In) {
	o.record(Effect{Kind: EffectWatch, Reply: who.ID(), Msg: o.up(gone)})
}

// Binding is what a runtime tells an Outbox about the incarnation that is
// receiving.
type Binding struct {
	Self  EntityID
	Asker Asker
	// Next returns the next ask/pending number of the incarnation (from 1).
	Next func() uint64
	// Misuse reports a protocol failure: an effect recorded after the
	// Receive, or through a Sub where it is not allowed.
	Misuse func(what string)
}

// recording is the effect list shared by an Outbox and its Subs.
type recording struct {
	binding Binding
	effects []Effect
	closed  bool
}

func (r *recording) add(e Effect) {
	if r.closed {
		r.misuse("an Outbox was used after its Receive")
		return
	}
	r.effects = append(r.effects, e)
}

func (r *recording) next() uint64 { return r.binding.Next() }

func (r *recording) misuse(what string) {
	if r.binding.Misuse != nil {
		r.binding.Misuse(what)
	}
}

// NewOutbox makes the Outbox of one Receive.
func NewOutbox[In, IO Payload](b Binding) *Outbox[In, IO] {
	return &Outbox[In, IO]{rec: &recording{binding: b}, up: identity}
}

// Close ends the Outbox's Receive and returns its effects in record order.
// Every later use of it, or of its Subs, is reported as misuse.
func Close[In, IO Payload](o *Outbox[In, IO]) []Effect {
	o.rec.closed = true
	effects := o.rec.effects
	o.rec.effects = nil
	return effects
}

func identity(msg Payload) Payload { return msg }

// Lift erases f, a function into the outbox's protocol (an answer's adapt,
// or a Sub's wrap), and puts its result through the outbox's own wraps. The
// assertion cannot fail: the runtime matched the answer to its request, and
// only a Sub's own methods pass its message type.
func Lift[In, IO, X Payload](o *Outbox[In, IO], f func(X) In) func(Payload) Payload {
	return func(v Payload) Payload { return o.up(f(v.(X))) } //nolint:forcetypeassert // see above
}

// NewEntityRef makes the handle of an actor. Runtimes only.
func NewEntityRef[P Payload](id EntityID) EntityRef[P] { return EntityRef[P]{id: id} }

// NewReplyTo makes the handle of one ask. Runtimes only (sim.Ask, live.Ask).
func NewReplyTo[R Payload](id ReplyID) ReplyTo[R] { return ReplyTo[R]{id: id} }

// NewServiceRef makes the handle of a service. Runtimes only.
func NewServiceRef[Req, Rep Payload](id ServiceID) ServiceRef[Req, Rep] {
	return ServiceRef[Req, Rep]{id: id}
}

// As asserts an erased message back to its type; ok is false on a mismatch
// (a runtime routing bug, or a foreign ref). Runtimes only.
func As[P Payload](msg Payload) (P, bool) {
	p, ok := msg.(P)
	return p, ok
}

// Record adds e to the effects of o, scoped by o's Sub path. It is how the
// generic functions of package actor (Ask, Reply, Perform, Call) record.
func Record[In, IO Payload](o *Outbox[In, IO], e Effect) { o.record(e) }

// Next returns the next ask/pending number of o's incarnation.
func Next[In, IO Payload](o *Outbox[In, IO]) uint64 { return o.rec.next() }

// AskerOf returns who asks through o: its incarnation.
func AskerOf[In, IO Payload](o *Outbox[In, IO]) Asker { return o.rec.binding.Asker }

// Sub makes the Outbox of a component with protocol C, scoped by name under
// o's own scope. What it schedules for itself comes back wrapped by wrap.
func Sub[In, IO, C Payload](o *Outbox[In, IO], name string, wrap func(C) In) *Outbox[C, IO] {
	scope := name
	if o.scope != "" {
		scope = o.scope + "/" + name
	}
	return &Outbox[C, IO]{rec: o.rec, scope: scope, up: Lift(o, wrap)}
}

// ServiceCall returns the service and the erased request of a bound request.
func ServiceCall[Rep Payload](r ServiceRequest[Rep]) (ServiceID, Payload) {
	return r.service, r.req
}
