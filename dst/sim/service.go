package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/core/actor"
)

// Service simulates one outside system (an object store, a log, the control
// plane over gRPC). Req is what it accepts, Rep what it answers. Draw every
// fault coin and every delay in Handle, so draws happen in issue order.
// Never answer at once: every answer is reply.After(d, ...).
//
// Every call into a service clears its cached state hash. A panic in a
// service halts the run.
type Service[Req, Rep actor.Payload] interface {
	Handle(ctx *ServiceCtx, req Req, reply Responder[Rep])
	StateHash() uint64
}

// crashObserver is an optional Service extra: it hears that an actor
// incarnation ended. Effects already applied stand; Later steps the service
// queued are not dropped, so a write on the wire can still land.
type crashObserver interface {
	OnCrash(ctx *ServiceCtx, who actor.Incarnation)
}

// recoveryObserver is an optional Service extra: no new faults for requests
// issued after EnterRecovery. Plans already made stand.
type recoveryObserver interface {
	EnterRecovery()
}

// serviceSlot is one service, whatever its types.
type serviceSlot interface {
	base() *serviceBase
	stateHash() uint64
	handle(req actor.Payload, to responderTarget)
	onCrash(who actor.Incarnation)
	enterRecovery()
}

// serviceBase is the part of a service that does not depend on its types.
type serviceBase struct {
	sys  *System
	idx  int
	name string
	ctx  *ServiceCtx
	hashCache
}

type serviceHost[Req, Rep actor.Payload] struct {
	serviceBase
	svc Service[Req, Rep]
}

// AddService adds a service and returns its handle. A nil service or a name
// used twice is a protocol failure and gives the zero ServiceRef.
func AddService[Req, Rep actor.Payload](sys *System, name string, svc Service[Req, Rep]) actor.ServiceRef[Req, Rep] {
	switch {
	case svc == nil:
		sys.Fail(FailProtocol, "add_service", "a nil Service was added")
		return actor.ServiceRef[Req, Rep]{}
	case name == "" || sys.svcNames[name]:
		sys.Fail(FailProtocol, "add_service", fmt.Sprintf("service name %q is empty or used twice", name))
		return actor.ServiceRef[Req, Rep]{}
	}
	h := &serviceHost[Req, Rep]{serviceBase: serviceBase{sys: sys, idx: len(sys.services), name: name}, svc: svc}
	h.ctx = &ServiceCtx{env: env{sys: sys}, sys: sys, b: &h.serviceBase}
	sys.svcNames[name] = true
	sys.services = append(sys.services, h)
	return actor.NewServiceRef[Req, Rep](actor.ServiceID{Runtime: sys.owner, Slot: uint32(h.idx), Name: name})
}

func (h *serviceHost[Req, Rep]) base() *serviceBase { return &h.serviceBase }

func (h *serviceHost[Req, Rep]) stateHash() uint64 { return h.svc.StateHash() }

func (h *serviceHost[Req, Rep]) handle(req actor.Payload, to responderTarget) {
	sys := h.sys
	r, ok := actor.As[Req](req)
	if !ok {
		sys.Fail(FailProtocol, "wrong_request_type", fmt.Sprintf("service %s: got %T", h.name, req))
		return
	}
	h.invalidateHash()
	reply := Responder[Rep]{sys: sys, source: "service/" + h.name, to: to, answered: new(bool)}
	if !sys.guard("Handle "+h.name, func() { h.svc.Handle(h.ctx, r, reply) }) {
		sys.halted = true
	}
}

func (h *serviceHost[Req, Rep]) onCrash(who actor.Incarnation) {
	if o, ok := h.svc.(crashObserver); ok {
		h.invalidateHash()
		if !h.sys.guard("OnCrash "+h.name, func() { o.OnCrash(h.ctx, who) }) {
			h.sys.halted = true
		}
	}
}

func (h *serviceHost[Req, Rep]) enterRecovery() {
	if o, ok := h.svc.(recoveryObserver); ok {
		o.EnterRecovery()
	}
}

// callService hands req to service id; its answer goes to `to`.
func (sys *System) callService(id actor.ServiceID, req actor.Payload, to responderTarget) {
	switch {
	case id.Runtime == nil:
		sys.Fail(FailProtocol, "zero address", "a zero ServiceRef was used")
	case id.Runtime != sys.owner || int(id.Slot) >= len(sys.services):
		sys.Fail(FailProtocol, "foreign address", "a ServiceRef of another System was used")
	default:
		sys.services[id.Slot].handle(req, to)
	}
}

// responderTarget is who waits for an answer: one awaited number of one
// incarnation. The zero target waits for nothing (TellService, Do).
type responderTarget struct {
	set    bool
	idx    int
	inc    actor.ID
	number uint64
}

// Responder answers one request. It holds the caller's identity and number;
// the service never sees them.
type Responder[R actor.Payload] struct {
	sys      *System
	source   string
	to       responderTarget
	answered *bool
}

// Wanted reports whether anyone still waits for this answer: a caller asked
// for it, and the caller's incarnation is still up.
func (r Responder[R]) Wanted() bool {
	return r.to.set && r.sys.actors[r.to.idx].base().alive(r.to.inc)
}

// After answers after d. It is dropped if nobody waits (TellService), or if
// the caller is gone or has forgotten the call by then. A second answer is a
// protocol failure.
func (r Responder[R]) After(d actor.Duration, answer R) {
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
		sys.dropped(dropUnwanted, r.source)
		return
	}
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

// ServiceCtx is lent to Handle, OnCrash and Later steps. It is the run's
// Rand, Faults and Assertions.
type ServiceCtx struct {
	env
	sys *System
	b   *serviceBase
}

// Now is the virtual time.
func (c *ServiceCtx) Now() actor.Mono { return actor.Mono(c.sys.q.now) }

// Later runs fn as a later step of this service after d. A Later step is not
// dropped when an actor crashes.
func (c *ServiceCtx) Later(d actor.Duration, fn func(ctx *ServiceCtx)) {
	sys := c.sys
	e := &entry{at: sys.q.now + Time(max(d, 0)), lane: NoLane, kind: kindServiceStep, node: -1, service: c.b.name}
	e.run = func() {
		sys.counters[countServiceSteps]++
		sys.logMsg("service "+c.b.name+" later", nil)
		c.b.invalidateHash()
		if !sys.guard("Later "+c.b.name, func() { fn(c) }) {
			sys.halted = true
			return
		}
		sys.attr++
	}
	sys.q.add(e)
}

// RecordFault records an injected fault: a "sim_fault" fact with the kind
// and the service name, then the extra fields. (Fault, from the embedded
// Faults, is the buggify coin.)
func (c *ServiceCtx) RecordFault(kind string, fields ...actor.Field) {
	all := append([]actor.Field{
		{Key: "service", Value: actor.StringValue(c.b.name)},
	}, fields...)
	c.sys.RecordFault(kind, all...)
}
