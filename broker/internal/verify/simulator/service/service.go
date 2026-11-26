// Package service simulates outside systems (an object store, the control
// plane over gRPC) behind an executor. A Service handles the requests an
// executor forwards to it and answers each through the sim.Reply of the
// Perform that issued it. The kernel knows nothing of services: Add folds the
// service's state hash into the trace with sim.System.Track, and Forward is
// sim.Hold plus Handle.
//
// Draw every fault coin and every delay in Handle, so draws happen in issue
// order. Never answer at once: every answer is reply.After(d, ...). A panic
// in Handle or in a Later step halts the run.
package service

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/sim/kit"
)

// Service simulates one outside system. Req is what it accepts, Rep what it
// answers.
type Service[Req, Rep actor.Payload] interface {
	Handle(ctx *Ctx, req Req, reply sim.Reply[Rep])
	StateHash() uint64
}

// Ctx is lent to Handle and to Later steps. Env gives the run's Rand, Faults
// and Assertions.
type Ctx struct {
	sys     *sim.System
	name    string
	tracked *sim.Tracked
}

// Env is the run's Rand, Faults and Assertions.
func (c *Ctx) Env() actor.Env { return c.sys.Env() }

// Now is the virtual time.
func (c *Ctx) Now() actor.Mono { return actor.Mono(c.sys.Now()) }

// Later runs fn as a later step of this service after d. A Later step is not
// dropped when an actor crashes.
func (c *Ctx) Later(d actor.Duration, fn func(ctx *Ctx)) {
	c.sys.After(d, func() {
		c.tracked.Touch()
		fn(c)
	})
}

// RecordFault records an injected fault: a "sim_fault" fact with the kind
// and the service name, then the extra fields. (Env().Fault is the buggify
// coin.)
func (c *Ctx) RecordFault(kind string, fields ...actor.Field) {
	all := append([]actor.Field{
		{Key: "service", Value: actor.StringValue(c.name)},
	}, fields...)
	kit.RecordFault(c.sys, kind, all...)
}

// Endpoint is a Service added to a System: what an executor forwards to.
type Endpoint[Req, Rep actor.Payload] struct {
	svc Service[Req, Rep]
	ctx *Ctx
}

// Add adds svc to sys under name and tracks its state hash in the trace. A
// nil service or a name that is empty or used twice is a protocol failure.
func Add[Req, Rep actor.Payload](sys *sim.System, name string, svc Service[Req, Rep]) *Endpoint[Req, Rep] {
	if svc == nil {
		sys.Fail(sim.FailProtocol, "add_service", "a nil Service was added")
		return &Endpoint[Req, Rep]{}
	}
	ctx := &Ctx{sys: sys, name: name}
	ctx.tracked = sys.Track(name, svc.StateHash)
	return &Endpoint[Req, Rep]{svc: svc, ctx: ctx}
}

// Forward hands the Perform that issued req to the service; the service's
// answer becomes the Perform's answer. The answer of a Do is dropped.
func Forward[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *sim.ExecCtx, req Req, to *Endpoint[Req, Rep]) {
	reply := sim.Hold(c, req)
	if to.svc == nil {
		return // Add already failed the run
	}
	to.ctx.tracked.Touch()
	to.svc.Handle(to.ctx, req, reply)
}

// EnterRecovery tells the service, if it wants to know (it has an
// EnterRecovery method), that no new faults are due for requests issued from
// now on. Plans already made stand.
func (e *Endpoint[Req, Rep]) EnterRecovery() {
	if o, ok := e.svc.(interface{ EnterRecovery() }); ok {
		e.ctx.tracked.Touch()
		o.EnterRecovery()
	}
}
