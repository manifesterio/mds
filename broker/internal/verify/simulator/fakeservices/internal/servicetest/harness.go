// Package servicetest is a small test System for the fake services. It is a
// real sim.System with a few caller actors. A test makes a caller perform a
// request, runs the System, and reads the answers the callers got.
//
// The package does not import "testing", so that it can be used by the
// determinism gate; tests give a TB.
package servicetest

//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/sim"
)

// TB is the part of testing.TB that the harness needs.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
	Cleanup(fn func())
}

// Delivery is one answer that reached a caller.
type Delivery struct {
	At    sim.Time
	To    string
	Label uint64 // the label the test gave the request
	Body  actor.Payload
}

// callerMsg is what a caller receives: a command of the test, or the answer
// of a request it performed.
//
//sumtype:decl
//dst:sum
type callerMsg interface {
	actor.Payload
	isCallerMsg()
}

//dst:cases callerMsg
type (
	// command makes the caller perform IO, labelled Label.
	command struct {
		Label uint64
		IO    core.IO
	}
	// answered is the answer of the request labelled Label.
	answered struct {
		Label uint64
		Body  actor.Payload
	}
)

// Harness is a System with caller actors.
type Harness struct {
	t TB
	// Sys is the System. Add services to it with sim.AddService, and set
	// Objects and ControlPlane to the services the callers use.
	Sys *sim.System
	// Objects takes the object store calls of the callers.
	Objects actor.ServiceRef[core.ObjectCall, core.ObjectDone]
	// ControlPlane takes the control-plane calls of the callers.
	ControlPlane actor.ServiceRef[core.ControlPlaneCall, core.ControlPlaneDone]
	// Delivered lists every answer that reached a caller, in order.
	Delivered []Delivery
	// Seed is the seed of the System.
	Seed uint64

	refs    map[string]actor.EntityRef[callerMsg]
	waiting int // commands told and not yet received
}

// New makes a System for seed with one caller actor for each name, and boots
// the callers. When the test ends, the harness finishes the System and
// reports any failure of the run (a panic, a broken Always).
func New(t TB, seed uint64, names ...string) *Harness {
	t.Helper()
	h := &Harness{t: t, Sys: sim.NewSystem(sim.Config{Seed: seed}), Seed: seed, refs: map[string]actor.EntityRef[callerMsg]{}}
	for _, n := range names {
		name := n
		h.refs[n] = sim.Spawn(h.Sys, actor.Props[callerMsg, core.IO]{
			Name: name,
			New:  func(actor.Env) actor.Actor[callerMsg, core.IO] { return &caller{h: h, name: name} },
		}, sim.WithExecutor(h.exec))
	}
	h.Sys.Start()
	t.Cleanup(func() {
		for _, f := range h.Sys.Finish().Failures {
			t.Errorf("run failure: %s: %s", f.Property, f.Detail)
		}
	})
	return h
}

// exec hands each request to its service.
func (h *Harness) exec(ctx *sim.ExecCtx, io core.IO) {
	switch x := io.(type) {
	case core.ObjectCall:
		sim.Forward(ctx, x, h.Objects)
	case core.ControlPlaneCall:
		sim.Forward(ctx, x, h.ControlPlane)
	case core.Connect, core.Send, core.CloseConn, core.PauseReads, core.ResumeReads:
		h.t.Fatalf("servicetest: no service for %T", io)
	}
}

// caller performs the IO of each command and records each answer.
type caller struct {
	h    *Harness
	name string
}

func (c *caller) Receive(_ actor.Now, msg callerMsg, o *actor.Outbox[callerMsg, core.IO]) {
	switch m := msg.(type) {
	case command:
		c.h.waiting--
		label := m.Label
		switch x := m.IO.(type) {
		case core.ObjectCall:
			actor.Perform(o, x, func(d core.ObjectDone) callerMsg { return answered{Label: label, Body: d} })
		case core.ControlPlaneCall:
			actor.Perform(o, x, func(d core.ControlPlaneDone) callerMsg { return answered{Label: label, Body: d} })
		case core.Connect, core.Send, core.CloseConn, core.PauseReads, core.ResumeReads:
			o.Do(m.IO)
		}
	case answered:
		c.h.Delivered = append(c.h.Delivered, Delivery{At: c.h.Sys.Now(), To: c.name, Label: m.Label, Body: m.Body})
	}
}

// StateHash is zero: a caller has no state that matters.
func (*caller) StateHash() uint64 { return 0 }

// Now is the virtual time.
func (h *Harness) Now() sim.Time { return h.Sys.Now() }

// Do makes caller `from` perform io, labelled label, at the current time. It
// is a command to the caller, so it runs when the System runs.
func (h *Harness) Do(from string, label uint64, io core.IO) { h.DoAfter(0, from, label, io) }

// DoAfter makes caller `from` perform io, labelled label, after delay d.
func (h *Harness) DoAfter(d actor.Duration, from string, label uint64, io core.IO) {
	h.waiting++
	sim.Tell(h.Sys, h.refs[from], d, callerMsg(command{Label: label, IO: io}))
}

// Issue makes caller `from` perform io now, and takes queue entries until it
// did. The service has drawn its random values for io when Issue returns, as
// if the service had been called directly at the current time.
func (h *Harness) Issue(from string, label uint64, io core.IO) {
	h.t.Helper()
	h.Do(from, label, io)
	for h.waiting > 0 {
		if !h.Step() {
			h.t.Fatalf("servicetest: the command of %s was never taken (the System halted?)", from)
		}
	}
}

// Run takes queue entries until the queue is empty.
func (h *Harness) Run() {
	h.t.Helper()
	h.Sys.RunUntil(nil)
}

// RunTo takes queue entries up to time at, and leaves the clock at at.
// Entries at exactly that time that come after the stop mark stay in the
// queue.
func (h *Harness) RunTo(at sim.Time) {
	h.t.Helper()
	reached := false
	h.Sys.At(at, func() { reached = true })
	h.Sys.RunUntil(func() bool { return reached })
}

// RunFor is RunTo(Now()+d).
func (h *Harness) RunFor(d actor.Duration) { h.RunTo(h.Now() + sim.Time(d)) }

// Step takes exactly one queue entry. It reports false when the queue is empty.
func (h *Harness) Step() bool {
	start := h.Sys.Steps()
	h.Sys.RunUntil(func() bool { return h.Sys.Steps() > start })
	return h.Sys.Steps() > start
}

// Crash ends the live incarnation of a caller. The services are told.
func (h *Harness) Crash(name string) { h.Sys.Crash(h.refs[name], "test") }

// Boot starts a new incarnation of a caller that is down.
func (h *Harness) Boot(name string) { h.Sys.Boot(h.refs[name]) }

// Recover moves the System to PhaseHeal. The System then calls EnterRecovery
// on every service: no new faults for requests issued after it.
func (h *Harness) Recover() { h.Sys.SetPhase(sim.PhaseHeal) }

// Faults lists the kinds of the faults that services injected, in order.
func (h *Harness) Faults() []string {
	var kinds []string
	for _, f := range h.Sys.Timeline().Snapshot("sim_fault") {
		if k, ok := f.Fact.Field("kind").(actor.StringValue); ok {
			kinds = append(kinds, string(k))
		}
	}
	return kinds
}

// Hit reports whether the assertion with that name passed at least once (a
// Reachable that was reached, a Sometimes that was true).
func (h *Harness) Hit(name string) bool {
	c, ok := h.Sys.Coverage()[name]
	return ok && c.Passes > 0
}

// Find returns the answer of the request labelled label (nil if none).
func (h *Harness) Find(label uint64) actor.Payload {
	for _, d := range h.Delivered {
		if d.Label == label {
			return d.Body
		}
	}
	return nil
}

// Trace renders the deliveries as text, for equality checks.
func (h *Harness) Trace() string {
	var s strings.Builder
	for _, d := range h.Delivered {
		fmt.Fprintf(&s, "%d %s %d %#v\n", d.At, d.To, d.Label, d.Body)
	}
	return s.String()
}
