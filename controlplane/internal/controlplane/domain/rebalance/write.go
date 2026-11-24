package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
)

// write is the one log write a group has in flight. The group is busy until
// its answer, or until its write timer ends the term (section 8).
type write struct {
	n       uint64 // its number: an answer to another write of the group is not its own
	kind    any    // installWrite, assignWrite or removeWrite: what to do with the answer
	dropped bool   // the caller abandoned the call: apply the answer, but send no reply
}

type (
	// installWrite ends a join round (rule 16).
	installWrite struct {
		op contract.GroupInstall
	}
	// assignWrite stores the leader's assignment (rule 22).
	assignWrite struct {
		call actor.ReplyTo[Reply]
		op   contract.GroupAssign
	}
	// removeWrite removes members (rules 12, 24, 26, 28). call and codes are
	// for a leave only.
	removeWrite struct {
		cause cause
		ids   []string
		call  actor.ReplyTo[Reply]
		codes []runtime.ErrCode
	}
)

// cause is why members are removed.
type cause uint8

const (
	causeLeave        cause = iota + 1 // a LeaveGroup call
	causeSession                       // a session timer
	causeRoundEnd                      // the round end
	causeSyncDeadline                  // the sync deadline
)

// caller returns the call that waits for w, if any.
func (w *write) caller() (actor.ReplyTo[Reply], bool) {
	switch k := w.kind.(type) {
	case assignWrite:
		return k.call, true
	case removeWrite:
		return k.call, k.cause == causeLeave
	}
	return actor.ReplyTo[Reply]{}, false
}

// startWrite logs op for g. g is busy until the answer.
func (m *Machine) startWrite(g *group, op runtime.ControlPlaneOperation, kind any) {
	w := &write{n: m.number(), kind: kind}
	name, n := g.name, w.n
	m.ask(op, true, func(done runtime.ControlPlaneDone) Input { return writeDone{group: name, n: n, done: done} })
	m.start(nil, writeKey(name), m.cfg.WriteTimeout, writeTimedOut{group: name})
	g.write = w
}

// writeDone takes the answer of a write. An answer that is not for the write
// in flight (a second copy) is ignored.
func (m *Machine) writeDone(s *serving, in writeDone) {
	g := s.groups[in.group]
	if g == nil || g.write == nil || g.write.n != in.n {
		return
	}
	m.duplicate(in)
	w := g.write
	g.write = nil
	m.stop(nil, writeKey(g.name))
	m.settle(s, g, w, in.done)
	if m.state == s {
		m.drain(s, g)
	}
}

// writeTimedOut ends the term when a write got no answer in time: the write
// may still change the log (rules 36, 38).
func (m *Machine) writeTimedOut(s *serving, in writeTimedOut) {
	g := s.groups[in.group]
	if g == nil || g.write == nil {
		return
	}
	w := g.write
	g.write = nil
	m.breakTerm(s, w)
}

// settle applies the answer of a write. OK: apply. ErrTooLarge or
// ErrInvalid: nothing was written, the caller gets the code. Anything else
// may have changed the log, or shows that memory and log differ: the term ends.
func (m *Machine) settle(s *serving, g *group, w *write, done runtime.ControlPlaneDone) {
	switch done.Err {
	case runtime.OK:
		m.applied(s, g, w)
	case runtime.ErrTooLarge, runtime.ErrInvalid:
		m.refused(s, g, w, done.Err)
	default:
		m.breakTerm(s, w)
	}
}

func (m *Machine) applied(s *serving, g *group, w *write) {
	switch k := w.kind.(type) {
	case installWrite:
		m.installed(g, k.op)
	case assignWrite:
		m.assigned(g, k, w.dropped)
	case removeWrite:
		m.removed(s, g, k, w.dropped)
	}
}

// refused gives the code of a refused write to its callers (rules 17 and 26).
func (m *Machine) refused(s *serving, g *group, w *write, code runtime.ErrCode) {
	switch k := w.kind.(type) {
	case installWrite:
		m.answerParked(g, g.phase.(*joining).joins, code, nil)
	case assignWrite:
		m.answerCaller(w, k.call, code, nil)
	case removeWrite:
		m.removeRefused(g, w, k, code)
	}
}

// answerCaller replies to the caller of w, unless it was abandoned.
func (m *Machine) answerCaller(w *write, call actor.ReplyTo[Reply], code runtime.ErrCode, body Answer) {
	if !w.dropped {
		m.reply(call, code, body)
	}
}

// drain takes the queued inputs of g in order, until one starts a write.
func (m *Machine) drain(s *serving, g *group) {
	if len(g.queue) > 0 && m.maybe(SiteLateDrain) {
		g.draining = true
		m.o.TellSelf(drainLater{group: g.name})
		return
	}
	for m.state == s && !g.busy() && len(g.queue) > 0 {
		in := g.queue[0]
		g.queue = slices.Delete(g.queue, 0, 1)
		m.handle(s, in)
	}
}

// drainNow takes the queue of a group whose drain was moved to a later
// Receive. Until then the group stays busy, so the order of its inputs is kept.
func (m *Machine) drainNow(s *serving, d drainLater) {
	if g := s.groups[d.group]; g != nil && g.draining {
		g.draining = false
		m.drain(s, g)
	}
}

// breakTerm ends the term after a write that may have changed the log (rule
// 36). w is the write that failed, already taken off its group. Every call
// gets ErrUnavailable, every timer and replica answer of the term is
// forgotten, memory is dropped, and every group is read again from a fresh tail.
func (m *Machine) breakTerm(s *serving, w *write) {
	m.dropMemory(s, w)
	m.startLoad(true)
}

// dropMemory forgets every timer and replica answer of the term and answers
// every waiting call with ErrUnavailable (rule 34). extra is a write that was
// already taken off its group. The watches of the callers stay: a reply
// removes them.
func (m *Machine) dropMemory(s *serving, extra *write) {
	m.o.ForgetPending()
	if extra != nil {
		if call, ok := extra.caller(); ok {
			m.answerCaller(extra, call, runtime.ErrUnavailable, nil)
		}
	}
	for _, name := range s.names {
		m.dropGroup(s.groups[name])
	}
	for _, d := range s.describes {
		m.reply(d.call, runtime.ErrUnavailable, nil)
	}
	for _, c := range s.commits {
		m.reply(c.call, runtime.ErrUnavailable, nil)
	}
	s.describes, s.commits = nil, nil
}

func (m *Machine) dropGroup(g *group) {
	if w := g.write; w != nil {
		if call, ok := w.caller(); ok {
			m.answerCaller(w, call, runtime.ErrUnavailable, nil)
		}
	}
	for _, mm := range g.members {
		if call, ok := g.parked(mm); ok {
			m.reply(call, runtime.ErrUnavailable, nil)
		}
	}
	for _, in := range g.queue {
		if call, ok := callOf(in); ok {
			m.reply(call, runtime.ErrUnavailable, nil)
		}
	}
}
