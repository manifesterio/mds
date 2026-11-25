package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
	dstsim "github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/verify/check"
)

// The host of the rebalance actor (docs/architecture.md section 11). In
// production server.Node runs it beside the replica. Here each node gets a
// second actor ("rb-0" beside "cp-0") that crashes and restarts with the
// replica; a restart builds a new machine, so it starts with no memory. The
// host does what server.Node does, and nothing else:
//
//   - The rebalance actor asks the replica directly: a command or a fresh
//     read as a ControlPlaneDeliver, any other read as a LocalRead.
//   - After each Receive of the replica, a change of "active appender and
//     Ready" is told to the rebalance actor as RoleChanged (the AfterReceive
//     hook of the replica in production).
//   - A group call is an ask of the rebalance actor that ends at its
//     deadline. When the deadline passes first, the caller is gone: the
//     actor's Watch on the call fires, and a later reply is dropped.
//   - OffsetFetch is not a machine call: behind the role bit it is a
//     LocalRead of the replica.
//
// The host reads the effects of both actors for its checks and for the
// coverage notes: the machine has no assertions of its own.

// groupsName is the actor name of the rebalance actor of node idx.
func groupsName(idx int) string { return fmt.Sprintf("rb-%d", idx) }

// machineConfig is the machine config, with the times scaled down like replicaConfig.
// The buggify sites of the machine come from the env of the run, so they fire and are
// counted (production has an env where none fires).
func machineConfig() rebalance.Config {
	return rebalance.Config{
		InitialDelay:     300 * actor.Millisecond,
		NewMemberTimeout: 4 * actor.Second,
		WriteTimeout:     2 * actor.Second,
	}
}

// groupHost hosts the rebalance actor of one node and the group calls that
// wait for it. It lives for the whole run; each boot of the node gives it a
// new machine, and the bookkeeping below starts again.
type groupHost struct {
	s   *sim
	n   *simNode
	ref actor.EntityRef[rebalance.Input]

	role bool // the role bit: the replica is the active appender and Ready
	told bool // the role the machine was told last (the last RoleChanged it took)

	writes map[actor.ReplyID]sentWrite // commands asked of the replica and not answered yet
	terms  int                         // the terms of the machine that ended: a break, or a role loss
	reads  map[actor.ReplyID]readNote  // reads asked of the replica and not answered yet
	calls  map[actor.ReplyID]*groupCall
	order  []*groupCall // the calls of this boot, in the order they were sent

	// The timers of the machine, by the trace fold of the message they
	// deliver: every timer, and the ones armed right after a command (the
	// timer of a write or a commit).
	timers      map[uint64]bool
	writeTimers map[uint64]bool
}

// sentWrite is a command of the machine that the replica has not answered yet.
type sentWrite struct {
	group string // the group of a write of the membership; "" for an offset commit
	term  int    // the value of groupHost.terms when it was sent
}

// groupCall is one call to the machine.
type groupCall struct {
	try     *callTry
	replies int  // replies of the machine to the call
	gone    bool // its deadline passed before a reply: the caller is gone
	closed  bool // the node went down before a reply: the caller saw the connection close
}

// readNote is what the host knows about one read of the machine, for the
// coverage notes.
type readNote struct {
	list  bool // a ListGroups read
	load  bool // a read of the load (not of a Describe call)
	fresh bool
}

// newGroupHost spawns the rebalance actor of n. It is down until n boots it.
func newGroupHost(s *sim, n *simNode) *groupHost {
	h := &groupHost{s: s, n: n}
	h.reset()
	p := rebalance.Props(machineConfig(), n.ref)
	p.Name = groupsName(n.idx)
	build := p.New
	p.New = func(env actor.Env) actor.Actor[rebalance.Input, actor.NoIO] {
		h.reset()
		return build(env)
	}
	h.ref = dstsim.Spawn(s.w, p, dstsim.AfterReceive(h.afterReceive))
	return h
}

// reset starts the bookkeeping of a new boot. The machine has no memory: it
// is in Standby until the replica becomes active and Ready.
func (h *groupHost) reset() {
	h.role, h.told = false, false
	h.writes = map[actor.ReplyID]sentWrite{}
	h.reads = map[actor.ReplyID]readNote{}
	h.calls = map[actor.ReplyID]*groupCall{}
	h.order = nil
	h.timers = map[uint64]bool{}
	h.writeTimers = map[uint64]bool{}
	h.terms = 0
}

// stop ends the boot with the node. Every call that waits gets ErrUnavailable,
// as a client does when the connection closes.
func (h *groupHost) stop() {
	for _, gc := range h.order {
		if gc.replies > 0 || gc.gone || gc.closed {
			continue
		}
		gc.closed = true
		h.s.after(h.s.store.netDelay(h.s.rand), func() { gc.try.answered(runtime.ErrUnavailable, nil) })
	}
	h.reset()
}

// writing reports that a command of the machine still waits for the replica.
func (h *groupHost) writing() bool { return len(h.writes) > 0 }

// endTerm notes that the machine ended its term (or lost its role): it will
// not take the answers of the writes in flight.
func (h *groupHost) endTerm() { h.terms++ }

// freeze stops the machine for good: the final phase needs a log that no
// longer changes. A frozen machine takes no message: they wait in its inbox.
func (h *groupHost) freeze() { h.s.w.Pause(h.ref) }

// afterReplica runs after each Receive of the replica. A write of the
// membership that the replica applied after the term that sent it ended
// makes its group late (groupChecks.late). A read that came back is noted.
// Then, when "active appender and Ready" changed, the machine is told.
func (h *groupHost) afterReplica(effects []actor.Effect) {
	for _, e := range effects {
		if e.Kind != actor.EffectReply {
			continue
		}
		d, _ := e.Msg.(runtime.ControlPlaneDone)
		if w, ok := h.writes[e.Reply]; ok {
			delete(h.writes, e.Reply)
			if w.group != "" && w.term < h.terms && d.Err == runtime.OK {
				h.s.oracle.groups.late[w.group] = true
				h.s.tracef("%s: a write of group %s that the machine gave up applied late", h.n.name(), w.group)
			}
		}
		if r, ok := h.reads[e.Reply]; ok {
			delete(h.reads, e.Reply)
			h.noteRead(r, d)
		}
	}
	r := h.n.m
	active := r.IsActive() && r.Ready()
	if active == h.role {
		return
	}
	h.role = active
	dstsim.Tell(h.s.w, h.ref, 0, rebalance.Input(rebalance.RoleChanged{Active: active}))
}

// startCall is a group call that reached the node. It asks the machine,
// until the deadline of the try. A reply goes back to the consumer after a
// network delay; at the deadline the consumer sees a timeout.
func (h *groupHost) startCall(t *callTry) {
	if !h.n.up() {
		h.refuse(t)
		return
	}
	gc := &groupCall{try: t}
	t.quiet = h.s.quiet
	dstsim.Ask(h.s.w, h.ref, func(to actor.ReplyTo[rebalance.Reply]) rebalance.Input {
		h.calls[to.ID()] = gc
		h.order = append(h.order, gc)
		return t.op.input(to)
	}, t.deadline, func(r rebalance.Reply, ok bool) { h.done(gc, r, ok) })
}

// done is the end of the ask of a call: the machine's reply, or the deadline.
func (h *groupHost) done(gc *groupCall, r rebalance.Reply, ok bool) {
	if gc.closed {
		return // the node went down first and the call was answered then
	}
	if ok {
		h.s.after(h.s.store.netDelay(h.s.rand), func() { gc.try.answered(r.Err, r.Body) })
		return
	}
	gc.gone = true
	h.s.env.Reachable(callAbandoned)
	if t := gc.try; !t.onPurpose && t.quiet {
		check.Violation(h.s.env, everyCallGetsAnswer, "%s: %s sent a call in the quiet tail and had no answer after %dms",
			h.n.name(), t.c.name, t.deadline/actor.Millisecond)
	}
	gc.try.answered(runtime.ErrTimeout, nil)
}

// refuse answers a call to a node that is down, as a refused connection.
func (h *groupHost) refuse(t *callTry) {
	h.s.after(h.s.store.netDelay(h.s.rand), func() { t.answered(runtime.ErrUnavailable, nil) })
}

// fetch is the OffsetFetch handler: behind the role bit, a LocalRead of the
// replica. The answer goes back to the consumer after a network delay; the
// consumer keeps its own deadline, so the ask ends at it too.
func (h *groupHost) fetch(t *callTry, within actor.Duration) {
	if !h.n.up() || !h.role {
		h.refuse(t)
		return
	}
	s, n := h.s, h.n
	var prefix runtime.LogPosition
	dstsim.AskDelayed(s.w, n.ref, 0, func(to actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData {
		return runtime.LocalRead{ReplyTo: to, Op: t.op.fetch}
	}, within, func(d runtime.ControlPlaneDone) actor.Duration {
		prefix = n.m.Applied()
		return s.store.netDelay(s.rand)
	}, func(d runtime.ControlPlaneDone, ok bool) {
		if !ok {
			return
		}
		t.c.fetched(t, d, prefix)
	})
}

// receiveNotes is what the host found in the effects of one Receive of the machine.
type receiveNotes struct {
	freshList   bool // a fresh ListGroups read: the term ended
	sentRead    bool
	unavailable bool // a call was answered ErrUnavailable
}

// afterReceive runs after each Receive of the machine. It does the
// bookkeeping of the role and of the commands and reads in flight, checks
// two promises of the machine to its host (docs/architecture.md sections 3
// and 9): each call gets at most one reply, and no call is answered OK while
// the machine was last told that it does not have the role. Then it marks
// the paths that the coverage report lists.
func (h *groupHost) afterReceive(_ actor.Incarnation, in rebalance.Input, effects []actor.Effect) {
	role, isRole := in.(rebalance.RoleChanged)
	if isRole {
		h.told = role.Active
		if !role.Active {
			h.endTerm()
		}
	}
	k := foldOf(in)
	timer, writeTimer := h.timers[k], h.writeTimers[k]
	o := h.noteEffects(in, effects)
	env := h.s.env
	switch {
	case writeTimer && o.freshList:
		env.Reachable(writeTimerEndsTerm)
	case timer && o.sentRead:
		env.Reachable(refusedLoadReadSentAgain)
	case !timer && !fromOutside(in) && o.freshList:
		// Not a call, a RoleChanged or a timer: the replica's answer to a
		// write of the machine.
		env.Reachable(failedWriteEndsTerm)
	}
	if isRole && !role.Active && o.unavailable {
		env.Reachable(roleLostWhileCallsWait)
	}
}

// noteEffects goes through the effects of one Receive of the machine in
// order: the asks of the replica, the timers and the replies.
func (h *groupHost) noteEffects(in rebalance.Input, effects []actor.Effect) receiveNotes {
	var r receiveNotes
	_, describeCall := in.(rebalance.Describe)
	afterCommand := false
	for _, e := range effects {
		command := false
		switch e.Kind {
		case actor.EffectAsk:
			command = h.asked(e, !describeCall, &r)
		case actor.EffectStartTimer, actor.EffectAfter:
			k := foldOf(e.Msg)
			h.timers[k] = true
			if afterCommand {
				h.writeTimers[k] = true // the machine arms the timer of a write right after it
			}
		case actor.EffectReply:
			h.replied(e, &r)
		default:
		}
		afterCommand = command
	}
	return r
}

// asked notes one ask of the replica, and reports whether it is a command.
func (h *groupHost) asked(e actor.Effect, load bool, r *receiveNotes) bool {
	if q, ok := e.Msg.(runtime.LocalRead); ok {
		_, list := q.Op.(contract.ListGroups)
		h.reads[e.Reply] = readNote{list: list, load: load}
		r.sentRead = true
		return false
	}
	q, ok := e.Msg.(runtime.ControlPlaneDeliver)
	if !ok {
		return false
	}
	_, list := q.Op.(contract.ListGroups)
	_, describe := q.Op.(contract.DescribeGroup)
	if list || describe {
		h.reads[e.Reply] = readNote{list: list, load: load, fresh: true}
		r.sentRead = true
		if list {
			r.freshList = true
			h.endTerm() // a fresh load: the term ended
		}
		return false
	}
	h.writes[e.Reply] = sentWrite{group: groupOf(q.Op), term: h.terms}
	if rm, ok := q.Op.(contract.GroupRemove); ok && rm.Reason == "expire" {
		h.s.consumers.expiring(rm)
	}
	return true
}

// replied checks one reply of the machine to a call.
func (h *groupHost) replied(e actor.Effect, r *receiveNotes) {
	reply, _ := e.Msg.(rebalance.Reply)
	if reply.Err == runtime.OK && !h.told {
		check.Violation(h.s.env, machineAnswersOnlyWithRole, "%s: the machine answered call %d OK after it was told that it lost the role", h.n.name(), e.Reply.Number)
	}
	r.unavailable = r.unavailable || reply.Err == runtime.ErrUnavailable
	gc := h.calls[e.Reply]
	switch {
	case gc == nil:
		check.Violation(h.s.env, everyCallGetsOneAnswer, "%s: the machine answered call %d, which it never got", h.n.name(), e.Reply.Number)
		return
	case gc.replies > 0:
		check.Violation(h.s.env, everyCallGetsOneAnswer, "%s: the machine answered call %d twice", h.n.name(), e.Reply.Number)
	case gc.gone:
		h.s.env.Reachable(lateReplyAbandonedCallDropped)
	}
	gc.replies++
}

// noteRead marks the paths of a read that came back.
func (h *groupHost) noteRead(r readNote, d runtime.ControlPlaneDone) {
	if d.Err != runtime.OK {
		return
	}
	if r.list && r.fresh {
		h.s.env.Reachable(groupsReadAgainFromFreshTail)
	}
	if desc, ok := d.Resp.(contract.DescribeGroupResp); ok && r.load && !r.fresh && len(desc.Members) > 0 {
		h.s.env.Reachable(takeoverLoadsGroupMembers)
	}
}

// groupOf is the group of a write of the membership, "" for an offset commit.
func groupOf(op runtime.ControlPlaneOperation) string {
	switch w := op.(type) {
	case contract.GroupInstall:
		return w.Group
	case contract.GroupAssign:
		return w.Group
	case contract.GroupRemove:
		return w.Group
	}
	return ""
}

// fromOutside reports whether in came from outside the machine: a call or a
// RoleChanged. (Not a switch: a switch over the sum must list every case,
// and the machine's own messages are private to its package.)
func fromOutside(in rebalance.Input) bool {
	return is[rebalance.Join](in) || is[rebalance.Sync](in) || is[rebalance.Heartbeat](in) ||
		is[rebalance.Leave](in) || is[rebalance.OffsetCommit](in) || is[rebalance.Describe](in) ||
		is[rebalance.List](in) || is[rebalance.RoleChanged](in)
}

func is[T rebalance.Input](in rebalance.Input) bool {
	_, ok := in.(T)
	return ok
}

// foldOf is the trace fold of a message: two messages fold the same when
// they have the same type and the same fields. A timer delivers the message
// it was armed with, so the fold names the timer.
func foldOf(msg actor.Payload) uint64 {
	h := trace.New()
	msg.Fold(&h)
	return h.Sum()
}
