// Package rebalance is the consumer group protocol (classic Kafka) as a dst
// actor (actor.Actor[Input, actor.NoIO]). It has no locks, goroutines,
// channels or clock: time comes with each Receive, randomness comes from the
// actor.Env given to New, log writes and reads are asks to the replica, and
// their answers come back as the machine's own messages. Rule numbers in
// comments are the classic-group machine's own; see docs/architecture.md.
//
// The machine is Standby (it serves nothing), Loading (it reads every group
// from the log) or Serving. The log holds decisions only. Phases, parked
// calls and timers are memory only, and are dropped when the role is lost or
// when a write that may have changed the log fails (a broken term).
package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
)

// Config holds the machine settings. A zero field takes its default.
type Config struct {
	// InitialDelay is how long a new group waits for more members (default 3 s).
	InitialDelay actor.Duration
	// NewMemberTimeout is how long a new member may wait for its first round (default 5 min).
	NewMemberTimeout actor.Duration
	// WriteTimeout bounds one log write (default 10 s).
	WriteTimeout actor.Duration
	// LoadRetry is the wait before a refused load read is sent again (default 100 ms).
	LoadRetry actor.Duration
}

// outbox is the outbox of the machine.
type outbox = actor.Outbox[Input, actor.NoIO]

// Machine is the group coordinator of one node.
type Machine struct {
	cfg     Config
	env     runtime.Environment // random numbers and buggify sites; no site fires in production
	replica actor.EntityRef[runtime.EventData]
	state   any    // *standby, *loading or *serving
	next    uint64 // the number of the last write, commit or describe

	// Set for the length of one Receive only.
	now  actor.Mono
	wall actor.Wall
	o    *outbox
}

// standby serves nothing.
type standby struct{}

// serving is the memory of one term.
type serving struct {
	groups    map[string]*group
	names     []string // the group names, sorted
	describes []*describe
	commits   []*commit
}

var _ actor.Actor[Input, actor.NoIO] = (*Machine)(nil)

// Props runs the machine as the actor "rebalance". replica is the actor that
// logs and reads its decisions.
func Props(cfg Config, replica actor.EntityRef[runtime.EventData]) actor.Props[Input, actor.NoIO] {
	return actor.Props[Input, actor.NoIO]{
		Name: "rebalance",
		New: func(env actor.Env) actor.Actor[Input, actor.NoIO] {
			return New(cfg, env, replica)
		},
	}
}

// New returns a machine in Standby. env gives it random numbers and the buggify sites
// (sites.go); the env of production never fires a site.
func New(cfg Config, env runtime.Environment, replica actor.EntityRef[runtime.EventData]) *Machine {
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = 3 * actor.Second
	}
	if cfg.NewMemberTimeout <= 0 {
		cfg.NewMemberTimeout = 300 * actor.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * actor.Second
	}
	if cfg.LoadRetry <= 0 {
		cfg.LoadRetry = 100 * actor.Millisecond
	}
	return &Machine{cfg: cfg, env: env, replica: replica, state: &standby{}}
}

// Receive takes one message.
func (m *Machine) Receive(now actor.Now, msg Input, o *outbox) {
	m.now, m.wall, m.o = now.Mono, now.Wall, o
	defer func() { m.o = nil }()
	switch s := m.state.(type) {
	case *serving:
		m.serve(s, msg)
	case *loading:
		m.loadStep(s, msg)
	default:
		m.standbyStep(msg)
	}
}

func (m *Machine) standbyStep(inbox Input) {
	if r, ok := inbox.(RoleChanged); ok && r.Active {
		m.startLoad(false)
		return
	}
	m.refuse(inbox)
}

// refuse answers a call when the machine does not serve (rules 1 and 2).
func (m *Machine) refuse(inbox Input) {
	if j, ok := inbox.(Join); ok && invalidJoin(j) {
		m.reply(j.ReplyTo, runtime.ErrInvalid, nil)
		return
	}
	if call, ok := callOf(inbox); ok {
		m.reply(call, runtime.ErrUnavailable, nil)
	}
}

func (m *Machine) serve(s *serving, inbox Input) {
	switch in := inbox.(type) {
	case RoleChanged:
		if !in.Active {
			m.dropMemory(s, nil)
			m.state = &standby{}
		}
	case Join:
		if invalidJoin(in) {
			m.reply(in.ReplyTo, runtime.ErrInvalid, nil)
			return
		}
		m.groupCall(s, in.Group, in.ReplyTo, in)
	case Sync:
		m.groupCall(s, in.Group, in.ReplyTo, in)
	case Heartbeat:
		m.groupCall(s, in.Group, in.ReplyTo, in)
	case Leave:
		m.groupCall(s, in.Group, in.ReplyTo, in)
	case OffsetCommit:
		m.commit(s, in)
	case Describe:
		m.describe(s, in)
	case List:
		m.list(s, in)
	case callGone:
		m.abandon(s, in)
	case sessionExpired:
		m.groupInput(s, in.group, in)
	case roundDue:
		m.groupInput(s, in.group, in)
	case delayDone:
		m.groupInput(s, in.group, in)
	case writeTimedOut:
		m.writeTimedOut(s, in)
	case commitTimedOut:
		m.commitTimeout(s, in)
	case writeDone:
		m.writeDone(s, in)
	case commitDone:
		m.commitDone(s, in)
	case describeDone:
		m.describeDone(s, in)
	case drainLater:
		m.drainNow(s, in)
	case loadRetryDue, loadDone:
		// Answers and timers of a load: the load of this term is over.
	}
}

// groupCall handles a call for one group, and watches its caller when the
// call is kept past this Receive: parked, queued, or waiting on a write.
func (m *Machine) groupCall(s *serving, name string, call actor.ReplyTo[Reply], in Input) {
	m.groupInput(s, name, in)
	if m.state != s {
		return
	}
	if g := s.groups[name]; g != nil && g.keeps(call) {
		m.o.Watch(call, callGone{group: name, call: call})
	}
}

// groupInput handles an input for one group, or queues it while the group
// waits for a log write (section 8).
func (m *Machine) groupInput(s *serving, name string, in Input) {
	if g := s.groups[name]; g != nil && g.busy() {
		g.queue = append(g.queue, in)
		return
	}
	m.handle(s, in)
}

// handle runs one group input on a group that is not busy.
func (m *Machine) handle(s *serving, in Input) {
	switch in := in.(type) {
	case Join:
		m.join(s, in)
	case Sync:
		m.syncGroup(s, in)
	case Heartbeat:
		m.heartbeat(s, in)
	case Leave:
		m.leave(s, in)
	case sessionExpired:
		m.sessionTimer(s, in)
	case roundDue:
		if g := s.groups[in.group]; g != nil {
			m.roundTimer(s, g)
		}
	case delayDone:
		m.delayTimer(s, in)
	case callGone:
		m.abandonParked(s, in)
	case OffsetCommit, Describe, List, RoleChanged, drainLater, writeTimedOut, commitTimedOut,
		loadRetryDue, writeDone, commitDone, describeDone, loadDone:
		// Not a group input: groupInput never queues these.
	}
}

func invalidJoin(j Join) bool {
	return j.Group == "" || j.ProtocolType == "" || len(j.Protocols) == 0 || j.SessionTimeoutMs <= 0
}

// callOf returns the ReplyTo of a call.
func callOf(inbox Input) (actor.ReplyTo[Reply], bool) {
	switch in := inbox.(type) {
	case Join:
		return in.ReplyTo, true
	case Sync:
		return in.ReplyTo, true
	case Heartbeat:
		return in.ReplyTo, true
	case Leave:
		return in.ReplyTo, true
	case OffsetCommit:
		return in.ReplyTo, true
	case Describe:
		return in.ReplyTo, true
	case List:
		return in.ReplyTo, true
	case RoleChanged, drainLater, callGone, sessionExpired, roundDue, delayDone, writeTimedOut,
		commitTimedOut, loadRetryDue, writeDone, commitDone, describeDone, loadDone:
		return actor.ReplyTo[Reply]{}, false
	}
	return actor.ReplyTo[Reply]{}, false
}

// reply answers call.
func (m *Machine) reply(call actor.ReplyTo[Reply], err runtime.ErrCode, body Answer) {
	actor.Reply(m.o, call, Reply{Err: err, Body: body})
}

// ask sends op to the replica; adapt turns the answer into a message of the
// machine. A command, and a read with fresh, waits for a fresh check of the
// log tail (ControlPlaneDeliver); a read without fresh is a LocalRead.
func (m *Machine) ask(op runtime.ControlPlaneOperation, fresh bool, adapt func(runtime.ControlPlaneDone) Input) {
	if fresh {
		actor.Ask(m.o, m.replica, func(to actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData {
			return runtime.ControlPlaneDeliver{ReplyTo: to, Op: op}
		}, adapt)
		return
	}
	actor.Ask(m.o, m.replica, func(to actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData {
		return runtime.LocalRead{ReplyTo: to, Op: op}
	}, adapt)
}

// number returns a new number for a write, a commit or a describe.
func (m *Machine) number() uint64 {
	m.next++
	return m.next
}

// wallMs is the wall clock in milliseconds, for TimeMs in logged commands.
func (m *Machine) wallMs() int64 { return int64(m.wall) / int64(actor.Millisecond) }

// newServing returns empty memory for a new term.
func newServing() *serving {
	return &serving{groups: map[string]*group{}}
}

// newGroup adds an empty group to the memory.
func newGroup(s *serving, name string) *group {
	g := &group{name: name, phase: empty{}}
	s.groups[name] = g
	s.names = insertName(s.names, name)
	return g
}

// newMember adds a member to g.
func newMember(g *group, id string) *member {
	g.joinSeq++
	mm := &member{id: id, order: g.joinSeq}
	g.insert(mm)
	return mm
}

// keeps reports whether call waits in g: parked, queued, or the caller of
// the write in flight.
func (g *group) keeps(call actor.ReplyTo[Reply]) bool {
	if slices.ContainsFunc(g.queue, func(in Input) bool { c, ok := callOf(in); return ok && c == call }) {
		return true
	}
	if g.write != nil {
		if c, ok := g.write.caller(); ok && c == call {
			return true
		}
	}
	return g.parkedCall(call) != nil
}

// ms is a duration of n milliseconds.
func ms(n int32) actor.Duration { return actor.Duration(n) * actor.Millisecond }

// after is the time from now until at, never below zero.
func (m *Machine) after(at actor.Mono) actor.Duration {
	return actor.Duration(max(at-m.now, 0))
}

// describeOp is the read of one group.
func describeOp(name string) contract.DescribeGroup { return contract.DescribeGroup{Group: name} }
