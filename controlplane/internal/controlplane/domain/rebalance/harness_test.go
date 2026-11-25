package rebalance

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/actor/actortest"
	"github.com/manifesterio/mds/dst/core/rng"
	"github.com/manifesterio/mds/dst/core/trace"
)

// The tests drive the machine with messages at explicit times and look at
// its replies. The harness plays the runtime: it runs each Receive on a fresh
// Outbox and carries out the effects. It keeps the keyed timers and fires
// them when the test moves the clock, answers each ask of the replica from a
// fake log (the metadata model of spec), at once unless hold is set, and
// forgets timers, self messages and answers on ForgetPending. A call is a
// number n; its ReplyTo is rt(n).

const (
	seed  = 7
	wall0 = 1000 * actor.Second
	sec   = actor.Second
	msec  = actor.Millisecond
	delay = 3 * actor.Second // the default initial delay
)

// rt is the ReplyTo of call n, as an outside caller's.
func rt(n uint64) actor.ReplyTo[Reply] {
	return actor.NewReplyTo[Reply](actor.ReplyID{Asker: actor.Asker{Kind: actor.SimOutsideAsker, Name: "caller"}, Number: n})
}

// gone is the message the machine gets when the caller of call n leaves.
// group is the group of the call, "" for a describe or a commit.
func gone(group string, n uint64) callGone { return callGone{group: group, call: rt(n)} }

// newID draws an id from r the way the simulator does: 16 bytes of the stream.
func newID(r *rng.Rand) actor.ID {
	var id actor.ID
	r.Bytes(id[:])
	return id
}

// armed is one keyed timer the machine started.
type armed struct {
	key   actor.TimerKey
	at    actor.Mono
	seq   int
	msg   Input
	epoch int
}

// queued is one message in the inbox. A message of the machine itself (self
// send, answer) dies with a ForgetPending; one from the test does not.
type queued struct {
	msg   Input
	own   bool
	epoch int
}

// asked is one ask of the replica that is not answered yet.
type asked struct {
	op    runtime.ControlPlaneOperation
	fresh bool // a ControlPlaneDeliver, not a LocalRead
	adapt func(actor.Payload) actor.Payload
	epoch int
}

// replied is one Reply the machine sent: the call number and the reply.
type replied struct {
	call uint64
	r    Reply
}

type harness struct {
	t         *testing.T
	m         *Machine
	replica   actor.EntityRef[runtime.EventData]
	now       actor.Mono
	log       *spec.ControlPlane
	env       *actortest.Env
	timers    []armed
	seq       int
	epoch     int // bumped by every ForgetPending
	asks      uint64
	inbox     []queued
	hold      bool    // keep asks in held for the test to answer
	held      []asked // asks not answered yet
	watched   map[actor.ReplyID]Input
	got       []replied
	effects   []actor.Effect // every effect of every Receive
	suffix    []string
	session   int32 // session timeout of the joins the harness makes
	rebalance int32 // rebalance timeout; 0 means the session timeout
}

// newHarness starts a machine over an empty log and makes it serve.
func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	return newHarnessFaults(t, cfg, nil)
}

// newHarnessFaults is newHarness with a fault source for the buggify sites.
func newHarnessFaults(t *testing.T, cfg Config, faults actor.Faults) *harness {
	t.Helper()
	h := newBareHarness(t, cfg, faults)
	h.session = 10000
	h.step(RoleChanged{Active: true})
	if h.phase("x") != stateDead {
		t.Fatal("the machine does not serve after the load")
	}
	return h
}

// newBareHarness makes a harness and its machine. The machine has not taken any message yet.
func newBareHarness(t *testing.T, cfg Config, faults actor.Faults) *harness {
	t.Helper()
	env := actortest.NewEnv(actortest.Config{
		Self: actor.Incarnation{Name: "rebalance"}, Rand: rng.New(seed), Faults: faults,
	})
	replica := actor.NewEntityRef[runtime.EventData](actor.EntityID{Runtime: actor.NewOwner(), Slot: 1, Name: "replica"})
	h := &harness{t: t, m: New(cfg, env, replica), replica: replica, env: env, log: spec.New(), watched: map[actor.ReplyID]Input{}}
	shadow := rng.New(seed)
	for range 64 {
		h.suffix = append(h.suffix, newMemberID("", newID(shadow)))
	}
	return h
}

// id is the id of the k-th new member (counted from 1) with no client id.
func (h *harness) id(k int) string { return h.suffix[k-1] }

// at is the time of the next Receive. The wall clock is the node clock plus wall0.
func (h *harness) at() actor.Now {
	return actor.Now{Mono: h.now, Wall: actor.Wall(wall0) + actor.Wall(h.now)}
}

// step delivers one message and every message that follows from it (answers
// of the log, sends to itself), and returns the replies, shown short. A
// callGone must be the gone message of a watch that is still on.
func (h *harness) step(in Input) []string {
	h.t.Helper()
	if g, ok := in.(callGone); ok {
		if w, on := h.watched[g.call.ID()]; !on || w != in {
			h.t.Fatalf("call %d is not watched as %+v (watch: %+v)", g.call.ID().Number, in, w)
		}
		delete(h.watched, g.call.ID())
	}
	h.inbox = append(h.inbox, queued{msg: in})
	for len(h.inbox) > 0 {
		next := h.inbox[0]
		h.inbox = h.inbox[1:]
		if next.own && next.epoch != h.epoch {
			continue
		}
		h.receive(next.msg)
	}
	return h.take()
}

// receive runs one Receive and carries out its effects in order.
func (h *harness) receive(in Input) {
	effects := actortest.Run(h.t, &h.asks, actor.Binding{
		Self:  actor.EntityID{Name: "rebalance"},
		Asker: actor.Asker{Kind: actor.ActorAsker, Name: "rebalance"},
		Misuse: func(what string) {
			h.t.Fatalf("misuse: %s", what)
		},
	}, func(o *actor.Outbox[Input, actor.NoIO]) {
		h.m.Receive(h.at(), in, o)
	})
	for _, e := range effects {
		h.effect(e)
	}
}

func (h *harness) effect(e actor.Effect) {
	h.effects = append(h.effects, e)
	switch e.Kind {
	case actor.EffectReply:
		r, ok := actor.As[Reply](e.Msg)
		if !ok {
			h.t.Fatalf("a reply that is not a Reply: %T", e.Msg)
		}
		h.got = append(h.got, replied{call: e.Reply.Number, r: r})
		delete(h.watched, e.Reply)
	case actor.EffectWatch:
		h.watched[e.Reply] = h.input(e.Msg)
	case actor.EffectStartTimer:
		h.cancel(e.Key)
		h.seq++
		h.timers = append(h.timers, armed{key: e.Key, at: h.now + actor.Mono(e.D), seq: h.seq, msg: h.input(e.Msg), epoch: h.epoch})
	case actor.EffectCancelTimer:
		h.cancel(e.Key)
	case actor.EffectTellSelf:
		h.inbox = append(h.inbox, queued{msg: h.input(e.Msg), own: true, epoch: h.epoch})
	case actor.EffectForgetPending:
		h.epoch++
		h.timers = nil
	case actor.EffectAsk:
		a := h.ask(e)
		if h.hold {
			h.held = append(h.held, a)
			return
		}
		h.inbox = append(h.inbox, queued{msg: h.apply(a), own: true, epoch: a.epoch})
	default:
		h.t.Fatalf("unexpected effect %d", e.Kind)
	}
}

func (h *harness) input(p actor.Payload) Input {
	in, ok := actor.As[Input](p)
	if !ok {
		h.t.Fatalf("not an Input: %T", p)
	}
	return in
}

// ask reads the operation of an ask of the replica.
func (h *harness) ask(e actor.Effect) asked {
	if e.To != h.replica.EntityID() {
		h.t.Fatalf("an ask to %v", e.To)
	}
	a := asked{adapt: e.Adapt, epoch: h.epoch}
	switch req := e.Msg.(type) {
	case runtime.ControlPlaneDeliver:
		a.op, a.fresh = req.Op, true
	case runtime.LocalRead:
		a.op = req.Op
	default:
		h.t.Fatalf("an ask of %T", e.Msg)
	}
	return a
}

func (h *harness) cancel(key actor.TimerKey) {
	h.timers = slices.DeleteFunc(h.timers, func(a armed) bool { return a.key == key })
}

// pending reports whether the timer key is set.
func (h *harness) pending(key actor.TimerKey) bool {
	return slices.ContainsFunc(h.timers, func(a armed) bool { return a.key == key })
}

// apply runs an ask on the log and returns the answer as the machine's message.
func (h *harness) apply(a asked) Input {
	done := h.log.Apply(a.op)
	return h.input(a.adapt(runtime.ControlPlaneDone{Resp: done.Resp, Err: done.Err, Outcome: done.Outcome}))
}

// answerMsg removes the i-th held ask, runs it on the log, and returns its
// answer as the machine's message.
func (h *harness) answerMsg(i int) Input {
	a := h.held[i]
	h.held = slices.Delete(h.held, i, i+1)
	return h.apply(a)
}

// answer answers the i-th held ask and removes it. With err OK the log runs
// it; with another code the log does not, and err is the answer. An answer
// to an ask that was forgotten is dropped, as the runtime drops it.
func (h *harness) answer(i int, err runtime.ErrCode) []string {
	a := h.held[i]
	h.held = slices.Delete(h.held, i, i+1)
	var msg Input
	if err == runtime.OK {
		msg = h.apply(a)
	} else {
		msg = h.input(a.adapt(runtime.ControlPlaneDone{Err: err, Outcome: runtime.MaybeApplied}))
	}
	if a.epoch != h.epoch {
		return nil
	}
	h.inbox = append(h.inbox, queued{msg: msg, own: true, epoch: a.epoch})
	for len(h.inbox) > 0 {
		next := h.inbox[0]
		h.inbox = h.inbox[1:]
		if next.own && next.epoch != h.epoch {
			continue
		}
		h.receive(next.msg)
	}
	return h.take()
}

// until moves the clock to at (from the start), firing every due timer in
// time order, and returns the replies.
func (h *harness) until(at actor.Duration) []string {
	h.t.Helper()
	target := actor.Mono(at)
	if target < h.now {
		h.t.Fatalf("time goes back: %d < %d", target, h.now)
	}
	var out []string
	for {
		i := h.due(target)
		if i < 0 {
			break
		}
		a := h.timers[i]
		h.timers = slices.Delete(h.timers, i, i+1)
		h.now = a.at
		out = append(out, h.step(a.msg)...)
	}
	h.now = target
	return out
}

func (h *harness) due(target actor.Mono) int {
	best := -1
	for i, a := range h.timers {
		if a.at <= target && (best < 0 || a.at < h.timers[best].at || (a.at == h.timers[best].at && a.seq < h.timers[best].seq)) {
			best = i
		}
	}
	return best
}

// take returns the replies so far, shown short, and forgets them.
func (h *harness) take() []string {
	out := make([]string, 0, len(h.got))
	for _, r := range h.got {
		out = append(out, h.show(r.call, r.r))
	}
	h.got = nil
	return out
}

// step is one line of a table: move the clock to at, then deliver in (nil:
// none), and expect exactly want (in any order) from both.
type step struct {
	at   actor.Duration
	in   Input
	want []string
}

func (h *harness) run(steps []step) {
	h.t.Helper()
	for i, st := range steps {
		got := h.until(st.at)
		if st.in != nil {
			got = append(got, h.step(st.in)...)
		}
		if !sameSet(got, st.want) {
			h.t.Fatalf("step %d (at %v, %T):\n got  %q\n want %q", i, st.at, st.in, got, st.want)
		}
	}
}

func sameSet(a, b []string) bool {
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}

// show prints a reply short: "call code body", with member ids as m1, m2, ...
func (h *harness) show(call uint64, r Reply) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %v", call, r.Err)
	switch a := r.Body.(type) {
	case JoinAnswer:
		fmt.Fprintf(&b, " gen=%d proto=%s leader=%s id=%s", a.Generation, a.Protocol, a.Leader, a.MemberID)
		if a.Members != nil {
			var ms []string
			for _, mm := range a.Members {
				ms = append(ms, h.short(mm.ID)+":"+string(mm.Metadata))
			}
			fmt.Fprintf(&b, " members=%v", sortedShort(ms))
		}
	case SyncAnswer:
		fmt.Fprintf(&b, " assign=%s", a.Assignment)
	case LeaveAnswer:
		fmt.Fprintf(&b, " %v", a.Errs)
	case CommitAnswer:
		fmt.Fprintf(&b, " %v", a.Errs)
	case DescribeAnswer:
		for _, d := range a.Groups {
			var ms []string
			for _, mm := range d.Members {
				ms = append(ms, mm.MemberID)
			}
			fmt.Fprintf(&b, " %s:%s gen=%d assigned=%d members=%v", d.Name, d.State, d.Generation, d.AssignedGeneration, ms)
		}
	case ListAnswer:
		for _, g := range a.Groups {
			fmt.Fprintf(&b, " %s:%s:%s", g.Name, g.ProtocolType, g.State)
		}
	}
	return h.short(b.String())
}

func sortedShort(s []string) []string {
	slices.Sort(s)
	return s
}

// short replaces each random member id part by m1, m2, ...
func (h *harness) short(s string) string {
	for k, x := range h.suffix {
		s = strings.ReplaceAll(s, x, fmt.Sprintf("m%d", k+1))
	}
	return s
}

// replies returns every Reply the machine sent, in order.
func (h *harness) replies() []Reply {
	var out []Reply
	for _, e := range h.effects {
		if r, ok := actor.As[Reply](e.Msg); ok && e.Kind == actor.EffectReply {
			out = append(out, r)
		}
	}
	return out
}

// effectHash folds every effect so far, for comparing two runs.
func (h *harness) effectHash() uint64 {
	th := trace.New()
	for _, e := range h.effects {
		e.Fold(&th)
	}
	return th.Sum()
}

// The calls, with the harness's session and rebalance timeouts.

func (h *harness) join(call uint64, group, member string, protos ...string) Join {
	j := Join{ReplyTo: rt(call), Group: group, MemberID: member, SessionTimeoutMs: h.session, RebalanceTimeoutMs: h.session, ProtocolType: "consumer"}
	if h.rebalance > 0 {
		j.RebalanceTimeoutMs = h.rebalance
	}
	if len(protos) == 0 {
		protos = []string{"range"}
	}
	for _, p := range protos {
		j.Protocols = append(j.Protocols, Protocol{Name: p, Metadata: []byte("meta-" + p)})
	}
	return j
}

func syncReq(call uint64, group, member string, gen int32, assigns ...Assignment) Sync {
	return Sync{ReplyTo: rt(call), Group: group, MemberID: member, Generation: gen, Assignments: assigns}
}

func hb(call uint64, group, member string, gen int32) Heartbeat {
	return Heartbeat{ReplyTo: rt(call), Group: group, MemberID: member, Generation: gen}
}

func leave(call uint64, group string, members ...string) Leave {
	return Leave{ReplyTo: rt(call), Group: group, Members: members}
}

func as(member, bytes string) Assignment { return Assignment{Member: member, Bytes: []byte(bytes)} }

// What the memory and the log hold.

func (h *harness) phase(group string) string {
	switch s := h.m.state.(type) {
	case *serving:
		return stateOf(s, group)
	case *loading:
		return "loading"
	}
	return "standby"
}

func (h *harness) group(group string) *group {
	s, ok := h.m.state.(*serving)
	if !ok || s.groups[group] == nil {
		h.t.Fatalf("no group %q in memory", group)
	}
	return s.groups[group]
}

// members lists the members in memory, shown short and sorted.
func (h *harness) members(group string) []string {
	var out []string
	for _, mm := range h.group(group).members {
		out = append(out, h.short(mm.id))
	}
	return sortedShort(out)
}

// parked lists the members with a parked call, shown short and sorted.
func (h *harness) parked(group string) []string {
	g := h.group(group)
	var out []string
	for _, mm := range g.members {
		if _, ok := g.parked(mm); ok {
			out = append(out, h.short(mm.id))
		}
	}
	return sortedShort(out)
}

// stored lists the members in the log, shown short and sorted.
func (h *harness) stored(group string) []string {
	d := contract.ResponseAs[contract.DescribeGroupResp](h.log.Apply(contract.DescribeGroup{Group: group}))
	var out []string
	for _, mm := range d.Members {
		out = append(out, h.short(mm.MemberID))
	}
	return sortedShort(out)
}

func (h *harness) check(what string, got, want any) {
	h.t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		h.t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// stableTwo makes group Stable with two members at generation 2, from t0.
// m<k> is the leader and m<k+1> the follower; it returns their ids. The
// clock ends at t0 + 3 s.
func (h *harness) stableTwo(group string, t0 actor.Duration, k int, call uint64) (string, string) {
	h.t.Helper()
	a, b := h.id(k), h.id(k+1)
	sa, sb := fmt.Sprintf("m%d", k), fmt.Sprintf("m%d", k+1)
	at := t0 + delay
	h.run([]step{
		{t0, h.join(call, group, ""), nil},
		{at, nil, []string{fmt.Sprintf("%d OK gen=1 proto=range leader=%s id=%s members=[%s:meta-range]", call, sa, sa, sa)}},
		{at, syncReq(call+1, group, a, 1, as(a, "a1")), []string{fmt.Sprintf("%d OK assign=a1", call+1)}},
		{at, h.join(call+2, group, ""), nil},
		{at, h.join(call+3, group, a), []string{
			fmt.Sprintf("%d OK gen=2 proto=range leader=%s id=%s", call+2, sa, sb),
			fmt.Sprintf("%d OK gen=2 proto=range leader=%s id=%s members=[%s:meta-range %s:meta-range]", call+3, sa, sa, sa, sb),
		}},
		{at, syncReq(call+4, group, b, 2), nil},
		{at, syncReq(call+5, group, a, 2, as(a, "a2"), as(b, "b2")), []string{
			fmt.Sprintf("%d OK assign=b2", call+4), fmt.Sprintf("%d OK assign=a2", call+5),
		}},
	})
	return a, b
}
