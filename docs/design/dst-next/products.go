//go:build ignore

// DESIGN SKETCH, NOT COMPILED. The real product machines on the frozen API
// (api.go, ADR.md). Each section shows only the part that exposed something
// the kv example did not need. The product's model stays the same; dst grows
// to fit it. Changes made while validating against the code at 674219b are
// listed in ADR.md section 9.

// =====================================================================
// 1. controlplane rebalance: parked gRPC calls, per-member timers, term loss
// =====================================================================
// Today: state any (*standby | *loading | *serving), reqs map[RequestID]any,
// owners map[TimerID]timerRef, nextReq, nextTimer, timer gens,
// SetTimer/CancelTimer/SendSelf/SubmitCommand/SubmitRead/Reply output cases,
// and CallAbandoned sent by the gRPC layer by hand.
//
// Rebalance does no product IO of its own: everything it writes goes through
// the replica, which it asks directly. So its IO type is actor.NoIO.

type groupOutbox = actor.Outbox[rebalance.Input, actor.NoIO]

// Every group call carries its ReplyTo. The answer keeps today's shape
// (rebalance.Reply{Err, Body}) so the gRPC handlers map errors exactly as now.
type Join struct {
	ReplyTo  actor.ReplyTo[rebalance.Reply] // was Call CallID
	Group    string
	MemberID string
	// ... the other fields of today's Join ...
}

// A JoinGroup call is parked until the round ends.
func (m *Machine) onJoin(s *serving, j Join, o *groupOutbox) {
	g := s.group(j.Group)
	g.park(j.MemberID, j.ReplyTo)                                           // was: where[CallID] = group
	actor.Watch(o, j.ReplyTo, callGone{Group: j.Group, Member: j.MemberID}) // was: CallAbandoned from gRPC
	m.arm(o, sessionKey(j.Group, j.MemberID), j.SessionTimeout(), sessionExpired{j.Group, j.MemberID})
	if g.roundComplete() {
		m.finishRound(g, o)
	}
}

// arm keeps today's buggify sites. SiteTimerLate stretches d. SiteStaleTimer
// used to re-send an old TimerFired that the gen check then ignored; with
// keyed timers a replaced timer can never fire (G3), so that site is retired
// (ADR section 8.2).
func (m *Machine) arm(o *groupOutbox, key actor.TimerKey, d actor.Duration, msg rebalance.Input) {
	if m.maybe(SiteTimerLate) {
		d += m.env.Duration(actor.Millisecond, lateMax)
	}
	o.StartTimer(key, d, msg)
}

// The round is done: answer every parked member.
func (m *Machine) finishRound(g *group, o *groupOutbox) {
	for _, mm := range g.members() {
		actor.Reply(o, mm.replyTo, rebalance.Reply{Body: JoinAnswer{Generation: g.generation, MemberID: mm.id, Leader: g.leader}})
	}
}

// A decision is written to the log through the replica; the answer comes back
// with its context. Was: s.reqs[w.req] = groupReq{group: g.name}, a
// SubmitCommand output routed by hand to the replica's inbox, and a lookup on
// ReplicaReply.
func (m *Machine) writeDecision(g *group, d decision, o *groupOutbox) {
	actor.Ask(o, m.replica,
		func(to actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData {
			return runtime.ControlPlaneDeliver{ReplyTo: to, Op: d.op()}
		},
		func(done runtime.ControlPlaneDone) rebalance.Input { return decisionWritten{Group: g.name, Done: done} })
	o.StartTimer(writeKey(g.name), m.cfg.WriteTimeout, writeTimedOut{Group: g.name})
}

// A write that may have changed the log failed: the term is broken.
func (m *Machine) loseTerm(s *serving, o *groupOutbox) {
	o.ForgetPending() // every timer and replica answer of the old term is dropped; Watches stay
	m.dropMemory(s, o) // actor.Reply(ErrUnavailable) to every parked, queued and writing call, as today
	m.startLoad(true, o) // as today: reload the groups under a new term
}

func sessionKey(group, member string) actor.TimerKey {
	return actor.TimerKey("session/" + group + "/" + member)
}

func writeKey(group string) actor.TimerKey { return actor.TimerKey("write/" + group) }

// =====================================================================
// 2. controlplane server: one process, two actors, gRPC callers
// =====================================================================
// Today: live.Run twice with hand-made inboxes, routeReplica/routeGroups
// switches, SubmitCommand forwarded into the replica's inbox with From/Req,
// setRole via AfterStep, call()/answer()/calls map/forget() and
// ask()/reply()/waiters map for gRPC, and onLoop through an inject channel.

func (n *Node) Start(ctx context.Context) error {
	n.host = live.NewHost(live.HostConfig{Clock: n.config.Clock, Env: n.envFor, Shutdown: n.shutdown,
		StopGrace: n.config.StopGrace})

	n.replica = live.Spawn(n.host, replica.Props(n.config.Machine, n.keep), n.executor.Run,
		live.AfterReceive[runtime.EventData](n.afterReplica))
	n.groups = live.Spawn(n.host, rebalance.Props(n.config.Groups, n.replica), nil)
	// rebalance asks the replica directly (actor.Ask): no forwarding code here.
	// nil exec is allowed only for NoIO.

	go func() { n.loopErr = n.host.Run(ctx); close(n.loopDone) }()
	return n.waitReady(ctx)
}

// afterReplica keeps today's AfterStep: metrics, and RoleChanged to rebalance
// when the role flips. The replica's protocol does not change.
func (n *Node) afterReplica() {
	m := n.m.Load() // stored by Props.New through n.keep
	n.epoch.Store(m.Epoch())
	n.stepDowns.Store(m.StepDowns())
	active := m.IsActive() && m.Ready()
	if n.active.Swap(active) != active {
		_ = live.Tell(n.host, n.groups, rebalance.Input(rebalance.RoleChanged{Active: active}))
	}
}

// Behaviour, errors and timeouts are the same as today's call():
// ctx ended -> ErrTimeout with groupOutcome (and rebalance's Watch fires, as
// CallAbandoned did); host stopped -> ErrUnavailable; Reply.Err != OK -> that
// error.
func (n *Node) call(ctx context.Context, read bool, input func(actor.ReplyTo[rebalance.Reply]) rebalance.Input) (rebalance.Reply, error) {
	r, err := live.Ask(ctx, n.host, n.groups, input)
	switch {
	case err == nil && r.Err != runtime.OK:
		return r, wire.ToRPCError(r.Err, groupOutcome(read, r.Err))
	case err == nil:
		return r, nil
	case ctx.Err() != nil:
		return rebalance.Reply{}, wire.ToRPCError(runtime.ErrTimeout, groupOutcome(read, runtime.ErrTimeout))
	default: // live.ErrHostStopped
		return rebalance.Reply{}, wire.ToRPCError(runtime.ErrUnavailable, groupOutcome(read, runtime.ErrUnavailable))
	}
}

// Same for the replica: today's ask() with its waiters map.
func (n *Node) ask(ctx context.Context, maybeApplied bool,
	body func(actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData) (runtime.ControlPlaneDone, error) {
	d, err := live.Ask(ctx, n.host, n.replica, body)
	switch {
	case err == nil:
		return d, nil
	case ctx.Err() != nil:
		outcome := runtime.NotApplied
		if maybeApplied {
			outcome = runtime.MaybeApplied
		}
		return runtime.ControlPlaneDone{Err: runtime.ErrTimeout, Outcome: outcome}, ctx.Err()
	default:
		return runtime.ControlPlaneDone{}, errors.New("controlplane/server: the run loop has ended")
	}
}

// GetStatus and BrokerHeartbeat keep onLoop, now live.OnLoop on the replica.
func (n *Node) onLoop(ctx context.Context, fn func(m *replica.Replica)) error {
	return live.OnLoop(ctx, n.host, n.replica, func() { fn(n.m.Load()) })
}

// =====================================================================
// 3. controlplane replica: the injected soft crash keeps its model
// =====================================================================
// The replica still simulates its own crash (stall, down for DownDur,
// restore). What changes is the bookkeeping: ForgetPending replaces timerGen++
// and the claimReq / inFlightAppends request-ID checks that ignore answers
// from before the crash.

type replicaOutbox = actor.Outbox[runtime.EventData, runtime.IO]

func (m *Replica) softCrash(o *replicaOutbox) {
	m.env.Reachable(replicaCrashed)
	o.ForgetPending() // old timers and IO answers can no longer arrive
	m.dropRole()
	m.ready, m.booting, m.crashed = false, true, true
	m.downUntil = m.now + actor.Mono(m.env.Duration(m.config.DownDur[0], m.config.DownDur[1]))
	m.clearLocal()
	m.refuseReads(o) // actor.Reply(ErrUnavailable) to every waiting reader's ReplyTo
}

// The claim keeps a flag where it kept a request id.
func (m *Replica) claim(o *replicaOutbox) {
	m.claiming = true // was: m.nextReq++; m.claimReq = m.nextReq
	actor.Perform(o, runtime.LogClaimEpoch{Expect: m.claimExpect, Owner: m.config.writer()},
		func(d runtime.LogClaimEpochDone) runtime.EventData { return claimAnswered{d} })
}

// =====================================================================
// 4. broker: sockets are a stream, not request/reply
// =====================================================================
// Produce and fetch arrive as bytes on a connection, and answers go back as
// bytes. That is Do (IO whose results are my own messages), not Ask.

type brokerOutbox = actor.Outbox[core.EventData, core.IO]

func (b *Broker) Receive(now actor.Now, msg core.EventData, o *brokerOutbox) {
	switch m := msg.(type) {
	case core.BytesReceived: // unsolicited: the executor Tells it
		for _, req := range b.conn(m.Conn).decode(m.Data) {
			b.handle(req, o)
		}
	case flushDue:
		b.seal(closeOnTimer, o)
	case segmentStored: // the Perform answer of an object store put
		b.onPutDone(m, o)
	}
}

func (b *Broker) answer(c core.ConnID, resp kafka.Response, o *brokerOutbox) {
	o.Do(core.Send{Conn: c, Data: resp.Encode()}) // Send loses its Req: nobody reads SendDone
}

func (b *Broker) put(s *segment, o *brokerOutbox) {
	actor.Perform(o, core.ObjectCall{Call: objectstore.Put{Key: s.key, Data: s.data}},
		func(d core.ObjectDone) core.EventData { return segmentStored{Seg: s.id, Result: d.Result} })
}

// The broker's control-plane calls are Perform too; in sim the executor
// Forwards them to the control-plane service (D22), in live to gRPC.
func (b *Broker) commitSegment(s *segment, o *brokerOutbox) {
	actor.Perform(o, core.ControlPlaneCall{Op: s.commitOp()},
		func(d core.ControlPlaneDone) core.EventData { return segmentCommitted{Seg: s.id, Done: d} })
}

// =====================================================================
// 5. broker sim clients: a reusable session inside a producer actor
// =====================================================================
// Today: session.Loop with its own states, timers and generation counters,
// driven by hand from the producer. With Sub, the session is a component with
// its own message type; its timers come back wrapped into the producer's and
// its keys live under "session/".

type producerOutbox = actor.Outbox[producerMsg, core.IO]

type producer struct {
	sess session.Session // dial, back off, reconnect, one outstanding request
}

func (p *producer) Receive(now actor.Now, msg producerMsg, o *producerOutbox) {
	sessOutbox := actor.Sub(o, "session", func(m session.Msg) producerMsg { return sessionMsg{m} })
	switch m := msg.(type) {
	case start:
		p.sess.Dial(sessOutbox)
	case sessionMsg: // the session's own timers and socket events
		if resp, ok := p.sess.Receive(now, m.Msg, sessOutbox); ok {
			p.onResponse(resp, o)
		}
	}
}
