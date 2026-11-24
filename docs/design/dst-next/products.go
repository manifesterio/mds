//go:build ignore

// DESIGN SKETCH, NOT COMPILED. The real product machines on the proposed API
// (api.go). Each section shows only the part that exposed something the kv
// example did not need. The product's model stays the same; dst grows to fit it.

// =====================================================================
// 1. controlplane rebalance: parked gRPC calls, per-member timers, term loss
// =====================================================================
// Today: state any (*standby | *loading | *serving), reqs map[RequestID]any,
// owners map[TimerID]timerRef, nextReq, nextTimer, SetTimer/CancelTimer/SendSelf
// output cases, and CallAbandoned sent by the gRPC layer by hand.

type groupOut = actor.Out[rebalance.Input, runtime.LogIO]

// A JoinGroup call is parked until the round ends. The call carries its ReplyTo.
func (s *serving) onJoin(j Join, o *groupOut) {
	g := s.group(j.Group)
	g.park(j.MemberID, j.ReplyTo)                               // was: where[CallID] = group
	actor.Watch(o, j.ReplyTo, callGone{Group: j.Group, Member: j.MemberID}) // was: CallAbandoned from gRPC
	o.StartTimer(sessionKey(j.Group, j.MemberID), j.SessionTimeout(), sessionExpired{j.Group, j.MemberID})
	if g.roundComplete() {
		s.finishRound(g, o)
	}
}

// The round is done: answer every parked member.
func (s *serving) finishRound(g *group, o *groupOut) {
	for _, m := range g.members() {
		actor.Reply(o, m.replyTo, JoinAnswer{Generation: g.generation, MemberID: m.id, Leader: g.leader})
	}
}

// A decision is written to the log; the answer comes back with its context.
// Was: s.reqs[w.req] = groupReq{group: g.name}, then a lookup on LogAppendDone.
func (s *serving) writeDecision(g *group, d decision, o *groupOut) {
	actor.Perform(o, runtime.LogSubmit{Op: d.op()},
		func(r runtime.LogSubmitDone) rebalance.Input { return decisionWritten{Group: g.name, Result: r} })
}

// A write that may have changed the log failed: the term is broken.
func (m *Machine) loseTerm(o *groupOut) {
	o.ForgetPending() // every timer and log answer of the old term is dropped
	m.state = standby{}
}

func sessionKey(group, member string) actor.TimerKey {
	return actor.TimerKey("session/" + group + "/" + member)
}

// =====================================================================
// 2. controlplane server: one process, two actors, gRPC callers
// =====================================================================
// Today: live.Run twice with hand-made inboxes, routeGroups forwarding
// SubmitCommand into the replica's inbox with From/Req, setRole via AfterStep,
// and call()/answer()/calls map/forget() for gRPC.

func (n *Node) Start(ctx context.Context) error {
	host := live.NewHost(live.HostConfig{Clock: n.config.Clock, Env: n.config.Env, Shutdown: n.shutdown})

	replicaRef := live.Spawn(host, replica.Props(n.config.Machine), n.executor.Run)
	n.groups = live.Spawn(host, rebalance.Props(n.config.Groups, replicaRef), n.executor.Run)
	// rebalance asks the replica directly (actor.Ask), and the replica Tells
	// rebalance RoleChanged: no forwarding code in the server.

	go func() { n.loopErr = host.Run(ctx) }()
	return n.waitReady(ctx)
}

func (n *Node) JoinGroup(ctx context.Context, req *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	answer, err := live.Ask(ctx, n.host, n.groups,
		func(to actor.ReplyTo[rebalance.JoinAnswer]) rebalance.Input { return rebalance.JoinFrom(req, to) })
	if err != nil {
		return nil, wire.ToRPCError(err)
	}
	return wire.JoinResponse(answer), nil
}

// =====================================================================
// 3. controlplane replica: the injected soft crash keeps its model
// =====================================================================
// The replica still simulates its own crash (stall, down for DownDur, restore).
// What changes is only the bookkeeping: ForgetPending replaces timerGen++ and
// the claimReq / inFlightAppends checks that ignore answers from before the crash.

func (m *Replica) softCrash(o *replicaOut) {
	m.env.Reachable(replicaCrashed)
	o.ForgetPending() // old timers and IO answers can no longer arrive
	m.dropRole()
	m.ready, m.booting, m.crashed = false, true, true
	m.downUntil = m.now + actor.Mono(m.env.Duration(m.config.DownDur[0], m.config.DownDur[1]))
	m.clearLocal()
	m.refuseReads(o) // actor.Reply to every waiting reader
}

// =====================================================================
// 4. broker: sockets are a stream, not request/reply
// =====================================================================
// Produce and fetch arrive as bytes on a connection, and answers go back as
// bytes. That is Do (IO whose results are my own messages), not Ask.

func (b *Broker) Receive(now actor.Now, msg core.EventData, o *brokerOut) {
	switch m := msg.(type) {
	case core.BytesReceived: // unsolicited: the executor delivers it
		for _, req := range b.conn(m.Conn).decode(m.Data) {
			b.handle(req, o)
		}
	case flushDue:
		b.seal(closeOnTimer, o)
	case segmentStored: // the Perform answer of an S3 Put
		b.onPutDone(m, o)
	}
}

func (b *Broker) answer(c core.ConnID, resp kafka.Response, o *brokerOut) {
	o.Do(core.Send{Conn: c, Data: resp.Encode()})
}

func (b *Broker) put(s *segment, o *brokerOut) {
	actor.Perform(o, core.ObjectPut{Key: s.key, Data: s.data},
		func(r core.ObjectPutDone) core.EventData { return segmentStored{Seg: s.id, Result: r} })
}

// =====================================================================
// 5. broker sim clients: a reusable session inside a producer actor
// =====================================================================
// Today: session.Loop with its own states, timers and generation counters,
// driven by hand from the producer. With Sub, the session is a component with
// its own message type, and its timers come back wrapped into the producer's.

type producer struct {
	sess session.Session // dial, back off, reconnect, one outstanding request
}

func (p *producer) Receive(now actor.Now, msg producerMsg, o *producerOut) {
	sessOut := actor.Sub(o, func(m session.Msg) producerMsg { return sessionMsg{m} })
	switch m := msg.(type) {
	case start:
		p.sess.Dial(sessOut)
	case sessionMsg: // the session's own timers and socket events
		if resp, ok := p.sess.Receive(now, m.Msg, sessOut); ok {
			p.onResponse(resp, o)
		}
	}
}
