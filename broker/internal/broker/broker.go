// Package broker is the Kafka broker actor: stateless, pure and deterministic
// (an actor.Actor), with S3 as the data plane and the control plane assigning
// offsets. The simulator (dst/sim) and the production host (dst/runtime/live)
// run the same code.
//
// Pipeline of a produce: the batch joins the open segment buffer; a flush
// (BatchWindow timer or MaxSegmentBytes) encodes it with format.Encode,
// uploads it (ObjectPut) and commits it (CommitSegment); only then is the client
// answered, with a "produce_acked" fact recorded right before the Send.
// Commits go out strictly in segment order, so a partition's batches get
// offsets in the order the broker admitted them. Every retry resends the
// identical request under the same segment key, which both the object store and
// the control plane treat idempotently.
//
// The broker is stateless. It has no registration, epoch or fencing, no
// idempotent producers, and it never proposes an offset. The segment key is a
// random id drawn from the injected Environment (seg/<32 hex>).
package broker

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/actor"
)

// brokerOutbox is the Outbox the broker records its effects on.
type brokerOutbox = actor.Outbox[core.EventData, core.IO]

// timerFlush is the key of the batch window timer of the open segment. Retry
// timers are keyed by the segment or operation they belong to (segmentTimer,
// opTimer).
const timerFlush actor.TimerKey = "flush"

// Broker is the actor. Build it with New or Props.
type Broker struct {
	env    core.Environment
	config Config
	codec  kafkawire.Codec

	started bool

	nextID   uint64
	stopping bool
	exited   bool

	conns  []*connection
	dirty  []*connection        // connections whose queue head may be ready (drained at end of Receive)
	topics []contract.TopicInfo // topic cache, refreshed from DescribeTopics on a miss
	live   []core.BrokerAddr    // live brokers from LiveBrokers; nil means self only

	now        actor.Mono // time of the message being received (set by Receive; not state)
	open       openSegment
	segs       []*segment // sealed segments in commit order
	ops        []*operation
	bufBytes   int        // produce bytes admitted and not yet answered
	pressure   bool       // above MaxBufferedBytes, until back at ResumeBufferedBytes
	pressureAt actor.Mono // when pressure began (fact timing only, not state)
	guess      uint64
}

// Factory builds a broker for one incarnation of a node.
type Factory func(env core.Environment) *Broker

// New returns the Factory of the broker.
func New(config Config) Factory {
	config = config.withDefaults()
	return func(env core.Environment) *Broker {
		return &Broker{env: env, config: config, nextID: firstID}
	}
}

// Props says how to run the broker. Every boot builds a new Broker. A
// graceful stop is a StopRequest; answers and timers are received before
// connection data (core.PriorityOf).
func Props(config Config) actor.Props[core.EventData, core.IO] {
	factory := New(config)
	return actor.Props[core.EventData, core.IO]{
		Name: "broker",
		New: func(env actor.Env) actor.Actor[core.EventData, core.IO] {
			return factory(env)
		},
		Boot: func(self actor.Incarnation) core.EventData { return core.NodeStarted{Self: self} },
		StopMsg: func(deadline actor.Mono) core.EventData {
			return core.StopRequest{Deadline: deadline}
		},
		Priority: core.PriorityOf,
	}
}

// firstID is one less than the first id of a segment or operation.
const firstID = 16

func (b *Broker) id() uint64 { b.nextID++; return b.nextID }

func (b *Broker) backoff(n int) actor.Duration {
	d := b.config.RetryBase
	for ; n > 1 && d < b.config.RetryMax; n-- {
		d *= 2
	}
	return min(d, b.config.RetryMax)
}

// Receive takes one message and records what the broker wants done, in
// order. Answers and timers that nothing waits for any more leave the state
// unchanged. After the broker exited every message is a no-op.
func (b *Broker) Receive(now actor.Now, msg core.EventData, outbox *brokerOutbox) {
	if b.exited {
		return
	}
	b.now = now.Mono
	switch m := msg.(type) {
	case core.NodeStarted:
		b.started = true
	case core.StopRequest:
		b.stop(outbox)
	case core.ConnOpened:
		b.onOpen(m, outbox)
	case core.BytesReceived:
		b.onBytes(m, outbox)
	case core.ConnClosed:
		b.onClosed(m, outbox)
	case core.FlushDue:
		b.onFlushDue(outbox)
	case core.SegmentRetryDue:
		b.onSegmentRetryDue(m.Seg, outbox)
	case core.OpDue:
		b.onOpDue(m.Op, outbox)
	case core.SegmentPutDone:
		b.onSegmentPutDone(m, outbox)
	case core.SegmentCommitDone:
		b.onSegmentCommitDone(m, outbox)
	case core.OpReadDone:
		b.onOpReadDone(m, outbox)
	case core.OpCallDone:
		b.onOpCallDone(m, outbox)
	case core.LiveBrokers:
		b.live = m.Brokers
	case core.ConnectDone, core.ObjectDone, core.ControlPlaneDone, core.ClientDue:
		// The broker never asks for these: its answers come adapted, with
		// the segment or operation they belong to.
	}
	b.finish(outbox)
}

// finish drains ready responses, applies admission control and ends a graceful
// stop.
func (b *Broker) finish(outbox *brokerOutbox) {
	b.drainDirty(outbox)
	b.updatePressure(outbox)
	b.syncReads(outbox)
	b.maybeExit(outbox)
}

// ---- stop ----

// stop stops admission, flushes the open segment and lets in-flight work
// finish; maybeExit stops the actor once everything is answered.
func (b *Broker) stop(outbox *brokerOutbox) {
	if b.stopping {
		return
	}
	b.stopping = true
	b.seal(closeOnStop, outbox)
	b.endParked(func(*operation) bool { return true }) // parked Fetches answer now
	b.failGroupCalls()
}

func (b *Broker) idle() bool {
	if len(b.ops) > 0 || len(b.segs) > 0 || len(b.open.batches) > 0 {
		return false
	}
	for _, c := range b.conns {
		if len(c.q) > 0 {
			return false
		}
	}
	return true
}

// maybeExit ends a graceful stop once nothing is left to answer. Stop is the
// last effect of the Receive: finish calls it last.
func (b *Broker) maybeExit(outbox *brokerOutbox) {
	if !b.stopping || b.exited || !b.idle() {
		return
	}
	b.exited = true
	for _, c := range b.conns {
		outbox.Do(core.CloseConn{Conn: c.id})
	}
	b.conns = nil
	recordFact(outbox, "broker_exited")
	outbox.Stop(actor.Exit, "")
}

// ---- admission control ----

// updatePressure turns pressure on when the broker holds too many unacked
// produce bytes and off once back at the low mark.
func (b *Broker) updatePressure(outbox *brokerOutbox) {
	switch {
	case !b.pressure && b.bufBytes >= b.config.MaxBufferedBytes:
		b.pressure, b.pressureAt = true, b.now
		b.env.Reachable(readsPausedByBackpressure)
		recordFact(outbox, "backpressure_on",
			actor.Field{Key: "buffered", Value: actor.Int64Value(b.bufBytes)})
	case b.pressure && b.bufBytes <= b.config.ResumeBufferedBytes:
		b.pressure = false
		b.env.Reachable(readsResumedAfterBackpressure)
		recordFact(outbox, "backpressure_off",
			actor.Field{Key: "buffered", Value: actor.Int64Value(b.bufBytes)},
			actor.Field{Key: "paused_ns", Value: actor.Int64Value(b.now - b.pressureAt)})
	}
}

// syncReads pauses reads on every client connection under pressure or while
// stopping, and resumes them otherwise.
func (b *Broker) syncReads(outbox *brokerOutbox) {
	want := b.pressure || b.stopping
	for _, c := range b.conns {
		if c.closing || c.paused == want {
			continue
		}
		c.paused = want
		if want {
			outbox.Do(core.PauseReads{Conn: c.id})
		} else {
			outbox.Do(core.ResumeReads{Conn: c.id})
		}
	}
}

// ---- timers ----

// onFlushDue seals the open segment when its batch window has passed. Seal
// cancels the timer, so an empty buffer means a timer that fired just
// before a seal by size.
func (b *Broker) onFlushDue(outbox *brokerOutbox) {
	b.seal(closeOnTimer, outbox)
}

func (b *Broker) onSegmentRetryDue(id uint64, outbox *brokerOutbox) {
	if s := b.findSegment(id); s != nil {
		b.onSegmentRetry(s, outbox)
	}
}

func (b *Broker) onOpDue(id uint64, outbox *brokerOutbox) {
	if o := b.findOp(id); o != nil {
		b.onOpTimer(o, outbox)
	}
}

func (b *Broker) findSegment(id uint64) *segment {
	for _, s := range b.segs {
		if s.id == id {
			return s
		}
	}
	return nil
}

// ---- answers ----

// The broker has at most one call in flight per segment and per operation,
// and the answer names the segment or operation. An answer for one that is
// gone, or that waits for another kind of answer, is ignored.

func (b *Broker) onSegmentPutDone(m core.SegmentPutDone, outbox *brokerOutbox) {
	if s := b.segmentIn(m.Seg, segmentUpload); s != nil {
		b.onPutDone(s, m.Done, outbox)
	}
}

func (b *Broker) onSegmentCommitDone(m core.SegmentCommitDone, outbox *brokerOutbox) {
	if s := b.segmentIn(m.Seg, segmentCommit); s != nil {
		b.onCommit(s, m.Done, outbox)
	}
}

func (b *Broker) onOpReadDone(m core.OpReadDone, outbox *brokerOutbox) {
	if o := b.opIn(m.Op, opGet); o != nil {
		b.onFetchGet(o, m.Done, outbox)
	}
}

func (b *Broker) onOpCallDone(m core.OpCallDone, outbox *brokerOutbox) {
	if o := b.opIn(m.Op, opControlPlane); o != nil {
		b.onOpCP(o, m.Done, outbox)
	}
}

// segmentIn returns segment id if it waits in state st, else nil.
func (b *Broker) segmentIn(id uint64, st segmentState) *segment {
	if s := b.findSegment(id); s != nil && s.state == st {
		return s
	}
	return nil
}

// opIn returns operation id if it waits in state st, else nil.
func (b *Broker) opIn(id uint64, st operationState) *operation {
	if o := b.findOp(id); o != nil && o.state == st {
		return o
	}
	return nil
}

func recordFact(outbox *brokerOutbox, name string, fs ...actor.Field) {
	outbox.Fact(actor.Fact{Name: name, Fields: fs})
}

// Broker is an actor.
var _ actor.Actor[core.EventData, core.IO] = (*Broker)(nil)
