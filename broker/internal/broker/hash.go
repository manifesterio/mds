package broker

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/trace"
)

// stateHasher folds broker state with trace.StateHasher. Domain folds are
// methods here; the FNV-1a primitives live in dst/core/trace.
type stateHasher struct{ trace.StateHasher }

func newStateHasher() stateHasher { return stateHasher{trace.NewStateHasher()} }

func (h *stateHasher) ref(r core.SegmentRef) {
	h.String(r.Key)
	h.Uint64(uint64(r.Off))
	h.Uint64(uint64(r.Len))
}

// StateHash folds everything that influences behaviour, in a fixed order.
func (b *Broker) StateHash() uint64 {
	h := newStateHasher()
	b.hashScalars(&h)
	h.topics(b.topics)
	h.conns(b.conns)
	h.openSeg(b.open)
	h.hashSegments(b.segs)
	h.ops(b.ops)
	return h.Sum()
}

func (b *Broker) hashScalars(h *stateHasher) {
	h.Uint64(uint64(b.config.InjectedBug))
	h.Bool(b.started)
	h.Uint64(b.nextID)
	h.Bool(b.stopping)
	h.Bool(b.exited)
	h.Uint64(uint64(b.bufBytes))
	h.Bool(b.pressure)
	h.Uint64(b.guess)
}

func (h *stateHasher) topics(topics []contract.TopicInfo) {
	h.Uint64(uint64(len(topics)))
	for _, t := range topics {
		h.String(t.Name)
		h.Uint64(uint64(t.Partitions))
	}
}

func (h *stateHasher) openSeg(o openSegment) {
	h.Uint64(uint64(o.since))
	h.Uint64(uint64(o.bytes))
	h.Uint64(uint64(len(o.parts)))
	h.batches(o.batches)
}

func (h *stateHasher) conns(conns []*connection) {
	h.Uint64(uint64(len(conns)))
	for _, c := range conns {
		h.Uint64(uint64(c.id))
		h.Uint64(c.rx.Sum())
		h.Uint64(uint64(c.fr.Buffered()))
		h.Bool(c.paused)
		h.Bool(c.closing)
		h.Bool(c.abort)
		h.Uint64(uint64(len(c.q)))
		for _, s := range c.q {
			h.Uint64(uint64(uint16(s.h.APIKey)))
			h.Uint64(uint64(uint32(s.h.CorrelationID)))
			h.Uint64(uint64(s.pending))
			h.Bool(s.ready)
			h.Bool(s.silent)
			h.Uint64(uint64(len(s.facts)))
			h.Uint64(uint64(uint32(s.fetchMax)))
		}
	}
}

func (h *stateHasher) hashSegments(segs []*segment) {
	h.Uint64(uint64(len(segs)))
	for _, s := range segs {
		h.Uint64(s.id)
		h.Uint64(uint64(s.state))
		h.Uint64(uint64(s.attempts))
		h.String(s.key)
		h.Uint64(uint64(len(s.data)))
		h.batches(s.batches)
	}
}

func (h *stateHasher) ops(ops []*operation) {
	h.Uint64(uint64(len(ops)))
	for _, o := range ops {
		h.Uint64(o.id)
		h.Uint64(uint64(o.kind))
		h.Uint64(uint64(o.state))
		h.Uint64(uint64(o.retry))
		h.Uint64(uint64(o.attempts))
		h.Uint64(uint64(o.s.conn.id))
		h.Uint64(uint64(uint32(o.s.h.CorrelationID)))
		h.Uint64(uint64(len(o.names)))
		for _, n := range o.names {
			h.String(n)
		}
		h.String(o.topicPartition.Topic)
		h.Uint64(uint64(uint32(o.topicPartition.Partition)))
		h.Uint64(uint64(o.from))
		h.Uint64(uint64(uint32(o.maxBytes)))
		if o.kind == opCreate { // only create ops carry these, so other hashes stay as they were
			h.String(o.create.Name)
			h.Uint64(uint64(uint32(o.create.Partitions)))
			h.Bool(o.validateOnly)
			h.Bool(o.maybeApplied)
		}
		h.Uint64(uint64(len(o.ranges)))
		for _, r := range o.ranges {
			h.Uint64(uint64(r.Base))
			h.Uint64(uint64(r.Count))
			h.ref(r.Loc)
		}
		h.Uint64(uint64(o.next))
		h.Bytes(o.out)
		h.Bool(o.corrupt)
	}
}

func (h *stateHasher) batches(bs []*partitionBatch) {
	h.Uint64(uint64(len(bs)))
	for _, pb := range bs {
		h.Uint64(uint64(pb.s.conn.id))
		h.Uint64(uint64(uint32(pb.s.h.CorrelationID)))
		h.String(pb.topicPartition.Topic)
		h.Uint64(uint64(uint32(pb.topicPartition.Partition)))
		h.Bytes(pb.data)
		h.Uint64(uint64(pb.records))
		h.ref(pb.loc)
		h.Bool(pb.answered)
	}
}
