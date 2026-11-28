package rebalance

import (
	"github.com/manifesterio/mds/dst/trace"
)

// StateHash implements actor.Actor. It folds the state that decides what the
// machine does next, in a fixed order.
func (m *Machine) StateHash() uint64 {
	h := trace.New()
	h.Uint64(m.next)
	switch s := m.state.(type) {
	case *serving:
		h.Byte(2)
		h.Uint64(uint64(len(s.describes)))
		h.Uint64(uint64(len(s.commits)))
		for _, name := range s.names {
			hashGroup(&h, s.groups[name])
		}
	case *loading:
		h.Byte(1)
		h.Bool(s.fresh)
		h.Bool(s.listed)
		h.Uint64(uint64(len(s.waiting)))
		h.Uint64(uint64(len(s.again)))
		h.Bool(s.retrying)
	default:
		h.Byte(0)
	}
	return h.Sum()
}

func hashGroup(h *trace.Hash, g *group) {
	h.String(g.name)
	h.Uint64(uint64(g.generation))
	h.String(g.protoType)
	h.String(g.protocol)
	h.String(g.leader)
	h.String(g.phase.state())
	h.Uint64(g.joinSeq)
	h.Bool(g.write != nil)
	h.Bool(g.draining)
	h.Uint64(uint64(len(g.queue)))
	if jn, ok := g.phase.(*joining); ok {
		h.Int64(int64(jn.start))
		h.Int64(int64(jn.end))
		h.Bool(jn.ended)
		h.Bool(jn.delay)
	}
	for _, mm := range g.members {
		h.String(mm.id)
		h.Uint64(mm.order)
		h.Bool(mm.isNew)
		h.Bool(mm.synced)
		call, parked := g.parked(mm)
		h.Bool(parked)
		if parked {
			call.Fold(h)
		}
	}
}
