package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// describe is a Describe call whose reads are in flight: one LocalRead per
// name, one reply when all are back.
type describe struct {
	n    uint64 // its number, in the answers of its reads
	call actor.ReplyTo[Reply]
	out  []GroupDescription
	back []bool // the reads that are back, by name index
	left int
}

// describe handles a Describe call (rule 32): the log's description of each
// group, with the state from memory. A describe that waits for its reads
// watches its caller.
func (m *Machine) describe(s *serving, in Describe) {
	d := &describe{call: in.ReplyTo, out: make([]GroupDescription, len(in.Groups)), back: make([]bool, len(in.Groups)), left: len(in.Groups)}
	if d.left == 0 {
		m.reply(in.ReplyTo, runtime.OK, DescribeAnswer{Groups: d.out})
		return
	}
	d.n = m.number()
	for i, name := range in.Groups {
		d.out[i].Name = name
		n := d.n
		m.ask(describeOp(name), false, func(done runtime.ControlPlaneDone) Input { return describeDone{n: n, i: i, done: done} })
	}
	s.describes = append(s.describes, d)
	m.o.Watch(in.ReplyTo, callGone{call: in.ReplyTo})
}

// describeDone takes one read. A failed read fails the call. The reads of a
// describe that is over are ignored.
func (m *Machine) describeDone(s *serving, in describeDone) {
	i := slices.IndexFunc(s.describes, func(d *describe) bool { return d.n == in.n })
	if i < 0 || s.describes[i].back[in.i] {
		return
	}
	m.duplicate(in)
	d := s.describes[i]
	d.back[in.i] = true
	if in.done.Err != runtime.OK {
		m.endDescribe(s, d)
		m.reply(d.call, runtime.ErrUnavailable, nil)
		return
	}
	d.out[in.i].DescribeGroupResp = runtime.ResponseAs[contract.DescribeGroupResp](in.done)
	if d.left--; d.left > 0 {
		return
	}
	m.endDescribe(s, d)
	for i := range d.out {
		d.out[i].State = stateOf(s, d.out[i].Name)
	}
	m.reply(d.call, runtime.OK, DescribeAnswer{Groups: d.out})
}

// abandonRead forgets the describe of call, if there is one.
func (m *Machine) abandonRead(s *serving, call actor.ReplyTo[Reply]) bool {
	i := slices.IndexFunc(s.describes, func(d *describe) bool { return d.call == call })
	if i >= 0 {
		m.endDescribe(s, s.describes[i])
	}
	return i >= 0
}

func (m *Machine) endDescribe(s *serving, d *describe) {
	s.describes = slices.DeleteFunc(s.describes, func(o *describe) bool { return o == d })
}

// stateOf is the Kafka state of a group in memory, Dead when there is none.
func stateOf(s *serving, name string) string {
	if g := s.groups[name]; g != nil {
		return g.phase.state()
	}
	return stateDead
}

// list handles a List call (rule 32): every group in memory, by name.
func (m *Machine) list(s *serving, in List) {
	out := make([]contract.GroupSummary, 0, len(s.names))
	for _, name := range s.names {
		g := s.groups[name]
		out = append(out, contract.GroupSummary{Name: name, ProtocolType: g.protoType, State: g.phase.state()})
	}
	m.reply(in.ReplyTo, runtime.OK, ListAnswer{Groups: out})
}
