package replica

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// caller is the handle of a caller waiting for a control-plane answer.
type caller = actor.ReplyTo[runtime.ControlPlaneDone]

// onControlPlaneDeliver handles one control-plane operation from a caller:
// reads wait for a fresh tail and answer from the fold when Ready; commits
// and creates go through Offer and are answered after apply.
func (m *Replica) onControlPlaneDeliver(d runtime.ControlPlaneDeliver, o *outbox) {
	operation := d.Op
	switch m.meta.Mode(operation) {
	case metadb.Logged:
		cmd, _ := metadb.EntryOf(operation)
		m.Offer(ClientOperation{ReplyTo: d.ReplyTo, Cmd: cmd}, o)
	case metadb.ReadOnly:
		m.offerRead(d.ReplyTo, func() (runtime.ControlPlaneDone, error) { return m.meta.Read(operation) }, o)
	default:
		m.reply(o, d.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrInvalid})
	}
}

// onLocalRead answers a read from the fold of this node at once, if the node
// is Ready. It does not wait for a fresh tail, so it is as fast as a local
// lookup and can be a little behind the log. A node that is not Ready refuses
// at once so the caller can try another node.
func (m *Replica) onLocalRead(r runtime.LocalRead, o *outbox) {
	if !m.Ready() {
		m.reply(o, r.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrUnavailable})
		return
	}
	if m.meta.Mode(r.Op) != metadb.ReadOnly {
		m.reply(o, r.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrInvalid})
		return
	}
	d, err := m.meta.Read(r.Op)
	if err != nil {
		m.reply(o, r.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrIO, Outcome: runtime.NotApplied})
		return
	}
	m.reply(o, r.ReplyTo, d)
}

// offerRead queues a read behind a fresh tail check. A node that is not Ready
// refuses at once so the caller tries the other replica.
func (m *Replica) offerRead(to caller, read func() (runtime.ControlPlaneDone, error), o *outbox) {
	if !m.Ready() {
		m.reply(o, to, runtime.ControlPlaneDone{Err: runtime.ErrUnavailable})
		return
	}
	m.queueRead(to, read)
}

// reply answers a caller. Nothing is recorded once the actor stopped: Stop
// is the last effect of a Receive.
func (m *Replica) reply(o *outbox, to caller, d runtime.ControlPlaneDone) {
	if m.exited {
		return
	}
	actor.Reply(o, to, d)
}
