package replica

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
)

// groupMarker prefixes a multi-command commit entry so drain can tell it from a
// single metadb.Encode payload (kinds 0..2).
const groupMarker byte = 0xC5

// ClientOperation is one client command for the commit queue. A caller's
// runtime.ControlPlaneDeliver of a logged operation becomes an Offer.
type ClientOperation struct {
	ReplyTo actor.ReplyTo[runtime.ControlPlaneDone] // answered once the command applies
	Cmd     metadb.Entry                            // ID is assigned when the group is built
}

// waitingCommand is a queued or in-flight client command.
type waitingCommand struct {
	replyTo caller
	cmd     metadb.Entry
}

// inFlightAppend is the commit or heartbeat LogAppend in flight (at most one).
type inFlightAppend struct {
	slot runtime.LogPosition
	id   string
	data []byte
	cmds []waitingCommand
}

// Offer enqueues a client command and flushes when no append is in flight.
// The caller is answered only after the winning slot applies. Standby or
// not-yet-Ready nodes refuse without a LogAppend.
//
// A CommitSegment whose key is already in the fold is answered immediately
// (no LogAppend). A retry for a key still queued or in flight retargets that
// waiter to the new caller, so a caller that gave up cannot orphan the apply
// answer.
func (m *Replica) Offer(operation ClientOperation, o *outbox) {
	if m.exited {
		return
	}
	if !m.canCommit() {
		m.reply(o, operation.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrUnavailable})
		return
	}
	if commit, isCommit := operation.Cmd.CommitSegment(); isCommit {
		if d, ok, err := m.meta.KnownCommit(commit); err == nil && ok {
			m.env.Reachable(replayedKeyAnsweredFromWindow)
			m.reply(o, operation.ReplyTo, d)
			return
		}
		if m.retargetCommit(operation) {
			return
		}
	}
	if !m.fitsAlone(operation.Cmd) {
		m.reply(o, operation.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrTooLarge})
		return
	}
	if len(m.queue) >= m.config.queueMax() {
		m.reply(o, operation.ReplyTo, runtime.ControlPlaneDone{Err: runtime.ErrUnavailable})
		return
	}
	cmd := operation.Cmd
	cmd.ID = ""
	m.queue = append(m.queue, waitingCommand{replyTo: operation.ReplyTo, cmd: cmd})
	m.flushCommits(o)
}

// retargetCommit points a queued or in-flight waiter for the same SegmentKey
// at this Offer. The earlier caller gave up; the apply must answer the live
// one. The earlier caller is never answered.
func (m *Replica) retargetCommit(operation ClientOperation) bool {
	wanted, _ := operation.Cmd.CommitSegment()
	key := wanted.SegmentKey
	retarget := func(c *waitingCommand) bool {
		if queued, ok := c.cmd.CommitSegment(); !ok || queued.SegmentKey != key {
			return false
		}
		c.replyTo = operation.ReplyTo
		return true
	}
	for i := range m.queue {
		if retarget(&m.queue[i]) {
			return true
		}
	}
	if f := m.inFlight; f != nil {
		for i := range f.cmds {
			if retarget(&f.cmds[i]) {
				return true
			}
		}
	}
	return false
}

func (m *Replica) flushCommits(o *outbox) {
	if m.exited || m.booting {
		return
	}
	// Demote still drains a non-empty queue; Offer already refused new work.
	if !m.isAppender() && len(m.queue) == 0 {
		return
	}
	if m.inFlight == nil && len(m.queue) > 0 {
		m.emitCommitGroup(o)
	}
}

func (m *Replica) emitCommitGroup(o *outbox) {
	limit := min(m.config.maxCmds(), len(m.queue))
	if limit <= 0 {
		return
	}
	m.seq++
	id := fmt.Sprintf("%s/e%d/%d", m.config.writer(), m.epoch, m.seq)

	// Take commands while the entry stays under MaxEntryBytes. The first one
	// always goes: Offer already refused any command that cannot fit alone.
	encoded := make([][]byte, 0, limit)
	size := m.entryFixedBytes(id)
	for _, c := range m.queue[:limit] {
		e := c.cmd
		e.ID = id
		enc := metadb.Encode(e)
		cost := len(enc) + binary.MaxVarintLen32
		if len(encoded) > 0 && size+cost > m.config.maxEntryBytes() {
			break
		}
		size += cost
		encoded = append(encoded, enc)
	}
	n := len(encoded)
	cmds := append([]waitingCommand(nil), m.queue[:n]...)
	m.queue = append(m.queue[:0:0], m.queue[n:]...)

	for i := range cmds {
		e := cmds[i].cmd
		e.ID = id
		cmds[i].cmd = e
	}
	inner := encodeGroupParts(id, encoded)
	data := wrap(m.epoch, m.config.writer(), inner)
	slot := m.alloc()
	m.env.Always(slot >= m.trimFloor, putNeverWinsBelowTrimFloor,
		assert.Pair{Key: "slot", Value: slot})
	m.env.Always(len(data) <= m.config.maxEntryBytes() || n == 1, groupEntryUnderByteCap,
		assert.Pair{Key: "bytes", Value: len(data)})
	m.waiters[id] = cmds
	m.emitAppend(o, &inFlightAppend{slot: slot, id: id, data: data, cmds: cmds})
}

// emitAppend makes f the append in flight and sends it.
func (m *Replica) emitAppend(o *outbox, f *inFlightAppend) {
	m.inFlight = f
	actor.Perform(commitScope(o), runtime.LogAppend{Slot: f.slot, Data: f.data},
		func(d runtime.LogAppendDone) runtime.EventData { return d })
}

// entryFixedBytes is the most bytes one entry adds around its commands: the
// envelope, the group marker, the id, and the two counts. Each command also
// costs a length prefix, which emitCommitGroup adds per command.
func (m *Replica) entryFixedBytes(id string) int {
	return len(wrap(math.MaxUint64, m.config.writer(), nil)) + 1 + binary.MaxVarintLen64 + len(id) + binary.MaxVarintLen64
}

// fitsAlone reports whether cmd fits in a log entry of its own. The id is
// sized for the largest sequence number, so a command that fits now fits later.
func (m *Replica) fitsAlone(cmd metadb.Entry) bool {
	id := m.config.writer() + "/" + strings.Repeat("9", 20)
	cmd.ID = id
	size := m.entryFixedBytes(id) + len(metadb.Encode(cmd)) + binary.MaxVarintLen32
	return size <= m.config.maxEntryBytes()
}

func (m *Replica) alloc() runtime.LogPosition {
	n := max(m.high, m.applied)
	m.high = n + 1
	return n
}

func (m *Replica) observeSlot(n runtime.LogPosition) {
	m.high = max(m.high, n+1)
}

func (m *Replica) onCommitAppendDone(d runtime.LogAppendDone, f *inFlightAppend, o *outbox) {
	if isBelowFloor(d) {
		// The slot is at or below the floor of the log: this node is far behind
		// and the put is not a commit. Refuse the clients and restore.
		m.env.Reachable(putRefusedBelowFloor)
		m.hitTrimmed(o)
		return
	}
	m.inFlight = nil
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		// Conditional put: NotApplied and MaybeApplied both retry the same
		// slot. Allocating a later slot here would leave an empty slot, and
		// drain would stop there for good.
		if d.Outcome == runtime.MaybeApplied {
			m.env.Reachable(putNotAnsweredSameSlot)
		}
		m.emitAppend(o, &inFlightAppend{slot: f.slot, id: f.id, data: f.data, cmds: f.cmds})
		m.flushCommits(o)
		return
	}
	m.observeSlot(f.slot)
	data := f.data
	lost := false
	if d.Taken {
		data = bytes.Clone(d.Existing)
		lost = !groupIDMatches(data, f.id)
	}
	if f.slot >= m.applied {
		m.buf[f.slot] = data
	}
	if err := m.drain(o); err != nil {
		m.trip(o, err)
		return
	}
	if m.fencedFlag {
		m.stepDown(o)
		m.progress(o)
		return
	}
	if lost {
		m.retryInFlightAppend(o, f)
	}
	m.progress(o)
	m.flushCommits(o)
}

// retryInFlightAppend appends the same entry (same id and cmds) at the next free slot
// after another writer won f.slot.
func (m *Replica) retryInFlightAppend(o *outbox, f *inFlightAppend) {
	m.env.Reachable(lostSlotRetriedLaterSlot)
	m.emitAppend(o, &inFlightAppend{slot: m.alloc(), id: f.id, data: f.data, cmds: f.cmds})
}

// encodeGroupParts packs commands that are already encoded.
func encodeGroupParts(id string, encoded [][]byte) []byte {
	b := []byte{groupMarker}
	b = binary.AppendUvarint(b, uint64(len(id)))
	b = append(b, id...)
	b = binary.AppendUvarint(b, uint64(len(encoded)))
	for _, enc := range encoded {
		b = binary.AppendUvarint(b, uint64(len(enc)))
		b = append(b, enc...)
	}
	return b
}

func decodeGroup(data []byte) (string, []metadb.Entry, bool) {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return "", nil, false
	}
	data = payload
	if len(data) == 0 || data[0] != groupMarker {
		return "", nil, false
	}
	rest := data[1:]
	n, rest, ok := takeUvarint(rest)
	if !ok || n > uint64(len(rest)) {
		return "", nil, false
	}
	id := string(rest[:n])
	rest = rest[n:]
	n, rest, ok = takeUvarint(rest)
	if !ok {
		return "", nil, false
	}
	cmds := make([]metadb.Entry, 0, n)
	for range n {
		ln, r2, ok := takeUvarint(rest)
		if !ok || ln > uint64(len(r2)) {
			return "", nil, false
		}
		e, err := metadb.Decode(r2[:ln])
		if err != nil {
			return "", nil, false
		}
		cmds = append(cmds, e)
		rest = r2[ln:]
	}
	if len(rest) != 0 {
		return "", nil, false
	}
	return id, cmds, true
}

func groupIDMatches(data []byte, id string) bool {
	got, _, ok := decodeGroup(data)
	return ok && got == id
}

func takeUvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, b, false
	}
	return v, b[n:], true
}

func (m *Replica) applySlot(slot runtime.LogPosition, data []byte, o *outbox) error {
	epoch, writer, payload, ok := unwrap(data)
	if !ok {
		return fmt.Errorf("replica: truncated log envelope at slot %d", slot)
	}
	enveloped := len(data) > 0 && data[0] == envelopeMarker
	m.observeWriter(epoch, writer, enveloped)
	if m.epoch > 0 && !m.active && enveloped && writer == m.config.writer() && epoch == m.epoch {
		// Our fence entry is applied: this node is the writer now.
		m.active = true
		m.env.Reachable(standbyTookOver)
	}

	if id, cmds, ok := decodeGroup(payload); ok {
		dones := make([]runtime.ControlPlaneDone, len(cmds))
		for i, c := range cmds {
			dones[i] = m.meta.Apply(int64(slot), c)
		}
		m.deliver(o, id, dones)
		m.afterApply(slot)
		return nil
	}
	d, err := m.meta.ApplyBytes(int64(slot), payload)
	if err != nil {
		return err
	}
	// Single-entry catch-up: deliver only if we were waiting on it.
	if e, err := metadb.Decode(payload); err == nil {
		m.deliver(o, e.ID, []runtime.ControlPlaneDone{d})
	}
	m.afterApply(slot)
	return nil
}

func (m *Replica) afterApply(slot runtime.LogPosition) {
	m.env.Reachable(replayedKeyAnsweredFromWindow)
	window := m.config.WindowSlots
	stride := m.config.TrimStride
	if stride <= 0 {
		stride = 16
	}
	if window > 0 && int64(slot)%stride == 0 && int64(slot) >= window {
		_, _ = m.meta.Trim(int64(slot) - window)
	}
}

func (m *Replica) deliver(o *outbox, id string, dones []runtime.ControlPlaneDone) {
	ps, ok := m.waiters[id]
	if !ok {
		return
	}
	delete(m.waiters, id)
	for i, p := range ps {
		d := runtime.ControlPlaneDone{Err: runtime.ErrInvalid}
		if i < len(dones) {
			d = dones[i]
			if d.Err == runtime.OK && d.Outcome == runtime.Applied && !m.isAppender() {
				m.env.Reachable(idempotentReplayAfterTakeover)
			}
		}
		m.reply(o, p.replyTo, d)
	}
}
