package replica

import (
	"bytes"
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

const defaultCheckpointPrefix = "checkpoint/"

// checkpointKeyFor names the object that holds the snapshot taken at slot.
// Every checkpoint has its own key, so a late or repeated upload can only
// rewrite the same bytes and never replaces a newer snapshot. The slot has a
// fixed width, so the last key in a sorted list is the newest checkpoint.
func (m *Replica) checkpointKeyFor(slot runtime.LogPosition) string {
	return fmt.Sprintf("%s%020d", m.checkpointPrefix(), slot)
}

func (m *Replica) checkpointPrefix() string {
	if m.config.CheckpointPrefix != "" {
		return m.config.CheckpointPrefix
	}
	return defaultCheckpointPrefix
}

func (m *Replica) checkpointEnabled() bool {
	return m.config.CheckpointEvery > 0 || m.config.CheckpointPrefix != ""
}

func (m *Replica) armCheckpoint(o *outbox) {
	if m.config.CheckpointEvery <= 0 || m.exited {
		return
	}
	o.StartTimer(timerCheckpoint, m.config.CheckpointEvery, runtime.CheckpointDue{})
}

func (m *Replica) onCheckpointTimer(o *outbox) {
	if m.needsCheckpoint || m.checkpointRetry {
		m.emitCheckpointList(o)
		return
	}
	if m.config.CheckpointEvery <= 0 {
		return
	}
	m.maybeCheckpoint(o)
	m.armCheckpoint(o)
}

// CheckpointDecision says what the checkpoint timer did and why.
type CheckpointDecision struct {
	Written        bool   // true: a checkpoint write was started
	Reason         string // why it was skipped; "ok" when written
	Applied        int64  // slots folded so far
	GoalTail       int64  // newest slot seen in the log
	LastCheckpoint int64  // slot of the last checkpoint written by this node
	MinSlots       int64  // slots needed since the last checkpoint
	PendingKind    string // the control I/O in flight, if any
}

func (k pendingKind) String() string {
	switch k {
	case pendingTail:
		return "tail_check"
	case pendingRead:
		return "log_read"
	case pendingObjList:
		return "checkpoint_list"
	case pendingObjGet:
		return "checkpoint_get"
	}
	return "none"
}

// decide reports one checkpoint decision to the optional observer.
func (m *Replica) decide(written bool, reason string) {
	if m.config.OnCheckpointDecision == nil {
		return
	}
	m.config.OnCheckpointDecision(CheckpointDecision{
		Written: written, Reason: reason,
		Applied: int64(m.applied), GoalTail: int64(m.goalTail), LastCheckpoint: int64(m.checkpointSlot),
		MinSlots: m.config.CheckpointMinSlots, PendingKind: m.pendingKind.String(),
	})
}

func (m *Replica) maybeCheckpoint(o *outbox) {
	switch {
	case m.checkpointStage != 0:
		m.decide(false, "checkpoint_in_flight")
		return
	case !m.isAppender():
		m.decide(false, "not_appender")
		return
	case !m.ready:
		m.decide(false, "not_ready")
		return
	}
	slot := m.applied
	if slot == 0 {
		m.decide(false, "nothing_applied")
		return
	}
	// A node behind the log tail must not write: its snapshot could replace a
	// newer one from the other node, and the log below that newer one may
	// already be trimmed, so a restart could never recover.
	if m.applied < m.goalTail {
		m.decide(false, "behind_tail")
		return
	}
	minSlots := m.config.CheckpointMinSlots
	if minSlots <= 0 {
		minSlots = 1
	}
	if int64(slot-m.checkpointSlot) < minSlots {
		m.decide(false, "too_few_new_slots")
		return
	}
	var buf bytes.Buffer
	if err := m.meta.Snapshot(&buf, int64(slot)); err != nil {
		m.trip(o, err)
		return
	}
	m.decide(true, "ok")
	m.checkpointData = buf.Bytes()
	m.checkpointPending = slot
	m.checkpointStage = stageUpload
	actor.Perform(work(o), runtime.ObjectPut{Key: m.checkpointKeyFor(slot), Data: m.checkpointData},
		func(d runtime.ObjectPutDone) runtime.EventData { return d })
}

// emitCheckpointList starts a restore: find the newest checkpoint object.
func (m *Replica) emitCheckpointList(o *outbox) {
	p := m.pull(o, pendingObjList)
	actor.Perform(p, runtime.ObjectList{Prefix: m.checkpointPrefix()},
		func(d runtime.ObjectListDone) runtime.EventData { return d })
}

func (m *Replica) emitCheckpointGet(o *outbox, key string) {
	m.restoreKey = key
	p := m.pull(o, pendingObjGet)
	actor.Perform(p, runtime.ObjectGet{Ref: runtime.SegmentRef{Key: key}},
		func(d runtime.ObjectGetDone) runtime.EventData { return d })
}

func (m *Replica) onObjListDone(d runtime.ObjectListDone, o *outbox) {
	if m.pendingKind != pendingObjList {
		return
	}
	m.pendingKind = pendingNone
	if d.Err != runtime.OK {
		// The list failed, so we do not know whether a checkpoint exists. The
		// log may be trimmed, so never catch up from slot 0 on a guess: try again.
		m.checkpointRetry = true
		m.armCheckpointRetry(o)
		return
	}
	if len(d.Keys) == 0 {
		m.noCheckpointFound(o)
		return
	}
	m.emitCheckpointGet(o, d.Keys[len(d.Keys)-1])
}

// noCheckpointFound handles a restore that found no checkpoint object.
func (m *Replica) noCheckpointFound(o *outbox) {
	m.checkpointRetry = false
	if m.needsCheckpoint {
		// The log was trimmed under us but no snapshot is there (yet). Retry;
		// do not catch up from slot 0 through the trimmed prefix.
		m.armCheckpointRetry(o)
		return
	}
	m.afterRestore(o)
}

func (m *Replica) onObjGetDone(d runtime.ObjectGetDone, o *outbox) {
	if m.pendingKind != pendingObjGet {
		return
	}
	m.pendingKind = pendingNone
	if d.Err == runtime.ErrNotFound {
		// Listed a moment ago and gone now: a newer checkpoint replaced it.
		m.emitCheckpointList(o)
		return
	}
	if d.Err != runtime.OK {
		m.checkpointRetry = true
		m.armCheckpointRetry(o)
		return
	}
	m.checkpointRetry = false
	if len(d.Data) > 0 && metadb.CheckSnapshot(d.Data) != nil {
		// A changed byte in the read: the stored object may be fine. Read it again.
		m.env.Reachable(checkpointReadBadCRC)
		m.emitCheckpointGet(o, m.restoreKey)
		return
	}
	if len(d.Data) == 0 {
		m.noCheckpointFound(o)
		return
	}
	slot, err := m.meta.Restore(bytes.NewReader(d.Data))
	if err != nil {
		m.trip(o, err)
		return
	}
	m.startAt(runtime.LogPosition(slot))
	m.checkpointSlot = runtime.LogPosition(slot)
	m.needsCheckpoint = false
	m.env.Reachable(replicaRestoredFromCheckpoint)
	m.afterRestore(o)
}

func (m *Replica) armCheckpointRetry(o *outbox) {
	after := m.config.Heartbeat
	if after <= 0 {
		after = 250 * actor.Millisecond
	}
	o.StartTimer(timerCheckpoint, after, runtime.CheckpointDue{})
}

func (m *Replica) onObjPutDone(d runtime.ObjectPutDone, o *outbox) {
	if m.checkpointStage != stageUpload {
		return
	}
	m.checkpointStage = 0
	slot := m.checkpointPending
	m.checkpointPending = 0
	m.checkpointData = nil
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		return
	}
	previous := m.checkpointSlot
	m.checkpointSlot = max(m.checkpointSlot, slot)
	m.checkpointAt = m.now
	m.emitTrim(o, slot)
	if previous > 0 && previous < slot {
		// The older snapshot is no longer needed. Fire and forget: a failed
		// delete only leaves one extra object, and restore picks the newest.
		work(o).Do(runtime.ObjectDelete{Key: m.checkpointKeyFor(previous)})
	}
}

func (m *Replica) emitTrim(o *outbox, checkpointSlot runtime.LogPosition) {
	keep := m.config.KeepSlots
	if keep <= 0 {
		return
	}
	below := checkpointSlot - runtime.LogPosition(keep)
	if below <= 0 {
		return
	}
	through := below - 1
	m.checkpointStage = stageTrim
	m.trimThrough = through
	actor.Perform(work(o), runtime.LogDeleteOld{Through: through},
		func(d runtime.LogDeleteOldDone) runtime.EventData { return d })
}

func (m *Replica) onTrimDone(d runtime.LogDeleteOldDone) {
	if m.checkpointStage != stageTrim {
		return
	}
	through := m.trimThrough
	m.checkpointStage, m.trimThrough = 0, 0
	if d.Err == runtime.OK && d.Outcome == runtime.Applied {
		m.trimFloor = through + 1
		m.env.Reachable(logTrimmedBelowCheckpoint)
	}
}

func (m *Replica) startAt(slot runtime.LogPosition) {
	m.applied = slot
	m.high = max(m.high, slot)
	m.goalTail = max(m.goalTail, slot)
	clear(m.buf)
}
