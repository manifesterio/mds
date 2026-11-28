package replica

import (
	"fmt"
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// One node holds the write role at a time (sticky appender / epoch fence). A
// node becomes the writer by claiming a new epoch in the log (a compare-and-swap
// on one item), and then it writes a fence entry at the tail. Every entry it
// writes carries the epoch. A node that loses a slot to a newer epoch, or that
// reads an entry from a newer epoch, stops writing and stays a standby: it keeps
// reading and serving reads. The first node (rank 0) claims at once on an empty
// log. Every other claim waits for TakeoverAfter of silence from other writers
// while the node's own reads still work.

// Ready reports that restore (if any) and the first catch-up round finished.
func (m *Replica) Ready() bool { return m.ready }

// IsActive reports the advisory appender role.
func (m *Replica) IsActive() bool { return m.active }

// Epoch is the writer epoch this node holds. It is zero when the node is not the writer.
func (m *Replica) Epoch() uint64 { return m.epoch }

// StepDowns is how many times this node stopped being the writer because it saw
// a newer epoch.
func (m *Replica) StepDowns() uint64 { return m.stepDowns }

// Promote makes this node the writer at once, without a claim in the log. It is
// a test and operator hook: it does not fence another writer.
func (m *Replica) Promote() {
	m.epoch = m.maxEpochSeen + 1
	m.maxEpochSeen = m.epoch
	m.active = true
}

// Demote makes this node a standby; in-flight appends still finish. A claim
// in flight is dropped: its answer is ignored.
func (m *Replica) Demote() { m.dropRole() }

// dropRole clears everything that makes this node the writer.
func (m *Replica) dropRole() {
	m.epoch, m.active, m.claiming, m.fencedFlag = 0, false, false, false
}

func (m *Replica) rolesArmed() bool {
	return m.config.Heartbeat > 0 || m.config.TakeoverAfter > 0
}

func (m *Replica) isAppender() bool {
	if !m.rolesArmed() {
		return true
	}
	return m.active
}

func (m *Replica) canCommit() bool {
	if m.booting || m.crashed || m.now < m.stallUntil {
		return false
	}
	if m.rolesArmed() {
		return m.ready && m.active
	}
	return true
}

func (m *Replica) roleEvery() actor.Duration {
	a, b := m.config.Heartbeat, m.config.TakeoverAfter
	switch {
	case a > 0 && b > 0:
		return min(a, b)
	case a > 0:
		return a
	default:
		return b
	}
}

func (m *Replica) armRole(o *outbox) {
	every := m.roleEvery()
	if every <= 0 || m.exited {
		return
	}
	o.StartTimer(timerRole, every, runtime.RoleDue{})
}

func (m *Replica) onRoleTimer(o *outbox) {
	if !m.rolesArmed() {
		return
	}
	m.roleTick(o)
	m.armRole(o)
}

func (m *Replica) roleTick(o *outbox) {
	if m.crashed {
		if m.now < m.downUntil {
			return
		}
		m.crashed = false
		m.afterCrashRestart(o)
		return
	}
	if m.now < m.stallUntil {
		return
	}
	if m.maybeReplicaFault(o); m.crashed || m.now < m.stallUntil {
		return
	}
	switch {
	case m.claimDue():
		m.emitClaim(o)
	case m.heartbeatDue():
		m.emitHeartbeat(o)
	}
}

// canClaim reports that this node is fully caught up and may ask for the write
// role. A node that is restoring or catching up never claims and never writes.
func (m *Replica) canClaim() bool {
	return m.rolesArmed() && !m.active && m.epoch == 0 && !m.claiming &&
		m.ready && !m.booting && !m.crashed && m.now >= m.stallUntil &&
		m.pendingKind == pendingNone && !m.checkpointRetry && !m.needsCheckpoint && m.applied >= m.goalTail
}

// claimDue reports that the silence rule says to claim the write role now.
func (m *Replica) claimDue() bool {
	take := m.config.TakeoverAfter
	if take <= 0 || !m.canClaim() {
		return false
	}
	wait := actor.Mono(take) + actor.Mono(m.config.Rank)*actor.Mono(m.config.TakeoverRankStep)
	return m.now-m.lastOtherAt > wait && m.now-m.lastPullOK < actor.Mono(take)
}

// claimNow claims at boot for rank 0 when the log has no writer epoch yet.
func (m *Replica) claimNow(o *outbox) {
	if m.config.Rank != 0 || m.maxEpochSeen != 0 || !m.canClaim() {
		return
	}
	m.emitClaim(o)
}

// emitClaim asks the log for the write role. The answer of an earlier claim
// that Demote dropped may still be out: it is forgotten first, so only this
// claim is answered.
func (m *Replica) emitClaim(o *outbox) {
	m.claiming = true
	m.claimExpect = m.maxEpochSeen
	role := roleScope(o)
	role.ForgetPending()
	actor.Perform(role, runtime.LogClaimEpoch{Expect: m.claimExpect, Owner: m.config.writer()},
		func(d runtime.LogClaimEpochDone) runtime.EventData { return d })
}

// onClaimDone handles the answer to a claim. A won claim makes this node the
// writer once its fence entry is applied. A lost claim only teaches the node
// the epoch that is stored now, and it waits a full silence period again.
func (m *Replica) onClaimDone(d runtime.LogClaimEpochDone, o *outbox) {
	if !m.claiming {
		return
	}
	m.claiming = false
	if d.Err != runtime.OK {
		m.lastOtherAt = m.now
		return
	}
	m.maxEpochSeen = max(m.maxEpochSeen, d.Epoch)
	// A claim that timed out may have landed: the log then shows our own name.
	if !d.Won && (d.Owner != m.config.writer() || d.Epoch != m.claimExpect+1) {
		m.lastOtherAt = m.now
		return
	}
	m.epoch = d.Epoch
	m.env.Reachable(epochClaimed)
	m.emitHeartbeat(o) // the fence entry: a no-op at the tail with the new epoch
}

// stepDown ends the write role after a newer epoch is seen. The node refuses
// the clients it was serving, keeps reading, and waits before it claims again.
// The answers of its appends and of its claim are forgotten; reads and the
// checkpoint lane go on.
func (m *Replica) stepDown(o *outbox) {
	m.env.Reachable(writerFencedNowReadOnly)
	m.stepDowns++
	m.refuseClients(o, runtime.ErrUnavailable, runtime.MaybeApplied)
	m.dropRole()
	commitScope(o).ForgetPending()
	roleScope(o).ForgetPending()
	m.queue = nil
	m.inFlight = nil
	m.waiters = map[string][]waitingCommand{}
	m.lastOtherAt = m.now
}

func (m *Replica) heartbeatDue() bool {
	hb := m.config.Heartbeat
	if !m.active || !m.ready || m.booting || hb <= 0 {
		return false
	}
	if m.now-m.lastOwnAt <= actor.Mono(hb) {
		return false
	}
	return len(m.queue) == 0 && m.inFlight == nil && m.pendingKind == pendingNone
}

func (m *Replica) maybeReplicaFault(o *outbox) {
	// Only impair the appender so a hot standby can take over (old fakelogservice
	// never impaired both replicas at once).
	if !m.active || !m.ready {
		return
	}
	if m.config.StallProb > 0 && m.env.Fault(siteStall, m.config.StallProb) {
		m.stallUntil = m.now + actor.Mono(betweenDur(m.env, m.config.StallDur))
		m.env.Reachable(replicaStalled)
		return
	}
	if m.config.CrashProb > 0 && m.env.Fault(siteCrash, m.config.CrashProb) {
		m.softCrash(o)
	}
}

func betweenDur(r interface{ Intn(n int) int }, d [2]actor.Duration) actor.Duration {
	if d[1] <= d[0] {
		return d[0]
	}
	return d[0] + actor.Duration(r.Intn(int(d[1]-d[0])+1))
}

// softCrash takes the node down for a while without ending the actor. Every
// log and object request still out, and the catch-up timer, are forgotten;
// queued and in-flight commands are dropped unanswered. The role timer stays:
// it brings the node back after downUntil.
func (m *Replica) softCrash(o *outbox) {
	m.env.Reachable(replicaCrashed)
	m.dropRole()
	m.ready = false
	m.booting = true
	m.crashed = true
	work(o).ForgetPending() // no catch-up while down
	m.downUntil = m.now + actor.Mono(betweenDur(m.env, m.config.DownDur))
	m.clearLocal()
	// Refuse client work until afterCrashRestart restores fold + catch-up.
	m.refuseReads(o)
}

// isBelowFloor reports a LogAppend that the log refused because its slot is at
// or below the floor (the trimmed part of the log). The put is not a commit.
// The log answers it as a conflict with no existing entry; a slot that is taken
// by a real entry always carries its bytes.
func isBelowFloor(d runtime.LogAppendDone) bool {
	return d.Err == runtime.ErrConflict && d.Taken && len(d.Existing) == 0
}

// hitTrimmed handles a read that found its next slot trimmed: refuse clients,
// drop local log state, and restore from the checkpoint. Catch-up from slot 0
// would re-enter the trimmed prefix, so needsCheckpoint blocks that until a
// snapshot is found. Every log and object request still out is forgotten.
func (m *Replica) hitTrimmed(o *outbox) {
	m.env.Reachable(replicaHitTrimmedSlot)
	m.refuseClients(o, runtime.ErrUnavailable, runtime.MaybeApplied)
	m.dropRole()
	m.ready = false
	m.booting = true
	m.needsCheckpoint = true
	work(o).ForgetPending()
	m.clearLocal()
	m.refuseReads(o)
	m.afterCrashRestart(o)
}

// clearLocal drops the local log state and every request lane. The answers
// of those requests are forgotten by the caller (ForgetPending on work).
func (m *Replica) clearLocal() {
	m.queue = nil
	m.inFlight = nil
	m.waiters = map[string][]waitingCommand{}
	m.buf = map[runtime.LogPosition][]byte{}
	m.pendingKind = pendingNone
	m.checkpointStage, m.trimThrough = 0, 0
	m.checkpointPending, m.checkpointData = 0, nil
}

func (m *Replica) refuseClients(o *outbox, err runtime.ErrCode, outcome runtime.Outcome) {
	refuse := func(c waitingCommand) {
		m.reply(o, c.replyTo, runtime.ControlPlaneDone{Err: err, Outcome: outcome})
	}
	seen := map[string]bool{}
	if f := m.inFlight; f != nil {
		seen[f.id] = true
		for _, c := range f.cmds {
			refuse(c)
		}
	}
	for _, c := range m.queue {
		refuse(c)
	}
	ids := make([]string, 0, len(m.waiters))
	for id := range m.waiters { //determinismcheck:ok keys are collected, then sorted before use
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		for _, c := range m.waiters[id] {
			refuse(c)
		}
	}
}

func (m *Replica) afterCrashRestart(o *outbox) {
	// An old catch-up timer must not start a tail check while the restore waits:
	// it would make the node Ready at slot 0 over a trimmed log.
	work(o).CancelTimer(timerCatchUp)
	// Close the old store first: a database may need its directory back.
	_ = m.meta.Close() // the old state is dropped whether or not close succeeds
	meta, err := m.config.openMetadata()
	if err != nil {
		m.trip(o, err)
		return
	}
	m.meta = meta
	m.reseedTopics()
	m.applied, m.high, m.goalTail = 0, 0, 0
	m.tailChecked = false
	m.seq = 0
	if m.checkpointEnabled() {
		m.emitCheckpointList(o)
		return
	}
	m.afterRestore(o)
}

func (m *Replica) emitHeartbeat(o *outbox) {
	m.seq++
	id := fmt.Sprintf("%s/e%d/%d", m.config.writer(), m.epoch, m.seq)
	inner := metadb.Encode(metadb.Entry{ID: id, Kind: metadb.KindNoop})
	data := wrap(m.epoch, m.config.writer(), inner)
	m.emitAppend(o, &inFlightAppend{slot: m.alloc(), id: id, data: data})
}

func (m *Replica) observeWriter(epoch uint64, writer string, enveloped bool) {
	if !enveloped || writer == "" {
		return
	}
	m.maxEpochSeen = max(m.maxEpochSeen, epoch)
	if writer == m.config.writer() {
		m.lastOwnAt = m.now
		return
	}
	m.lastOtherAt = m.now
	if m.epoch > 0 && epoch > m.epoch {
		m.fencedFlag = true
	}
}

func (m *Replica) notePullOK() {
	m.lastPullOK = m.now
}
