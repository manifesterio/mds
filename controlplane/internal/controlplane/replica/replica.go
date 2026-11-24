package replica

import (
	"bytes"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
)

// outbox is the Outbox the replica records its effects on.
type outbox = actor.Outbox[runtime.EventData, runtime.IO]

// Timer keys. The catch-up timer lives in the work scope (see work), so
// ForgetPending on work drops it; the role and checkpoint timers live at the
// actor level and survive it.
const (
	timerCatchUp    actor.TimerKey = "catchup"
	timerRole       actor.TimerKey = "role"
	timerCheckpoint actor.TimerKey = "checkpoint"
)

// Replica is the control-plane actor. Build it with New or Props.
type Replica struct {
	env    runtime.Environment
	self   actor.Incarnation
	config Config

	openErr error // the first store could not open; NodeStarted stops the actor

	started  bool
	stopping bool
	exited   bool
	booting  bool // restore/catch-up in progress; Ready flips when done
	ready    bool
	active   bool // advisory appender role

	meta *metadb.DB

	now          actor.Mono
	seq          uint64
	applied      runtime.LogPosition // next slot to apply
	goalTail     runtime.LogPosition // CheckTail answer for the current round
	high         runtime.LogPosition // next slot to allocate for commit appends
	buf          map[runtime.LogPosition][]byte
	epoch        uint64 // writer epoch this node holds; zero when it is not the writer
	claiming     bool   // a LogClaimEpoch is in flight
	claimExpect  uint64 // epoch that claim expected to find
	fencedFlag   bool   // a newer epoch was seen while applying; step down after the drain
	stepDowns    uint64 // times this node stopped being the writer because of a newer epoch
	tailChecked  bool   // a tail check finished since the last restore
	maxEpochSeen uint64 // highest writer epoch seen in applied log entries
	restoreKey   string // checkpoint object being read
	// pendingKind is the catch-up or restore request in flight (one at a
	// time, in the pull scope); pendingNone when idle.
	pendingKind pendingKind

	lastPullOK  actor.Mono
	lastOtherAt actor.Mono
	lastOwnAt   actor.Mono

	checkpointSlot    runtime.LogPosition // newest checkpoint slot (written or restored)
	checkpointAt      actor.Mono
	checkpointPending runtime.LogPosition // slot of in-flight ObjectPut snapshot
	checkpointData    []byte
	// The checkpoint lane: the snapshot upload, then the log trim, one request
	// at a time. It never uses pendingKind, so a slow upload cannot block
	// catch-up and catch-up cannot block the checkpoint. Zero when idle.
	checkpointStage checkpointStage
	trimThrough     runtime.LogPosition // Through of the in-flight trim

	queue           []waitingCommand // client ops waiting for a commit slot
	inFlight        *inFlightAppend  // the commit or heartbeat LogAppend in flight; nil when none
	waiters         map[string][]waitingCommand
	trimFloor       runtime.LogPosition // slots below are trimmed
	stallUntil      actor.Mono
	downUntil       actor.Mono
	crashed         bool
	needsCheckpoint bool // fell behind a trim; must restore before catch-up from zero
	checkpointRetry bool // the last checkpoint read failed; read again before catch-up
	seeds           []topicSeed

	readWaiters  []readWaiter // lookups and describes waiting for a fresh tail
	tailsStarted uint64       // tail checks started so far
	tailRound    uint64       // round of the tail check in flight
}

type topicSeed struct {
	name  string
	parts int32
}

type pendingKind uint8

const (
	pendingNone pendingKind = iota
	pendingTail
	pendingRead
	pendingObjList
	pendingObjGet
)

// checkpointStage says which request the checkpoint lane is waiting for.
type checkpointStage uint8

const (
	stageUpload checkpointStage = iota + 1
	stageTrim
)

// Factory builds a replica for one incarnation of a node.
type Factory func(env runtime.Environment) *Replica

// New returns the Factory of the control-plane actor. A factory cannot return
// an error, so a store that cannot open (Config.OpenStore) is remembered and
// the actor stops with Trip on NodeStarted: the process cannot serve without
// its state.
func New(config Config) Factory {
	return func(env runtime.Environment) *Replica {
		meta, err := config.openMetadata()
		if err != nil {
			meta = metadb.NewMem() // placeholder; NodeStarted stops the actor
		}
		return &Replica{
			env: env, self: env.Self(), config: config, openErr: err,
			meta:    meta,
			buf:     map[runtime.LogPosition][]byte{},
			waiters: map[string][]waitingCommand{},
		}
	}
}

// Props says how to run the replica. Every boot builds a new Replica and,
// when keep is not nil, hands it to keep, so the caller can read it (Ready,
// Metadata, ...). A graceful stop is a StopRequest, received before data.
func Props(config Config, keep func(*Replica)) actor.Props[runtime.EventData, runtime.IO] {
	factory := New(config)
	return actor.Props[runtime.EventData, runtime.IO]{
		Name: "replica",
		New: func(env actor.Env) actor.Actor[runtime.EventData, runtime.IO] {
			m := factory(env)
			if keep != nil {
				keep(m)
			}
			return m
		},
		Boot: func(self actor.Incarnation) runtime.EventData { return runtime.NodeStarted{Self: self} },
		StopMsg: func(deadline actor.Mono) runtime.EventData {
			return runtime.StopRequest{Deadline: deadline}
		},
		Priority: func(msg runtime.EventData) actor.Priority {
			if _, ok := msg.(runtime.StopRequest); ok {
				return actor.Control
			}
			return actor.Data
		},
	}
}

// Metadata is the metadb after the applied prefix. Tests compare its
// digest to an independent oracle.
func (m *Replica) Metadata() *metadb.DB { return m.meta }

// Applied is the next slot to apply (slots below are folded).
func (m *Replica) Applied() runtime.LogPosition { return m.applied }

// SeedTopic pre-creates a topic on the fold before NodeStarted (sim campaign setup).
func (m *Replica) SeedTopic(name string, partitions int32) runtime.ErrCode {
	err := m.meta.CreateTopic(name, partitions)
	if err == runtime.OK {
		m.seeds = append(m.seeds, topicSeed{name: name, parts: partitions})
	}
	return err
}

func (m *Replica) reseedTopics() {
	for _, t := range m.seeds {
		_ = m.meta.CreateTopic(t.name, t.parts)
	}
}

// Replica is an actor.
var _ actor.Actor[runtime.EventData, runtime.IO] = (*Replica)(nil)

// Scopes. Every log and object request and the catch-up timer are recorded
// in work, so ForgetPending on work drops all of them at once (a soft crash,
// a trimmed slot). Commit and heartbeat appends are in work/commit and the
// claim in work/role, so a step-down drops just those. The catch-up and
// restore requests are in work/pull: one of them is in flight at a time, and
// starting a new one while another is out forgets the old one.

func identity(e runtime.EventData) runtime.EventData { return e }

func work(o *outbox) *outbox { return actor.Sub(o, "work", identity) }

func commitScope(o *outbox) *outbox { return actor.Sub(work(o), "commit", identity) }

func roleScope(o *outbox) *outbox { return actor.Sub(work(o), "role", identity) }

func pullScope(o *outbox) *outbox { return actor.Sub(work(o), "pull", identity) }

// Receive takes one message and records what the replica wants done, in
// order. Answers that nothing waits for any more leave the state unchanged.
// After a Stop every message is a no-op.
func (m *Replica) Receive(now actor.Now, msg runtime.EventData, o *outbox) {
	if m.exited {
		return
	}
	m.now = now.Mono
	switch body := msg.(type) {
	case runtime.NodeStarted:
		m.onStarted(o)
	case runtime.StopRequest:
		m.stop(o)
	case runtime.CatchUpDue:
		m.onCatchUpDue(o)
	case runtime.RoleDue:
		m.onRoleTimer(o)
	case runtime.CheckpointDue:
		m.onCheckpointTimer(o)
	case runtime.LogGetEndDone:
		m.onCheckTailDone(body, o)
	case runtime.LogReadDone:
		m.onReadDone(body, o)
	case runtime.LogAppendDone:
		m.onAppendDone(body, o)
	case runtime.LogClaimEpochDone:
		m.onClaimDone(body, o)
	case runtime.ObjectListDone:
		m.onObjListDone(body, o)
	case runtime.ObjectGetDone:
		m.onObjGetDone(body, o)
	case runtime.ObjectPutDone:
		m.onObjPutDone(body, o)
	case runtime.LogDeleteOldDone:
		m.onTrimDone(body)
	case runtime.ControlPlaneDeliver:
		m.onControlPlaneDeliver(body, o)
		return
	case runtime.LocalRead:
		m.onLocalRead(body, o)
		return
	}
	m.serveReads(o)
}

func (m *Replica) onStarted(o *outbox) {
	m.started = true
	if m.openErr != nil {
		m.trip(o, m.openErr)
		return
	}
	if m.checkpointEnabled() {
		m.booting = true
		m.ready = false
		m.emitCheckpointList(o)
		return
	}
	m.afterRestore(o)
}

func (m *Replica) afterRestore(o *outbox) {
	if m.config.CatchUpEvery > 0 {
		m.booting = true
		m.ready = false
		m.armCatchUp(o)
		return
	}
	m.finishBoot(o)
}

func (m *Replica) finishBoot(o *outbox) {
	m.booting = false
	m.ready = true
	m.lastOtherAt, m.lastPullOK, m.lastOwnAt = m.now, m.now, m.now
	m.claimNow(o)
	m.armRole(o)
	m.armCheckpoint(o)
}

// maybeBecomeReady finishes the boot when the restore and the first catch-up
// round are done.
func (m *Replica) maybeBecomeReady(o *outbox) {
	if !m.booting || m.pendingKind != pendingNone || m.applied < m.goalTail {
		return
	}
	// A restore is not over until the checkpoint is read and a tail check
	// finished after it. An append completion alone must never make the node ready.
	if m.checkpointRetry || m.needsCheckpoint || (m.config.CatchUpEvery > 0 && !m.tailChecked) {
		return
	}
	m.finishBoot(o)
}

func (m *Replica) onCatchUpDue(o *outbox) {
	if m.pendingKind != pendingNone || m.config.CatchUpEvery <= 0 {
		return
	}
	m.emitCheckTail(o)
}

func (m *Replica) onCheckTailDone(d runtime.LogGetEndDone, o *outbox) {
	if m.pendingKind != pendingTail {
		return
	}
	m.pendingKind = pendingNone
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		m.armCatchUp(o)
		return
	}
	m.notePullOK()
	m.tailChecked = true
	m.goalTail = d.Tail
	m.noteTail(m.tailRound, d.Tail)
	m.progress(o)
}

func (m *Replica) onReadDone(d runtime.LogReadDone, o *outbox) {
	if m.pendingKind != pendingRead {
		return
	}
	m.pendingKind = pendingNone
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		m.armCatchUp(o)
		return
	}
	m.notePullOK()
	for _, e := range d.Entries {
		if e.Slot < m.applied {
			continue
		}
		m.buf[e.Slot] = bytes.Clone(e.Data)
		m.observeSlot(e.Slot)
	}
	// The writer appends one slot at a time and retries a slot until it knows
	// the result, so the log has no empty slot below its tail. LogRead starts at
	// m.applied and returns only slots that exist: if the next slot is missing but a
	// later one exists, this node fell behind a trim.
	if len(d.Entries) > 0 && m.nextSlotMissing() {
		m.hitTrimmed(o)
		return
	}
	if err := m.drain(o); err != nil {
		m.trip(o, err)
		return
	}
	if m.fencedFlag {
		m.stepDown(o)
	}
	m.progress(o)
	m.flushCommits(o)
}

// onAppendDone handles the answer to the commit or heartbeat append in
// flight. The slot must match: an answer for an append that was replaced is
// not this one's.
func (m *Replica) onAppendDone(d runtime.LogAppendDone, o *outbox) {
	if m.inFlight == nil || d.Slot != m.inFlight.slot {
		return
	}
	m.onCommitAppendDone(d, m.inFlight, o)
}

func (m *Replica) drain(o *outbox) error {
	for {
		data, ok := m.buf[m.applied]
		if !ok {
			return nil
		}
		delete(m.buf, m.applied)
		if err := m.applySlot(m.applied, data, o); err != nil {
			return err
		}
		m.applied++
	}
}

func (m *Replica) progress(o *outbox) {
	if m.pendingKind != pendingNone {
		return
	}
	if m.applied < m.goalTail || m.nextSlotMissing() {
		m.emitRead(o)
		return
	}
	m.maybeBecomeReady(o)
	m.armCatchUp(o)
}

// nextSlotMissing reports that a later slot is buffered (one of our own
// appends landed) while the next slot to apply has not been read yet.
func (m *Replica) nextSlotMissing() bool {
	if len(m.buf) == 0 {
		return false
	}
	_, ok := m.buf[m.applied]
	return !ok
}

// pull returns the scope of the catch-up and restore requests, with any
// request still out in it forgotten: only the newest one is answered.
func (m *Replica) pull(o *outbox, kind pendingKind) *outbox {
	p := pullScope(o)
	if m.pendingKind != pendingNone {
		p.ForgetPending()
	}
	m.pendingKind = kind
	return p
}

func (m *Replica) emitCheckTail(o *outbox) {
	p := m.pull(o, pendingTail)
	m.tailsStarted++
	m.tailRound = m.tailsStarted
	actor.Perform(p, runtime.LogGetEnd{}, func(d runtime.LogGetEndDone) runtime.EventData { return d })
}

func (m *Replica) emitRead(o *outbox) {
	p := m.pull(o, pendingRead)
	actor.Perform(p, runtime.LogRead{From: m.applied, Limit: m.config.readLimit()},
		func(d runtime.LogReadDone) runtime.EventData { return d })
}

func (m *Replica) armCatchUp(o *outbox) {
	if m.config.CatchUpEvery <= 0 || m.exited {
		return
	}
	work(o).StartTimer(timerCatchUp, m.config.CatchUpEvery, runtime.CatchUpDue{})
}

// trip ends the actor: its memory can not be trusted any more, so the
// runtime must start a new incarnation. Stop is the last effect of the
// Receive: once exited, nothing else is recorded.
func (m *Replica) trip(o *outbox, err error) {
	m.exited = true
	o.Stop(actor.Trip, err.Error())
}

// stop marks the actor shutting down and ends it. With no in-flight work
// yet that must drain, the end is immediate.
func (m *Replica) stop(o *outbox) {
	if m.stopping {
		return
	}
	m.stopping = true
	m.exited = true
	o.Stop(actor.Exit, "")
}

// StateHash folds every field that influences behaviour, including the applied
// metadb digest (comparable across replicas that folded the same prefix).
func (m *Replica) StateHash() uint64 {
	h := trace.New()
	h.Bool(m.started)
	h.Bool(m.stopping)
	h.Bool(m.exited)
	h.Bool(m.booting)
	h.Bool(m.ready)
	h.Bool(m.active)
	h.Uint64(m.seq)
	h.Uint64(uint64(m.applied))
	h.Uint64(uint64(m.goalTail))
	h.Uint64(uint64(m.high))
	h.Uint64(uint64(m.pendingKind))
	h.String(m.restoreKey)
	h.Bool(m.checkpointRetry)
	h.Uint64(m.epoch)
	h.Uint64(m.maxEpochSeen)
	h.Bool(m.claiming)
	h.Bool(m.fencedFlag)
	h.Uint64(m.stepDowns)
	h.Bool(m.tailChecked)
	h.Uint64(uint64(m.checkpointSlot))
	h.Uint64(uint64(m.checkpointStage))
	h.Uint64(uint64(len(m.queue)))
	h.Bool(m.inFlight != nil)
	h.Uint64(uint64(len(m.waiters)))
	h.Uint64(uint64(len(m.readWaiters)))
	h.Uint64(m.tailsStarted)
	h.Uint64(uint64(m.config.CatchUpEvery))
	h.Uint64(uint64(m.config.ReadLimit))
	h.Uint64(uint64(m.config.MaxCmdsPerSlot))
	h.Uint64(uint64(m.config.QueueMax))
	h.Uint64(uint64(m.config.Rank))
	h.Uint64(uint64(m.config.Heartbeat))
	h.Uint64(uint64(m.config.TakeoverAfter))
	h.Uint64(uint64(m.config.CheckpointEvery))
	h.Uint64(uint64(m.config.CheckpointMinSlots))
	h.Uint64(uint64(m.config.KeepSlots))
	h.Uint64(m.meta.StateHash())
	return h.Sum()
}
