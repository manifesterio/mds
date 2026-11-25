package runtime

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/actor"
)

// EventData is every message the replica takes: lifecycle, its own timers,
// the answers of its IO, and the requests of its callers.
//
//sumtype:decl
//dst:sum
type EventData interface {
	actor.Payload
	isEventData()
}

// ---- lifecycle ----

// NodeStarted is the first message of every boot. The node rebuilds what it
// needs from the log and the object store; nothing in memory survived.
//
//dst:cases EventData
type NodeStarted struct{ Self actor.Incarnation }

// StopRequest asks the node to shut down gracefully by Deadline (node-local
// time): stop admitting, finish or hand off in-flight work, then Stop with
// Exit. If the deadline passes first the runtime kills the node. Priority:
// Control.
//
//dst:cases EventData
type StopRequest struct{ Deadline actor.Mono }

// ---- timers ----

//dst:cases EventData
type (
	// CatchUpDue says it is time to check the log tail and pull.
	CatchUpDue struct{}
	// RoleDue says it is time for the role tick (claim, heartbeat, crash recovery).
	RoleDue struct{}
	// CheckpointDue says it is time to consider a checkpoint.
	CheckpointDue struct{}
)

// ---- answers of IO ----

// ObjectPutDone reports that an object PUT finished.
//
//dst:cases EventData
type ObjectPutDone struct {
	Key     string
	Err     ErrCode
	Outcome Outcome
}

// ObjectGetDone reports that an object GET finished.
//
//dst:cases EventData
type ObjectGetDone struct {
	Data []byte
	Err  ErrCode
}

// ObjectListDone reports that an object LIST finished.
//
//dst:cases EventData
type ObjectListDone struct {
	Keys []string // sorted
	Err  ErrCode
}

// ObjectDeleteDone reports that an object DELETE finished. The replica does
// not wait for it.
//
//dst:payload
type ObjectDeleteDone struct {
	Err     ErrCode
	Outcome Outcome
}

// LogEntry is one slot's payload as returned by LogRead.
type LogEntry struct {
	Slot LogPosition
	Data []byte
}

// LogAppendDone reports that a LogAppend finished. Taken means the slot was
// already held; Existing is the winner's bytes when Taken is true.
//
//dst:cases EventData
type LogAppendDone struct {
	Slot     LogPosition
	Taken    bool
	Existing []byte
	Err      ErrCode
	Outcome  Outcome
}

// LogReadDone reports that a LogRead finished. Entries are in slot order.
//
//dst:cases EventData
type LogReadDone struct {
	Entries []LogEntry
	Err     ErrCode
	Outcome Outcome
}

// LogGetEndDone reports the log tail (next slot to write).
//
//dst:cases EventData
type LogGetEndDone struct {
	Tail    LogPosition
	Err     ErrCode
	Outcome Outcome
}

// LogDeleteOldDone reports that a LogDeleteOld finished.
//
//dst:cases EventData
type LogDeleteOldDone struct {
	Err     ErrCode
	Outcome Outcome
}

// LogClaimEpochDone reports that a LogClaimEpoch finished. Epoch is the new
// epoch when Won is true, and the epoch stored now when Won is false. Owner is
// the owner stored with that epoch, so a node whose claim timed out can see
// that the claim had landed (same Owner, Epoch == Expect+1).
//
//dst:cases EventData
type LogClaimEpochDone struct {
	Epoch   uint64
	Owner   string
	Won     bool
	Err     ErrCode
	Outcome Outcome
}

// ---- callers ----

// ControlPlaneDeliver is one control-plane operation from a caller (the gRPC
// server or rebalance), answered once it is applied.
//
//dst:cases EventData
type ControlPlaneDeliver struct {
	ReplyTo actor.ReplyTo[ControlPlaneDone]
	Op      ControlPlaneOperation
}

// LocalRead asks the node to answer a read from its own state, now. Unlike a
// read in ControlPlaneDeliver it does not wait for a fresh check of the log
// tail, so the answer can be a little behind the log.
//
//dst:cases EventData
type LocalRead struct {
	ReplyTo actor.ReplyTo[ControlPlaneDone]
	Op      ControlPlaneOperation
}

// ControlPlaneDone is the answer to a control-plane operation. Resp is the
// in-process response (nil unless Err == OK). Slot is the log slot the
// operation was applied at, when metadb reports it.
//
//dst:payload
type ControlPlaneDone struct {
	Slot    LogPosition
	Resp    ControlPlaneResponse
	Err     ErrCode
	Outcome Outcome
}

// ResponseAs returns d.Resp asserted to T, or the zero value.
func ResponseAs[T any](d ControlPlaneDone) T {
	r, _ := d.Resp.(T)
	return r
}

// Answer returns the answer without the slot: the plain contract.Done that
// the reference model (package spec) also gives.
func (e ControlPlaneDone) Answer() contract.Done {
	return contract.Done{Resp: e.Resp, Err: e.Err, Outcome: e.Outcome}
}
