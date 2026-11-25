package runtime

//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen

import "github.com/manifesterio/mds/dst/core/actor"

// IO is the product IO of the replica: calls to the shared command log and to
// the object store. The replica hands a case to actor.Perform; the executor
// runs it and answers with the case's //dst:answer type. A lost answer comes
// back as ErrTimeout with MaybeApplied, never as silence.
//
//sumtype:decl
//dst:sum
type IO interface {
	actor.Payload
	isIO()
}

// ---- object store ----

// ObjectPut uploads Data under Key. Key is unique per upload, so a
// MaybeApplied PUT is safe to retry.
//
//dst:cases IO
//dst:answer ObjectPutDone
type ObjectPut struct {
	Key  string
	Data []byte
}

// ObjectGet reads the byte range Ref.
//
//dst:cases IO
//dst:answer ObjectGetDone
type ObjectGet struct{ Ref SegmentRef }

// ObjectDelete removes the object at Key.
//
//dst:cases IO
//dst:answer ObjectDeleteDone
type ObjectDelete struct{ Key string }

// ObjectList lists the keys under Prefix.
//
//dst:cases IO
//dst:answer ObjectListDone
type ObjectList struct{ Prefix string }

// ---- shared command log ----

// LogAppend is an if-absent put of Data at Slot.
//
//dst:cases IO
//dst:answer LogAppendDone
type LogAppend struct {
	Slot LogPosition
	Data []byte
}

// LogRead reads up to Limit entries at or after From, in slot order.
//
//dst:cases IO
//dst:answer LogReadDone
type LogRead struct {
	From  LogPosition
	Limit int32
}

// LogGetEnd reports the highest contiguous written slot plus one (the tail).
//
//dst:cases IO
//dst:answer LogGetEndDone
type LogGetEnd struct{}

// LogClaimEpoch asks the log to make a node the one writer. The log keeps one
// writer epoch. The claim succeeds only if the stored epoch still equals
// Expect (zero when none was stored yet). Then the stored epoch becomes
// Expect+1 and Owner is recorded. A claim that loses reports the epoch that is
// stored now. Nothing is written to a log slot.
//
//dst:cases IO
//dst:answer LogClaimEpochDone
type LogClaimEpoch struct {
	Expect uint64
	Owner  string
}

// LogDeleteOld removes every slot at or below Through. The log first raises
// its floor to Through, and only then deletes the slots. A LogAppend that puts
// into a slot at or below the floor is not a commit: the log answers it as a
// conflict with no existing entry.
//
//dst:cases IO
//dst:answer LogDeleteOldDone
type LogDeleteOld struct{ Through LogPosition }
