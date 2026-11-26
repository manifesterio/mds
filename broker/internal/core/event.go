package core

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/shared/objectstore"
)

// EventData is every message the broker takes: lifecycle, connection events,
// its own timers, and the answers of its IO. It is a closed set of
// plain-data structs.
//
//sumtype:decl
//dst:sum
type EventData interface {
	actor.Payload
	isEventData()
}

// ---- lifecycle ----

// NodeStarted is the first message of every boot. The node rebuilds what it
// needs from the control plane and the object store; nothing in memory
// survived.
//
//dst:cases EventData
type NodeStarted struct{ Self actor.Incarnation }

// StopRequest asks the node to shut down gracefully by Deadline (node-local
// time): stop admitting, finish or hand off in-flight work, then Stop with
// Exit. If the deadline passes first the runtime kills the node.
//
//dst:cases EventData
type StopRequest struct{ Deadline actor.Mono }

// ---- network (inbound, unsolicited) ----

//dst:cases EventData
type (
	// ConnOpened reports that a connection was accepted.
	ConnOpened struct {
		Conn ConnID
		Peer string // remote address, informational only
	}

	// BytesReceived is a chunk of a TCP byte stream. Chunks may be
	// arbitrarily short: framing is the receiver's job.
	BytesReceived struct {
		Conn ConnID
		Data []byte
	}

	// ConnClosed reports EOF (graceful) or reset (abort) from the peer or
	// the network.
	ConnClosed struct {
		Conn  ConnID
		Abort bool
	}
)

// ---- answers of IO ----

// ConnectDone is the answer of Connect. Conn is valid only when Err == OK.
//
//dst:cases EventData
type ConnectDone struct {
	Conn ConnID
	Err  ErrCode
}

// ObjectDone is the answer of ObjectCall. Result is one of
// objectstore.PutDone, GetDone, DeleteDone or ListDone; its Req field is not
// used. The case is a wrapper because a sealed sum cannot get a case from
// another package.
//
//dst:cases EventData
//dst:fold manual
type ObjectDone struct{ Result objectstore.Result }

// ControlPlaneDone is the answer of ControlPlaneCall. Resp is the in-process
// response (nil unless Err == OK).
//
//dst:cases EventData
type ControlPlaneDone struct {
	Resp    contract.Response
	Err     ErrCode
	Outcome Outcome
}

// ResponseAs returns d.Resp asserted to T, or the zero value.
func ResponseAs[T any](d ControlPlaneDone) T {
	r, _ := d.Resp.(T)
	return r
}

// The broker hands its object store and control plane calls to Perform with
// an adapter that puts the answer into one of these, together with the
// segment or operation the call belongs to. Seg and Op are the broker's own
// ids, unique within one incarnation.
//
//dst:cases EventData
type (
	// SegmentPutDone is the answer of the PUT of segment Seg.
	SegmentPutDone struct {
		Seg  uint64
		Done ObjectDone
	}

	// SegmentCommitDone is the answer of the CommitSegment of segment Seg.
	SegmentCommitDone struct {
		Seg  uint64
		Done ControlPlaneDone
	}

	// OpReadDone is the answer of an object store read of operation Op.
	OpReadDone struct {
		Op   uint64
		Done ObjectDone
	}

	// OpCallDone is the answer of a control-plane call of operation Op.
	OpCallDone struct {
		Op   uint64
		Done ControlPlaneDone
	}
)

// ---- timers ----

//dst:cases EventData
type (
	// FlushDue says that the batch window of the open segment has passed.
	FlushDue struct{}

	// SegmentRetryDue says that the backoff of segment Seg has passed: its
	// PUT or its commit goes out again.
	SegmentRetryDue struct{ Seg uint64 }

	// OpDue says that operation Op has waited long enough: its backoff
	// has passed, or a parked Fetch part polls again.
	OpDue struct{ Op uint64 }

	// ClientDue is a timer of a simulated client, named by the client's own
	// Key. The broker never receives it.
	ClientDue struct{ Key string }
)

// ---- broker live set ----

// LiveBrokers gives the broker the live broker set from the control plane. The
// production shell sends it after each heartbeat, and sends an empty list when
// the set is stale (the control plane is down). The broker keeps the last list
// and answers Metadata with it. Metadata always lists the broker itself, so an
// empty list means "self only". The simulator never sends it.
//
//dst:cases EventData
type LiveBrokers struct{ Brokers []BrokerAddr }
