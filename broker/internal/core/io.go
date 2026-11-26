package core

//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/shared/objectstore"
)

// IO is the product IO of the broker and of the simulated clients: sockets,
// the object store and the control plane.
//
// The socket cases are handed to Outbox.Do. What happens on a socket later
// (bytes, a close by the peer) comes back as unsolicited messages
// (BytesReceived, ConnClosed), not as an answer.
//
// The other cases are requests with one answer each (//dst:answer), handed to
// actor.Perform. A live incarnation always gets that answer: a lost response
// comes back as ErrTimeout with MaybeApplied, never as silence.
//
//sumtype:decl
//dst:sum
type IO interface {
	actor.Payload
	isIO()
}

// ---- sockets ----

//dst:cases IO
type (
	// Send writes bytes to a connection, in order with earlier Sends on it.
	Send struct {
		Conn ConnID
		Data []byte
	}

	// CloseConn closes gracefully (FIN after queued bytes) or aborts (RST).
	CloseConn struct {
		Conn  ConnID
		Abort bool
	}

	// PauseReads stops reading from a connection (admission control). Bytes
	// already received are still delivered. In production the reader
	// goroutine parks; in simulation the bytes stay in the sender's
	// flow-control window.
	PauseReads struct{ Conn ConnID }

	// ResumeReads undoes PauseReads.
	ResumeReads struct{ Conn ConnID }
)

// Connect opens an outbound connection. Simulated clients use it; brokers
// reach the control plane through ControlPlaneCall, not raw dials.
//
//dst:cases IO
//dst:answer ConnectDone
type Connect struct{ Addr string }

// ---- object store ----

// ObjectCall is one request to the object store. Call is one of
// objectstore.Put, Get, Delete or List; its Req field is not used (the answer
// finds its way back through Perform). The case is a wrapper because a sealed
// sum cannot get a case from another package.
//
//dst:cases IO
//dst:answer ObjectDone
//dst:fold manual
type ObjectCall struct{ Call objectstore.Call }

// ---- control plane ----

// ControlPlaneCall asks the executor to perform one control-plane call. Op is
// the in-process vocabulary of controlplane/contract; the gRPC layer maps it
// to the wire messages. Typed request fields stay off this struct: importing
// the generated package would pull grpc into the deterministic core.
//
//dst:cases IO
//dst:answer ControlPlaneDone
type ControlPlaneCall struct {
	Op contract.Operation
	// Timeout is the call's deadline. Zero takes the executor's default. Group
	// joins and syncs wait for a rebalance, so they set a long one.
	Timeout actor.Duration
}
