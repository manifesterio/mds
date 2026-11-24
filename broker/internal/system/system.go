// Package system is the production seam: narrow interfaces over real I/O that
// the production exec function and the socket goroutines use. SDK and HTTP
// types never leak past an adapter. The simulator does not implement these;
// it runs the same broker actor over simulated IO. The production runtime is
// a dst/runtime/live Host (package production).
package system

import (
	"context"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectStore is the S3 adapter. The interface is defined in shared/objectstore.
type ObjectStore = objectstore.Store

// ControlPlane is the client adapter for the control plane.
type ControlPlane interface {
	Call(ctx context.Context, operation contract.Operation) (contract.Response, error)
}

// Connections writes to and closes accepted connections. Writes on one connection
// are applied in call order (one writer goroutine per connection).
type Connections interface {
	Write(conn core.ConnID, data []byte) error
	Close(conn core.ConnID, abort bool) error
	// PauseReads parks the connection's reader goroutine; ResumeReads wakes it.
	PauseReads(conn core.ConnID)
	ResumeReads(conn core.ConnID)
}

// ErrorClassifier is the single place SDK/HTTP/net errors become plain data.
// Getting Outcome right here is what makes acks honest.
type ErrorClassifier interface {
	// Classify reduces err to plain data. It logs err; the core only sees the
	// code. The Outcome depends on which operation failed, not only on err.
	Classify(io core.IO, err error) (core.ErrCode, core.Outcome)
}

// Inbox is where the unsolicited inputs of the broker go: accepted
// connections, bytes read, closes by the peer and live broker sets. The
// broker's priority function picks the queue (core.PriorityOf); connection
// input is Data, whose real backpressure is PauseReads.
type Inbox interface {
	// Tell gives msg to the broker and never blocks. It returns an error once
	// the broker has stopped; the message is then dropped.
	Tell(msg core.EventData) error
}
