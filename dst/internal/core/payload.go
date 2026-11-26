// Package core is the implementation of the actor API. dst/actor re-exports
// what products use; dst/sim and dst/live use the rest. Products cannot import it.
package core

import "github.com/manifesterio/mds/dst/trace"

// Payload is every message, request, answer and IO value. Fold writes the
// value into the trace hash: its frozen tag first, then its fields. dstgen
// writes Fold from //dst:cases and //dst:payload.
type Payload interface{ Fold(h *trace.Hash) }

// NoIO is the IO type of an actor that does no product IO. It has no cases,
// so nothing can be passed to Do or Perform on an Outbox[In, NoIO].
type NoIO interface {
	Payload
	noIO()
}
