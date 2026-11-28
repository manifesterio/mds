// Package errors is dstgen test input: every misuse.
package errors

import "github.com/manifesterio/mds/dst/trace"

type Message interface{ Fold(h *trace.Hash) }

//dst:sum
type noDecl interface {
	Message
	isNoDecl()
}

//sumtype:decl
//dst:sum
type noFold interface{ isNoFold() }

//sumtype:decl
//dst:sum
type noMarker interface{ Message }

//dst:sum
type notIface struct{}

//sumtype:decl
//dst:sum
type good interface {
	Message
	isGood()
}

//dst:cases good
type (
	ptr     struct{ P *int }
	fn      struct{ F func() }
	ch      struct{ C chan int }
	badMap  struct{ M map[[2]int]string }
	plain   struct{ I interface{ M() } }
	handTag struct{}
)

func (handTag) Tag() uint64 { return 9 }

//dst:cases missing
type unknownSum struct{}

//dst:cases good
//dst:payload
type both struct{}

//dst:payload
//dst:answer nothing
type answerNoSum struct{}

//dst:cases good
//dst:answer notPayload
type askBad struct{}

type notPayload struct{}

//dst:cases good
//dst:fold manual
type manualMissing struct{}

//dst:cases good
//dst:fold manual
type manualWrongTag struct{}

func (manualWrongTag) Fold(h *trace.Hash) { h.Uint64(999) }

//dst:frobnicate
type unknownNote struct{}

//dst:payload extra
type extraArg struct{}

//dst:payload
type generic[T any] struct{ V T }

//dst:state
type stateNotStruct int

//sumtype:decl
//dst:sum open
type openWithDecl interface {
	Message
	isOpenWithDecl()
}

//dst:sum closed
type sumBadArg interface {
	Message
	isSumBadArg()
}
