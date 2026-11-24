package core

import (
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectCall and ObjectDone wrap a sum of another package, which has no Fold.
// Their Fold is written here: the tag, then the case number below, then the
// fields of the case. The Req fields of the objectstore types are not folded:
// the broker never sets them.

// The case numbers of the object store calls and results in the trace hash.
// They are frozen.
const (
	objectPut    = 1
	objectGet    = 2
	objectDelete = 3
	objectList   = 4
)

// Fold writes the call into the trace hash.
func (x ObjectCall) Fold(h *trace.Hash) {
	h.Uint64(x.Tag())
	switch c := x.Call.(type) {
	case objectstore.Put:
		h.Uint64(objectPut)
		h.String(c.Key)
		h.Bytes(c.Data)
	case objectstore.Get:
		h.Uint64(objectGet)
		foldSegmentRef(h, c.Ref)
	case objectstore.Delete:
		h.Uint64(objectDelete)
		h.String(c.Key)
	case objectstore.List:
		h.Uint64(objectList)
		h.String(c.Prefix)
	case nil:
		h.Uint64(0)
	}
}

// Fold writes the answer into the trace hash.
func (x ObjectDone) Fold(h *trace.Hash) {
	h.Uint64(x.Tag())
	switch r := x.Result.(type) {
	case objectstore.PutDone:
		h.Uint64(objectPut)
		h.String(r.Key)
		h.Uint64(uint64(r.Err))
		h.Uint64(uint64(r.Outcome))
	case objectstore.GetDone:
		h.Uint64(objectGet)
		h.Bytes(r.Data)
		h.Uint64(uint64(r.Err))
	case objectstore.DeleteDone:
		h.Uint64(objectDelete)
		h.Uint64(uint64(r.Err))
		h.Uint64(uint64(r.Outcome))
	case objectstore.ListDone:
		h.Uint64(objectList)
		h.Uint64(uint64(r.Err))
		h.Uint64(uint64(len(r.Keys)))
		for _, k := range r.Keys {
			h.String(k)
		}
	case nil:
		h.Uint64(0)
	}
}

// foldSegmentRef folds a byte range of an object.
func foldSegmentRef(h *trace.Hash, s SegmentRef) {
	h.String(s.Key)
	h.Int64(s.Off)
	h.Int64(s.Len)
}
