package rebalance

//go:generate go run github.com/manifesterio/mds/dst/cmd/dstgen

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/trace"
)

// Fold writes the reply into the trace hash: its tag, the code, then the
// body. The answer types are generated from the proto files and have no Fold
// of their own, so foldAnswer folds them here.
func (r Reply) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Uint64(uint64(r.Err))
	foldAnswer(h, r.Body)
}

// foldAnswer folds the body of a Reply. No body (nil) is tag 1200.
func foldAnswer(h *trace.Hash, a Answer) {
	switch a := a.(type) {
	case nil:
		h.Uint64(1200)
	case JoinAnswer:
		h.Uint64(1201)
		h.String(a.MemberID)
		h.Int64(int64(a.Generation))
		h.String(a.Protocol)
		h.String(a.Leader)
		h.Uint64(uint64(len(a.Members)))
		for _, mem := range a.Members {
			h.String(mem.ID)
			h.String(mem.InstanceID)
			h.Bytes(mem.Metadata)
		}
	case SyncAnswer:
		h.Uint64(1202)
		h.Bytes(a.Assignment)
	case LeaveAnswer:
		h.Uint64(1203)
		h.Uint64(uint64(len(a.Errs)))
		for _, e := range a.Errs {
			h.Uint64(uint64(e))
		}
	case CommitAnswer:
		h.Uint64(1204)
		a.Fold(h) // the Fold of the embedded contract.OffsetCommitResp
	case DescribeAnswer:
		h.Uint64(1205)
		h.Uint64(uint64(len(a.Groups)))
		for _, g := range a.Groups {
			h.String(g.Name)
			g.Fold(h) // the Fold of the embedded contract.DescribeGroupResp
		}
	case ListAnswer:
		h.Uint64(1206)
		contract.ListGroupsResp{Groups: a.Groups}.Fold(h)
	}
}
