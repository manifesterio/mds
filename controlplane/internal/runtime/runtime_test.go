package runtime

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
)

func sum(p actor.Payload) uint64 {
	h := trace.New()
	p.Fold(&h)
	return h.Sum()
}

// Every message and IO value folds its own tag, so no two cases fold alike.
func TestCasesFoldDistinctly(t *testing.T) {
	all := []actor.Payload{
		NodeStarted{}, StopRequest{}, CatchUpDue{}, RoleDue{}, CheckpointDue{},
		ObjectPutDone{}, ObjectGetDone{}, ObjectListDone{}, ObjectDeleteDone{},
		LogAppendDone{}, LogReadDone{}, LogGetEndDone{}, LogDeleteOldDone{}, LogClaimEpochDone{},
		ControlPlaneDeliver{}, LocalRead{}, ControlPlaneDone{},
		ObjectPut{}, ObjectGet{}, ObjectDelete{}, ObjectList{},
		LogAppend{}, LogRead{}, LogGetEnd{}, LogClaimEpoch{}, LogDeleteOld{},
	}
	seen := map[uint64]string{}
	for _, p := range all {
		s := sum(p)
		if other, dup := seen[s]; dup {
			t.Fatalf("%T folds like %s", p, other)
		}
		seen[s] = ""
	}
}

// Each IO request names its answer type (checked by the compiler through
// AsRequest); this pins the pairs a reader would look for.
func TestRequestsAnswerTypes(t *testing.T) {
	var (
		_ actor.Request[IO, LogAppendDone]     = LogAppend{}
		_ actor.Request[IO, LogReadDone]       = LogRead{}
		_ actor.Request[IO, LogGetEndDone]     = LogGetEnd{}
		_ actor.Request[IO, LogClaimEpochDone] = LogClaimEpoch{}
		_ actor.Request[IO, LogDeleteOldDone]  = LogDeleteOld{}
		_ actor.Request[IO, ObjectPutDone]     = ObjectPut{}
		_ actor.Request[IO, ObjectGetDone]     = ObjectGet{}
		_ actor.Request[IO, ObjectDeleteDone]  = ObjectDelete{}
		_ actor.Request[IO, ObjectListDone]    = ObjectList{}
	)
}

func TestControlPlaneDoneAnswer(t *testing.T) {
	d := ControlPlaneDone{Req: 9, Err: ErrUnavailable, Outcome: NotApplied, Resp: contract.ListGroupsResp{}}
	got := d.Answer()
	if got.Err != ErrUnavailable || got.Outcome != NotApplied {
		t.Fatalf("%+v", got)
	}
	if _, ok := ResponseAs[contract.ListGroupsResp](d), true; !ok {
		t.Fatal("ResponseAs")
	}
}
