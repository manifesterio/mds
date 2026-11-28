package specbridge

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
)

// TestExecCarriesTheSpecAnswer checks that Exec gives the answer of the spec
// unchanged.
func TestExecCarriesTheSpecAnswer(t *testing.T) {
	c := New()
	if code := c.CreateTopic("t", 2); code != core.OK {
		t.Fatalf("create topic: %v", code)
	}
	commit := contract.CommitSegment{SegmentKey: "s", Batches: []contract.PendingBatch{
		{TopicPartition: contract.NewTopicPartition("t", 1), Records: 3},
	}}
	got := c.Exec(core.ControlPlaneCall{Op: commit})
	if got.Err != core.OK || got.Outcome != core.Applied {
		t.Fatalf("commit: %+v", got)
	}
	if assigned := got.Resp.(contract.CommitSegmentResp).Assigned; len(assigned) != 1 || assigned[0].Base != 0 || assigned[0].Count != 3 {
		t.Fatalf("assigned %+v", assigned)
	}
	if end := c.End(contract.NewTopicPartition("t", 1)); end != 3 {
		t.Fatalf("end %d, want 3", end)
	}
	bad := c.Exec(core.ControlPlaneCall{Op: contract.FindSegments{From: -1}})
	if bad.Err != core.ErrInvalid || bad.Outcome != core.NotApplied {
		t.Fatalf("bad lookup: %+v", bad)
	}
	if code := c.CreateTopic("", 1); code != core.ErrInvalid {
		t.Fatalf("empty topic name: %v", code)
	}
}
