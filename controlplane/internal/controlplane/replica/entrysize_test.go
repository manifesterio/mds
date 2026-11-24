package replica_test

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
)

// dynamoItemLimit is the largest DynamoDB item, in bytes (400 KiB).
const dynamoItemLimit = 400 << 10

// sizedCommit is a commit with the given number of batches, one per partition.
func sizedCommit(key string, batches int) metadb.Entry {
	bs := make([]contract.PendingBatch, batches)
	for i := range bs {
		bs[i] = contract.PendingBatch{
			TopicPartition: contract.NewTopicPartition("orders", int32(i)), Records: 100,
			Loc: runtime.SegmentRef{Key: key, Off: int64(i) * 1000, Len: 1000},
		}
	}
	return metadb.Entry{Kind: metadb.KindCommit, Op: contract.CommitSegment{SegmentKey: key, Batches: bs}}
}

// appendsUntilQueueEmpty finishes each append in turn, so the replica builds
// the next group from the queue, and returns every LogAppend it sent.
func appendsUntilQueueEmpty(t *testing.T, h *harness, first actor.Effect) []runtime.LogAppend {
	t.Helper()
	cur := first
	ap := cur.Msg.(runtime.LogAppend)
	all := []runtime.LogAppend{ap}
	for h.m.QueueDepth() > 0 {
		next, nextEffect, ok := find[runtime.LogAppend](h.answer(cur, applied(ap)))
		if !ok {
			t.Fatalf("queue still holds %d commands but no group was sent", h.m.QueueDepth())
		}
		all = append(all, next)
		cur, ap = nextEffect, next
	}
	return all
}

func commandsIn(t *testing.T, ap runtime.LogAppend) int {
	t.Helper()
	_, cmds, ok := replica.DecodeGroup(ap.Data)
	if !ok {
		t.Fatal("entry is not a group")
	}
	return len(cmds)
}

func TestGroupStopsAtTheEntryByteLimit(t *testing.T) {
	const limit = 20000
	h := newHarness(t, replica.Config{MaxEntryBytes: limit})
	_, first := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: sizedCommit("seg/0", 50)}))
	const queued = 80
	for i := 2; i <= queued+1; i++ {
		h.offer(uint64(i), replica.ClientOperation{Cmd: sizedCommit(fmt.Sprintf("seg/%d", i), 50)})
	}
	appends := appendsUntilQueueEmpty(t, h, first)
	if len(appends) < 3 {
		t.Fatalf("groups = %d, want the queue split into several groups", len(appends))
	}
	total := 0
	for i, ap := range appends {
		if len(ap.Data) > limit {
			t.Errorf("group %d is %d bytes, over the limit of %d", i, len(ap.Data), limit)
		}
		total += commandsIn(t, ap)
	}
	if total != queued+1 {
		t.Errorf("commands in all groups = %d, want %d (none lost)", total, queued+1)
	}
}

func TestDefaultLimitKeepsEntriesUnderTheDynamoDBItemLimit(t *testing.T) {
	h := newHarness(t, replica.Config{})
	_, first := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: sizedCommit("seg/0", 256)}))
	const queued = 200 // 256 commands of 256 batches made a 725 KB entry before the limit
	for i := 2; i <= queued+1; i++ {
		h.offer(uint64(i), replica.ClientOperation{Cmd: sizedCommit(fmt.Sprintf("seg/%d", i), 256)})
	}
	total := 0
	for i, ap := range appendsUntilQueueEmpty(t, h, first) {
		if len(ap.Data) > 350<<10 || len(ap.Data) >= dynamoItemLimit {
			t.Errorf("group %d is %d bytes, over the default limit of 350 KiB", i, len(ap.Data))
		}
		total += commandsIn(t, ap)
	}
	if total != queued+1 {
		t.Errorf("commands in all groups = %d, want %d (none lost)", total, queued+1)
	}
}
