package replica_test

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func commitCmd(key string, records int64) metadb.Entry {
	return metadb.Entry{
		Kind: metadb.KindCommit,
		Op: contract.CommitSegment{
			SegmentKey: key,
			Batches: []contract.PendingBatch{
				{TopicPartition: contract.NewTopicPartition("t", 0), Records: records, Loc: runtime.SegmentRef{Key: key, Off: 0, Len: records}},
			},
		},
	}
}

// applied answers an append as written.
func applied(ap runtime.LogAppend) runtime.LogAppendDone {
	return runtime.LogAppendDone{Slot: ap.Slot, Err: runtime.OK, Outcome: runtime.Applied}
}

func TestGroupPackingCapsAtMaxCmdsPerSlot(t *testing.T) {
	h := newHarness(t, replica.Config{MaxCmdsPerSlot: 3, Writer: "w"})
	// First Offer flushes alone (low load). Further Offers queue while in flight;
	// the next flush packs up to MaxCmdsPerSlot, matching logcp flusher take().
	first, firstEffect := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: commitCmd("g0", 1)}))
	for i := 2; i <= 6; i++ {
		if effects := h.offer(uint64(i), replica.ClientOperation{
			Cmd: commitCmd("g"+string(rune('0'+i-1)), 1),
		}); len(effects) != 0 {
			t.Fatalf("Offer while in-flight returned %v", describe(effects))
		}
	}
	if h.m.QueueDepth() != 5 {
		t.Fatalf("queue depth = %d, want 5", h.m.QueueDepth())
	}
	effects := h.answer(firstEffect, applied(first)) // also answers g0
	packed, packedEffect := only[runtime.LogAppend](t, effects)
	id, cmds, ok := replica.DecodeGroup(packed.Data)
	if !ok || len(cmds) != 3 {
		t.Fatalf("packed group id=%q cmds=%d ok=%v, want 3 cmds", id, len(cmds), ok)
	}
	if h.m.QueueDepth() != 2 {
		t.Fatalf("queue depth = %d, want 2 left after packing 3", h.m.QueueDepth())
	}
	ans := replies(t, h.answer(packedEffect, applied(packed)))
	if len(ans) != 3 {
		t.Fatalf("packed answers = %d, want 3", len(ans))
	}
	for i, a := range ans {
		if a.n != uint64(i+2) || a.done.Err != runtime.OK {
			t.Fatalf("answer[%d] = %+v", i, a)
		}
	}
	d, err := h.m.Metadata().Read(contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 0)})
	if end := runtime.ResponseAs[contract.FindSegmentsResp](d).End; err != nil || end != 4 { // g0 + 3 packed
		t.Fatalf("end = %d (err %v), want 4", end, err)
	}
}

func TestOfferKnownCommitSkipsAppend(t *testing.T) {
	h := newHarness(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w"})
	first, firstEffect := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: commitCmd("known", 2)}))
	if ans := replies(t, h.answer(firstEffect, applied(first))); len(ans) != 1 || ans[0].done.Err != runtime.OK {
		t.Fatalf("first answers = %+v", ans)
	}
	effects := h.offer(2, replica.ClientOperation{Cmd: commitCmd("known", 2)})
	if len(performs(effects)) != 0 {
		t.Fatalf("known Offer returned %v, want immediate answer", describe(effects))
	}
	ans := replies(t, effects)
	if len(ans) != 1 || ans[0].n != 2 || ans[0].done.Err != runtime.OK {
		t.Fatalf("known answers = %+v", ans)
	}
	response, ok := ans[0].done.Resp.(contract.CommitSegmentResp)
	if !ok || len(response.Assigned) != 1 || response.Assigned[0].Count != 2 {
		t.Fatalf("known resp = %+v", ans[0].done.Resp)
	}
}

func TestOfferRetargetsInFlightCommit(t *testing.T) {
	h := newHarness(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w"})
	first, firstEffect := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: commitCmd("retry", 1)}))
	effects := h.offer(2, replica.ClientOperation{Cmd: commitCmd("retry", 1)})
	if len(effects) != 0 {
		t.Fatalf("retarget Offer returned %v", describe(effects))
	}
	if h.m.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d, want 0 (coalesced)", h.m.QueueDepth())
	}
	ans := replies(t, h.answer(firstEffect, applied(first)))
	if len(ans) != 1 || ans[0].n != 2 || ans[0].done.Err != runtime.OK {
		t.Fatalf("answers = %+v, want only the newer caller 2", ans)
	}
}

// Keys leave the idempotency window only at slots that are a multiple of
// TrimStride, so every replica forgets the same keys at the same slot. A replica
// that forgets on its own clock lets a replay
// commit again as a new segment, with new offsets.
func TestWindowForgetsKeysOnlyOnLogStride(t *testing.T) {
	h := newHarness(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w", WindowSlots: 4, TrimStride: 8})
	var last runtime.LogPosition
	n := uint64(0)
	commit := func(key string) {
		n++
		ap, apEffect := only[runtime.LogAppend](t, h.offer(n, replica.ClientOperation{Cmd: commitCmd(key, 1)}))
		h.answer(apEffect, applied(ap))
		last = ap.Slot
	}
	commit("first") // slot 0
	for last < 7 {
		commit(fmt.Sprintf("filler%d", last))
	}
	n++
	if effects := h.offer(n, replica.ClientOperation{Cmd: commitCmd("first", 1)}); len(performs(effects)) != 0 {
		t.Fatalf("at slot %d the key is forgotten, but the window keeps it until slot 8: %v", last, describe(effects))
	}
	commit("filler8") // slot 8 is a stride slot, so the keys below slot 4 go
	n++
	only[runtime.LogAppend](t, h.offer(n, replica.ClientOperation{Cmd: commitCmd("first", 1)}))
}
