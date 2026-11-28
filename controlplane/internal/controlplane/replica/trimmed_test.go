package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// A put that the log refuses because the slot is at or below the floor is not a
// commit. The node must not apply or answer it: it refuses the clients and
// restores from the checkpoint.
func TestBelowFloorAppendRestoresFromCheckpoint(t *testing.T) {
	h := newHarness(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w", CheckpointPrefix: "checkpoint/t/"})
	ap, apEffect := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: commitCmd("a", 1)}))
	effects := h.answer(apEffect, runtime.LogAppendDone{
		Slot: ap.Slot, Taken: true, Existing: nil,
		Err: runtime.ErrConflict, Outcome: runtime.NotApplied,
	})
	if !h.m.Booting() {
		t.Fatal("want booting after an append was refused below the floor")
	}
	if h.m.Applied() != 0 {
		t.Fatalf("Applied = %d, the refused put must not be applied", h.m.Applied())
	}
	ans := replies(t, effects)
	if len(ans) != 1 || ans[0].n != 1 || ans[0].done.Err != runtime.ErrUnavailable {
		t.Fatalf("answers = %+v, want one ErrUnavailable for request 1", ans)
	}
	ps := performs(effects)
	if list, ok := ps[len(ps)-1].Msg.(runtime.ObjectList); !ok || list.Prefix != "checkpoint/t/" {
		t.Fatalf("effects = %v, want a final ObjectList checkpoint/t/", describe(effects))
	}
}
