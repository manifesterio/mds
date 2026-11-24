package replica_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
)

// ckptKey is the object key of the checkpoint taken at slot, under the test prefix.
func ckptKey(slot runtime.LogPosition) string { return fmt.Sprintf("checkpoint/%020d", slot) }

// startRestore runs NodeStarted and returns the only effect, which must be
// the list of checkpoint objects.
func startRestore(t *testing.T, h *harness) actor.Effect {
	t.Helper()
	effects := h.recvAt(0, runtime.NodeStarted{})
	list, e := only[runtime.ObjectList](t, effects)
	if len(effects) != 1 || list.Prefix != "checkpoint/" {
		t.Fatalf("Started = %v, want one ObjectList checkpoint/", describe(effects))
	}
	return e
}

func TestCheckpointPublishAndTrim(t *testing.T) {
	h := newHarness(t, replica.Config{
		Writer: "w", Rank: 0,
		CatchUpEvery:       actor.Millisecond,
		Heartbeat:          actor.Second,
		TakeoverAfter:      actor.Second,
		CheckpointEvery:    10 * actor.Millisecond,
		CheckpointMinSlots: 1,
		KeepSlots:          2,
		CheckpointPrefix:   "checkpoint/",
	})
	// NodeStarted, empty list, catch-up, Ready, claim, fence entry, active (rank 0).
	list := startRestore(t, h)
	h.answer(list, runtime.ObjectListDone{})
	_, tail := only[runtime.LogGetEnd](t, h.fireAt(0, keyCatchUp))
	effects := h.answer(tail, runtime.LogGetEndDone{Tail: 0, Err: runtime.OK, Outcome: runtime.Applied})
	if !h.m.Ready() {
		t.Fatal("not Ready after catch-up")
	}
	if _, ok := h.armed(keyCheckpoint); !ok {
		t.Fatalf("checkpoint timer missing after %v", describe(effects))
	}
	winClaim(t, h, effects, 1, 0) // the fence entry takes slot 0
	// Commit one slot so applied=2.
	ap, apEffect := only[runtime.LogAppend](t, h.offer(1, replica.ClientOperation{Cmd: commitCmd("c", 1)}))
	h.answer(apEffect, applied(ap))
	if h.m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", h.m.Applied())
	}
	effects = h.fireAt(actor.Mono(20*actor.Millisecond), keyCheckpoint)
	put, putEffect := only[runtime.ObjectPut](t, effects)
	if put.Key != ckptKey(2) || len(put.Data) == 0 {
		t.Fatalf("checkpoint IO = %#v, want ObjectPut", put)
	}
	slot, err := metadb.NewMem().Restore(bytes.NewReader(put.Data))
	if err != nil || slot != 2 {
		t.Fatalf("published snapshot slot=%d err=%v, want 2", slot, err)
	}
	effects = h.answer(putEffect, runtime.ObjectPutDone{Key: put.Key, Err: runtime.OK, Outcome: runtime.Applied})
	// KeepSlots=2, checkpointSlot=2: below=0, no trim. No older checkpoint to delete.
	if len(effects) != 0 {
		t.Fatalf("unexpected effects after the first checkpoint: %v", describe(effects))
	}

	// Apply more slots and checkpoint again so trim fires.
	ap2, ap2Effect := only[runtime.LogAppend](t, h.offer(2, replica.ClientOperation{Cmd: commitCmd("d", 1)}))
	h.answer(ap2Effect, applied(ap2))
	ap3, ap3Effect := only[runtime.LogAppend](t, h.offer(3, replica.ClientOperation{Cmd: commitCmd("e", 1)}))
	h.answer(ap3Effect, applied(ap3))
	if h.m.Applied() != 4 {
		t.Fatalf("Applied = %d, want 4", h.m.Applied())
	}
	// The checkpoint timer was armed again when it fired.
	effects = h.fireAt(actor.Mono(40*actor.Millisecond), keyCheckpoint)
	put, putEffect = only[runtime.ObjectPut](t, effects)
	if put.Key != ckptKey(4) {
		t.Fatalf("second checkpoint key = %s, want %s", put.Key, ckptKey(4))
	}
	effects = h.answer(putEffect, runtime.ObjectPutDone{Key: put.Key, Err: runtime.OK, Outcome: runtime.Applied})
	ps := performs(effects)
	if del, ok := ps[len(ps)-1].Msg.(runtime.ObjectDelete); !ok || del.Key != ckptKey(2) {
		t.Fatalf("after put IO = %v, want a final ObjectDelete of the older checkpoint %s", describe(effects), ckptKey(2))
	}
	trim, ok := ps[0].Msg.(runtime.LogDeleteOld)
	if !ok {
		t.Fatalf("after put IO = %v, want LogTrim", describe(effects))
	}
	// checkpointSlot=4, KeepSlots=2: below=2, Through=1.
	if trim.Through != 1 {
		t.Fatalf("LogTrim Through = %d, want 1", trim.Through)
	}
	h.answer(ps[0], runtime.LogDeleteOldDone{Err: runtime.OK, Outcome: runtime.Applied})
}

// A listed checkpoint can be deleted by the time it is read, because a newer
// one replaced it. The node lists again instead of starting from slot 0.
func TestVanishedCheckpointIsListedAgain(t *testing.T) {
	h := newHarness(t, replica.Config{Writer: "w", CatchUpEvery: actor.Millisecond, CheckpointPrefix: "checkpoint/"})
	list := startRestore(t, h)
	_, get := only[runtime.ObjectGet](t, h.answer(list, runtime.ObjectListDone{Keys: []string{ckptKey(5)}}))
	effects := h.answer(get, runtime.ObjectGetDone{Err: runtime.ErrNotFound})
	if _, _, ok := find[runtime.ObjectList](effects); !ok || len(effects) != 1 || h.m.Ready() {
		t.Fatalf("want one new ObjectList and not Ready, got %v", describe(effects))
	}
}
