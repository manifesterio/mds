package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
)

// bootEmpty boots a replica on an empty log and returns the effects of the
// last Receive (the tail check that makes the node Ready).
func bootEmpty(t *testing.T, h *harness, at actor.Mono) []actor.Effect {
	t.Helper()
	effects := h.recvAt(at, runtime.NodeStarted{})
	if _, ok := h.armed(keyCatchUp); !ok {
		t.Fatalf("Started = %v, want the catch-up timer", describe(effects))
	}
	_, tail := only[runtime.LogGetEnd](t, h.fireAt(at, keyCatchUp))
	effects = h.answerAt(at, tail, runtime.LogGetEndDone{Tail: 0, Err: runtime.OK, Outcome: runtime.Applied})
	if !h.m.Ready() {
		t.Fatal("not Ready after empty catch-up")
	}
	return effects
}

// winClaim answers the claim in effects as won with the given epoch, checks
// that the node writes a fence entry before it is the writer, and applies the
// fence.
func winClaim(t *testing.T, h *harness, effects []actor.Effect, epoch uint64, at actor.Mono) (runtime.LogAppend, []actor.Effect) {
	t.Helper()
	claim, claimEffect := must[runtime.LogClaimEpoch](t, effects)
	effects = h.answerAt(at, claimEffect, runtime.LogClaimEpochDone{
		Epoch: epoch, Owner: claim.Owner, Won: true, Err: runtime.OK, Outcome: runtime.Applied,
	})
	fence, fenceEffect := must[runtime.LogAppend](t, effects)
	if h.m.IsActive() {
		t.Fatal("writer before the fence entry is applied")
	}
	got, _, ok := replica.EntryWriter(fence.Data)
	if !ok || got != epoch {
		t.Fatalf("fence epoch = %d (ok=%v), want %d", got, ok, epoch)
	}
	last := h.answerAt(at, fenceEffect, runtime.LogAppendDone{Slot: fence.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	if !h.m.IsActive() || h.m.Epoch() != epoch {
		t.Fatalf("after the fence: active=%v epoch=%d, want active epoch %d", h.m.IsActive(), h.m.Epoch(), epoch)
	}
	return fence, last
}

func TestFencedByNewerEpochEntry(t *testing.T) {
	h := newHarness(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: actor.Millisecond, Heartbeat: actor.Second, MaxCmdsPerSlot: 1,
	})
	effects := bootEmpty(t, h, 0)
	winClaim(t, h, effects, 1, 0)
	ap, apEffect := only[runtime.LogAppend](t, h.offer(7, replica.ClientOperation{Cmd: commitCmd("k", 1)}))

	// The commit's slot is taken by an entry written in epoch 2 by another node.
	foreign := replica.EncodeEnvelopedForTest(2, "other", metadb.Encode(metadb.Entry{
		ID: "other/e2/1", Kind: metadb.KindNoop,
	}))
	effects = h.answer(apEffect, runtime.LogAppendDone{
		Slot: ap.Slot, Taken: true, Existing: foreign, Err: runtime.OK, Outcome: runtime.Applied,
	})
	if h.m.IsActive() || h.m.Epoch() != 0 {
		t.Fatalf("fenced writer still active: active=%v epoch=%d", h.m.IsActive(), h.m.Epoch())
	}
	if _, _, ok := find[runtime.LogAppend](effects); ok {
		t.Fatalf("fenced writer retried the append: %v", describe(effects))
	}
	ans := replies(t, effects)
	if len(ans) != 1 || ans[0].n != 7 || ans[0].done.Err != runtime.ErrUnavailable {
		t.Fatalf("answers = %+v, want one ErrUnavailable for request 7", ans)
	}
	// It stays a standby: new commits are refused, reads still work.
	if got := h.offer(8, replica.ClientOperation{Cmd: commitCmd("k2", 1)}); len(performs(got)) != 0 {
		t.Fatalf("read-only node accepted a commit: %v", describe(got))
	}
}

// A soft crash forgets every log request still out: the answer of the commit
// append in flight never reaches the replica, and neither does the catch-up
// timer. The role timer stays armed, because it brings the node back.
func TestSoftCrashForgetsWorkButKeepsRoleTimer(t *testing.T) {
	h := newHarness(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: actor.Millisecond, Heartbeat: actor.Second, MaxCmdsPerSlot: 1,
		CrashProb: 1, DownDur: [2]actor.Duration{actor.Second, actor.Second},
	})
	effects := bootEmpty(t, h, 0)
	winClaim(t, h, effects, 1, 0)
	_, apEffect := only[runtime.LogAppend](t, h.offer(7, replica.ClientOperation{Cmd: commitCmd("k", 1)}))

	h.env.FaultAlways = true
	h.fireAt(actor.Mono(2*actor.Second), keyRole)
	h.env.FaultAlways = false
	if h.m.Ready() || h.m.IsActive() {
		t.Fatalf("after the soft crash: ready=%v active=%v, want down", h.m.Ready(), h.m.IsActive())
	}
	if h.waited(apEffect) {
		t.Fatal("the append answer is still waited for after the soft crash")
	}
	if _, ok := h.armed(keyCatchUp); ok {
		t.Fatal("the catch-up timer survived the soft crash")
	}
	if _, ok := h.armed(keyRole); !ok {
		t.Fatal("the role timer did not survive the soft crash")
	}
	// After the down time the role timer restarts the node: catch-up again.
	h.fireAt(actor.Mono(4*actor.Second), keyRole)
	if _, ok := h.armed(keyCatchUp); !ok || !h.m.Booting() {
		t.Fatalf("after the down time: catch-up armed=%v booting=%v, want a new catch-up", ok, h.m.Booting())
	}
}

// A claim answer that arrives after Demote is ignored.
func TestClaimAnswerAfterDemoteIsIgnored(t *testing.T) {
	h := newHarness(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: actor.Millisecond, Heartbeat: actor.Second,
	})
	effects := bootEmpty(t, h, 0)
	claim, claimEffect := must[runtime.LogClaimEpoch](t, effects)
	h.m.Demote()
	effects = h.answer(claimEffect, runtime.LogClaimEpochDone{
		Epoch: 1, Owner: claim.Owner, Won: true, Err: runtime.OK, Outcome: runtime.Applied,
	})
	if len(effects) != 0 || h.m.Epoch() != 0 {
		t.Fatalf("claim answer after Demote: effects %v epoch %d, want ignored", describe(effects), h.m.Epoch())
	}
}
