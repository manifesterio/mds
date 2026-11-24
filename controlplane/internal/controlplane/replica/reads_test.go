package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
)

// A read that arrives while a tail check is in flight waits for the NEXT check.
func TestReadWaitsForFreshTail(t *testing.T) {
	h := newHarness(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  actor.Millisecond,
		TakeoverAfter: 10 * actor.Second,
	})
	bootEmpty(t, h, 0)
	_, tailA := only[runtime.LogGetEnd](t, h.fireAt(1, keyCatchUp))
	got := h.recvAt(2, runtime.ControlPlaneDeliver{ReplyTo: caller(7), Op: contract.DescribeTopics{}})
	if len(got) != 0 {
		t.Fatalf("read answered at once: %v", describe(got))
	}
	effects := h.answerAt(3, tailA, runtime.LogGetEndDone{Err: runtime.OK, Outcome: runtime.Applied})
	if rs := replies(t, effects); len(rs) != 0 {
		t.Fatalf("read answered from a tail check that started before it: %+v", rs)
	}
	_, tailB := only[runtime.LogGetEnd](t, h.fireAt(4, keyCatchUp))
	effects = h.answerAt(5, tailB, runtime.LogGetEndDone{Err: runtime.OK, Outcome: runtime.Applied})
	for _, r := range replies(t, effects) {
		if r.n == 7 && r.done.Err == runtime.OK {
			return
		}
	}
	t.Fatalf("no reply after a fresh tail: %v", describe(effects))
}

// A node that is not Ready refuses a read at once, so the caller can fail over.
func TestReadRefusedWhenNotReady(t *testing.T) {
	h := newHarness(t, replica.Config{Writer: "s1", CatchUpEvery: actor.Millisecond})
	h.recv(runtime.NodeStarted{})
	r := onlyReply(t, h.recvAt(1, runtime.ControlPlaneDeliver{ReplyTo: caller(8), Op: contract.DescribeTopics{}}))
	if r.done.Err != runtime.ErrUnavailable {
		t.Fatalf("want ErrUnavailable reply, got %+v", r)
	}
}
