package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

func readyHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  actor.Millisecond,
		TakeoverAfter: 10 * actor.Second,
	})
	bootEmpty(t, h, 0)
	if !h.m.Ready() {
		t.Fatal("replica is not Ready")
	}
	return h
}

// onlyReply returns the one effect, which must be a Reply.
func onlyReply(t *testing.T, effects []actor.Effect) reply {
	t.Helper()
	rs := replies(t, effects)
	if len(effects) != 1 || len(rs) != 1 {
		t.Fatalf("want one reply, got %v", describe(effects))
	}
	return rs[0]
}

// A LocalRead on a Ready node is answered in the same Receive, with no wait
// for a tail check, and the reply goes to the caller.
func TestLocalReadAnswersAtOnce(t *testing.T) {
	h := readyHarness(t)
	r := onlyReply(t, h.recv(runtime.LocalRead{ReplyTo: caller(21), Op: contract.DescribeTopics{}}))
	if r.n != 21 || r.done.Err != runtime.OK {
		t.Fatalf("reply = %+v", r)
	}
	if _, ok := r.done.Resp.(contract.DescribeTopicsResp); !ok {
		t.Fatalf("resp = %#v", r.done.Resp)
	}
}

// A node that is not Ready refuses at once.
func TestLocalReadRefusedWhenNotReady(t *testing.T) {
	h := newHarness(t, replica.Config{Writer: "s1", CatchUpEvery: actor.Millisecond})
	h.recv(runtime.NodeStarted{})
	r := onlyReply(t, h.recv(runtime.LocalRead{ReplyTo: caller(3), Op: contract.DescribeTopics{}}))
	if r.done.Err != runtime.ErrUnavailable || r.n != 3 {
		t.Fatalf("reply = %+v, want ErrUnavailable", r)
	}
}

// A command is not a read: it is refused and nothing reaches the log.
func TestLocalReadRefusesACommand(t *testing.T) {
	h := readyHarness(t)
	r := onlyReply(t, h.recv(runtime.LocalRead{ReplyTo: caller(4), Op: contract.CreateTopic{Name: "x", Partitions: 1}}))
	if r.done.Err != runtime.ErrInvalid || r.n != 4 {
		t.Fatalf("reply = %+v, want ErrInvalid", r)
	}
}

// An operation that the replica does not know is refused with ErrInvalid, and
// nothing reaches the log.
func TestUnknownOperationIsInvalid(t *testing.T) {
	h := newHarness(t, replica.Config{Writer: "w"})
	r := onlyReply(t, h.recv(runtime.ControlPlaneDeliver{ReplyTo: caller(9), Op: nil}))
	if r.done.Err != runtime.ErrInvalid || r.n != 9 {
		t.Fatalf("reply = %+v, want ErrInvalid to the sender", r)
	}
}
