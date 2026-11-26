package rebalance

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// Tests of log writes, the queue, the term and the load (sections 8 and 9).

// Rule 38: a write with no answer after 10 s ends the term. The late answer
// is ignored, and the fresh reads see the write that applied late.
func TestWriteTimerEndsTerm(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, nil},
		{delay + 10*sec - 1, nil, nil},
		{delay + 10*sec, nil, []string{"1 ErrUnavailable"}},
	})
	h.check("loading", []any{h.phase("g"), heldOps(h)}, []any{"loading", "[GroupInstall ListGroups fresh]"})
	h.check("late answer", h.answer(0, runtime.OK), []string(nil))
	h.hold = false
	h.answer(0, runtime.OK)
	h.check("after the fresh load", []any{h.phase("g"), h.group("g").generation, h.members("g")}, []any{stateJoining, 1, []string{"m1"}})
}

// Rule 39: a load read refused with ErrUnavailable is sent again after 100 ms.
func TestLoadRetry(t *testing.T) {
	h := newBareHarness(t, Config{}, nil)
	h.session, h.hold = 10000, true
	h.step(RoleChanged{Active: true})
	h.check("list read", heldOps(h), "[ListGroups]")
	h.answer(0, runtime.ErrUnavailable)
	h.check("waits", []any{heldOps(h), h.until(99 * msec)}, []any{"[]", []string(nil)})
	h.until(100 * msec)
	h.check("sent again", heldOps(h), "[ListGroups]")
	h.check("a call while loading", h.step(List{ReplyTo: rt(1)}), []string{"1 ErrUnavailable"})
	h.answer(0, runtime.OK)
	h.check("serving", h.phase("g"), stateDead)
}

// Section 8: while a write is in flight, the inputs of its group wait in a
// queue and are taken in order after the answer; other calls do not wait.
func TestQueueOrder(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.hold = true
	h.run([]step{{0, h.join(1, "g", ""), nil}, {delay, nil, nil}})
	h.check("install in flight", heldOps(h), "[GroupInstall]")
	h.run([]step{
		{delay, hb(2, "g", a, 1), nil},
		{delay, syncReq(3, "g", a, 1, as(a, "x")), nil},
		{delay, leave(4, "g", "zz"), nil},
		{delay, List{ReplyTo: rt(5)}, []string{"5 OK g:consumer:PreparingRebalance"}},
		{delay, hb(6, "other", a, 1), []string{"6 ErrUnknownMember"}},
	})
	// The install applies, then the queue runs until the sync starts its own write.
	got := h.answer(0, runtime.OK)
	h.check("after install", got, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]", "2 OK"})
	h.check("assign in flight", heldOps(h), "[GroupAssign]")
	h.check("after assign", h.answer(0, runtime.OK), []string{"3 OK assign=x", "4 OK [ErrUnknownMember]"})
}

// Rule 33 with the queue: a queued call that is abandoned is taken out; a
// call whose write is in flight gets no reply, but the write is applied.
func TestAbandonedQueuedAndWritingCalls(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.hold = true
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, nil},
		{delay, hb(2, "g", a, 1), nil},
		{delay, gone("g", 2), nil},
	})
	h.check("install", h.answer(0, runtime.OK), []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"})
	h.run([]step{
		{delay, syncReq(3, "g", a, 1, as(a, "x")), nil},
		{delay, gone("g", 3), nil},
	})
	h.check("assign", h.answer(0, runtime.OK), []string(nil))
	h.check("applied", []any{h.phase("g"), h.group("g").phase.(*stable).assign[a]}, []any{stateStable, []byte("x")})
	g := h.group("g")
	h.check("nothing waits", []any{len(g.queue), g.write == nil, h.parked("g"), len(h.watched)}, []any{0, true, []string(nil), 0})
}

// Rule 34 and 35: losing the role drops the memory; taking it again reads
// the log. A group in its first round was never logged, so it is gone.
func TestTakeoverDropsState(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.stableTwo("g", 0, 1, 1)
	h.run([]step{
		{delay, h.join(10, "new", ""), nil},
		{delay, h.join(11, "g", a), nil},
		{delay, RoleChanged{Active: false}, []string{"10 ErrUnavailable", "11 ErrUnavailable"}},
		{delay, RoleChanged{Active: true}, nil},
		{delay, List{ReplyTo: rt(12)}, []string{"12 OK g:consumer:Stable"}},
	})
	g := h.group("g")
	h.check("loaded", []any{h.phase("new"), g.generation, h.members("g"), g.find(a).synced, g.find(b).isNew}, []any{stateDead, 2, []string{"m1", "m2"}, true, false})
	h.run([]step{{delay, syncReq(13, "g", b, 2), []string{"13 OK assign=b2"}}})
}

// Section 8: a business code from the log shows that memory and log differ:
// the term ends. ErrTooLarge and ErrInvalid wrote nothing: the term goes on.
func TestWriteAnswers(t *testing.T) {
	cases := []struct {
		code  runtime.ErrCode
		want  []string
		phase string
	}{
		{runtime.ErrIllegalGeneration, []string{"1 ErrUnavailable"}, "loading"},
		{runtime.ErrTimeout, []string{"1 ErrUnavailable"}, "loading"},
		{runtime.ErrTooLarge, []string{"1 ErrTooLarge"}, stateJoining},
		{runtime.ErrInvalid, []string{"1 ErrInvalid"}, stateJoining},
	}
	for _, tc := range cases {
		h := newHarness(t, Config{})
		h.hold = true
		h.run([]step{{0, h.join(1, "g", ""), nil}, {delay, nil, nil}})
		h.check(tc.code.String(), []any{h.answer(0, tc.code), h.phase("g")}, []any{tc.want, tc.phase})
	}
}

// Rule 31: a failed commit write is ErrUnavailable and the term goes on;
// so is a commit whose write timer fires.
func TestCommitFailedWrite(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	op := contract.OffsetCommit{Group: "x", Generation: -1}
	h.step(OffsetCommit{ReplyTo: rt(1), Op: op})
	h.check("failed", []any{h.answer(0, runtime.ErrTimeout), h.phase("x")}, []any{[]string{"1 ErrUnavailable"}, stateDead})
	h.step(OffsetCommit{ReplyTo: rt(2), Op: op})
	h.check("timeout", h.until(10*sec), []string{"2 ErrUnavailable"})
	h.check("late answer", h.answer(0, runtime.OK), []string(nil))
	h.step(OffsetCommit{ReplyTo: rt(3), Op: op})
	h.check("abandoned", h.step(gone("", 3)), []string(nil))
	h.check("its answer", h.answer(0, runtime.OK), []string(nil))
	h.check("still serving", h.phase("x"), stateDead)
}

// Section 7: a timer started again replaces the old one, so only the new
// one fires; a second copy of a replica answer is ignored.
func TestRestartedTimerAndDuplicateAnswer(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	h.run([]step{{0, h.join(1, "g", ""), nil}, {0, h.join(2, "g", ""), nil}})
	starts := 0
	for _, e := range h.effects {
		if e.Kind == actor.EffectStartTimer && e.Key == delayKey("g") {
			starts++
		}
	}
	h.check("delay set twice", starts, 2)
	h.check("one delay timer", len(slices.DeleteFunc(slices.Clone(h.timers), func(a armed) bool { return a.key != delayKey("g") })), 1)
	h.check("no write yet", heldOps(h), "[]")
	h.until(delay)
	r := h.answerMsg(0)
	h.check("answer", len(h.step(r)), 2)
	h.check("second copy", h.step(r), []string(nil))
	h.check("phase", h.phase("g"), stateSyncing)
}

// The machine is deterministic: the same inputs give the same effects and hash.
func TestSameInputsSameRun(t *testing.T) {
	run := func() (int, uint64, uint64) {
		h := newHarness(t, Config{})
		h.stableTwo("g", 0, 1, 1)
		h.step(h.join(20, "g", ""))
		h.until(30 * sec)
		return len(h.effects), h.effectHash(), h.m.StateHash()
	}
	n1, e1, h1 := run()
	n2, e2, h2 := run()
	if n1 != n2 || e1 != e2 || h1 != h2 {
		t.Fatalf("runs differ: %d %d effects, effect hash %x %x, state hash %x %x", n1, n2, e1, e2, h1, h2)
	}
}

// Section 8: a timer that fires while its group waits for a write waits in
// the queue. Started again before the queue is taken, it does not run: only
// the new timer counts.
func TestQueuedTimerStartedAgain(t *testing.T) {
	h := newHarness(t, Config{})
	h.session = 5000
	a, b := h.stableTwo("g", 0, 1, 1) // both sessions end at 8 s
	h.hold = true
	h.run([]step{{7 * sec, leave(10, "g", b), nil}})
	h.check("remove in flight", heldOps(h), "[GroupRemove]")
	h.until(8 * sec)
	h.check("both sessions queued", len(h.group("g").queue), 2)
	// A commit of A starts its session again; its queued timer is dropped.
	h.step(OffsetCommit{ReplyTo: rt(11), Op: contract.OffsetCommit{Group: "g", Member: a, Generation: 2}})
	h.check("one session queued", len(h.group("g").queue), 1)
	h.check("leave", h.answer(0, runtime.OK), []string{"10 OK [OK]"})
	h.check("commit", h.answer(0, runtime.OK), []string{"11 OK []"})
	h.check("A stays", []any{h.phase("g"), h.members("g")}, []any{stateJoining, []string{"m1"}})
}
