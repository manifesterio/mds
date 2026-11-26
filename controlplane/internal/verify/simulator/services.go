package simulator

import (
	"context"
	"slices"
	"sort"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/verify/check"
	"github.com/manifesterio/mds/dst/actor"
	dstsim "github.com/manifesterio/mds/dst/sim"
)

// Bug names one planted fault. The tests use it to prove that the simulator
// finds a bug when there is one. A normal run uses BugNone.
type Bug string

// The planted bugs.
const (
	BugNone Bug = ""
	// BugLogOverwrite makes the log say "stored" for a put into a slot that is
	// already taken, and keep the old bytes.
	BugLogOverwrite Bug = "log-overwrite"
	// BugClaimAlwaysWins makes a claim of the writer epoch win even when the
	// stored epoch is not the one expected, so two nodes can hold one epoch.
	BugClaimAlwaysWins Bug = "claim-always-wins"
)

// callMode is what the fault plan decides for one call to the log or the
// object store.
type callMode uint8

const (
	callOK      callMode = iota
	callFail             // error reply, nothing applied
	callTimeout          // timeout reply, nothing applied
	callLost             // applied, but the reply is lost: a timeout reply
)

const (
	callTimeoutAfter = 1 * actor.Second
	netMin           = 1 * actor.Millisecond
	netMax           = 4 * actor.Millisecond
	slowMin          = 50 * actor.Millisecond // extra delay of a slow call or a slow answer
	slowMax          = 400 * actor.Millisecond
)

// storage is the shared fake log and the fake object store, and the executor
// of the replicas' IO. Calls reach it with a delay, take effect at one
// instant, and the answer comes back with another delay. Faults and
// partitions act on the calls of one node.
type storage struct {
	s    *sim
	log  *executor.MemoryLog
	objs *executor.MemoryObjects
	ctx  context.Context

	// history is the first entry that was committed in each slot. Trims delete
	// slots from the log but not from here: it is the truth for the oracle.
	history     map[runtime.LogPosition][]byte
	epochWriter check.Holders[string]        // writer epoch -> writer that wrote entries with it
	epochOwner  check.Holders[string]        // writer epoch -> owner that won the claim
	end         runtime.LogPosition          // history has every slot below end
	changes     map[runtime.LogPosition]bool // slots that carry a command that changes state (not a heartbeat or a fence)
}

func newStorage(s *sim) *storage {
	return &storage{
		s: s, log: executor.NewMemoryLog(), objs: executor.NewMemoryObjects(), ctx: context.Background(),
		history: map[runtime.LogPosition][]byte{},
		changes: map[runtime.LogPosition]bool{},
	}
}

func (st *storage) netDelay(r actor.Rand) actor.Duration {
	return dstsim.DrawBetween(r, netMin, netMax) + dstsim.MaybeAdd(r, st.s.faults.Slow, slowMin, slowMax)
}

// ioKind returns whether call may change data, then whether it goes to the
// log (else to the object store).
func ioKind(call runtime.IO) (bool, bool) {
	switch call.(type) {
	case runtime.LogAppend, runtime.LogDeleteOld, runtime.LogClaimEpoch:
		return true, true
	case runtime.LogRead, runtime.LogGetEnd:
		return false, true
	case runtime.ObjectPut, runtime.ObjectDelete:
		return true, false
	case runtime.ObjectGet, runtime.ObjectList:
	}
	return false, false
}

func isWrite(call runtime.IO) bool {
	write, _ := ioKind(call)
	return write
}

func isLogCall(call runtime.IO) bool {
	_, log := ioKind(call)
	return log
}

func (st *storage) decide(r actor.Rand, call runtime.IO) callMode {
	f := st.s.faults
	if st.s.quiet {
		return callOK
	}
	if r.Chance(f.Fail) {
		return callFail
	}
	if r.Chance(f.Timeout) {
		return callTimeout
	}
	if isWrite(call) && r.Chance(f.Lost) {
		return callLost
	}
	return callOK
}

// exec is the executor of the replicas' IO. The call reaches the storage
// after a network delay drawn now; there it takes effect (or not), and the
// answer goes back to the replica that issued it after another delay. A call
// on the wire still lands after its caller died; the System drops the answer
// to a dead incarnation.
func (st *storage) exec(ctx *dstsim.ExecCtx, call runtime.IO) {
	n := st.s.byName[ctx.Issuer().Name]
	st.s.after(st.netDelay(st.s.rand), func() {
		res, back := st.execute(n, call)
		answer(ctx, call, back, res)
	})
}

// answer gives res to the Perform that issued call, after back. An
// ObjectDelete is sent with Do: nothing waits for its answer.
func answer(ctx *dstsim.ExecCtx, call runtime.IO, back actor.Duration, res actor.Payload) {
	switch c := call.(type) {
	case runtime.LogAppend:
		answerAs[runtime.LogAppendDone](ctx, c, back, res)
	case runtime.LogRead:
		answerAs[runtime.LogReadDone](ctx, c, back, res)
	case runtime.LogGetEnd:
		answerAs[runtime.LogGetEndDone](ctx, c, back, res)
	case runtime.LogDeleteOld:
		answerAs[runtime.LogDeleteOldDone](ctx, c, back, res)
	case runtime.LogClaimEpoch:
		answerAs[runtime.LogClaimEpochDone](ctx, c, back, res)
	case runtime.ObjectPut:
		answerAs[runtime.ObjectPutDone](ctx, c, back, res)
	case runtime.ObjectGet:
		answerAs[runtime.ObjectGetDone](ctx, c, back, res)
	case runtime.ObjectList:
		answerAs[runtime.ObjectListDone](ctx, c, back, res)
	case runtime.ObjectDelete:
	}
}

// answerAs answers req with res, which execute built for req's own answer type.
func answerAs[Rep actor.Payload, Req actor.Request[runtime.IO, Rep]](ctx *dstsim.ExecCtx, req Req, back actor.Duration, res actor.Payload) {
	rep, _ := res.(Rep)
	dstsim.Answer[runtime.IO, Rep](ctx, req, back, rep)
}

// execute takes the effect of call (or not) for node n, and returns the
// answer with the delay after which it arrives.
func (st *storage) execute(n *simNode, call runtime.IO) (actor.Payload, actor.Duration) {
	timeoutReply := func() (actor.Payload, actor.Duration) {
		out := runtime.NotApplied
		if isWrite(call) {
			out = runtime.MaybeApplied
		}
		return errorReply(call, runtime.ErrTimeout, out), callTimeoutAfter
	}
	if n.cut {
		st.s.stats.CutCalls++
		return timeoutReply()
	}
	r := st.s.rand
	switch st.decide(r, call) {
	case callFail:
		st.s.stats.FailedCalls++
		return errorReply(call, runtime.ErrUnavailable, runtime.NotApplied), st.netDelay(r)
	case callTimeout:
		st.s.stats.FailedCalls++
		return timeoutReply()
	case callLost:
		st.s.stats.LostReplies++
		st.apply(call)
		return timeoutReply()
	case callOK:
	}
	res := st.apply(call)
	return res, st.netDelay(r) + st.slowAnswer(r, call)
}

// errorReply is the failure answer that matches call.
func errorReply(call runtime.IO, code runtime.ErrCode, out runtime.Outcome) actor.Payload {
	switch c := call.(type) {
	case runtime.LogAppend:
		return runtime.LogAppendDone{Slot: c.Slot, Err: code, Outcome: out}
	case runtime.LogRead:
		return runtime.LogReadDone{Err: code, Outcome: out}
	case runtime.LogGetEnd:
		return runtime.LogGetEndDone{Err: code, Outcome: out}
	case runtime.LogDeleteOld:
		return runtime.LogDeleteOldDone{Err: code, Outcome: out}
	case runtime.LogClaimEpoch:
		return runtime.LogClaimEpochDone{Err: code, Outcome: out}
	case runtime.ObjectPut:
		return runtime.ObjectPutDone{Key: c.Key, Err: code, Outcome: out}
	case runtime.ObjectGet:
		return runtime.ObjectGetDone{Err: code}
	case runtime.ObjectDelete:
		return runtime.ObjectDeleteDone{Err: code, Outcome: out}
	case runtime.ObjectList:
		return runtime.ObjectListDone{Err: code}
	}
	panic("simulator: unknown IO call")
}

// apply runs call on the real in-memory log and object store, and answers like
// the executor does.
func (st *storage) apply(call runtime.IO) actor.Payload {
	switch c := call.(type) {
	case runtime.LogAppend:
		return st.applyAppend(c)
	case runtime.LogRead:
		ents, err := st.log.Read(st.ctx, c.From, int(c.Limit))
		if err != nil {
			return st.failed(call, err)
		}
		return runtime.LogReadDone{Entries: st.shortPage(ents), Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogGetEnd:
		tail, err := st.log.CheckTail(st.ctx)
		if err != nil {
			return st.failed(call, err)
		}
		return runtime.LogGetEndDone{Tail: tail, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogDeleteOld:
		if err := st.log.TrimThrough(st.ctx, c.Through); err != nil {
			return st.failed(call, err)
		}
		return runtime.LogDeleteOldDone{Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.LogClaimEpoch:
		return st.applyClaim(c)
	case runtime.ObjectPut:
		if err := st.objs.Put(st.ctx, c.Key, c.Data); err != nil {
			return st.failed(call, err)
		}
		return runtime.ObjectPutDone{Key: c.Key, Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.ObjectGet:
		data, err := st.objs.Get(st.ctx, c.Ref)
		if err != nil {
			return st.failed(call, err)
		}
		if len(data) > 0 && !st.s.quiet && st.s.rand.Chance(st.s.faults.Corrupt) {
			data[len(data)/2] ^= 0xff // Get returns a copy, so the stored object is not changed
		}
		return runtime.ObjectGetDone{Data: data}
	case runtime.ObjectDelete:
		if err := st.objs.Delete(st.ctx, c.Key); err != nil {
			return st.failed(call, err)
		}
		return runtime.ObjectDeleteDone{Err: runtime.OK, Outcome: runtime.Applied}
	case runtime.ObjectList:
		keys, err := st.objs.List(st.ctx, c.Prefix)
		if err != nil {
			return st.failed(call, err)
		}
		sort.Strings(keys) // the real store lists in key order; MemoryObjects does not
		return runtime.ObjectListDone{Keys: keys}
	}
	return errorReply(call, runtime.ErrInvalid, runtime.NotApplied)
}

func (st *storage) failed(call runtime.IO, err error) actor.Payload {
	code, out := executor.Classify(call, err)
	return errorReply(call, code, out)
}

func (st *storage) applyAppend(c runtime.LogAppend) actor.Payload {
	existing, taken, err := st.log.Append(st.ctx, c.Slot, c.Data)
	if err != nil {
		return st.failed(c, err)
	}
	switch {
	case taken && len(existing) == 0:
		// At or below the floor: not a commit, and no entry to show.
		return runtime.LogAppendDone{Slot: c.Slot, Taken: true, Err: runtime.ErrConflict, Outcome: runtime.NotApplied}
	case taken && st.s.bug == BugLogOverwrite:
		return runtime.LogAppendDone{Slot: c.Slot, Err: runtime.OK, Outcome: runtime.Applied}
	case taken:
		return runtime.LogAppendDone{Slot: c.Slot, Taken: true, Existing: existing, Err: runtime.OK, Outcome: runtime.Applied}
	}
	st.committed(c.Slot, c.Data)
	return runtime.LogAppendDone{Slot: c.Slot, Err: runtime.OK, Outcome: runtime.Applied}
}

// committed records a new entry in the history and checks that one epoch has
// one writer.
func (st *storage) committed(slot runtime.LogPosition, data []byte) {
	st.history[slot] = slices.Clone(data)
	for _, ok := st.history[st.end]; ok; _, ok = st.history[st.end] {
		st.end++
	}
	if cmds, err := replica.EntryCommands(data); err == nil {
		st.changes[slot] = slices.ContainsFunc(cmds, func(c metadb.Entry) bool { return c.Op != nil })
	}
	epoch, writer, ok := replica.EntryWriter(data)
	if !ok {
		return
	}
	if first, ok := st.epochWriter.Note(epoch, writer); !ok {
		check.Violation(st.s.env, oneWriterPerEpoch, "epoch %d has entries from %q and from %q (slot %d)", epoch, first, writer, slot)
	}
}

func (st *storage) applyClaim(c runtime.LogClaimEpoch) actor.Payload {
	epoch, owner, won, err := st.log.ClaimEpoch(st.ctx, c.Expect, c.Owner)
	if err != nil {
		return st.failed(c, err)
	}
	if !won && st.s.bug == BugClaimAlwaysWins {
		epoch, owner, won = c.Expect+1, c.Owner, true
	}
	if won {
		if first, ok := st.epochOwner.Note(epoch, owner); !ok {
			check.Violation(st.s.env, oneWriterPerEpoch, "epoch %d was won by %q and by %q", epoch, first, owner)
		}
	}
	return runtime.LogClaimEpochDone{Epoch: epoch, Owner: owner, Won: won, Err: runtime.OK, Outcome: runtime.Applied}
}

// logEnd is the number of slots in the history if it has no hole, and the
// first hole otherwise.
func (st *storage) logEnd() (runtime.LogPosition, bool) {
	return st.end, len(st.history) > int(st.end)
}

// storage is the check.Log of the oracle.
var _ check.Log = (*storage)(nil)

// End implements check.Log.
func (st *storage) End() (check.Slot, bool) {
	end, hole := st.logEnd()
	return check.Slot(end), hole
}

// Entry implements check.Log.
func (st *storage) Entry(slot check.Slot) []byte { return st.history[runtime.LogPosition(slot)] }

// QuietAfter implements check.Log.
func (st *storage) QuietAfter(from check.Slot) bool { return st.quietAfter(runtime.LogPosition(from)) }

// quietAfter reports that the slots from `from` to the end of the log carry no
// command that changes state. A node that has applied up to `from` then has the
// final state, even if a heartbeat is still on its way to it.
func (st *storage) quietAfter(from runtime.LogPosition) bool {
	end, _ := st.logEnd()
	for slot := from; slot < end; slot++ {
		if st.changes[slot] {
			return false
		}
	}
	return true
}
