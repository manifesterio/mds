package rebalance

import (
	"slices"
	"strconv"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// commit is an offset commit whose write is in flight. It does not wait in
// a group queue (rule 31).
type commit struct {
	n    uint64 // its number, in its timer key and its answer
	call actor.ReplyTo[Reply]
}

func commitKey(n uint64) actor.TimerKey { return actor.TimerKey("commit/" + strconv.FormatUint(n, 10)) }

// commit handles an OffsetCommit call (rule 31): Kafka's checks on memory
// first, then a write. The log checks again when it applies the write. A
// commit that waits for its write watches its caller.
func (m *Machine) commit(s *serving, in OffsetCommit) {
	op := in.Op
	if g := s.groups[op.Group]; g != nil && (op.Generation >= 0 || len(g.members) > 0) {
		_, mm, code := check(s, op.Group, op.Member, op.Generation)
		if _, ok := g.phase.(*syncing); ok && code == runtime.OK {
			code = runtime.ErrRebalanceInProgress
		}
		if code != runtime.OK {
			m.reply(in.ReplyTo, code, nil)
			return
		}
		m.touch(g, mm) // the group is PreparingRebalance or Stable here
	}
	op.TimeMs = m.wallMs()
	c := &commit{n: m.number(), call: in.ReplyTo}
	n := c.n
	m.ask(op, true, func(done runtime.ControlPlaneDone) Input { return commitDone{n: n, done: done} })
	m.start(nil, commitKey(n), m.cfg.WriteTimeout, commitTimedOut{n: n})
	s.commits = append(s.commits, c)
	m.o.Watch(in.ReplyTo, callGone{call: in.ReplyTo})
}

// findCommit returns the commit with number n, or nil when it is over.
func findCommit(s *serving, n uint64) *commit {
	if i := slices.IndexFunc(s.commits, func(c *commit) bool { return c.n == n }); i >= 0 {
		return s.commits[i]
	}
	return nil
}

// commitDone answers a commit. A failed write is ErrUnavailable and does not
// end the term; a code of the log's rules goes to the caller as it is. The
// answer of a commit that is over (timed out, abandoned) is ignored.
func (m *Machine) commitDone(s *serving, in commitDone) {
	c := findCommit(s, in.n)
	if c == nil {
		return
	}
	m.duplicate(in)
	m.endCommit(s, c)
	switch done := in.done; {
	case done.Err == runtime.OK:
		m.reply(c.call, runtime.OK, CommitAnswer{OffsetCommitResp: runtime.ResponseAs[contract.OffsetCommitResp](done)})
	case logRule(done.Err):
		m.reply(c.call, done.Err, nil)
	default:
		m.reply(c.call, runtime.ErrUnavailable, nil)
	}
}

// commitTimeout answers a commit whose write timer fired. Its late answer is ignored.
func (m *Machine) commitTimeout(s *serving, in commitTimedOut) {
	c := findCommit(s, in.n)
	if c == nil {
		return
	}
	m.endCommit(s, c)
	m.reply(c.call, runtime.ErrUnavailable, nil)
}

// abandonCommit forgets the commit of call, if there is one. Its write may still apply.
func (m *Machine) abandonCommit(s *serving, call actor.ReplyTo[Reply]) bool {
	i := slices.IndexFunc(s.commits, func(c *commit) bool { return c.call == call })
	if i < 0 {
		return false
	}
	m.endCommit(s, s.commits[i])
	return true
}

func (m *Machine) endCommit(s *serving, c *commit) {
	m.stop(nil, commitKey(c.n))
	s.commits = slices.DeleteFunc(s.commits, func(o *commit) bool { return o == c })
}

// logRule reports whether code is an answer of the log's rules, not a failure of the write.
func logRule(code runtime.ErrCode) bool {
	switch code {
	case runtime.ErrInvalid, runtime.ErrTooLarge, runtime.ErrUnknownMember, runtime.ErrIllegalGeneration,
		runtime.ErrUnknownPartition, runtime.ErrRebalanceInProgress:
		return true
	}
	return false
}
