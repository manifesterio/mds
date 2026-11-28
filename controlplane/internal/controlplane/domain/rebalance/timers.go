package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/dst/actor"
)

// The timer keys. A timer started again under the same key replaces the old
// one, and a replaced or cancelled timer never arrives.

func sessionKey(group, member string) actor.TimerKey {
	return actor.TimerKey("session/" + group + "/" + member)
}
func roundKey(group string) actor.TimerKey { return actor.TimerKey("round/" + group) }
func delayKey(group string) actor.TimerKey { return actor.TimerKey("delay/" + group) }
func writeKey(group string) actor.TimerKey { return actor.TimerKey("write/" + group) }

// loadRetryKey is the timer of the refused load reads.
const loadRetryKey actor.TimerKey = "load/retry"

// timerKey returns the key of a group timer message.
func timerKey(in Input) (actor.TimerKey, bool) {
	if t, ok := in.(sessionExpired); ok {
		return sessionKey(t.group, t.member), true
	}
	if t, ok := in.(roundDue); ok {
		return roundKey(t.group), true
	}
	if t, ok := in.(delayDone); ok {
		return delayKey(t.group), true
	}
	return "", false
}

// start sets the timer key to deliver msg after d. A timer with the same key
// is replaced. The timers of a group can wait in its queue while it is busy:
// one that is started again is taken out there too, so only the new one
// arrives. g is nil for a timer that is not a group's.
func (m *Machine) start(g *group, key actor.TimerKey, d actor.Duration, msg Input) {
	if m.maybe(SiteTimerLate) {
		d += actor.Duration(m.env.Between(int64(actor.Millisecond), int64(lateMax)))
	}
	unqueue(g, key)
	m.o.StartTimer(key, d, msg)
}

// stop cancels the timer key, also when it waits in the queue of g.
func (m *Machine) stop(g *group, key actor.TimerKey) {
	unqueue(g, key)
	m.o.CancelTimer(key)
}

func unqueue(g *group, key actor.TimerKey) {
	if g == nil {
		return
	}
	g.queue = slices.DeleteFunc(g.queue, func(in Input) bool { k, ok := timerKey(in); return ok && k == key })
}

func (m *Machine) startSession(g *group, mm *member, d actor.Duration) {
	m.start(g, sessionKey(g.name, mm.id), d, sessionExpired{group: g.name, member: mm.id})
}

func (m *Machine) startRound(g *group, d actor.Duration) {
	m.start(g, roundKey(g.name), d, roundDue{group: g.name})
}

func (m *Machine) startDelay(g *group, d actor.Duration) {
	m.start(g, delayKey(g.name), d, delayDone{group: g.name})
}

// touch starts the session of mm again (rule 29). A new member keeps its new
// member timer (rule 30).
func (m *Machine) touch(g *group, mm *member) {
	if !mm.isNew {
		m.startSession(g, mm, ms(mm.sessionMs))
	}
}

// sessionTimer runs a session timer on a group that is not busy.
func (m *Machine) sessionTimer(s *serving, in sessionExpired) {
	g := s.groups[in.group]
	if g == nil {
		return
	}
	if mm := g.find(in.member); mm != nil {
		m.sessionEnded(s, g, mm)
	}
}

// delayTimer ends the initial delay of a round, on a group that is not busy.
func (m *Machine) delayTimer(s *serving, in delayDone) {
	g := s.groups[in.group]
	if g == nil {
		return
	}
	if jn, ok := g.phase.(*joining); ok {
		jn.delay = false
		m.maybeComplete(s, g, false)
	}
}

// sessionEnded removes a member whose session timer fired (rule 28), unless
// it is an existing member with a parked join or sync (rule 30). Its timer
// starts again when that call ends.
func (m *Machine) sessionEnded(s *serving, g *group, mm *member) {
	if _, waits := g.parked(mm); waits && !mm.isNew {
		return
	}
	m.remove(s, g, []string{mm.id}, removeWrite{cause: causeSession})
}

// roundTimer is the round end in Joining (rule 12) or the sync deadline in
// Syncing and Stable (rule 24).
func (m *Machine) roundTimer(s *serving, g *group) {
	if _, ok := g.phase.(*joining); ok {
		m.maybeComplete(s, g, true)
		return
	}
	late := g.ids(func(mm *member) bool { return !mm.synced })
	if len(late) > 0 {
		m.remove(s, g, late, removeWrite{cause: causeSyncDeadline})
	}
}
