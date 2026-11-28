package rebalance

import (
	"cmp"
	"slices"
	"strings"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// loading reads every group from the log (section 9). It answers every call
// ErrUnavailable. fresh: the reads wait for a fresh log tail (after a broken
// term); else they are LocalReads (after a takeover).
type loading struct {
	fresh    bool
	listed   bool     // the ListGroups read is back
	names    []string // the groups to read, from ListGroups
	descs    map[string]contract.DescribeGroupResp
	waiting  map[loadRead]bool // the reads in flight
	again    []loadRead        // refused reads, sent again when the retry timer fires (rule 39)
	retrying bool              // the retry timer runs
}

// loadRead is one read of the load: ListGroups, or DescribeGroup of name.
type loadRead struct {
	list bool
	name string
}

// startLoad drops into Loading and reads the list of groups.
func (m *Machine) startLoad(fresh bool) {
	l := &loading{fresh: fresh, descs: map[string]contract.DescribeGroupResp{}, waiting: map[loadRead]bool{}}
	m.state = l
	m.sendLoadRead(l, loadRead{list: true})
}

func (m *Machine) sendLoadRead(l *loading, r loadRead) {
	var op runtime.ControlPlaneOperation = contract.ListGroups{}
	if !r.list {
		op = describeOp(r.name)
	}
	l.waiting[r] = true
	m.ask(op, l.fresh, func(done runtime.ControlPlaneDone) Input { return loadDone{read: r, done: done} })
}

func (m *Machine) loadStep(l *loading, inbox Input) {
	switch in := inbox.(type) {
	case RoleChanged:
		if !in.Active {
			// The reads in flight and the retry timer belong to this load only.
			m.o.ForgetPending()
			m.state = &standby{}
		}
	case loadDone:
		m.loadReply(l, in)
	case loadRetryDue:
		l.retrying = false
		again := l.again
		l.again = nil
		for _, r := range again {
			m.sendLoadRead(l, r)
		}
	case Join, Sync, Heartbeat, Leave, OffsetCommit, Describe, List:
		m.refuse(inbox)
	case drainLater, callGone, sessionExpired, roundDue, delayDone, writeTimedOut, commitTimedOut,
		writeDone, commitDone, describeDone:
		// Messages of a term that is over.
	}
}

// loadReply takes one load read. A refused read is sent again after LoadRetry.
func (m *Machine) loadReply(l *loading, in loadDone) {
	if !l.waiting[in.read] {
		return
	}
	delete(l.waiting, in.read)
	if in.done.Err != runtime.OK || m.maybe(SiteLoadRetry) {
		l.again = append(l.again, in.read)
		if !l.retrying {
			l.retrying = true
			m.start(nil, loadRetryKey, m.cfg.LoadRetry, loadRetryDue{})
		}
		return
	}
	if in.read.list {
		l.listed = true
		for _, s := range runtime.ResponseAs[contract.ListGroupsResp](in.done).Groups {
			l.names = insertName(l.names, s.Name)
			m.sendLoadRead(l, loadRead{name: s.Name})
		}
	} else {
		l.descs[in.read.name] = runtime.ResponseAs[contract.DescribeGroupResp](in.done)
	}
	if l.listed && len(l.waiting) == 0 && len(l.again) == 0 {
		s := newServing()
		m.state = s
		for _, name := range l.names {
			m.loadGroup(s, name, l.descs[name])
		}
	}
}

// loadGroup fills a group from the log (rule 35). Each member's session
// starts now; no member is new or owes a sync. The group is Stable when its
// assignment is for its generation and its leader is a member, Empty with
// no member, else in a round from now with no delay.
func (m *Machine) loadGroup(s *serving, name string, d contract.DescribeGroupResp) {
	g := newGroup(s, name)
	g.generation, g.protoType, g.protocol, g.leader = d.Generation, d.ProtocolType, d.Protocol, d.Leader
	infos := slices.Clone(d.Members)
	slices.SortStableFunc(infos, func(a, b contract.GroupMemberInfo) int {
		return cmp.Or(cmp.Compare(a.JoinedTimeMs, b.JoinedTimeMs), strings.Compare(a.MemberID, b.MemberID))
	})
	assign := map[string][]byte{}
	for _, info := range infos {
		mm := newMember(g, info.MemberID)
		mm.clientID, mm.clientHost, mm.instanceID = info.ClientID, info.ClientHost, info.GroupInstanceID
		mm.sessionMs, mm.rebalanceMs = info.SessionTimeoutMs, info.RebalanceTimeoutMs
		mm.synced = true
		m.touch(g, mm)
		if len(info.Assignment) > 0 {
			assign[info.MemberID] = info.Assignment
		}
	}
	switch {
	case len(g.members) == 0:
		m.toEmpty(g)
	case d.AssignedGeneration == d.Generation && g.find(d.Leader) != nil:
		g.phase = &stable{assign: assign}
	default:
		m.openRound(g, false)
	}
}
