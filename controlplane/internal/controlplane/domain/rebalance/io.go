package rebalance

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// The plain call types of the group protocol (Protocol, Member, Assignment) and
// the answers JoinAnswer, SyncAnswer, LeaveAnswer, DescribeAnswer and
// GroupDescription are generated from api/controlplane/v1/controlplane.proto
// into wire_types_gen.go. The inputs below carry the ReplyTo of their call, so
// they are written by hand.

// Input is what the machine receives: the group calls, RoleChanged, and the
// private messages it sends itself (timers, replica answers, gone calls,
// drainLater). Tag, Fold and the marker method of every case are written by
// dstgen into zz_dst_gen.go.
//
//sumtype:decl
//dst:sum
type Input interface {
	actor.Payload
	isInput()
}

// The calls. Each gets exactly one Reply on its ReplyTo, unless its caller
// is gone first.
//
//dst:cases Input
type (
	// Join is a JoinGroup call. An empty MemberID asks for a new member.
	// RebalanceTimeoutMs 0 means the session timeout.
	Join struct {
		ReplyTo            actor.ReplyTo[Reply]
		Group              string
		MemberID           string
		GroupInstanceID    string
		ClientID           string
		ClientHost         string
		SessionTimeoutMs   int32
		RebalanceTimeoutMs int32
		ProtocolType       string
		Protocols          []Protocol // in the member's order of preference
	}
	// Sync is a SyncGroup call. Only the leader's Assignments are used.
	Sync struct {
		ReplyTo     actor.ReplyTo[Reply]
		Group       string
		MemberID    string
		Generation  int32
		Assignments []Assignment
	}
	// Heartbeat is a Heartbeat call.
	Heartbeat struct {
		ReplyTo    actor.ReplyTo[Reply]
		Group      string
		MemberID   string
		Generation int32
	}
	// Leave is a LeaveGroup call. The answer has one code per member.
	Leave struct {
		ReplyTo actor.ReplyTo[Reply]
		Group   string
		Members []string
	}
	// OffsetCommit is an OffsetCommit call. The machine sets Op.TimeMs.
	OffsetCommit struct {
		ReplyTo actor.ReplyTo[Reply]
		Op      contract.OffsetCommit
	}
	// Describe is a DescribeGroups call.
	Describe struct {
		ReplyTo actor.ReplyTo[Reply]
		Groups  []string
	}
	// List is a ListGroups call.
	List struct {
		ReplyTo actor.ReplyTo[Reply]
	}
	// RoleChanged says whether the replica is now the active appender and Ready.
	RoleChanged struct {
		Active bool
	}
)

// Reply answers one call. Body is nil when Err is not OK, and for a heartbeat.
//
//dst:payload
//dst:fold manual
type Reply struct {
	Err  runtime.ErrCode
	Body Answer
}

// Answer is the body of a Reply: JoinAnswer, SyncAnswer, LeaveAnswer,
// CommitAnswer, DescribeAnswer or ListAnswer.
//
//sumtype:decl
type Answer interface{ isAnswer() }

type (
	// CommitAnswer has one code per offset of the call, in order.
	CommitAnswer struct {
		contract.OffsetCommitResp
	}
	// ListAnswer has every group in memory, by name.
	ListAnswer struct {
		Groups []contract.GroupSummary
	}
)

func (JoinAnswer) isAnswer()     {}
func (SyncAnswer) isAnswer()     {}
func (LeaveAnswer) isAnswer()    {}
func (CommitAnswer) isAnswer()   {}
func (DescribeAnswer) isAnswer() {}
func (ListAnswer) isAnswer()     {}

// The private messages the machine sends itself.
//
//dst:cases Input
type (
	// drainLater takes the queue of group in a later Receive (SiteLateDrain).
	drainLater struct{ group string }
	// callGone says that the caller of call left before its reply. group is
	// the group the call was for; it is empty for a describe or a commit.
	callGone struct {
		group string
		call  actor.ReplyTo[Reply]
	}
	// sessionExpired: the session (or new member) timer of member ended.
	sessionExpired struct{ group, member string }
	// roundDue: the round end (Joining) or the sync deadline (Syncing, Stable).
	roundDue struct{ group string }
	// delayDone: the initial delay of a new round ended.
	delayDone struct{ group string }
	// writeTimedOut: the write of group got no answer in WriteTimeout.
	writeTimedOut struct{ group string }
	// commitTimedOut: the write of commit n got no answer in WriteTimeout.
	commitTimedOut struct{ n uint64 }
	// loadRetryDue: the refused load reads are sent again.
	loadRetryDue struct{}
	// writeDone is the replica's answer to write n of group.
	writeDone struct {
		group string
		n     uint64
		done  runtime.ControlPlaneDone
	}
	// commitDone is the replica's answer to the write of commit n.
	commitDone struct {
		n    uint64
		done runtime.ControlPlaneDone
	}
	// describeDone is the replica's answer to read i of describe n.
	describeDone struct {
		n    uint64
		i    int
		done runtime.ControlPlaneDone
	}
	// loadDone is the replica's answer to one read of the load.
	loadDone struct {
		read loadRead
		done runtime.ControlPlaneDone
	}
)
