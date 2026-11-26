package wire

import (
	"slices"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/dst/actor"
)

// The converters between the group RPCs and the inputs and answers of the
// rebalance machine (domain/rebalance). The answers JoinAnswer, SyncAnswer,
// LeaveAnswer and DescribeAnswer are generated with their converters
// (JoinGroupResponseFromCore and so on, see wire_gen.go). The inputs carry
// the ReplyTo of their call, so they are written by hand. A request takes the
// ReplyTo that the server's ask gives the RPC.

// JoinToMachine is the Join input of a JoinGroup request.
func JoinToMachine(to actor.ReplyTo[rebalance.Reply], m *cpv1.JoinGroupRequest) rebalance.Join {
	return rebalance.Join{
		ReplyTo:            to,
		Group:              m.GetGroup(),
		MemberID:           m.GetMemberId(),
		GroupInstanceID:    m.GetGroupInstanceId(),
		ClientID:           m.GetClientId(),
		ClientHost:         m.GetClientHost(),
		SessionTimeoutMs:   m.GetSessionTimeoutMs(),
		RebalanceTimeoutMs: m.GetRebalanceTimeoutMs(),
		ProtocolType:       m.GetProtocolType(),
		Protocols:          mapSlice(m.GetProtocols(), GroupProtocolToCore),
	}
}

// SyncToMachine is the Sync input of a SyncGroup request.
func SyncToMachine(to actor.ReplyTo[rebalance.Reply], m *cpv1.SyncGroupRequest) rebalance.Sync {
	return rebalance.Sync{
		ReplyTo:     to,
		Group:       m.GetGroup(),
		MemberID:    m.GetMemberId(),
		Generation:  m.GetGeneration(),
		Assignments: mapSlice(m.GetAssignments(), GroupAssignmentToCore),
	}
}

// HeartbeatToMachine is the Heartbeat input of a Heartbeat request.
func HeartbeatToMachine(to actor.ReplyTo[rebalance.Reply], m *cpv1.HeartbeatRequest) rebalance.Heartbeat {
	return rebalance.Heartbeat{ReplyTo: to, Group: m.GetGroup(), MemberID: m.GetMemberId(), Generation: m.GetGeneration()}
}

// LeaveToMachine is the Leave input of a LeaveGroup request.
func LeaveToMachine(to actor.ReplyTo[rebalance.Reply], m *cpv1.LeaveGroupRequest) rebalance.Leave {
	return rebalance.Leave{ReplyTo: to, Group: m.GetGroup(), Members: slices.Clone(m.GetMembers())}
}

// OffsetCommitToMachine is the OffsetCommit input of an OffsetCommit request.
func OffsetCommitToMachine(to actor.ReplyTo[rebalance.Reply], m *cpv1.OffsetCommitRequest) rebalance.OffsetCommit {
	return rebalance.OffsetCommit{ReplyTo: to, Op: OffsetCommitRequestToCore(m)}
}

// DescribeToMachine is the Describe input of a DescribeGroups request.
func DescribeToMachine(to actor.ReplyTo[rebalance.Reply], m *cpv1.DescribeGroupsRequest) rebalance.Describe {
	return rebalance.Describe{ReplyTo: to, Groups: slices.Clone(m.GetGroups())}
}

// ListToMachine is the List input of a ListGroups request.
func ListToMachine(to actor.ReplyTo[rebalance.Reply], _ *cpv1.ListGroupsRequest) rebalance.List {
	return rebalance.List{ReplyTo: to}
}

// CommitAnswerToWire is the OffsetCommit response of a commit answer.
func CommitAnswerToWire(a rebalance.CommitAnswer) *cpv1.OffsetCommitResponse {
	return OffsetCommitResponseFromCore(a.OffsetCommitResp)
}

// ListAnswerToWire is the ListGroups response of a list answer.
func ListAnswerToWire(a rebalance.ListAnswer) *cpv1.ListGroupsResponse {
	return ListGroupsResponseFromCore(contract.ListGroupsResp{Groups: a.Groups})
}

// HeartbeatAnswerToWire is the empty Heartbeat response: a heartbeat answer has no body.
func HeartbeatAnswerToWire(rebalance.Answer) *cpv1.HeartbeatResponse {
	return &cpv1.HeartbeatResponse{}
}
