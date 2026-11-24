package server

import (
	"context"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/wire"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/runtime/live"
)

// Group calls go to the rebalance actor with live.Ask. The actor asks the
// replica itself and hears about role changes from the replica's
// AfterReceive hook (Node.setRole).

// call sends one group call to the rebalance actor and waits for its reply.
// When ctx ends first, the actor's Watch on the call fires, and a reply that
// comes later is dropped. When the node stops, the call is Unavailable. A
// failed answer is an RPC error; read says that the call writes nothing.
func (n *Node) call(ctx context.Context, read bool, input func(actor.ReplyTo[rebalance.Reply]) rebalance.Input) (rebalance.Reply, error) {
	if n.host == nil {
		return rebalance.Reply{}, wire.ToRPCError(runtime.ErrUnavailable, groupOutcome(read, runtime.ErrUnavailable))
	}
	r, err := live.Ask(ctx, n.host, n.groups, input)
	switch {
	case err == nil && r.Err != runtime.OK:
		return r, wire.ToRPCError(r.Err, groupOutcome(read, r.Err))
	case err == nil:
		return r, nil
	case ctx.Err() != nil:
		return rebalance.Reply{}, wire.ToRPCError(runtime.ErrTimeout, groupOutcome(read, runtime.ErrTimeout))
	}
	return rebalance.Reply{}, wire.ToRPCError(runtime.ErrUnavailable, groupOutcome(read, runtime.ErrUnavailable))
}

// groupOutcome is the outcome of a group call that failed. A read changed
// nothing. A write that is unavailable or timed out may have been logged.
func groupOutcome(read bool, code runtime.ErrCode) runtime.Outcome {
	if !read && (code == runtime.ErrUnavailable || code == runtime.ErrTimeout) {
		return runtime.MaybeApplied
	}
	return runtime.NotApplied
}

// groupRPC runs one group call on the machine and converts its answer, an A,
// to the wire type Out.
func groupRPC[Req any, A rebalance.Answer, Out any](ctx context.Context, n *Node, read bool, request Req,
	input func(actor.ReplyTo[rebalance.Reply], Req) rebalance.Input, convert func(A) Out) (Out, error) {
	var zero Out
	r, err := n.call(ctx, read, func(to actor.ReplyTo[rebalance.Reply]) rebalance.Input { return input(to, request) })
	if err != nil {
		return zero, err
	}
	a, _ := r.Body.(A) // a heartbeat has no body
	return convert(a), nil
}

// JoinGroup implements ControlPlane.
func (n *Node) JoinGroup(ctx context.Context, request *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.JoinToMachine), wire.JoinGroupResponseFromCore)
}

// SyncGroup implements ControlPlane.
func (n *Node) SyncGroup(ctx context.Context, request *cpv1.SyncGroupRequest) (*cpv1.SyncGroupResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.SyncToMachine), wire.SyncGroupResponseFromCore)
}

// Heartbeat implements ControlPlane.
func (n *Node) Heartbeat(ctx context.Context, request *cpv1.HeartbeatRequest) (*cpv1.HeartbeatResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.HeartbeatToMachine), wire.HeartbeatAnswerToWire)
}

// LeaveGroup implements ControlPlane. The answer has one code per member.
func (n *Node) LeaveGroup(ctx context.Context, request *cpv1.LeaveGroupRequest) (*cpv1.LeaveGroupResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.LeaveToMachine), wire.LeaveGroupResponseFromCore)
}

// OffsetCommit implements ControlPlane. The answer has one code per partition.
func (n *Node) OffsetCommit(ctx context.Context, request *cpv1.OffsetCommitRequest) (*cpv1.OffsetCommitResponse, error) {
	return groupRPC(ctx, n, false, request, asInput(wire.OffsetCommitToMachine), wire.CommitAnswerToWire)
}

// DescribeGroups implements ControlPlane. Any group that fails fails the call.
func (n *Node) DescribeGroups(ctx context.Context, request *cpv1.DescribeGroupsRequest) (*cpv1.DescribeGroupsResponse, error) {
	return groupRPC(ctx, n, true, request, asInput(wire.DescribeToMachine), wire.DescribeGroupsResponseFromCore)
}

// ListGroups implements ControlPlane.
func (n *Node) ListGroups(ctx context.Context, request *cpv1.ListGroupsRequest) (*cpv1.ListGroupsResponse, error) {
	return groupRPC(ctx, n, true, request, asInput(wire.ListToMachine), wire.ListAnswerToWire)
}

// asInput turns a converter that returns one input type into one that returns the sum Input.
func asInput[Req any, In rebalance.Input](convert func(actor.ReplyTo[rebalance.Reply], Req) In) func(actor.ReplyTo[rebalance.Reply], Req) rebalance.Input {
	return func(to actor.ReplyTo[rebalance.Reply], request Req) rebalance.Input { return convert(to, request) }
}

// OffsetFetch implements ControlPlane. It is not a call of the machine: it
// reads the log of this node at once, behind the role bit, so a node that does
// not serve groups now answers Unavailable and the client tries the next
// address.
func (n *Node) OffsetFetch(ctx context.Context, request *cpv1.OffsetFetchRequest) (*cpv1.OffsetFetchResponse, error) {
	if !n.active.Load() {
		return nil, wire.ToRPCError(runtime.ErrUnavailable, runtime.NotApplied)
	}
	d, err := n.Exec(ctx, wire.OffsetFetchRequestToCore(request))
	if err != nil || d.Err != runtime.OK {
		return nil, wire.ToRPCError(runtime.ErrUnavailable, runtime.NotApplied)
	}
	return wire.OffsetFetchResponseFromCore(runtime.ResponseAs[contract.OffsetFetchResp](d)), nil
}
