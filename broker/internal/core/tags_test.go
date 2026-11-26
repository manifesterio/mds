package core

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// These tests keep the tag numbers and the Fold methods safe. Every case has one sample
// value, with every field set. The sample lists are the ones of the old tag tests.

var (
	sampleInstance = actor.Incarnation{Name: "n1", Boot: actor.ID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	sampleRef      = SegmentRef{Key: "seg/1", Off: 10, Len: -1}
	sampleTP       = TopicPartition{Topic: "t", Partition: -2}
	sampleRange    = contract.BatchRange{TopicPartition: sampleTP, Base: 5, Count: 3, Loc: sampleRef}
	samplePO       = contract.PartitionOffset{TopicPartition: sampleTP, Offset: 9, LeaderEpoch: -1, Metadata: "m"}
	sampleMember   = contract.GroupMember{MemberID: "m1", ClientID: "c", ClientHost: "h", GroupInstanceID: "i", SessionTimeoutMs: 10, RebalanceTimeoutMs: -20, Metadata: []byte("md")}
	sampleInfo     = contract.GroupMemberInfo{GroupMember: sampleMember, JoinedTimeMs: 77, Assignment: []byte("as")}
	sampleAssign   = []contract.GroupAssignment{{Member: "m1", Bytes: []byte("x")}, {Member: "m2", Bytes: nil}}
	sampleDescribe = contract.DescribeGroupResp{
		Exists: true, Generation: 4, ProtocolType: "consumer", Protocol: "range", Leader: "m1",
		AssignedGeneration: 3, State: "Stable", Members: []contract.GroupMemberInfo{sampleInfo},
	}
)

// sampleEvents has one value of every EventData case, with every field set.
// ObjectDone has one value per object store result.
func sampleEvents() map[string]EventData {
	putDone := ObjectDone{Result: objectstore.PutDone{Key: "k", Err: ErrTimeout, Outcome: MaybeApplied}}
	cpDone := ControlPlaneDone{Resp: contract.GroupSyncResp{Assignment: []byte("z")}, Err: OK, Outcome: Applied}
	return map[string]EventData{
		"NodeStarted": NodeStarted{Self: sampleInstance}, "StopRequest": StopRequest{Deadline: -5},
		"ConnOpened": ConnOpened{Conn: 3, Peer: "p"}, "BytesReceived": BytesReceived{Conn: 3, Data: []byte("abc")},
		"ConnClosed":        ConnClosed{Conn: 4, Abort: true},
		"ConnectDone":       ConnectDone{Conn: 6, Err: ErrConnClosed},
		"ObjectPutDone":     putDone,
		"ObjectGetDone":     ObjectDone{Result: objectstore.GetDone{Data: []byte("d"), Err: ErrNotFound}},
		"ObjectDeleteDone":  ObjectDone{Result: objectstore.DeleteDone{Err: ErrIO, Outcome: Applied}},
		"ObjectListDone":    ObjectDone{Result: objectstore.ListDone{Keys: []string{"a", "b"}, Err: ErrUnavailable}},
		"ControlPlaneDone":  cpDone,
		"SegmentPutDone":    SegmentPutDone{Seg: 17, Done: putDone},
		"SegmentCommitDone": SegmentCommitDone{Seg: 17, Done: cpDone},
		"OpReadDone":        OpReadDone{Op: 18, Done: putDone},
		"OpCallDone":        OpCallDone{Op: 18, Done: cpDone},
		"FlushDue":          FlushDue{}, "SegmentRetryDue": SegmentRetryDue{Seg: 19}, "OpDue": OpDue{Op: 20},
		"ClientDue":   ClientDue{Key: "wake"},
		"LiveBrokers": LiveBrokers{Brokers: []BrokerAddr{{NodeID: 1, Host: "h", Port: 9092}, {NodeID: -1}}},
	}
}

// sampleIO has one value of every IO case, with every field set. ObjectCall
// has one value per object store call.
func sampleIO() map[string]IO {
	return map[string]IO{
		"Send": Send{Conn: 2, Data: []byte("s")}, "CloseConn": CloseConn{Conn: 2, Abort: true},
		"PauseReads": PauseReads{Conn: 2}, "ResumeReads": ResumeReads{Conn: 3},
		"Connect":      Connect{Addr: "a:1"},
		"ObjectPut":    ObjectCall{Call: objectstore.Put{Key: "k", Data: []byte("v")}},
		"ObjectGet":    ObjectCall{Call: objectstore.Get{Ref: sampleRef}},
		"ObjectDelete": ObjectCall{Call: objectstore.Delete{Key: "k"}},
		"ObjectList":   ObjectCall{Call: objectstore.List{Prefix: "p/"}},
		"ControlPlaneCall": ControlPlaneCall{
			Op: contract.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3}, Timeout: actor.Second,
		},
	}
}

// sampleOperations has one value of every control plane operation case, with every field set.
// CreateTopic has two values, because its tag depends on FailIfExists.
func sampleOperations() map[string]contract.Operation {
	return map[string]contract.Operation{
		"CommitSegment":        contract.CommitSegment{SegmentKey: "s", Batches: []contract.PendingBatch{{TopicPartition: sampleTP, Records: 4, Loc: sampleRef}}},
		"FindSegments":         contract.FindSegments{TopicPartition: sampleTP, From: 4, MaxBytes: -3},
		"DescribeTopics":       contract.DescribeTopics{Topics: []string{"a", "b"}},
		"CreateTopic":          contract.CreateTopic{Name: "t", Partitions: 3},
		"CreateTopicExclusive": contract.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true},
		"GroupInstall": contract.GroupInstall{
			Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 2, Leader: "m1", TimeMs: 9,
			Members: []contract.GroupMember{sampleMember},
		},
		"GroupAssign":   contract.GroupAssign{Group: "g", Generation: 2, Leader: "m1", TimeMs: 9, Assignments: sampleAssign},
		"GroupRemove":   contract.GroupRemove{Group: "g", Members: []string{"m1", "m2"}, TimeMs: 9, Reason: "leave"},
		"OffsetCommit":  contract.OffsetCommit{Group: "g", Member: "m1", Generation: -1, TimeMs: 9, Offsets: []contract.PartitionOffset{samplePO}},
		"OffsetFetch":   contract.OffsetFetch{Group: "g", Partitions: []TopicPartition{sampleTP}},
		"DescribeGroup": contract.DescribeGroup{Group: "g"}, "ListGroups": contract.ListGroups{},
		"GroupJoin": contract.GroupJoin{
			Group: "g", MemberID: "m1", GroupInstanceID: "i", ClientID: "c", ClientHost: "h", SessionTimeoutMs: 10,
			RebalanceTimeoutMs: 20, ProtocolType: "consumer", Protocols: []contract.GroupProtocol{{Name: "range", Metadata: []byte("q")}},
		},
		"GroupSync":      contract.GroupSync{Group: "g", MemberID: "m1", Generation: 3, Assignments: sampleAssign},
		"GroupHeartbeat": contract.GroupHeartbeat{Group: "g", MemberID: "m1", Generation: 3},
		"GroupLeave":     contract.GroupLeave{Group: "g", Members: []string{"m1"}},
		"DescribeGroups": contract.DescribeGroups{Groups: []string{"g", "h"}},
	}
}

// sampleResponses has one value of every control plane response case, with every field set.
func sampleResponses() map[string]contract.Response {
	return map[string]contract.Response{
		"CommitSegmentResp":  contract.CommitSegmentResp{Assigned: []contract.BatchRange{sampleRange}},
		"FindSegmentsResp":   contract.FindSegmentsResp{Ranges: []contract.BatchRange{sampleRange}, End: 12},
		"DescribeTopicsResp": contract.DescribeTopicsResp{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}},
		"OffsetCommitResp":   contract.OffsetCommitResp{Errs: []ErrCode{OK, ErrUnknownPartition}},
		"OffsetFetchResp":    contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{samplePO}},
		"DescribeGroupResp":  sampleDescribe,
		"ListGroupsResp":     contract.ListGroupsResp{Groups: []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}},
		"GroupJoinResp": contract.GroupJoinResp{
			Generation: 3, Protocol: "range", Leader: "m1", MemberID: "m2",
			Members: []contract.GroupJoinMember{{MemberID: "m1", GroupInstanceID: "i", Metadata: []byte("md")}},
		},
		"GroupSyncResp":      contract.GroupSyncResp{Assignment: []byte("as")},
		"GroupLeaveResp":     contract.GroupLeaveResp{Errs: []ErrCode{OK, ErrUnknownMember}},
		"DescribeGroupsResp": contract.DescribeGroupsResp{Groups: []contract.GroupDescription{{Name: "g", DescribeGroupResp: sampleDescribe}}},
	}
}

// sampleFieldValues has one value of every field value case. They live in dst/actor and
// have no Tag method: Fold writes the tag.
func sampleFieldValues() map[string]actor.FieldValue {
	return map[string]actor.FieldValue{
		"Int64Value": actor.Int64Value(-4), "Uint64Value": actor.Uint64Value(4), "StringValue": actor.StringValue("s"),
		"Bool": actor.Bool(true), "Bytes": actor.Bytes("by"),
	}
}

// foldOf folds one value into a fresh hash and returns the hash.
func foldOf(f func(h *trace.Hash)) trace.Hash {
	h := trace.New()
	f(&h)
	return h
}

// checkFold checks that Fold writes something, and writes the same bytes every time.
func checkFold(t *testing.T, name string, f func(h *trace.Hash)) {
	t.Helper()
	a, b := foldOf(f), foldOf(f)
	if a == trace.New() {
		t.Errorf("%s: Fold wrote nothing", name)
	}
	if a != b {
		t.Errorf("%s: Fold is not deterministic", name)
	}
}

// tagged is a message with a frozen tag.
type tagged interface{ Tag() uint64 }

// Every tag of an event, IO case, operation or response is in its range, and no two types
// share a tag. Values of one type (the object store wrappers) share its tag. A value
// without a tag does not compile.
func TestTagsAreInRangeAndUnique(t *testing.T) {
	seen := map[uint64]string{}
	check := func(kind string, v tagged, tag, lo, hi uint64) {
		t.Helper()
		name := fmt.Sprintf("%T", v)
		if tag < lo || tag > hi {
			t.Errorf("%s %s: tag %d is outside %d..%d", kind, name, tag, lo, hi)
		}
		if prev, dup := seen[tag]; dup && prev != kind+" "+name {
			t.Errorf("%s %s and %s have the same tag %d", kind, name, prev, tag)
		}
		seen[tag] = kind + " " + name
	}
	for _, v := range sampleEvents() {
		check("event", v.(tagged), v.(tagged).Tag(), 1, 99)
	}
	for _, v := range sampleIO() {
		check("io", v.(tagged), v.(tagged).Tag(), 101, 199)
	}
	for _, v := range sampleOperations() {
		check("operation", v, v.Tag(), 201, 299)
	}
	for _, v := range sampleResponses() {
		check("response", v, v.Tag(), 301, 399)
	}
}

// Fold of every case writes bytes, the same bytes each time, and two cases never fold alike.
func TestFoldWritesDistinctDeterministicBytes(t *testing.T) {
	hashes := map[trace.Hash]string{}
	add := func(name string, f func(h *trace.Hash)) {
		t.Helper()
		checkFold(t, name, f)
		h := foldOf(f)
		if prev, dup := hashes[h]; dup {
			t.Errorf("%s and %s fold alike", name, prev)
		}
		hashes[h] = name
	}
	for n, v := range sampleEvents() {
		add("event "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleIO() {
		add("io "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleOperations() {
		add("operation "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleResponses() {
		add("response "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	for n, v := range sampleFieldValues() {
		add("field "+n, func(h *trace.Hash) { v.Fold(h) })
	}
	// A value with a nil operation or response shares the tag of its type, so these are only
	// folded here, not in the tag check above.
	add("event ControlPlaneDone nil response", func(h *trace.Hash) {
		ControlPlaneDone{Err: ErrTimeout, Outcome: MaybeApplied}.Fold(h)
	})
	add("io ControlPlaneCall nil operation", func(h *trace.Hash) {
		ControlPlaneCall{Timeout: 4}.Fold(h)
	})
	fact := actor.Fact{Name: "fact", Fields: []actor.Field{{Key: "a", Value: actor.Int64Value(-1)}}}
	add("fact", func(h *trace.Hash) { fact.Fold(h) })
}
