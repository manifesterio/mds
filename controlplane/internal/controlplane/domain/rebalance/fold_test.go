package rebalance

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
)

// inputSamples has one value of every Input case, with every field set.
func inputSamples() []Input {
	done := runtime.ControlPlaneDone{Req: 1, Err: runtime.ErrIO}
	return []Input{
		Join{ReplyTo: rt(1), Group: "g", MemberID: "m", GroupInstanceID: "i", ClientID: "c", ClientHost: "h",
			SessionTimeoutMs: 2, RebalanceTimeoutMs: 3, ProtocolType: "consumer", Protocols: []Protocol{{Name: "range", Metadata: []byte("x")}}},
		Sync{ReplyTo: rt(1), Group: "g", MemberID: "m", Generation: 2, Assignments: []Assignment{{Member: "m", Bytes: []byte("a")}}},
		Heartbeat{ReplyTo: rt(1), Group: "g", MemberID: "m", Generation: 2},
		Leave{ReplyTo: rt(1), Group: "g", Members: []string{"a", "b"}},
		OffsetCommit{ReplyTo: rt(1), Op: contract.OffsetCommit{Group: "g"}},
		Describe{ReplyTo: rt(1), Groups: []string{"g"}},
		List{ReplyTo: rt(1)},
		RoleChanged{Active: true},
		drainLater{group: "g"},
		callGone{group: "g", call: rt(1)},
		sessionExpired{group: "g", member: "m"},
		roundDue{group: "g"},
		delayDone{group: "g"},
		writeTimedOut{group: "g"},
		commitTimedOut{n: 1},
		loadRetryDue{},
		writeDone{group: "g", n: 1, done: done},
		commitDone{n: 1, done: done},
		describeDone{n: 1, i: 2, done: done},
		loadDone{read: loadRead{name: "g"}, done: done},
	}
}

// inputName has an exhaustive switch. The linter (gochecksumtype) reports a
// new case that is missing here, so the sample list above can not fall behind.
func inputName(in Input) string {
	switch in.(type) {
	case Join:
		return "Join"
	case Sync:
		return "Sync"
	case Heartbeat:
		return "Heartbeat"
	case Leave:
		return "Leave"
	case OffsetCommit:
		return "OffsetCommit"
	case Describe:
		return "Describe"
	case List:
		return "List"
	case RoleChanged:
		return "RoleChanged"
	case drainLater:
		return "drainLater"
	case callGone:
		return "callGone"
	case sessionExpired:
		return "sessionExpired"
	case roundDue:
		return "roundDue"
	case delayDone:
		return "delayDone"
	case writeTimedOut:
		return "writeTimedOut"
	case commitTimedOut:
		return "commitTimedOut"
	case loadRetryDue:
		return "loadRetryDue"
	case writeDone:
		return "writeDone"
	case commitDone:
		return "commitDone"
	case describeDone:
		return "describeDone"
	case loadDone:
		return "loadDone"
	}
	return ""
}

func foldOf(m actor.Payload) uint64 {
	h := trace.New()
	m.Fold(&h)
	return h.Sum()
}

func TestInputTags(t *testing.T) {
	samples := inputSamples()
	if len(samples) != 20 {
		t.Fatalf("%d input samples, want 20 (one per case)", len(samples))
	}
	tags := map[uint64]string{}
	sums := map[uint64]string{}
	for _, in := range samples {
		name := inputName(in)
		if name == "" {
			t.Fatalf("%T has no name in the name switch", in)
		}
		tag := in.(interface{ Tag() uint64 }).Tag()
		if tag < 1001 || tag > 1099 {
			t.Errorf("%s: tag %d is outside the input range 1001 to 1099", name, tag)
		}
		if other, ok := tags[tag]; ok {
			t.Errorf("%s and %s share tag %d", name, other, tag)
		}
		tags[tag] = name
		sum := foldOf(in)
		if sum != foldOf(in) {
			t.Errorf("%s: Fold is not repeatable", name)
		}
		if other, ok := sums[sum]; ok {
			t.Errorf("%s and %s fold to the same hash", name, other)
		}
		sums[sum] = name
	}
}

// The tags of the calls and of Reply did not move when the machine became an actor.
func TestFrozenTags(t *testing.T) {
	want := map[string]uint64{"Join": 1001, "Sync": 1002, "Heartbeat": 1003, "Leave": 1004, "OffsetCommit": 1005,
		"Describe": 1006, "List": 1007, "RoleChanged": 1009, "drainLater": 1012}
	for _, in := range inputSamples() {
		if n, ok := want[inputName(in)]; ok {
			if got := in.(interface{ Tag() uint64 }).Tag(); got != n {
				t.Errorf("%s: tag %d, want %d", inputName(in), got, n)
			}
		}
	}
	if got := (Reply{}).Tag(); got != 1103 {
		t.Errorf("Reply: tag %d, want 1103", got)
	}
}

// TestFoldSeesEveryAnswer checks that each kind of Reply body changes the hash.
func TestFoldSeesEveryAnswer(t *testing.T) {
	bodies := []Answer{
		nil,
		JoinAnswer{MemberID: "m", Members: []Member{{ID: "m", Metadata: []byte("x")}}},
		SyncAnswer{Assignment: []byte("a")},
		LeaveAnswer{Errs: []contract.ErrCode{contract.ErrIO}},
		CommitAnswer{OffsetCommitResp: contract.OffsetCommitResp{Errs: []contract.ErrCode{contract.ErrIO}}},
		DescribeAnswer{Groups: []GroupDescription{{Name: "g"}}},
		ListAnswer{Groups: []contract.GroupSummary{{Name: "g"}}},
	}
	seen := map[uint64]int{}
	for i, b := range bodies {
		sum := foldOf(Reply{Body: b})
		if j, ok := seen[sum]; ok {
			t.Errorf("answers %d and %d fold to the same hash", j, i)
		}
		seen[sum] = i
	}
}
