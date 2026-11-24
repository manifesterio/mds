package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
)

// cpFailure is one way a control plane call can fail.
type cpFailure struct {
	name string
	done core.ControlPlaneDone
}

// retriedFailures are failures the broker retries: the control plane is down,
// or the call timed out and may have taken effect.
var retriedFailures = []cpFailure{
	{name: "unavailable", done: core.ControlPlaneDone{Err: core.ErrUnavailable, Outcome: core.NotApplied}},
	{name: "timeout", done: core.ControlPlaneDone{Err: core.ErrTimeout, Outcome: core.MaybeApplied}},
}

// failingCP makes every control plane call fail with done, from now on.
func failingCP(h *harness, done core.ControlPlaneDone) {
	h.cpCalls = nil
	h.cpHook = func(core.ControlPlaneCall, func() core.ControlPlaneDone) core.ControlPlaneDone { return done }
}

// warm fills the broker's topic cache with a Metadata call while the control
// plane works.
func warm(h *harness, c *client) {
	h.call(c, verMetadata, kmsg.NewPtrMetadataRequest())
}

func listLatest(topic string, partition int32) *kmsg.ListOffsetsRequest {
	q := kmsg.NewPtrListOffsetsRequest()
	q.ReplicaID = -1
	q.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic,
		Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: partition, Timestamp: -1, CurrentLeaderEpoch: -1}}}}
	return q
}

func metadataOf(topics ...string) *kmsg.MetadataRequest {
	q := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		q.Topics = append(q.Topics, kmsg.MetadataRequestTopic{Topic: new(t)})
	}
	return q
}

func describeTopicAndBroker(topic string) *kmsg.DescribeConfigsRequest {
	q := kmsg.NewPtrDescribeConfigsRequest()
	q.Resources = []kmsg.DescribeConfigsRequestResource{
		{ResourceType: resourceTopic, ResourceName: topic},
		{ResourceType: kmsg.ConfigResourceType(4), ResourceName: "1"},
	}
	return q
}

// exhaustCase is one request kind whose control plane call keeps failing.
type exhaustCase struct {
	name string
	warm bool // fill the topic cache before the control plane fails
	ver  int16
	req  func() kmsg.Request
	// code reads the Kafka error code of the failed part of the answer.
	code func(t *testing.T, r kmsg.Response) kafkaErrorCode
	want kafkaErrorCode
}

var exhaustCases = []exhaustCase{
	{
		// The topic cache cannot be refreshed: the request runs with what the
		// cache has, which does not know the topic.
		name: "refresh before ListOffsets", ver: verList,
		req: func() kmsg.Request { return listLatest("t", 0) },
		code: func(_ *testing.T, r kmsg.Response) kafkaErrorCode {
			return kafkaErrorCode(r.(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0].ErrorCode)
		},
		want: errUnknownTopicOrPart,
	},
	{
		name: "Metadata", ver: verMetadata,
		req: func() kmsg.Request { return metadataOf("t") },
		code: func(_ *testing.T, r kmsg.Response) kafkaErrorCode {
			return kafkaErrorCode(r.(*kmsg.MetadataResponse).Topics[0].ErrorCode)
		},
		want: errLeaderNotAvailable,
	},
	{
		name: "ListOffsets", warm: true, ver: verList,
		req: func() kmsg.Request { return listLatest("t", 0) },
		code: func(_ *testing.T, r kmsg.Response) kafkaErrorCode {
			return kafkaErrorCode(r.(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0].ErrorCode)
		},
		want: errRequestTimedOut,
	},
	{
		name: "CreateTopics", ver: verCreate,
		req: func() kmsg.Request { return createReq(false, newTopic("never", 1, 1)) },
		code: func(_ *testing.T, r kmsg.Response) kafkaErrorCode {
			return kafkaErrorCode(r.(*kmsg.CreateTopicsResponse).Topics[0].ErrorCode)
		},
		want: errRequestTimedOut,
	},
	{
		name: "CreateTopics validate only", ver: verCreate,
		req: func() kmsg.Request { return createReq(true, newTopic("never", 1, 1)) },
		code: func(_ *testing.T, r kmsg.Response) kafkaErrorCode {
			return kafkaErrorCode(r.(*kmsg.CreateTopicsResponse).Topics[0].ErrorCode)
		},
		want: errRequestTimedOut,
	},
	{
		// The topic resource fails; the broker resource keeps its own refusal.
		name: "DescribeConfigs", ver: verDescribeConfigs,
		req: func() kmsg.Request { return describeTopicAndBroker("t") },
		code: func(t *testing.T, r kmsg.Response) kafkaErrorCode {
			res := r.(*kmsg.DescribeConfigsResponse).Resources
			if kafkaErrorCode(res[1].ErrorCode) != errInvalidRequest {
				t.Errorf("broker resource: %+v", res[1])
			}
			return kafkaErrorCode(res[0].ErrorCode)
		},
		want: errRequestTimedOut,
	},
}

// A control plane that fails every call makes the broker try MaxAttempts
// times and then answer the request with an error, not hang.
func TestControlPlaneFailuresExhaustRetries(t *testing.T) {
	for _, tc := range exhaustCases {
		for _, f := range retriedFailures {
			t.Run(tc.name+"/"+f.name, func(t *testing.T) {
				config := DefaultConfig()
				h := startedHarness(t, config)
				c := h.open(1)
				if tc.warm {
					warm(h, c)
				}
				failingCP(h, f.done)
				h.send(c, tc.ver, tc.req())
				h.fireAll()
				if got := tc.code(t, h.next(c)); got != tc.want {
					t.Errorf("error code %d, want %d", got, tc.want)
				}
				if len(h.cpCalls) != config.MaxAttempts {
					t.Errorf("%d control plane calls, want %d", len(h.cpCalls), config.MaxAttempts)
				}
				if len(c.out) != 0 {
					t.Errorf("%d extra responses", len(c.out))
				}
				h.noViolations()
			})
		}
	}
}

// A control plane that refuses a call as invalid is not retried: the request
// is answered at once.
func TestControlPlaneInvalidIsNotRetried(t *testing.T) {
	want := map[string]kafkaErrorCode{
		"refresh before ListOffsets": errUnknownTopicOrPart,
		"Metadata":                   errUnknownServer,
		"ListOffsets":                errUnknownServer,
		"CreateTopics":               errInvalidRequest,
		"DescribeConfigs":            errUnknownServer,
	}
	for _, tc := range exhaustCases {
		code, ok := want[tc.name]
		if !ok {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			h := startedHarness(t, DefaultConfig())
			c := h.open(1)
			if tc.warm {
				warm(h, c)
			}
			failingCP(h, core.ControlPlaneDone{Err: core.ErrInvalid, Outcome: core.NotApplied})
			r := h.call(c, tc.ver, tc.req())
			if got := tc.code(t, r); got != code {
				t.Errorf("error code %d, want %d", got, code)
			}
			if len(h.cpCalls) != 1 {
				t.Errorf("%d control plane calls, want 1", len(h.cpCalls))
			}
		})
	}
}

// A failed refresh does not stop a later request: once the control plane is
// back, the topic is found.
func TestRefreshFailureIsNotRemembered(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	failingCP(h, core.ControlPlaneDone{Err: core.ErrUnavailable, Outcome: core.NotApplied})
	h.send(c, verList, listLatest("t", 1))
	h.fireAll()
	if p := h.next(c).(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0]; kafkaErrorCode(p.ErrorCode) != errUnknownTopicOrPart {
		t.Fatalf("while down: %+v", p)
	}
	h.cpHook = nil
	if p := h.call(c, verList, listLatest("t", 1)).(*kmsg.ListOffsetsResponse).Topics[0].Partitions[0]; p.ErrorCode != 0 || p.Offset != 0 {
		t.Fatalf("after recovery: %+v", p)
	}
	if _, ok := h.cpCalls[len(h.cpCalls)-1].(contract.FindSegments); !ok {
		t.Fatalf("last call %#v, want FindSegments", h.cpCalls[len(h.cpCalls)-1])
	}
	h.noViolations()
}
