package fakeservices

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/faulty"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/actor/actortest"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/shared/objectstore"
)

const ms = actor.Duration(1_000_000)

// noisy is a mix of timing and fault chances that gives refusals, timeouts and
// lost replies often.
func noisy() faulty.Faults {
	ts := sim.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return faulty.Faults{ApplyLatency: ts, RespondLatency: ts, Unavailable503: 0.1, Timeout: 0.1, LostResponse: 0.1, CallerTimeout: 1000 * ms}
}

// These two tests keep the old guard against a stale cached spec hash, now for
// the two specs behind Faulty: the hooks Mutating and Changed (and Bump) must
// clear the cache on every path that can change the spec. The generic test of
// the cache is in broker/internal/verify/simulator/faulty.

func TestSpecHashCacheObjectStore(t *testing.T) {
	h := servicetest.New(t, 5, "a", "b", "c")
	e := NewObjectStore(ObjectStoreFaults{Faults: noisy()})
	h.Objects = sim.AddService[core.ObjectCall, core.ObjectDone](h.Sys, e.Name(), e)
	r := actortest.NewEnv(actortest.Config{Seed: 99, Stream: true})
	for i := range 400 {
		h.RunFor(ms)
		k := string(rune('a' + r.Between(0, 5)))
		request := uint64(i)
		switch r.Between(0, 3) {
		case 0, 1:
			h.Issue("a", request, core.ObjectCall{Call: objectstore.Put{Key: k, Data: []byte{byte(i)}}})
		case 2:
			h.Issue("a", request, core.ObjectCall{Call: objectstore.Delete{Key: k}})
		default:
			h.Issue("a", request, core.ObjectCall{Call: objectstore.Get{Ref: core.SegmentRef{Key: k}}})
		}
		if r.Chance(0.3) {
			h.Run()
		}
		if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	h.Run()
	if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
		t.Fatal("final: cached hash stale")
	}
}

func TestSpecHashCacheControlPlane(t *testing.T) {
	h := servicetest.New(t, 6, "a", "b", "c")
	e := NewControlPlane(ControlPlaneFaults{Faults: noisy()})
	h.ControlPlane = sim.AddService[core.ControlPlaneCall, core.ControlPlaneDone](h.Sys, e.Name(), e)
	r := actortest.NewEnv(actortest.Config{Seed: 77, Stream: true})
	nodes := []string{"a", "b", "c"}
	tps := []core.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("t", 1)}
	if e.CreateTopic(tps[0].Topic, 2) != core.OK {
		t.Fatal("create topic")
	}
	if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
		t.Fatal("after CreateTopic: stale")
	}
	for i := range 400 {
		h.RunFor(ms)
		n := nodes[r.Between(0, 2)]
		request := uint64(i)
		switch r.Between(0, 1) {
		case 0:
			// Keys repeat (replays, and conflicts when the request differs).
			h.Issue(n, request, core.ControlPlaneCall{Op: contract.CommitSegment{
				SegmentKey: string(rune('a' + r.Between(0, 7))),
				Batches:    []contract.PendingBatch{{TopicPartition: tps[r.Between(0, 1)], Records: 1 + r.Between(0, 2)}},
			}})
		default:
			h.Issue(n, request, core.ControlPlaneCall{Op: contract.FindSegments{TopicPartition: tps[0]}})
		}
		if r.Chance(0.3) {
			h.Run()
		}
		if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	h.Run()
	if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
		t.Fatal("final: cached hash stale")
	}
}
