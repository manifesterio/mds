package campaign_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/dst/explore/campaign"
	"github.com/manifesterio/mds/dst/sim"
)

func TestRunFoldsHashesInSeedOrder(t *testing.T) {
	const first, seeds = 10, 5
	want := trace.New()
	for i := range seeds {
		seed := first + uint64(i)
		want.Uint64(seed * 1000)
	}

	got := campaign.RunSeeds(first, seeds, 4, func(seed uint64) campaign.Outcome {
		// Finish later seeds first so a buggy fold would scramble order.
		time.Sleep(time.Duration(seeds-int(seed-first)) * time.Millisecond)
		return campaign.Outcome{Result: sim.Result{Hash: seed * 1000}}
	})

	if got.Hash != want.Sum() {
		t.Fatalf("fold order: got %#x want %#x", got.Hash, want.Sum())
	}
	if got.Counts != (campaign.Counts{Seeds: seeds, OK: seeds}) {
		t.Fatalf("counts: %+v", got.Counts)
	}
	if len(got.Failed) != 0 {
		t.Fatalf("failed: %+v", got.Failed)
	}
}

func TestRunCollectsFailuresAndMergesCoverage(t *testing.T) {
	got := campaign.RunSeeds(1, 4, 2, func(seed uint64) campaign.Outcome {
		r := sim.Result{
			Hash: seed,
			Coverage: map[string]assert.Coverage{
				"path": {Kind: assert.KindReachable, Checks: 1, Passes: seed},
			},
		}
		if seed%2 != 0 {
			r.Failures = []sim.Failure{{Property: "p"}}
		}
		return campaign.Outcome{Result: r}
	})

	if got.Counts.Failed != 2 || got.Counts.OK != 2 {
		t.Fatalf("counts: %+v", got.Counts)
	}
	if len(got.Failed) != 2 {
		t.Fatalf("failed len=%d", len(got.Failed))
	}
	if got.Failed[0].Seed != 1 || got.Failed[1].Seed != 3 {
		t.Fatalf("failed seed order: %+v", got.Failed)
	}
	c := got.Coverage["path"]
	if c.Kind != assert.KindReachable || c.Checks != 4 || c.Passes != 1+2+3+4 {
		t.Fatalf("coverage: %+v", c)
	}
}

func TestRunFoldsSitesAndNeverFired(t *testing.T) {
	run := func(first uint64) campaign.Summary {
		return campaign.RunSeeds(first, 4, 2, func(seed uint64) campaign.Outcome {
			sites := map[string]fault.SiteCount{
				"fires": {On: seed%2 == 0, Calls: 10},
				"quiet": {On: true, Calls: 5},
			}
			if seed == 2 {
				sites["fires"] = fault.SiteCount{On: true, Calls: 10, Fired: 3}
			}
			return campaign.Outcome{Result: sim.Result{Sites: sites}}
		})
	}
	var total campaign.Summary
	total.Merge(run(1))
	total.Merge(run(1))
	f := total.Sites["fires"]
	if f != (campaign.SiteTotal{Reached: 8, On: 4, Calls: 80, Fired: 6}) {
		t.Fatalf("fires: %+v", f)
	}
	if q := total.Sites["quiet"]; q != (campaign.SiteTotal{Reached: 8, On: 8, Calls: 40}) {
		t.Fatalf("quiet: %+v", q)
	}
	got := campaign.NeverFired(total.Sites, []string{"gone", "fires", "quiet"})
	if len(got) != 2 || got[0] != "gone" || got[1] != "quiet" {
		t.Fatalf("never fired: %v", got)
	}
}

func TestRunZeroSeeds(t *testing.T) {
	var calls atomic.Int32
	got := campaign.RunSeeds(1, 0, 2, func(uint64) campaign.Outcome {
		calls.Add(1)
		return campaign.Outcome{}
	})
	if calls.Load() != 0 {
		t.Fatalf("ran %d times", calls.Load())
	}
	if got.Counts.Seeds != 0 || got.Hash != 0 {
		t.Fatalf("summary: %+v", got)
	}
}

func TestRunStampsSeedOnFailedResults(t *testing.T) {
	got := campaign.RunSeeds(7, 3, 1, func(seed uint64) campaign.Outcome {
		r := sim.Result{Hash: seed}
		if seed == 8 {
			r.Failures = []sim.Failure{{Property: "p"}}
		}
		return campaign.Outcome{Result: r}
	})
	if len(got.Failed) != 1 || got.Failed[0].Seed != 8 {
		t.Fatalf("seed stamp: %+v", got.Failed)
	}
}

func TestRunSumsCountersAndMergeAndMissing(t *testing.T) {
	run := func(first uint64) campaign.Summary {
		return campaign.RunSeeds(first, 3, 2, func(seed uint64) campaign.Outcome {
			return campaign.Outcome{
				Result:   sim.Result{Steps: 10, Coverage: map[string]assert.Coverage{"a": {Kind: assert.KindReachable, Checks: 1, Passes: 1}}},
				Counters: map[string]uint64{"writes": 1},
			}
		})
	}
	var total campaign.Summary
	total.Merge(run(1))
	total.Merge(run(10))
	if total.Counters["steps"] != 60 || total.Counters["writes"] != 6 || total.Coverage["a"].Passes != 6 {
		t.Fatalf("merge: %+v %+v", total.Counters, total.Coverage)
	}
	got := campaign.Missing(total.Coverage, []string{"z", "a", "b"})
	if len(got) != 2 || got[0] != "b" || got[1] != "z" {
		t.Fatalf("missing: %v", got)
	}
}
