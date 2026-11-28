package kafka

import (
	"flag"
	"testing"

	"github.com/manifesterio/mds/dst/spectest"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/golden_hashes.txt from the current code")

const goldenFile = "testdata/golden_hashes.txt"

// goldenSeeds is how many seeds, from seed 1, are pinned.
const goldenSeeds = 5

// goldenHashes runs the pinned seeds of the kafka scenario and returns seed to hash.
func goldenHashes() map[uint64]uint64 {
	got := make(map[uint64]uint64, goldenSeeds)
	for seed := uint64(1); seed <= goldenSeeds; seed++ {
		got[seed] = Run(Config{Seed: seed}).Hash
	}
	return got
}

// TestGoldenTraceHashes pins the trace of a few seeds. TestDeterminismCanary
// only compares a seed with itself, so it cannot see a change in behaviour.
// This test can. When a change is meant to alter the traces, review why, then
// run: go test ./internal/verify/simulator/kafka -run TestGoldenTraceHashes -update-golden
func TestGoldenTraceHashes(t *testing.T) {
	got := goldenHashes()
	if *updateGolden {
		if err := spectest.SaveHashes(goldenFile, got); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := spectest.LoadHashes(goldenFile)
	if err != nil {
		t.Fatalf("%v (create it with -update-golden)", err)
	}
	spectest.AssertCampaign(t, got, want)
}
