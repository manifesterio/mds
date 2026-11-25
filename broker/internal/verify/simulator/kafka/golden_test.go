package kafka

import (
	"flag"
	"testing"

	"github.com/manifesterio/mds/dst/verify/golden"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/golden_hashes.txt from the current code")

const goldenFile = "testdata/golden_hashes.txt"

// goldenSeeds is how many seeds, from seed 1, are pinned.
const goldenSeeds = 5

// goldenRecords runs the pinned seeds of the kafka scenario and returns one
// record per seed.
func goldenRecords() []golden.Record {
	out := make([]golden.Record, 0, goldenSeeds)
	for seed := uint64(1); seed <= goldenSeeds; seed++ {
		r := Run(Config{Seed: seed})
		out = append(out, golden.Record{
			Scenario: "kafka",
			Seed:     seed,
			Steps:    r.Steps,
			Rand:     r.RandCalls,
			Hash:     r.Hash,
		})
	}
	return out
}

// TestGoldenTraceHashes pins the trace of a few seeds. TestDeterminismCanary
// only compares a seed with itself, so it cannot see a change in behaviour.
// This test can. When a change is meant to alter the traces, review why, then
// run: go test ./internal/verify/simulator/kafka -run TestGoldenTraceHashes -update-golden
func TestGoldenTraceHashes(t *testing.T) {
	golden.AssertFile(t, goldenFile, goldenRecords(), *updateGolden)
}
