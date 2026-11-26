package simulator

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/spectest"
)

// goldenHashes pins named scenario/seed traces. An accidental determinism
// change fails this test in review; bump a pin only after intentional review.
var goldenHashes = map[string]uint64{
	"basic/17": 0x2b4c182054e51c2d,
	"chaos/42": 0xb16808383471a2d,
}

// goldenWithoutGroups are the pins of the same seeds with the rebalance actors
// and the consumers left out.
var goldenWithoutGroups = map[string]uint64{
	"basic/17": 0x253b9820ce5efc6a,
	"chaos/42": 0xf9140b51b5c0be1,
}

// TestGoldenWithoutGroups pins the run with the consumer groups left out.
func TestGoldenWithoutGroups(t *testing.T) {
	for _, c := range []struct {
		scenario string
		seed     uint64
	}{{"basic", 17}, {"chaos", 42}} {
		name := fmt.Sprintf("%s/%d", c.scenario, c.seed)
		r := mustRun(t, Config{Scenario: c.scenario, Seed: c.seed, noGroups: true})
		if !r.OK() {
			t.Fatal(Report(r))
		}
		spectest.AssertHash(t, name, r.Hash, goldenWithoutGroups)
	}
}

// TestGoldenTraceHashes pins a few seeds so a silent determinism drift fails CI.
// TestSameSeedSameHash only compares a seed with itself.
func TestGoldenTraceHashes(t *testing.T) {
	cases := []struct {
		scenario string
		seed     uint64
	}{
		{"basic", 17},
		{"chaos", 42},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%s/%d", c.scenario, c.seed)
		r := mustRun(t, Config{Scenario: c.scenario, Seed: c.seed})
		if !r.OK() {
			t.Fatal(Report(r))
		}
		spectest.AssertHash(t, name, r.Hash, goldenHashes)
	}
}
