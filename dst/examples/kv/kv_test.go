package kv

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/spectest"
)

// script puts two keys, reads both, overwrites one and reads it again.
var script = []Step{
	{Key: "a", Value: "1", IsPut: true},
	{Key: "b", Value: "2", IsPut: true},
	{Key: "a", Value: "1"},
	{Key: "b", Value: "2"},
	{Key: "a", Value: "3", IsPut: true},
	{Key: "a", Value: "3"},
	{Key: "c", Value: ""}, // never written: reads the empty value
}

// reads is how many gets the script has.
const reads = 4

func TestDeterministic(t *testing.T) {
	a, b := Run(42, script), Run(42, script)
	if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
		t.Fatalf("seed 42 drifted: %x/%d/%d vs %x/%d/%d", a.Hash, a.RandCalls, a.Steps, b.Hash, b.RandCalls, b.Steps)
	}
	if a.Steps == 0 || !a.OK() {
		t.Fatalf("steps=%d failures=%+v", a.Steps, a.Failures)
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	a, b := Run(1, script), Run(2, script)
	if a.Hash == b.Hash && a.RandCalls == b.RandCalls {
		t.Fatal("two seeds gave the same result")
	}
}

// TestCampaign runs many seeds, each one twice. It fails on a failure, on a
// read that was not checked, or on a seed that does not repeat.
func TestCampaign(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		a := Run(seed, script)
		if !a.OK() || a.Steps == 0 {
			t.Fatalf("seed %d: steps=%d failures=%+v", seed, a.Steps, a.Failures)
		}
		if c := a.Coverage[readSeesLastWrite.Name()]; c.Checks != reads || c.Passes != reads {
			t.Fatalf("seed %d: read check %+v", seed, c)
		}
		b := Run(seed, script)
		if a.Hash != b.Hash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d does not repeat", seed)
		}
	}
}

// TestCatchesLostWrites plants a store that drops every put: the client's
// check must fail on the first seed.
func TestCatchesLostWrites(t *testing.T) {
	res := run(1, script, &store{values: map[string]string{}, loseWrites: true})
	for _, f := range res.Failures {
		if f.Property == readSeesLastWrite.Name() {
			return
		}
	}
	t.Fatalf("a store that loses writes was not caught: %+v", res.Failures)
}

// goldenHashes pin the trace hash of seeds 1 to 3. A change here means the
// runtime or the example changed behavior.
var goldenHashes = map[string]uint64{
	"kv/1": 0x7870bc2eabec2780,
	"kv/2": 0x2d73a4b4ab803953,
	"kv/3": 0x18f596f0dc051a0d,
}

func TestGoldenHashes(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		spectest.AssertHash(t, fmt.Sprintf("kv/%d", seed), Run(seed, script).Hash, goldenHashes)
	}
}

func ExampleRun() {
	res := Run(42, script)
	fmt.Println(res.OK(), res.Steps > 0)
	// Output:
	// true true
}
