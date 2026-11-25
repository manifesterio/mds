package golden_test

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/verify/golden"
)

type fatalTB struct {
	testing.TB
	msg string
}

func (f *fatalTB) Helper() {}

func (f *fatalTB) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
}

func TestAssert(t *testing.T) {
	want := map[string]uint64{"kv": 0xabc}
	golden.Assert(t, "kv", 0xabc, want)
}

func TestAssertMismatch(t *testing.T) {
	want := map[string]uint64{"kv": 0xabc}
	tb := &fatalTB{}
	golden.Assert(tb, "kv", 0xdef, want)
	if tb.msg == "" {
		t.Fatal("want Assert to fail on mismatch")
	}
}

func TestAssertCampaignAndFile(t *testing.T) {
	got := map[uint64]uint64{1: 0x11, 2: 0x22}
	want := map[uint64]uint64{1: 0x11, 2: 0x22}
	golden.AssertCampaign(t, got, want)

	path := filepath.Join(t.TempDir(), "golden.txt")
	if err := golden.Save(path, got); err != nil {
		t.Fatal(err)
	}
	loaded, err := golden.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertCampaign(t, loaded, want)

	bad := map[uint64]uint64{1: 0x11, 3: 0x33}
	tb := &fatalTB{}
	golden.AssertCampaign(tb, bad, want)
	if tb.msg == "" {
		t.Fatal("want AssertCampaign to fail")
	}
}

func TestRecordRoundTripAndAssertFile(t *testing.T) {
	records := []golden.Record{
		{Scenario: "kafka", Seed: 1, Steps: 10, Rand: 20, Hash: 0x6a2453eb4022c76f},
		{Scenario: "kafka", Seed: 2, Steps: 11, Rand: 21, Hash: 0x6ddaef7601925330},
	}
	line := golden.FormatRecord(records[0])
	wantLine := "kafka seed=1 steps=10 rand=20 hash=6a2453eb4022c76f"
	if line != wantLine {
		t.Fatalf("format %q want %q", line, wantLine)
	}
	parsed, err := golden.ParseRecord(line)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != records[0] {
		t.Fatalf("parse %+v want %+v", parsed, records[0])
	}

	path := filepath.Join(t.TempDir(), "golden_hashes.txt")
	if err := golden.SaveRecords(path, records); err != nil {
		t.Fatal(err)
	}
	golden.AssertFile(t, path, records, false)

	tb := &fatalTB{TB: t}
	bad := slices.Clone(records)
	bad[0].Hash = 0xdead
	golden.AssertFile(tb, path, bad, false)
	if tb.msg == "" {
		t.Fatal("want AssertFile to fail on mismatch")
	}

	golden.AssertFile(t, path, bad, true)
	golden.AssertFile(t, path, bad, false)
}
