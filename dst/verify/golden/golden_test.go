package golden_test

import (
	"fmt"
	"path/filepath"
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

func TestAssertAndCampaign(t *testing.T) {
	want := map[string]uint64{"kv": 0xabc}
	golden.Assert(t, "kv", 0xabc, want)
	tb := &fatalTB{}
	golden.Assert(tb, "kv", 0xdef, want)
	if tb.msg == "" {
		t.Fatal("want Assert to fail on mismatch")
	}

	got := map[uint64]uint64{1: 0x11, 2: 0x22}
	path := filepath.Join(t.TempDir(), "golden.txt")
	if err := golden.Save(path, got); err != nil {
		t.Fatal(err)
	}
	loaded, err := golden.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	golden.AssertCampaign(t, loaded, got)
}
