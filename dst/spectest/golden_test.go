package spectest_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/manifesterio/mds/dst/spectest"
)

type fatalTB struct {
	testing.TB
	msg string
}

func (f *fatalTB) Helper() {}

func (f *fatalTB) Fatalf(format string, args ...any) {
	f.msg = fmt.Sprintf(format, args...)
}

func TestAssertHashAndCampaign(t *testing.T) {
	want := map[string]uint64{"kv": 0xabc}
	spectest.AssertHash(t, "kv", 0xabc, want)
	tb := &fatalTB{}
	spectest.AssertHash(tb, "kv", 0xdef, want)
	if tb.msg == "" {
		t.Fatal("want Assert to fail on mismatch")
	}

	got := map[uint64]uint64{1: 0x11, 2: 0x22}
	path := filepath.Join(t.TempDir(), "golden.txt")
	if err := spectest.SaveHashes(path, got); err != nil {
		t.Fatal(err)
	}
	loaded, err := spectest.LoadHashes(path)
	if err != nil {
		t.Fatal(err)
	}
	spectest.AssertCampaign(t, loaded, got)
}
