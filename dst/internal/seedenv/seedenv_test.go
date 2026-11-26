package seedenv

import (
	"strings"
	"testing"
	"time"
)

func env(pairs ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return func(k string) string { return m[k] }
}

func TestDescribeNamesAllVariables(t *testing.T) {
	d := Describe("XY")
	for _, name := range []string{"XY_SEED", "XY_SEEDS", "XY_CAMPAIGN"} {
		if !strings.Contains(d, name) {
			t.Fatalf("the help text does not name %s: %q", name, d)
		}
	}
}

func TestRead(t *testing.T) {
	got, err := Read("MDS", env())
	if err != nil || got != (Settings{}) {
		t.Fatalf("nothing set: %+v err %v", got, err)
	}
	got, err = Read("MDS", env("MDS_SEEDS", "40", "MDS_SEED", "0", "MDS_CAMPAIGN", "30s", "OTHER_SEEDS", "9"))
	want := Settings{Seeds: 40, HasSeed: true, Seed: 0, Duration: 30 * time.Second}
	if err != nil || got != want {
		t.Fatalf("all set: %+v err %v", got, err)
	}
	got, err = Read("MDS", env("MDS_SEEDS", "", "MDS_SEED", "", "MDS_CAMPAIGN", ""))
	if err != nil || got != (Settings{}) {
		t.Fatalf("empty values are not set: %+v err %v", got, err)
	}
	if _, err = Read("MDS", env("MDS_SEED", "-1")); err == nil || !strings.Contains(err.Error(), "MDS_SEED has the value") {
		t.Fatalf("bad seed: %v", err)
	}
}
