package live_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
	"github.com/manifesterio/mds/dst/runtime/live"
)

var (
	envAlways      = assert.NewAlways("live env test: always")
	envOrUnreached = assert.NewAlwaysOrUnreachable("live env test: always or unreachable")
	envSometimes   = assert.NewSometimes("live env test: sometimes")
	envReachable   = assert.NewReachable("live env test: reachable")
	envUnreachable = assert.NewUnreachable("live env test: unreachable")
	envSite        = fault.NewSite("live env test: site")
)

func TestNewEnvIntnBetweenChance(t *testing.T) {
	e := live.NewEnv(actor.Incarnation{Name: "a"}, nil)
	checkChaChaRanges(t, e)
	if e.Self().Name != "a" {
		t.Fatalf("Self = %+v", e.Self())
	}
}

func TestNewLoggingEnvIntnBetweenChance(t *testing.T) {
	e := live.NewLoggingEnv(actor.Incarnation{Name: "broker"}, nil)
	checkChaChaRanges(t, e)
	if e.Self().Name != "broker" {
		t.Fatalf("Self = %+v", e.Self())
	}
}

func checkChaChaRanges(t *testing.T, e actor.Env) {
	t.Helper()
	if e.Intn(0) != 0 || e.Intn(-3) != 0 {
		t.Fatal("Intn with n <= 0 must give 0")
	}
	seen := map[int64]bool{}
	for range 500 {
		if n := e.Intn(5); n < 0 || n >= 5 {
			t.Fatalf("Intn(5) = %d", n)
		}
		v := e.Between(-2, 2)
		if v < -2 || v > 2 {
			t.Fatalf("Between(-2, 2) = %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 5 {
		t.Fatalf("Between(-2, 2) gave %d different values, want both ends included", len(seen))
	}
	if e.Between(4, 4) != 4 || e.Between(9, 3) != 9 {
		t.Fatal("Between with hi <= lo must give lo")
	}
	_ = e.Between(-1<<63, 1<<63-1) // the whole range must not divide by zero
	for range 100 {
		if d := e.Duration(3, 5); d < 3 || d > 5 {
			t.Fatalf("Duration(3, 5) = %d", d)
		}
	}
	if e.Chance(0) || e.Chance(-1) {
		t.Fatal("Chance(p <= 0) must be false")
	}
	for range 100 {
		if !e.Chance(1) {
			t.Fatal("Chance(1) must be true")
		}
	}
	hits := 0
	for range 2000 {
		if e.Chance(0.5) {
			hits++
		}
	}
	if hits < 800 || hits > 1200 {
		t.Fatalf("Chance(0.5) hit %d of 2000", hits)
	}
}

func TestNewEnvOnFail(t *testing.T) {
	var got []string
	e := live.NewEnv(actor.Incarnation{Name: "a"}, func(prop, detail string) {
		got = append(got, prop+"|"+detail)
	})
	e.Always(true, envAlways)
	e.Always(false, envAlways, assert.Pair{Key: "k", Value: 1})
	e.AlwaysOrUnreachable(false, envOrUnreached)
	e.Unreachable(envUnreachable, assert.Pair{Key: "x", Value: "y"})
	if len(got) != 3 {
		t.Fatalf("onFail calls = %v", got)
	}
	if !strings.Contains(got[0], envAlways.Name()) || !strings.Contains(got[0], "k") {
		t.Fatalf("Always fail = %q", got[0])
	}
	if !strings.Contains(got[1], envOrUnreached.Name()) {
		t.Fatalf("AlwaysOrUnreachable fail = %q", got[1])
	}
	if !strings.Contains(got[2], envUnreachable.Name()) {
		t.Fatalf("Unreachable fail = %q", got[2])
	}
}

func TestNewLoggingEnvLogsViolations(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	e := live.NewLoggingEnv(actor.Incarnation{Name: "broker"}, log)
	e.Always(true, envAlways)
	e.Always(false, envAlways, assert.Pair{Key: "k", Value: 7})
	e.AlwaysOrUnreachable(false, envOrUnreached)
	e.Unreachable(envUnreachable, assert.Pair{Key: "x", Value: "y"})
	out := buf.String()
	for _, want := range []string{
		`"msg":"assertion violated"`,
		`"kind":"always"`,
		`"prop":"` + envAlways.Name() + `"`,
		`"k":7`,
		`"kind":"always_or_unreachable"`,
		`"prop":"` + envOrUnreached.Name() + `"`,
		`"kind":"unreachable"`,
		`"prop":"` + envUnreachable.Name() + `"`,
		`"x":"y"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q\n%s", want, out)
		}
	}
}

func TestNewCounterEnvSequence(t *testing.T) {
	e := live.NewCounterEnv(actor.Incarnation{Name: "replica"})
	if e.Self().Name != "replica" {
		t.Fatalf("Self = %+v", e.Self())
	}
	if e.Uint64() != 1 || e.Uint64() != 2 || e.Uint64() != 3 {
		t.Fatal("Uint64 must count 1, 2, 3, ...")
	}
	// next draw is 4; Intn(3) -> 4%3 = 1
	if e.Intn(3) != 1 {
		t.Fatal("Intn must use the counter")
	}
	if e.Intn(0) != 0 || e.Intn(-1) != 0 {
		t.Fatal("Intn with n <= 0 must give 0")
	}
	if e.Between(5, 5) != 5 || e.Between(9, 3) != 9 {
		t.Fatal("Between with hi <= lo must give lo")
	}
	// fresh env: Between(10,12) -> lo + Intn(3); first draw 1 -> 1%3 = 1 -> 11
	e2 := live.NewCounterEnv(actor.Incarnation{})
	if v := e2.Between(10, 12); v != 11 {
		t.Fatalf("Between(10, 12) = %d, want 11", v)
	}
	id := live.NewCounterEnv(actor.Incarnation{}).NewID()
	if id[0] != 1 {
		t.Fatalf("NewID first byte = %d, want 1", id[0])
	}
}

func TestNewCounterEnvChance(t *testing.T) {
	e := live.NewCounterEnv(actor.Incarnation{})
	if e.Chance(0) || e.Chance(-1) {
		t.Fatal("Chance(p <= 0) must be false")
	}
	// draws 1: 1%1000=1 < 1000 -> true for p=1
	if !e.Chance(1) {
		t.Fatal("Chance(1) must be true")
	}
}

func TestProductionEnvsInertFaults(t *testing.T) {
	envs := []actor.Env{
		live.NewEnv(actor.Incarnation{}, nil),
		live.NewLoggingEnv(actor.Incarnation{}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))),
		live.NewCounterEnv(actor.Incarnation{}),
	}
	for _, e := range envs {
		if e.Maybe(envSite) || e.MaybeP(envSite, 1) || e.Fault(envSite, 1) {
			t.Fatal("production Env must not fire faults")
		}
		e.Sometimes(true, envSometimes)
		e.Reachable(envReachable)
	}
	// Counter ignores assert failures.
	c := live.NewCounterEnv(actor.Incarnation{})
	c.Always(false, envAlways)
	c.Unreachable(envUnreachable)
}
