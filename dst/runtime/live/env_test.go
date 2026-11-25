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
	envUnreachable = assert.NewUnreachable("live env test: unreachable")
	envSite        = fault.NewSite("live env test: site")
)

func TestNewEnvBetweenAndChance(t *testing.T) {
	e := live.NewEnv(actor.Incarnation{Name: "a"}, nil)
	if e.Self().Name != "a" {
		t.Fatalf("Self = %+v", e.Self())
	}
	_ = e.Between(-1<<63, 1<<63-1) // full int64 range must not divide by zero
	if e.Between(4, 4) != 4 || e.Between(9, 3) != 9 {
		t.Fatal("Between with hi <= lo must give lo")
	}
	if e.Chance(0) || e.Chance(-1) {
		t.Fatal("Chance(p <= 0) must be false")
	}
	for range 20 {
		if !e.Chance(1) {
			t.Fatal("Chance(1) must be true")
		}
	}
	if e.Maybe(envSite) || e.MaybeP(envSite, 1) || e.Fault(envSite, 1) {
		t.Fatal("production Env must not fire faults")
	}
}

func TestNewLoggingEnvWires(t *testing.T) {
	var buf bytes.Buffer
	e := live.NewLoggingEnv(actor.Incarnation{Name: "broker"}, slog.New(slog.NewJSONHandler(&buf, nil)))
	e.Always(false, envAlways, assert.Pair{Key: "k", Value: 7})
	e.Unreachable(envUnreachable)
	out := buf.String()
	for _, want := range []string{`"msg":"assertion violated"`, `"kind":"always"`, `"k":7`, `"kind":"unreachable"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q\n%s", want, out)
		}
	}
}

func TestNewCounterEnv(t *testing.T) {
	e := live.NewCounterEnv(actor.Incarnation{Name: "replica"})
	if e.Uint64() != 1 || e.Uint64() != 2 || e.Uint64() != 3 {
		t.Fatal("Uint64 must count 1, 2, 3, ...")
	}
	if e.Intn(3) != 1 { // next draw is 4; 4%3 = 1
		t.Fatal("Intn must use the counter")
	}
	e2 := live.NewCounterEnv(actor.Incarnation{})
	if v := e2.Between(10, 12); v != 11 { // first draw 1 -> 1%3 = 1 -> 11
		t.Fatalf("Between(10, 12) = %d, want 11", v)
	}
	if e2.Chance(0) || !live.NewCounterEnv(actor.Incarnation{}).Chance(1) {
		t.Fatal("Chance edges")
	}
	e.Always(false, envAlways) // inert
}
