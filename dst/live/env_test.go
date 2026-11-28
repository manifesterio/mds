package live_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
)

var (
	envAlways      = actor.NewAlways("live env test: always")
	envUnreachable = actor.NewUnreachable("live env test: unreachable")
	envSite        = actor.NewSite("live env test: site")
)

func TestNewEnvBetweenAndChance(t *testing.T) {
	e := live.NewEnv(actor.Incarnation{Name: "a"})
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

func TestLogViolationsWires(t *testing.T) {
	var buf bytes.Buffer
	e := live.NewEnv(actor.Incarnation{Name: "broker"}, live.LogViolations(slog.New(slog.NewJSONHandler(&buf, nil))))
	e.Always(false, envAlways, actor.Pair{Key: "k", Value: 7})
	e.Unreachable(envUnreachable)
	out := buf.String()
	for _, want := range []string{`"msg":"assertion violated"`, `"kind":"always"`, `"k":7`, `"kind":"unreachable"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q\n%s", want, out)
		}
	}
}

func TestOnViolationAndInertEnv(t *testing.T) {
	var props, details []string
	e := live.NewEnv(actor.Incarnation{}, live.OnViolation(func(p, d string) {
		props, details = append(props, p), append(details, d)
	}))
	e.Always(true, envAlways)
	e.Always(false, envAlways, actor.Pair{Key: "k", Value: 7})
	e.Unreachable(envUnreachable)
	if len(props) != 2 || props[0] != envAlways.Name() || props[1] != envUnreachable.Name() || details[0] != "[{k 7}]" {
		t.Fatalf("violations %v %v", props, details)
	}
	quiet := live.NewEnv(actor.Incarnation{}) // no option: ignored
	quiet.Always(false, envAlways)
	quiet.Unreachable(envUnreachable)
}
