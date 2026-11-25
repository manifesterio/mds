package sim_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/rng"
	"github.com/manifesterio/mds/dst/sim"
)

func TestTimelinePhaseStartAndFacts(t *testing.T) {
	t.Parallel()
	tl := sim.NewTimeline()
	tl.Append(actor.Incarnation{}, 10, actor.Fact{
		Name:   "sim_phase",
		Fields: []actor.Field{{Key: "phase", Value: actor.Uint64Value(uint64(sim.PhaseHeal))}},
	})
	at, ok := tl.PhaseStart(sim.PhaseHeal)
	if !ok || at != 10 {
		t.Fatalf("PhaseStart Heal = %d, %v", at, ok)
	}
	if _, ok := tl.PhaseStart(sim.PhaseDrain); ok {
		t.Fatal("PhaseStart Drain unexpectedly found")
	}

	tl.Append(actor.Incarnation{}, 1, actor.Fact{
		Name:   "sim_crashed",
		Fields: []actor.Field{{Key: "cause", Value: actor.StringValue("attrition")}},
	})
	tl.Append(actor.Incarnation{}, 2, actor.Fact{Name: "sim_exited"})
	if !tl.HasCrashCause("attrition") || tl.HasCrashCause("other") {
		t.Fatal("HasCrashCause")
	}
	if !tl.HasExited() {
		t.Fatal("HasExited")
	}
}

type stubAssert struct{ sometimes []string }

func (a *stubAssert) Always(bool, assert.Always, ...assert.Pair)                           {}
func (a *stubAssert) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (a *stubAssert) Sometimes(cond bool, p assert.Sometimes) {
	if cond {
		a.sometimes = append(a.sometimes, p.Name())
	}
}
func (a *stubAssert) Reachable(assert.Reachable)                     {}
func (a *stubAssert) Unreachable(assert.Unreachable, ...assert.Pair) {}

type stubView struct {
	tl *sim.Timeline
	as actor.Assertions
}

func (v stubView) Step() uint64             { return 0 }
func (v stubView) Now() sim.Time            { return 0 }
func (v stubView) Phase() sim.Phase         { return sim.PhaseFaults }
func (v stubView) Timeline() *sim.Timeline  { return v.tl }
func (v stubView) Assert() actor.Assertions { return v.as }

func TestFaultCoverageRule(t *testing.T) {
	t.Parallel()
	names := sim.FaultCoverage{
		HardCrash:    assert.NewSometimes("hard"),
		GracefulStop: assert.NewSometimes("grace"),
	}
	rule := sim.FaultCoverageRule(names)
	if rule.Name() != "FaultCoverage" {
		t.Fatalf("Name = %q", rule.Name())
	}
	tl := sim.NewTimeline()
	tl.Append(actor.Incarnation{}, 1, actor.Fact{
		Name:   "sim_crashed",
		Fields: []actor.Field{{Key: "cause", Value: actor.StringValue("attrition")}},
	})
	tl.Append(actor.Incarnation{}, 2, actor.Fact{Name: "sim_exited"})
	as := &stubAssert{}
	rule.AtEnd(stubView{tl: tl, as: as})
	if len(as.sometimes) != 2 {
		t.Fatalf("Sometimes props = %v", as.sometimes)
	}
}

func TestRecordFaultAndBootstrap(t *testing.T) {
	t.Parallel()
	sys, env, r := sim.Bootstrap(sim.Config{Seed: 1})
	if env == nil || r == nil {
		t.Fatal("Bootstrap returned nil Env or Rand")
	}
	sys.RecordFault("host_fault", actor.Field{Key: "service", Value: actor.StringValue("net")})
	faults := sys.Timeline().Snapshot("sim_fault")
	if len(faults) != 1 {
		t.Fatalf("%d sim_fault facts", len(faults))
	}
	if k, _ := faults[0].Fact.Field("kind").(actor.StringValue); string(k) != "host_fault" {
		t.Fatalf("kind = %v", k)
	}
}

func TestQuiesceEveryAndQuietElapsed(t *testing.T) {
	t.Parallel()
	if !sim.QuietElapsed(5, 3, 10, true) || sim.QuietElapsed(2, 3, 10, true) {
		t.Fatal("QuietElapsed ready-before-min")
	}
	if !sim.QuietElapsed(10, 3, 10, false) {
		t.Fatal("QuietElapsed max")
	}

	sys := sim.NewSystem(sim.Config{Seed: 2, End: sim.Time(2*actor.Second + 1)})
	n := 0
	sys.QuiesceEvery(actor.Second, func() bool {
		n++
		return n >= 2
	})
	res := sys.Run()
	quiet := sys.Timeline().Snapshot("sim_quiesce")
	if len(quiet) != 2 {
		t.Fatalf("%d quiesce facts (result steps=%d)", len(quiet), res.Steps)
	}
	if v, _ := quiet[0].Fact.Field("quiescent").(actor.Bool); bool(v) {
		t.Fatalf("first quiesce unexpectedly quiescent: %+v", quiet[0])
	}
	if v, _ := quiet[1].Fact.Field("quiescent").(actor.Bool); !bool(v) {
		t.Fatalf("second quiesce not quiescent: %+v", quiet[1])
	}
}

func TestAskDelayed(t *testing.T) {
	t.Parallel()
	w := newWorld(11)
	server := w.spawn("server", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if a, ok := m.(asked); ok {
			actor.Reply(o, a.ReplyTo, answer{V: "pong"})
		}
	})

	var gotAns answer
	var okDone bool
	var backhaulDraws int
	sim.AskDelayed(w.sys, server, 5*ms,
		func(to actor.ReplyTo[answer]) msg {
			return asked{Q: "ping", ReplyTo: to}
		},
		actor.Second,
		func(a answer) actor.Duration {
			backhaulDraws++
			gotAns = a
			return 3 * ms
		},
		func(a answer, ok bool) {
			okDone = ok
			gotAns = a
		},
	)
	mustOK(t, w.sys.Run())
	if backhaulDraws != 1 || !okDone || gotAns.V != "pong" {
		t.Fatalf("backhaul=%d ok=%v ans=%+v", backhaulDraws, okDone, gotAns)
	}
}

func TestDrawBetweenMaybeAdd(t *testing.T) {
	t.Parallel()
	r := rng.New(3)
	d := sim.DrawBetween(r, actor.Millisecond, 2*actor.Millisecond)
	if d < actor.Millisecond || d > 2*actor.Millisecond {
		t.Fatalf("DrawBetween = %d", d)
	}
	if sim.MaybeAdd(rng.New(1), 0, actor.Second, 2*actor.Second) != 0 {
		t.Fatal("MaybeAdd p=0")
	}
}
