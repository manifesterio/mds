package sim_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/sim"
)

func TestBootstrapRecordFaultQuiesce(t *testing.T) {
	t.Parallel()
	sys, env, r := sim.Bootstrap(sim.Config{Seed: 1, End: sim.Time(2*actor.Second + 1)})
	if env == nil || r == nil {
		t.Fatal("Bootstrap nil Env/Rand")
	}
	sys.RecordFault("host_fault")
	if len(sys.Timeline().Snapshot("sim_fault")) != 1 {
		t.Fatal("sim_fault missing")
	}
	sys.SetPhase(sim.PhaseHeal)
	if at, ok := sys.Timeline().PhaseStart(sim.PhaseHeal); !ok || at != sys.Now() {
		t.Fatalf("PhaseStart = %d,%v now=%d", at, ok, sys.Now())
	}
	n := 0
	sys.QuiesceEvery(actor.Second, func() bool { n++; return n >= 2 })
	_ = sys.Run()
	if len(sys.Timeline().Snapshot("sim_quiesce")) != 2 {
		t.Fatal("quiesce ticks")
	}
	if !sim.QuietElapsed(5, 3, 10, true) || sim.QuietElapsed(2, 3, 10, true) {
		t.Fatal("QuietElapsed")
	}
}

func TestEveryWhile(t *testing.T) {
	t.Parallel()
	sys, _, _ := sim.Bootstrap(sim.Config{Seed: 2, End: sim.Time(10 * actor.Second)})
	n := 0
	sim.EveryWhile(sys, actor.Second, func() bool { return n < 3 }, func() { n++ })
	sim.EveryWhile(sys, 0, func() bool { return true }, func() { t.Fatal("mean 0") })
	_ = sys.Run()
	if n != 3 {
		t.Fatalf("ticks = %d", n)
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
	var got answer
	var okDone bool
	sim.AskDelayed(w.sys, server, 5*ms,
		func(to actor.ReplyTo[answer]) msg { return asked{Q: "ping", ReplyTo: to} },
		actor.Second, nil, func(a answer, ok bool) { okDone, got = ok, a },
	)
	mustOK(t, w.sys.Run())
	if !okDone || got.V != "pong" {
		t.Fatalf("ok=%v ans=%+v", okDone, got)
	}
}
