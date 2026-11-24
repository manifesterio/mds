package sim_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/sim"
)

// store is a service: it keeps values and answers reads after a delay.
type store struct {
	values  map[string]string
	twice   bool
	handled int
}

func (s *store) StateHash() uint64 { return uint64(len(s.values)*1000 + s.handled) }

func (s *store) Handle(ctx *sim.ServiceCtx, req io, reply sim.Responder[readDone]) {
	s.handled++
	switch r := req.(type) {
	case write:
		s.values[r.Key] = "set"
		reply.After(ms, readDone{"ignored"}) // nobody waits: dropped
	case read:
		d := ctx.Duration(ms, 5*ms)
		reply.After(d, readDone{s.values[r.Key]})
		if s.twice {
			reply.After(d, readDone{"again"})
		}
	}
}

func storeRun(seed uint64, twice bool) (*world, sim.Result) {
	w := newWorld(seed)
	svc := sim.AddService[io, readDone](w.sys, "store", &store{values: map[string]string{}, twice: twice})
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			svc.Tell(o, write{"k"})
			actor.Call(o, svc.Request(read{"k"}), func(d readDone) msg { return got(d) })
			actor.Call(o, svc.Request(read{"missing"}), func(d readDone) msg { return got(d) })
		}
	})
	return w, w.sys.Run()
}

func TestServiceCallAndTell(t *testing.T) {
	w, r := storeRun(12, false)
	mustOK(t, r)
	got := w.lines("a")
	slices.Sort(got)
	if want := []string{"got{V:set}", "got{V:}", "start{}"}; !slices.Equal(got, want) {
		t.Errorf("a: %v", got)
	}
}

func TestServiceAnswersTwiceIsProtocolFailure(t *testing.T) {
	if _, r := storeRun(12, true); !hasFailure(r, "answered_twice") {
		t.Errorf("failures: %+v", r.Failures)
	}
}

func TestSameSeedSameHash(t *testing.T) {
	_, a := storeRun(13, false)
	_, b := storeRun(13, false)
	_, c := storeRun(14, false)
	if a.Hash != b.Hash || a.Steps != b.Steps || a.RandCalls != b.RandCalls {
		t.Fatalf("seed 13 twice: %x/%x", a.Hash, b.Hash)
	}
	if a.Hash == c.Hash {
		t.Errorf("seeds 13 and 14 gave the same hash")
	}
}

func TestProtocolFailures(t *testing.T) {
	w := newWorld(15)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			actor.EntityRef[msg]{}.Tell(o, note{"x"})
		}
	})
	if r := w.sys.Run(); !hasFailure(r, "zero address") {
		t.Errorf("zero ref: %+v", r.Failures)
	}

	w = newWorld(15)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.Do(write{"x"})
		}
	})
	if r := w.sys.Run(); !hasFailure(r, "no_executor") {
		t.Errorf("no executor: %+v", r.Failures)
	}

	other := newWorld(16)
	foreign := other.spawn("z", nil)
	w = newWorld(15)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			foreign.Tell(o, note{"x"})
		}
	})
	if r := w.sys.Run(); !hasFailure(r, "foreign address") {
		t.Errorf("foreign ref: %+v", r.Failures)
	}
}

func TestWorkloadMayNotStop(t *testing.T) {
	w := newWorld(17)
	w.spawn("c", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.Stop(actor.Exit, "")
		}
	}, sim.Workload())
	if r := w.sys.Run(); !hasFailure(r, "workload_ended") {
		t.Errorf("failures: %+v", r.Failures)
	}
}

func TestTellFromHostAndOnPhase(t *testing.T) {
	w := newWorld(18)
	a := w.spawn("a", nil, sim.OnPhase(func(p sim.Phase) (msg, bool) { return note{p.String()}, true }), sim.Workload())
	sim.Tell(w.sys, a, 2*ms, msg(note{"hello"}))
	sim.SchedulePhases(w.sys, sim.Phases{Faults: 5 * ms, Heal: 5 * ms, Drain: 5 * ms})
	mustOK(t, w.sys.Run())
	got := w.lines("a")
	if len(got) < 3 || got[1] != "note{S:hello}" {
		t.Errorf("a: %v", got)
	}
}
