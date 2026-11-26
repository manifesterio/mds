package sim_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
)

// store is outside state behind an executor: it keeps values and answers
// reads after a delay. Its hash is Tracked.
type store struct {
	values  map[string]string
	twice   bool
	handled int
}

func (s *store) hash() uint64 { return uint64(len(s.values)*1000 + s.handled) }

// serve handles one IO and answers through a held Reply, from a later step.
func (s *store) serve(sys *sim.System, tr *sim.Tracked, ctx *sim.ExecCtx, x io) {
	tr.Touch()
	s.handled++
	switch r := x.(type) {
	case write:
		s.values[r.Key] = "set"
	case read:
		d := sys.Env().Duration(ms, 5*ms)
		reply := sim.Hold(ctx, r)
		sys.After(d, func() {
			tr.Touch()
			reply.After(0, readDone{s.values[r.Key]})
			if s.twice {
				reply.After(0, readDone{"again"})
			}
		})
	}
}

func storeRun(seed uint64, twice bool) (*world, sim.Result) {
	w := newWorld(seed)
	st := &store{values: map[string]string{}, twice: twice}
	tr := w.sys.Track("store", st.hash)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.Do(write{"k"})
			actor.Perform(o, read{"k"}, toGot)
			actor.Perform(o, read{"missing"}, toGot)
		}
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, x io) { st.serve(w.sys, tr, ctx, x) }))
	return w, w.sys.Run()
}

func TestHoldAnswersLater(t *testing.T) {
	w, r := storeRun(12, false)
	mustOK(t, r)
	got := w.lines("a")
	slices.Sort(got)
	if want := []string{"got{V:set}", "got{V:}", "start{}"}; !slices.Equal(got, want) {
		t.Errorf("a: %v", got)
	}
}

func TestHoldAnswersTwiceIsProtocolFailure(t *testing.T) {
	if _, r := storeRun(12, true); !hasFailure(r, "answered_twice") {
		t.Errorf("failures: %+v", r.Failures)
	}
}

// The answer of a Do is dropped, without a failure, however late it is given.
func TestHoldOfDoIsDropped(t *testing.T) {
	w := newWorld(12)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.Do(read{"k"})
		}
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, x io) {
		reply := sim.Hold(ctx, x.(read))
		if reply.Wanted() {
			t.Error("a Do is wanted")
		}
		reply.After(ms, readDone{"late"})
	}))
	mustOK(t, w.sys.Run())
	if got := w.lines("a"); !slices.Equal(got, []string{"start{}"}) {
		t.Errorf("a: %v", got)
	}
}

func TestHoldTwiceIsProtocolFailure(t *testing.T) {
	w := newWorld(12)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			actor.Perform(o, read{"k"}, toGot)
		}
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, x io) {
		r := x.(read)
		sim.Hold(ctx, r)
		sim.Hold(ctx, r)
	}))
	if r := w.sys.Run(); !hasFailure(r, "answered_twice") {
		t.Errorf("failures: %+v", r.Failures)
	}
}

// Wanted is true while the issuer waits and false once it is gone.
func TestReplyWanted(t *testing.T) {
	w := newWorld(12)
	var wanted []bool
	p := w.props("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		switch m.(type) {
		case start:
			actor.Perform(o, read{"k"}, toGot)
			o.After(2*ms, note{"trip"})
		case note:
			o.Stop(actor.Trip, "test")
		}
	})
	sim.Spawn(w.sys, p, sim.WithExecutor(func(ctx *sim.ExecCtx, x io) {
		reply := sim.Hold(ctx, x.(read))
		wanted = append(wanted, reply.Wanted())
		w.sys.After(5*ms, func() { wanted = append(wanted, reply.Wanted()) })
	}))
	mustOK(t, w.sys.Run())
	if !slices.Equal(wanted, []bool{true, false}) {
		t.Errorf("wanted: %v", wanted)
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

// Tracked state is part of the trace hash; a change without Touch is caught
// by VerifyHashes.
func TestTrackedHash(t *testing.T) {
	run := func(seed uint64, verify bool, change func(tr *sim.Tracked, n *uint64)) sim.Result {
		sys := sim.NewSystem(sim.Config{Seed: seed, VerifyHashes: verify})
		var n uint64
		tr := sys.Track("x", func() uint64 { return n })
		sys.After(ms, func() {})
		sys.After(2*ms, func() { change(tr, &n) })
		sys.After(3*ms, func() {})
		return sys.Run()
	}
	touched := func(tr *sim.Tracked, n *uint64) { *n = 7; tr.Touch() }
	silent := func(_ *sim.Tracked, n *uint64) { *n = 7 }
	none := func(*sim.Tracked, *uint64) {}
	a, b := run(1, true, touched), run(1, false, touched)
	mustOK(t, a)
	if a.Hash != b.Hash {
		t.Errorf("verify changed the hash: %x vs %x", a.Hash, b.Hash)
	}
	if c := run(1, false, none); c.Hash == a.Hash {
		t.Error("tracked state is not in the trace hash")
	}
	if r := run(1, true, silent); !hasFailure(r, "stale_state_hash") {
		t.Errorf("untouched change: %+v", r.Failures)
	}
}

func TestTrackProtocolFailures(t *testing.T) {
	sys := sim.NewSystem(sim.Config{Seed: 1})
	sys.Track("x", func() uint64 { return 0 })
	sys.Track("x", func() uint64 { return 0 })
	sys.Track("y", nil)
	if r := sys.Run(); !hasFailure(r, "track") {
		t.Errorf("failures: %+v", r.Failures)
	}
	sys = sim.NewSystem(sim.Config{Seed: 1})
	sys.Track("p", func() uint64 { panic("boom") })
	sys.After(ms, func() {})
	if r := sys.Run(); !hasFailure(r, "panic") {
		t.Errorf("panic in hash: %+v", r.Failures)
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

func TestTellFromHost(t *testing.T) {
	w := newWorld(18)
	a := w.spawn("a", nil, sim.Workload())
	sim.Tell(w.sys, a, 2*ms, msg(note{"hello"}))
	mustOK(t, w.sys.Run())
	got := w.lines("a")
	if len(got) != 2 || got[1] != "note{S:hello}" {
		t.Errorf("a: %v", got)
	}
}

func TestAfterReceiveSeesEffectsAndOnEndFires(t *testing.T) {
	w := newWorld(19)
	var kinds []actor.EffectKind
	var ends []string
	a := w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.After(ms, tick{1})
			o.Fact(actor.Fact{Name: "f"})
		}
	},
		sim.AfterReceive(func(_ actor.Incarnation, m msg, effects []actor.Effect) {
			for _, e := range effects {
				kinds = append(kinds, e.Kind)
			}
		}),
		sim.OnEnd(func(self actor.Incarnation, how actor.End, cause string) {
			ends = append(ends, self.Name+" "+how.String()+" "+cause)
		}))
	w.sys.After(5*ms, func() { w.sys.Crash(a, "test") })
	mustOK(t, w.sys.Run())
	if !slices.Equal(kinds, []actor.EffectKind{actor.EffectAfter, actor.EffectFact}) {
		t.Errorf("effects seen: %v", kinds)
	}
	if !slices.Equal(ends, []string{"a crash test"}) {
		t.Errorf("ends: %v", ends)
	}
}
