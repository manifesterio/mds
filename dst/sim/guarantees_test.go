package sim_test

// One test per runtime guarantee: what an actor may rely on.

import (
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
)

func mustOK(t *testing.T, r sim.Result) {
	t.Helper()
	if !r.OK() {
		t.Fatalf("failures: %+v", r.Failures)
	}
}

func hasFailure(r sim.Result, property string) bool {
	return slices.ContainsFunc(r.Failures, func(f sim.Failure) bool { return f.Property == property })
}

// effects take effect after Receive, in record order; a panicking
// Receive applies none of its effects.
func TestEffectsAppliedInOrder(t *testing.T) {
	w := newWorld(1)
	var target actor.EntityRef[msg]
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		switch m := m.(type) {
		case start:
			o.Fact(actor.Fact{Name: "f1"})
			target.Tell(o, note{"1"})
			o.Fact(actor.Fact{Name: "f2"})
			target.Tell(o, note{"2"})
			o.TellSelf(note{"self"})
		case note:
			if m.S == "self" {
				o.Fact(actor.Fact{Name: "lost"})
				target.Tell(o, note{"lost"})
				panic("boom")
			}
		}
	})
	target = w.spawn("b", nil)
	r := w.sys.Run()
	if got := w.lines("b"); !slices.Equal(got, []string{"start{}", "note{S:1}", "note{S:2}"}) {
		t.Errorf("b received %v", got)
	}
	var facts []string
	for _, f := range w.sys.Timeline().Snapshot("f1") {
		facts = append(facts, f.Fact.Name)
	}
	if len(facts) != 1 || len(w.sys.Timeline().Snapshot("f2")) != 1 || len(w.sys.Timeline().Snapshot("lost")) != 0 {
		t.Errorf("facts: f1 %d, f2 %d, lost %d", len(facts), len(w.sys.Timeline().Snapshot("f2")), len(w.sys.Timeline().Snapshot("lost")))
	}
	if r.Crashes != 1 || !hasFailure(r, "panic") {
		t.Errorf("a panicking Receive crashes the actor: crashes %d failures %+v", r.Crashes, r.Failures)
	}
}

// same-time events run in seeded order; per-pair FIFO holds; the same
// seed gives the same order and hash.
func TestSameTimeOrderIsSeeded(t *testing.T) {
	run := func(seed uint64) (string, uint64) {
		w := newWorld(seed)
		var target actor.EntityRef[msg]
		for _, name := range []string{"x", "y", "z"} {
			w.spawn(name, func(_ *scripted, _ actor.Now, m msg, o *outbox) {
				if _, ok := m.(start); ok {
					for i := range 3 {
						target.Tell(o, note{name + string(rune('0'+i))})
					}
				}
			})
		}
		target = w.spawn("t", nil)
		r := w.sys.Run()
		mustOK(t, r)
		got := w.lines("t")[1:]
		for _, name := range []string{"x", "y", "z"} {
			var mine []string
			for _, l := range got {
				if strings.Contains(l, "S:"+name) {
					mine = append(mine, l)
				}
			}
			want := []string{"note{S:" + name + "0}", "note{S:" + name + "1}", "note{S:" + name + "2}"}
			if !slices.Equal(mine, want) {
				t.Fatalf("seed %d: per-pair FIFO broken for %s: %v", seed, name, mine)
			}
		}
		return strings.Join(got, ","), r.Hash
	}
	orders := map[string]bool{}
	for seed := uint64(1); seed <= 40; seed++ {
		o1, h1 := run(seed)
		o2, h2 := run(seed)
		if o1 != o2 || h1 != h2 {
			t.Fatalf("seed %d is not repeatable", seed)
		}
		orders[o1] = true
	}
	if len(orders) < 5 {
		t.Errorf("40 seeds explored only %d same-time orders", len(orders))
	}
}

// CancelTimer and StartTimer drop a timer that fired but is not yet
// received.
func TestCancelDropsFiredTimer(t *testing.T) {
	if got := cancelScenario(nil); !slices.Equal(got, []string{"start{}", "note{S:cancel}", "note{S:restart}", "tick{N:2}"}) {
		t.Errorf("received %v", got)
	}
}

// cancelScenario arms timer k (due now) behind a note that cancels it, then
// arms k again behind a note that restarts it; only the last arm may fire.
func cancelScenario(plant func(*sim.System)) []string {
	w := newWorld(3)
	if plant != nil {
		plant(w.sys)
	}
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		switch m := m.(type) {
		case start:
			o.TellSelf(note{"cancel"})
			o.StartTimer("k", 0, tick{0})
			o.TellSelf(note{"restart"})
			o.StartTimer("j", 0, tick{1})
		case note:
			if m.S == "cancel" {
				o.CancelTimer("k") // tick 0 already fired: it waits behind this note
			} else {
				o.StartTimer("j", 5*ms, tick{2}) // replaces tick 1, which fired too
			}
		}
	})
	w.sys.Run()
	return w.lines("a")
}

// The planted bug: a runtime that forgets to drop fired timers. The cancel
// scenario must catch it.
func TestG3CatchesPlantedBug(t *testing.T) {
	got := cancelScenario(sim.PlantKeepFiredTimers)
	if slices.Contains(got, "tick{N:0}") || slices.Contains(got, "tick{N:1}") {
		return // caught: a cancelled or replaced timer was received
	}
	t.Fatalf("the planted bug was not caught: %v", got)
}

// a Reply to an asker whose incarnation ended is dropped, even if the
// asker restarted.
func TestReplyToCrashedAskerDropped(t *testing.T) {
	w := newWorld(4)
	var b actor.EntityRef[msg]
	boots := 0
	pa := w.props("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			boots++
		}
		if _, ok := m.(start); ok && boots == 1 {
			actor.Ask(o, b, func(to actor.ReplyTo[answer]) msg { return asked{Q: "q", ReplyTo: to} },
				func(a answer) msg { return got(a) })
		}
	})
	pa.Restart = func(actor.End, *actor.Rand) (actor.Duration, bool) { return ms, true }
	a := sim.Spawn(w.sys, pa)
	b = w.spawn("b", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		switch m := m.(type) {
		case asked:
			if m.Q == "q" {
				o.After(10*ms, replyLate{V: "late", ReplyTo: m.ReplyTo})
			}
		case replyLate:
			actor.Reply(o, m.ReplyTo, answer{m.V})
		}
	})
	w.sys.After(5*ms, func() { w.sys.Crash(a, "test") })
	w.sys.RunUntil(func() bool { return w.sys.Now() > sim.Time(30*ms) })
	r := w.sys.Finish()
	mustOK(t, r)
	for _, l := range w.lines("a") {
		if strings.HasPrefix(l, "got") {
			t.Fatalf("a restarted incarnation got the reply to its predecessor: %v", w.lines("a"))
		}
	}
	if len(w.lines("b")) < 3 {
		t.Fatalf("b did not reply: %v", w.lines("b"))
	}
}

// ForgetPending drops pending timers and answers, also those already
// waiting in the inbox; effects recorded after it are kept.
func TestForgetPendingDropsAnswers(t *testing.T) {
	w := newWorld(5)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		switch m := m.(type) {
		case start:
			actor.Perform(o, read{"k"}, toGot)
			o.StartTimer("t", 5*ms, tick{1})
			o.After(0, tick{2}) // fires at once: waits behind the note below
			o.After(20*ms, note{"forget"})
		case note:
			if m.S == "forget" {
				o.ForgetPending()
				o.StartTimer("t", ms, tick{3})
			}
		}
	}, sim.WithExecutor(executor))
	mustOK(t, w.sys.Run())
	got := w.lines("a")
	want := []string{"start{}", "tick{N:2}", "tick{N:1}", "got{V:v:k}", "note{S:forget}", "tick{N:3}"}
	if !slices.Equal(got, want) {
		t.Fatalf("baseline: %v", got)
	}

	// Now forget before anything arrives.
	w = newWorld(5)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		switch m.(type) {
		case start:
			actor.Perform(o, read{"k"}, toGot)
			o.StartTimer("t", 5*ms, tick{1})
			o.After(0, tick{2})
			o.TellSelf(note{"forget"})
		case note:
			o.ForgetPending()
			o.After(ms, tick{3})
		}
	}, sim.WithExecutor(executor))
	r := w.sys.Run()
	mustOK(t, r)
	// tick 2 fired at once but waits behind the note (self lane, record
	// order? no: TellSelf is recorded after After(0), so tick 2 comes first).
	if got := w.lines("a"); !slices.Equal(got, []string{"start{}", "tick{N:2}", "note{S:forget}", "tick{N:3}"}) {
		t.Fatalf("after ForgetPending: %v", got)
	}
}

// Watch delivers gone once when the asker ends before a Reply; never
// after a Reply; at once when the asker is already gone.
func TestWatchFiresOnce(t *testing.T) {
	w := newWorld(6)
	a := w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if m, ok := m.(asked); ok {
			o.Watch(m.ReplyTo, goneMsg{m.Q})
			if m.Q == "answered" {
				actor.Reply(o, m.ReplyTo, answer{"yes"})
			}
		}
	})
	var results []string
	ask := func(q string, within actor.Duration) {
		sim.Ask(w.sys, a, func(to actor.ReplyTo[answer]) msg { return asked{Q: q, ReplyTo: to} }, within,
			func(ans answer, ok bool) {
				if ok {
					results = append(results, q+"="+ans.V)
				} else {
					results = append(results, q+" timed out")
				}
			})
	}
	w.sys.After(ms, func() { ask("abandoned", 5*ms) })
	w.sys.After(ms, func() { ask("answered", 5*ms) })
	w.sys.After(2*ms, func() { ask("instant", 0) }) // its deadline passes before a receives it
	mustOK(t, w.sys.Run())
	gone := 0
	for _, l := range w.lines("a") {
		if strings.HasPrefix(l, "goneMsg") {
			gone++
			if strings.Contains(l, "answered") {
				t.Errorf("gone after a Reply: %v", w.lines("a"))
			}
		}
	}
	if gone != 2 {
		t.Errorf("want gone for abandoned and instant once each, got %d: %v", gone, w.lines("a"))
	}
	slices.Sort(results)
	if want := []string{"abandoned timed out", "answered=yes", "instant timed out"}; !slices.Equal(results, want) {
		t.Errorf("results %v", results)
	}
}

// Watch also fires when the asker is an actor that crashes.
func TestWatchFiresWhenAskingActorEnds(t *testing.T) {
	w := newWorld(7)
	var a actor.EntityRef[msg]
	b := w.spawn("b", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			actor.Ask(o, a, func(to actor.ReplyTo[answer]) msg { return asked{Q: "q", ReplyTo: to} },
				func(x answer) msg { return got(x) })
		}
	})
	a = w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if m, ok := m.(asked); ok {
			o.Watch(m.ReplyTo, goneMsg{"b"})
		}
	})
	w.sys.After(3*ms, func() { w.sys.Crash(b, "test") })
	mustOK(t, w.sys.Run())
	if got := w.lines("a"); !slices.Contains(got, "goneMsg{Who:b}") {
		t.Errorf("a: %v", got)
	}
}

// Stop ends the incarnation; everything it owned is dropped; the restart
// policy boots a fresh one. An effect after Stop is a protocol failure.
func TestStopDropsEverything(t *testing.T) {
	w := newWorld(8)
	boots := 0
	p := w.props("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			boots++
		}
		if _, ok := m.(start); ok && boots == 1 {
			actor.Perform(o, read{"k"}, toGot)
			o.StartTimer("t", ms, tick{1})
			o.TellSelf(note{"x"})
			o.Stop(actor.Trip, "test")
		}
	})
	p.Restart = func(how actor.End, _ *actor.Rand) (actor.Duration, bool) { return 2 * ms, how == actor.Trip }
	sim.Spawn(w.sys, p, sim.WithExecutor(executor))
	r := w.sys.Run()
	mustOK(t, r)
	if got := w.lines("a"); !slices.Equal(got, []string{"start{}", "start{}"}) {
		t.Errorf("a: %v", got)
	}
	if r.Trips != 1 {
		t.Errorf("trips %d", r.Trips)
	}

	w = newWorld(8)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			o.Stop(actor.Exit, "")
			o.TellSelf(note{"after"})
		}
	})
	if r := w.sys.Run(); !hasFailure(r, "effect_after_stop") {
		t.Errorf("effect after Stop: %+v", r.Failures)
	}
}

// effects through a Sub behave like the parent's, wrapped; its timers
// and ForgetPending are scoped; Stop through a Sub is refused.
func TestSubWrapsMessages(t *testing.T) {
	w := newWorld(9)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		comp := actor.Sub(o, "comp", func(c compMsg) msg { return fromComp{c} })
		switch m := m.(type) {
		case start:
			comp.StartTimer("t", ms, compMsg{"timer"})
			o.StartTimer("t", 2*ms, tick{1}) // same key, other scope
			actor.Perform(comp, read{"k"}, func(d readDone) compMsg { return compMsg{d.V} })
			comp.TellSelf(compMsg{"now"})
		case fromComp:
			if m.C.S == "now" {
				comp.ForgetPending() // drops the component's timer and read, not the parent's tick
			}
		}
	}, sim.WithExecutor(executor))
	mustOK(t, w.sys.Run())
	if got := w.lines("a"); !slices.Equal(got, []string{"start{}", "fromComp{C:{S:now}}", "tick{N:1}"}) {
		t.Errorf("a: %v", got)
	}

	w = newWorld(9)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		actor.Sub(o, "comp", func(c compMsg) msg { return fromComp{c} }).Stop(actor.Exit, "")
	})
	if r := w.sys.Run(); !hasFailure(r, "outbox_misuse") {
		t.Errorf("Stop through Sub: %+v", r.Failures)
	}
}

// a pending timer is part of the trace hash, even if it never fires.
func TestTraceFoldsPendingTimers(t *testing.T) {
	run := func(arm bool) uint64 {
		sys := sim.NewSystem(sim.Config{Seed: 10, End: sim.Time(5 * ms)})
		var log []string
		sim.Spawn(sys, actor.Props[msg, io]{
			Name: "a",
			Boot: func(actor.Incarnation) msg { return start{} },
			New: func(actor.Env) actor.Actor[msg, io] {
				return &scripted{name: "a", log: &log, fn: func(_ *scripted, _ actor.Now, m msg, o *outbox) {
					if _, ok := m.(start); ok && arm {
						o.StartTimer("far", actor.Second, tick{1})
					}
				}}
			},
		})
		return sys.Run().Hash
	}
	if run(true) == run(false) {
		t.Error("a pending timer does not change the hash")
	}
}

// a Perform answer reaches only the incarnation that issued it; a second
// executor answer is a protocol failure.
func TestPerformAnswerScoped(t *testing.T) {
	w := newWorld(11)
	boots := 0
	p := w.props("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			boots++
		}
		if _, ok := m.(start); ok && boots == 1 {
			actor.Perform(o, read{"k"}, toGot)
			o.After(2*ms, note{"trip"})
		}
		if m, ok := m.(note); ok && m.S == "trip" {
			o.Stop(actor.Trip, "test")
		}
	})
	p.Restart = func(actor.End, *actor.Rand) (actor.Duration, bool) { return ms, true }
	sim.Spawn(w.sys, p, sim.WithExecutor(executor))
	mustOK(t, w.sys.Run())
	for _, l := range w.lines("a") {
		if strings.HasPrefix(l, "got") {
			t.Fatalf("a new incarnation got its predecessor's answer: %v", w.lines("a"))
		}
	}

	w = newWorld(11)
	w.spawn("a", func(_ *scripted, _ actor.Now, m msg, o *outbox) {
		if _, ok := m.(start); ok {
			actor.Perform(o, read{"k"}, toGot)
		}
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, x io) {
		r := x.(read)
		sim.Answer(ctx, r, ms, readDone{"1"})
		sim.Answer(ctx, r, ms, readDone{"2"})
	}))
	if r := w.sys.Run(); !hasFailure(r, "answered_twice") {
		t.Errorf("second answer: %+v", r.Failures)
	}
}
