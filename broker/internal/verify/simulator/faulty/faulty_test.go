package faulty_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/simulator/faulty"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/service"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/sim/kit"
	"github.com/manifesterio/mds/dst/trace"
)

// The tests use a tiny spec: a map from a name to a number. A "set" op stores a
// number and a "get" op reads it. A caller actor issues the requests and keeps
// every answer it gets. Nothing here is specific to a product.

const ms = actor.Millisecond

// request is one call to the service.
type request struct {
	id    uint64
	key   string
	set   bool // true: store val under key. false: read key.
	val   int64
	limit bool // the test limit refuses this request
	quiet bool // a set that changes the state, but whose answer says "no change"
}

// AsRequest makes request an IO that answers with an answer.
func (r request) AsRequest() (request, answer) { return r, answer{} }

func (r request) Fold(h *trace.Hash) {
	h.Uint64(1)
	h.Uint64(r.id)
	h.String(r.key)
	h.Bool(r.set)
	h.Int64(r.val)
	h.Bool(r.limit)
	h.Bool(r.quiet)
}

// answer is what the caller gets back. It keeps the id of the request it answers.
type answer struct {
	id      uint64
	val     int64
	changed bool // the spec changed its state
	failure faulty.Failure
}

func (a answer) Fold(h *trace.Hash) {
	h.Uint64(3)
	h.Uint64(a.id)
	h.Int64(a.val)
	h.Bool(a.changed)
	h.Uint64(uint64(a.failure))
}

// input is the input of the caller actor: a command to issue a request, or an
// answer from the service.
type input struct {
	issue bool
	req   request
	ans   answer
}

func (i input) Fold(h *trace.Hash) {
	h.Uint64(2)
	h.Bool(i.issue)
	i.req.Fold(h)
	i.ans.Fold(h)
}

// spec is the fault-free map.
type spec struct{ m map[string]int64 }

func (s *spec) Apply(r request) answer {
	if r.set {
		old, had := s.m[r.key]
		s.m[r.key] = r.val
		return answer{id: r.id, val: r.val, changed: (!had || old != r.val) && !r.quiet}
	}
	return answer{id: r.id, val: s.m[r.key]}
}

func (s *spec) StateHash() uint64 {
	keys := make([]string, 0, len(s.m))
	//determinismcheck:ok the keys are sorted below
	for k := range s.m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	h := trace.New()
	for _, k := range keys {
		h.String(k)
		h.Int64(s.m[k])
	}
	return h.Sum()
}

// The assertions of the test service.
var (
	testAsserts = faulty.NamedAsserts("test")                 //determinismcheck:ok catalog handles, set once at start
	slotAsserts = faulty.NamedAsserts("slot")                 //determinismcheck:ok catalog handles, set once at start
	testFlipped = actor.NewReachable("test answer corrupted") //determinismcheck:ok catalog handle, set once at start
)

// Which extras a test turns on.
type extras struct {
	corruptProb float64
	limit       bool
}

func shape(e extras) faulty.Shape[request, answer, request, answer] {
	s := faulty.ShapeOf[request, answer, request, answer]("testservice", "test", testAsserts)
	s.Op = func(r request) request { return r }
	s.Mutating = func(r request) bool { return r.set }
	s.Changed = func(a answer) bool { return a.changed }
	s.Reply = func(_ request, a answer) answer { return a }
	s.Fail = func(r request, f faulty.Failure) answer {
		return answer{id: r.id, failure: f}
	}
	if e.limit {
		s.Limit = func(r request) (answer, bool) {
			if r.limit {
				return answer{id: r.id, failure: faulty.Unavailable}, true
			}
			return answer{}, false
		}
	}
	if e.corruptProb > 0 {
		s.Corrupt = &faulty.Corruption[request, answer]{
			Prob:    e.corruptProb,
			Targets: func(r request) bool { return !r.set },
			Flip: func(a answer, bits uint64) (answer, uint64, bool) {
				bit := bits % 8
				a.val ^= 1 << bit
				return a, bit, true
			},
			Kind:    "test_corrupt",
			Reached: testFlipped,
		}
	}
	return s
}

// delivery is one answer that reached the caller.
type delivery struct {
	at  sim.Time
	ans answer
}

// caller is the actor that issues the requests and keeps the answers.
type caller struct{ t *testWorld }

func (c caller) Receive(_ actor.Now, in input, o *actor.Outbox[input, request]) {
	if in.issue {
		actor.Perform(o, in.req, func(a answer) input { return input{ans: a} })
		return
	}
	c.t.got = append(c.t.got, delivery{at: c.t.sys.Now(), ans: in.ans})
}

func (caller) StateHash() uint64 { return 0 }

// testWorld is a System with one caller actor "a" and the service.
type testWorld struct {
	tb   testing.TB
	sys  *sim.System
	sp   *spec
	svc  *faulty.Service[request, answer, request, answer]
	call *service.Endpoint[request, answer]
	who  actor.EntityRef[input]
	got  []delivery
	next uint64
}

func newWorld(tb testing.TB, seed uint64, f faulty.Faults, e extras) *testWorld {
	tb.Helper()
	t := &testWorld{tb: tb, sys: sim.NewSystem(sim.Config{Seed: seed}), sp: &spec{m: map[string]int64{}}}
	t.svc = faulty.New(t.sp, f, shape(e))
	t.call = service.Add[request, answer](t.sys, "testservice", t.svc)
	t.who = sim.Spawn(t.sys, actor.Props[input, request]{
		Name: "a",
		New:  func(actor.Env) actor.Actor[input, request] { return caller{t} },
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, r request) { service.Forward(ctx, r, t.call) }))
	t.sys.Start()
	tb.Cleanup(func() {
		for _, f := range t.sys.Finish().Failures {
			tb.Errorf("run failure: %s: %s", f.Property, f.Detail)
		}
	})
	return t
}

// base is a service with timing and no faults.
func base() faulty.Faults {
	ts := kit.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return faulty.Faults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

// do makes the caller issue r after a delay. It gives r the next id if it has none.
func (t *testWorld) do(after actor.Duration, r request) {
	if r.id == 0 {
		t.next++
		r.id = t.next
	}
	sim.Tell(t.sys, t.who, after, input{issue: true, req: r})
}

func (t *testWorld) set(id uint64, key string, val int64) {
	t.do(0, request{id: id, key: key, set: true, val: val})
}

func (t *testWorld) get(id uint64, key string) { t.do(0, request{id: id, key: key}) }

func (t *testWorld) run() { t.sys.RunUntil(nil) }

func (t *testWorld) runTo(at sim.Time) {
	reached := false
	t.sys.At(at, func() { reached = true })
	t.sys.RunUntil(func() bool { return reached })
}

// step takes exactly one queue entry.
func (t *testWorld) step() bool {
	start := t.sys.Steps()
	t.sys.RunUntil(func() bool { return t.sys.Steps() > start })
	return t.sys.Steps() > start
}

func (t *testWorld) recover() {
	kit.SetPhase(t.sys, kit.PhaseHeal)
	t.call.EnterRecovery()
}

// find returns the answer to request id (false if none came).
func (t *testWorld) find(id uint64) (delivery, bool) {
	for _, d := range t.got {
		if d.ans.id == id {
			return d, true
		}
	}
	return delivery{}, false
}

// faults lists the kinds of the injected faults, in order.
func (t *testWorld) faults() []string {
	var kinds []string
	for _, f := range t.sys.Timeline().Snapshot("sim_fault") {
		if k, ok := f.Fact.Field("kind").(actor.StringValue); ok {
			kinds = append(kinds, string(k))
		}
	}
	return kinds
}

// hit reports whether the assertion passed at least once.
func (t *testWorld) hit(name string) bool {
	c, ok := t.sys.Coverage()[name]
	return ok && c.Passes > 0
}

func (t *testWorld) value(key string) (int64, bool) {
	v, ok := t.sp.m[key]
	return v, ok
}

func TestNamedAssertsAndShapeOf(t *testing.T) {
	a := slotAsserts
	if a.Outage.Name() != "slot outage started" || a.Timeout.Name() != "slot timeout injected" {
		t.Fatalf("%+v", a)
	}
	s := faulty.ShapeOf[request, answer, request, answer]("svc", "slot", a)
	if s.Name != "svc" || s.Prefix != "slot" || s.Asserts != a {
		t.Fatal(s)
	}
}

// Without faults the service gives the spec answers, applies each change once,
// and answers after the apply time and the respond time.
func TestNoFaultsGivesTheSpecAnswers(t *testing.T) {
	w := newWorld(t, 1, base(), extras{})
	w.set(1, "k", 7)
	w.run()
	w.get(2, "k")
	w.get(3, "nope")
	w.run()
	if d, _ := w.find(1); d.ans.failure != 0 || !d.ans.changed {
		t.Fatal(d)
	}
	if d, _ := w.find(2); d.ans.val != 7 {
		t.Fatal(d)
	}
	if d, _ := w.find(3); d.ans.val != 0 {
		t.Fatal(d)
	}
	if w.svc.Name() != "testservice" {
		t.Fatal(w.svc.Name())
	}
	if w.svc.Gen() != 1 {
		t.Fatal("a change and two reads must count one change, got", w.svc.Gen())
	}
	if got := w.faults(); len(got) != 0 {
		t.Fatal("no fault was asked for:", got)
	}
}

// Slow reply: with the slow tail always on, the answer comes after one slow
// apply time and one slow respond time.
func TestSlowReply(t *testing.T) {
	f := base()
	f.ApplyLatency.SlowProb, f.RespondLatency.SlowProb = 1, 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 1)
	w.run()
	d, ok := w.find(1)
	lo := sim.Time(f.ApplyLatency.SlowMin + f.RespondLatency.SlowMin)
	hi := sim.Time(f.ApplyLatency.SlowMax + f.RespondLatency.SlowMax)
	if !ok || d.at < lo || d.at > hi {
		t.Fatal("answer time", d.at, "want in", lo, hi)
	}
	// And with the tail off, the answer is fast.
	g := newWorld(t, 1, base(), extras{})
	g.set(1, "k", 1)
	g.run()
	if d, _ := g.find(1); d.at > sim.Time(base().ApplyLatency.FastMax+base().RespondLatency.FastMax) {
		t.Fatal("fast answer came late:", d.at)
	}
}

// Error with nothing applied: the call is refused, the spec never sees it.
func TestRefusedAppliesNothing(t *testing.T) {
	f := base()
	f.Unavailable503 = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 7)
	w.run()
	d, ok := w.find(1)
	if !ok || d.ans.failure != faulty.Unavailable || len(w.got) != 1 {
		t.Fatal(w.got)
	}
	if _, has := w.value("k"); has || w.svc.Gen() != 0 {
		t.Fatal("a refused call was applied")
	}
	if !slices.Contains(w.faults(), "test_503") || !w.hit("test 503 injected") {
		t.Fatal(w.faults())
	}
}

// Timeout: the caller sees TimedOut after CallerTimeout, and the op is applied
// in some runs and not in others.
func TestTimeoutAppliesOrNot(t *testing.T) {
	f := base()
	f.Timeout = 1
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		w := newWorld(t, seed, f, extras{})
		w.set(1, "k", 7)
		w.run()
		d, ok := w.find(1)
		if !ok || d.ans.failure != faulty.TimedOut || len(w.got) != 1 || d.at != sim.Time(f.CallerTimeout) {
			t.Fatal(w.got)
		}
		if _, has := w.value("k"); has {
			applied++
		} else {
			notApplied++
		}
		if !slices.Contains(w.faults(), "test_timeout") {
			t.Fatal(w.faults())
		}
	}
	if applied == 0 || notApplied == 0 {
		t.Fatal("the coin must give both:", applied, notApplied)
	}
}

// Applied, but the reply is lost: the caller sees TimedOut, the op is applied
// every time, and a lost read still answers exactly once.
func TestLostReplyStillApplies(t *testing.T) {
	f := base()
	f.LostResponse = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 7)
	w.run()
	d, ok := w.find(1)
	if !ok || d.ans.failure != faulty.TimedOut || d.at != sim.Time(f.CallerTimeout) {
		t.Fatal(w.got)
	}
	if v, has := w.value("k"); !has || v != 7 {
		t.Fatal("the lost call was not applied")
	}
	w.get(2, "k")
	w.run()
	n := 0
	for _, g := range w.got {
		if g.ans.id == 2 {
			n++
			if g.ans.failure != faulty.TimedOut {
				t.Fatal(g)
			}
		}
	}
	if n != 1 || !slices.Contains(w.faults(), "test_lost_response") || !w.hit("test lost response injected") {
		t.Fatal(n, w.faults())
	}
}

// The retry after a lost reply: the op was applied, the caller retries the same
// op in recovery, and the spec answers as for a replay. The service adds no
// effect of its own, so the retry is exactly as safe as the spec says.
func TestRetryAfterLostReply(t *testing.T) {
	f := base()
	f.LostResponse = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 7)
	w.run() // applied, the caller saw TimedOut
	w.recover()
	w.set(2, "k", 7) // the retry
	w.run()
	d, ok := w.find(2)
	if !ok || d.ans.failure != 0 || d.ans.val != 7 || d.ans.changed {
		t.Fatal("the retry must see the stored value and no change:", d)
	}
	if w.svc.Gen() != 1 {
		t.Fatal("the retry counted as a change:", w.svc.Gen())
	}
}

// Outage window: while it lasts, every call is refused. Only one window starts.
func TestOutageWindow(t *testing.T) {
	f := base()
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]actor.Duration{100 * ms, 200 * ms}}
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 1)
	w.runTo(50 * sim.Time(ms)) // still inside the window
	w.set(2, "k", 1)
	w.run()
	for _, d := range w.got {
		if d.ans.failure != faulty.Unavailable {
			t.Fatal(d)
		}
	}
	faults := w.faults()
	if n := slices.Index(faults, "test_outage"); n < 0 || slices.Index(faults[n+1:], "test_outage") >= 0 {
		t.Fatal("want exactly one window", faults)
	}
	if _, has := w.value("k"); has {
		t.Fatal("a call was applied in the outage")
	}
	if !w.hit("test outage started") {
		t.Fatal("outage not recorded")
	}
	w.recover()
	w.runTo(10_000 * sim.Time(ms))
	w.set(3, "k", 1)
	w.run()
	if d, _ := w.find(3); d.ans.failure != 0 {
		t.Fatal(d)
	}
}

// Corruption: one bit of the answer is flipped. The stored value stays whole.
func TestCorruptedAnswer(t *testing.T) {
	w := newWorld(t, 1, base(), extras{corruptProb: 1})
	w.set(1, "k", 100)
	w.run()
	w.get(2, "k")
	w.run()
	d, _ := w.find(2)
	if d.ans.val == 100 || bitsOf(d.ans.val^100) != 1 {
		t.Fatal("want one flipped bit:", d)
	}
	if v, _ := w.value("k"); v != 100 {
		t.Fatal("the stored value was damaged")
	}
	if !slices.Contains(w.faults(), "test_corrupt") || !w.hit("test answer corrupted") {
		t.Fatal(w.faults())
	}
	// A set is not a target: its answer stays whole.
	if d, _ := w.find(1); d.ans.val != 100 {
		t.Fatal("a set was corrupted:", d)
	}
}

func bitsOf(x int64) int {
	n := 0
	for ; x != 0; x &= x - 1 {
		n++
	}
	return n
}

// Limit: an op that the limit refuses never reaches the spec, and changes nothing.
func TestLimitRefusesBeforeTheSpec(t *testing.T) {
	w := newWorld(t, 1, base(), extras{limit: true})
	w.do(0, request{id: 1, key: "k", set: true, val: 1, limit: true})
	w.set(2, "j", 2)
	w.run()
	if d, _ := w.find(1); d.ans.failure != faulty.Unavailable {
		t.Fatal(d)
	}
	if _, has := w.value("k"); has {
		t.Fatal("a refused op reached the spec")
	}
	if d, _ := w.find(2); d.ans.failure != 0 || w.svc.Gen() != 1 {
		t.Fatal(d, w.svc.Gen())
	}
}

// The caller dies after it issued the call: the effect lands, and nothing is
// delivered to the dead caller.
func TestLandsAfterCallerCrash(t *testing.T) {
	w := newWorld(t, 1, base(), extras{})
	w.set(1, "k", 7)
	w.step() // the caller issues the call
	w.sys.Crash(w.who, "test")
	w.run()
	if len(w.got) != 0 {
		t.Fatal("delivered to a dead actor")
	}
	if v, ok := w.value("k"); !ok || v != 7 {
		t.Fatal("the call did not land")
	}
}

// The caller dies between the landing and the reply: the effect stands, and the
// reply goes neither to the dead incarnation nor to the next one.
func TestCrashBetweenLandingAndReply(t *testing.T) {
	w := newWorld(t, 2, base(), extras{})
	w.set(1, "k", 7)
	w.step()
	for _, ok := w.value("k"); !ok; _, ok = w.value("k") {
		if !w.step() {
			t.Fatal("the call never landed")
		}
	}
	w.sys.Crash(w.who, "test")
	w.sys.Boot(w.who) // a new incarnation must not get the answer of the old one
	w.run()
	if len(w.got) != 0 {
		t.Fatal("delivered to another incarnation")
	}
}

// A caller that died before the landing gets no answer, and the corruption step
// does not run for it: no bit is flipped and no fault is recorded.
func TestNoCorruptionForADeadCaller(t *testing.T) {
	w := newWorld(t, 1, base(), extras{corruptProb: 1})
	w.set(1, "k", 100)
	w.run()
	w.get(2, "k")
	w.step() // the caller issues the read
	w.sys.Crash(w.who, "test")
	w.run()
	if len(w.got) != 1 {
		t.Fatal("the dead caller got an answer:", w.got)
	}
	if slices.Contains(w.faults(), "test_corrupt") {
		t.Fatal("a bit was flipped for a caller that is gone")
	}
}

// After EnterRecovery no new fault is drawn, and the corruption coin too.
func TestRecoveryStopsFaults(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 1, 1, 1
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]actor.Duration{ms, 2 * ms}}
	w := newWorld(t, 1, f, extras{corruptProb: 1})
	w.set(1, "k", 100) // faulted
	w.run()
	w.recover()
	w.set(2, "j", 100)
	w.run()
	if d, _ := w.find(1); d.ans.failure != faulty.Unavailable {
		t.Fatal(d)
	}
	if d, _ := w.find(2); d.ans.failure != 0 {
		t.Fatal(d)
	}
	w.get(3, "j")
	w.run()
	if d, _ := w.find(3); d.ans.val != 100 {
		t.Fatal("corrupted in recovery:", d)
	}
}

// A plan made before EnterRecovery stands. It is not drawn again.
func TestPlanInFlightStandsAfterRecovery(t *testing.T) {
	f := base()
	f.Unavailable503 = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 1)
	w.step() // issued: the plan is a refusal
	w.recover()
	w.set(2, "j", 2)
	w.run()
	if d, _ := w.find(1); d.ans.failure != faulty.Unavailable {
		t.Fatal("the plan in flight must stand:", d)
	}
	if d, _ := w.find(2); d.ans.failure != 0 {
		t.Fatal(d)
	}
}

// The cached spec hash is never stale, after any mix of calls and faults.
func TestSpecHashCacheIsNeverStale(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	w := newWorld(t, 5, f, extras{})
	for i := range 200 {
		key := string(rune('a' + i%5))
		switch {
		case i%3 == 2:
			w.get(uint64(i+1), key)
		case i%4 == 0:
			// A change that the answer does not name: only Mutating can clear the cache.
			w.do(0, request{id: uint64(i + 1), key: key, set: true, val: int64(i), quiet: true})
		default:
			w.set(uint64(i+1), key, int64(i%4))
		}
		w.runTo(w.sys.Now() + sim.Time(ms))
		if got := w.svc.SpecStateHash(); got != w.sp.StateHash() {
			t.Fatalf("op %d: cached spec hash is stale", i)
		}
	}
	w.run()
	if w.svc.SpecStateHash() != w.sp.StateHash() {
		t.Fatal("final: cached spec hash is stale")
	}
	// A change made outside a call needs Bump.
	w.sp.m["x"] = 1
	w.svc.Bump()
	if w.svc.SpecStateHash() != w.sp.StateHash() {
		t.Fatal("Bump did not clear the cached hash")
	}
}

// The same seed gives the same run, a different seed gives another, and the calls
// in flight are part of the state hash.
func TestDeterminism(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	f.Outage = faulty.Outage{Prob: 0.05, Dur: [2]actor.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) ([]delivery, uint64, uint64) {
		w := newWorld(t, seed, f, extras{corruptProb: 0.3})
		for i := range 60 {
			key := string(rune('a' + i%5))
			if i%3 == 0 {
				w.do(actor.Duration(i)*ms, request{id: uint64(i + 1), key: key, set: true, val: int64(i)})
			} else {
				w.do(actor.Duration(i)*ms, request{id: uint64(i + 1), key: key})
			}
		}
		w.runTo(30 * sim.Time(ms))
		mid := w.svc.StateHash()
		w.run()
		return w.got, w.svc.StateHash(), mid
	}
	g1, h1, m1 := run(7)
	g2, h2, m2 := run(7)
	if !slices.Equal(g1, g2) || h1 != h2 || m1 != m2 {
		t.Fatal("the same seed diverged")
	}
	if g3, _, _ := run(8); slices.Equal(g1, g3) {
		t.Fatal("two seeds gave one run")
	}
	if m1 == h1 {
		t.Fatal("the calls in flight are not in the state hash")
	}
}
