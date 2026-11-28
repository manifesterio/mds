package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
	"github.com/manifesterio/mds/dst/internal/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// Time is global virtual time in nanoseconds since the run started. Actors
// never see it directly: they see actor.Now, with the clock skew of their
// node.
type Time int64

// laneID is an ordering group of the event queue. Entries on one lane at one
// time run in the order they were added (FIFO).
type laneID uint64

// entryID names one queue entry, so that it can be cancelled.
type entryID uint64

// The lanes the System gives a meaning to.
const (
	// noLane is no FIFO group: every entry gets a fresh random tie.
	noLane laneID = 0
	// hostLane is the lane of the System itself (restarts and kills).
	// Lanes at or above it belong to the System: it keeps one wake-up lane
	// per actor there. Pair lanes (one per sender and receiver) are just
	// below it; host code may use the lanes below those.
	hostLane laneID = 1 << 62
)

const (
	defaultMaxSteps uint64 = 10_000_000
)

// Config configures one run.
type Config struct {
	Seed         uint64
	WallStart    actor.Wall     // UTC at time zero
	End          Time           // stop before the first entry after End; 0: no end
	MaxSteps     uint64         // 0 means 10_000_000; more steps is FailMaxSteps
	ClockSkewMax actor.Duration // per-node clock skew in [0, ClockSkewMax], drawn once at Spawn
	Log          bool           // keep every step line in Result.Log
	Tail         int            // keep the last Tail step lines for Result.Tail of a failed run
	HaltOnFirst  bool           // stop the run at the first failure
	VerifyHashes bool           // recompute cached state hashes every step; a mismatch fails the seed
}

// FailKind says what kind of failure a seed had. It is not folded into the
// trace hash.
type FailKind uint8

// The kinds of failure.
const (
	FailViolation     FailKind = iota // an Always, AlwaysOrUnreachable or Unreachable broke
	FailProtocol                      // the API was used the wrong way (effect after Stop, zero address, ...)
	FailPanic                         // a panic in a Receive, New, an executor, a tracked hash, a host task or a rule
	FailMaxSteps                      // the run took more than Config.MaxSteps steps
	FailNotRepeatable                 // the same seed gave another run (set by the campaign canary)
)

// Failure is the first failure of one property in a run. Later failures of
// the same property only raise Repeats.
type Failure struct {
	Kind     FailKind
	Property string
	Detail   string
	Step     uint64
	At       Time
	Repeats  int
}

// LogEntry is one step line. Its text is never hashed.
type LogEntry struct {
	Step uint64
	At   Time
	Text string
}

// Result is one run. (Steps, RandCalls, Hash) is what a golden pin holds.
type Result struct {
	Seed, Steps, RandCalls, Hash uint64
	Crashes, Trips, Exits        int
	Failures                     []Failure
	Coverage                     map[string]Coverage
	Sites                        map[string]SiteCount
	Log                          []LogEntry // every step, only with Config.Log
	Tail                         []LogEntry // the last Config.Tail steps, only on failure
}

// OK reports a run without failures.
func (r Result) OK() bool { return len(r.Failures) == 0 }

// slot is one actor of the System, whatever its types. It is the one seam
// between the untyped System and a typed host, so it is wide on purpose.
type slot interface { //nolint:interfacebloat // see above
	base() *actorBase
	boot(sys *System)
	pump(sys *System)
	stateHash() uint64
	inboxLens() (int, int)
	drop()
	// ended runs the OnEnd hook of the incarnation that just ended.
	ended(sys *System, who actor.Incarnation, how actor.End, cause string)
	stopRequest(sys *System, deadline actor.Mono) bool
	// arrive puts a message that reached the actor in its inbox. n is the
	// owned number it carries (a timer or an answer), 0 for none.
	arrive(sys *System, msg actor.Payload, inc actor.ID, toInc bool, n uint64)
	// answerArrives handles the answer for awaited number n of incarnation inc.
	answerArrives(sys *System, answer actor.Payload, inc actor.ID, n uint64)
}

// System is one simulation run: one seed, one random stream, one event queue,
// the actors, the tracked outside state and the rules.
// It is not safe for concurrent use. Use one System per run.
type System struct {
	cfg     Config
	src     *rng.Rand // the run stream source; Calls counts its draws
	rand    *actor.Rand
	q       *queue
	faults  *injector
	asserts *assertLog
	hash    trace.Hash
	tl      *Timeline

	owner      *core.Owner
	actors     []slot
	names      map[string]bool
	trackNames map[string]bool
	tracked    []*Tracked
	rules      []Rule
	onStep     []func()

	pairLanes    map[laneKey2]laneID
	nextPairLane laneID
	watches      []watch
	outside      map[uint64]*outsideAsk
	outsideAsks  uint64

	// plantKeepFiredTimers is a planted bug, set only by tests.
	plantKeepFiredTimers bool

	steps    uint64
	halted   bool
	started  bool
	finished bool
	result   Result

	crashes, trips, exits int
	failures              []Failure
	lines                 []logLine
	tail                  *ring[logLine]
}

// NewSystem makes a System for cfg.
func NewSystem(cfg Config) *System {
	if cfg.MaxSteps == 0 {
		cfg.MaxSteps = defaultMaxSteps
	}
	src := rng.New(cfg.Seed)
	sys := &System{
		cfg:        cfg,
		src:        src,
		rand:       actor.NewRand(src),
		hash:       trace.New(),
		tl:         NewTimeline(),
		owner:      core.NewOwner(),
		names:      map[string]bool{},
		trackNames: map[string]bool{},
		pairLanes:  map[laneKey2]laneID{},
		outside:    map[uint64]*outsideAsk{},
	}
	sys.q = newQueue(sys.src)
	sys.faults = newInjector(sys.rand)
	sys.asserts = newAssertLog(func(property, detail string) { sys.Fail(FailViolation, property, detail) })
	sys.asserts.SetStepCounter(sys.Steps)
	if cfg.Tail > 0 {
		sys.tail = newRing[logLine](cfg.Tail)
	}
	return sys
}

// Now is the current virtual time.
func (sys *System) Now() Time { return sys.q.now }

// wall is the UTC time now: Config.WallStart plus Now.
func (sys *System) wall() actor.Wall { return sys.cfg.WallStart + actor.Wall(sys.q.now) }

// Steps is the number of queue entries taken so far.
func (sys *System) Steps() uint64 { return sys.steps }

// Draws is the number of draws from the run stream so far.
func (sys *System) Draws() uint64 { return sys.src.Calls() }

// Env is the run's Rand, Faults and Assertions for host code. Its Self is
// the zero Incarnation.
func (sys *System) Env() actor.Env { return sys.env(actor.Incarnation{}) }

// Coverage returns a copy of the per-property stats so far.
func (sys *System) Coverage() map[string]Coverage { return sys.asserts.Coverage() }

// Timeline is the fact timeline of the run.
func (sys *System) Timeline() *Timeline { return sys.tl }

// Note adds a log line. It is never hashed.
func (sys *System) Note(text string) { sys.logMsg(text, nil) }

// Fact appends a host fact (source zero) to the timeline.
func (sys *System) Fact(f actor.Fact) { sys.appendFact(actor.Incarnation{}, f) }

// SetEnd sets the end of the run: the run stops before the first entry after
// t (see Config.End). Host code that learns its schedule after NewSystem
// (kit.SchedulePhases) uses it.
func (sys *System) SetEnd(t Time) { sys.cfg.End = t }

// SetFaults turns buggify Fault sites on or off (Maybe sites still draw) and
// appends the "sim_faults" fact with field on. A System starts with faults on.
func (sys *System) SetFaults(on bool) {
	sys.faults.SetQuiet(!on)
	sys.systemFact("sim_faults", actor.Field{Key: "on", Value: actor.Bool(on)})
}

// FaultsOn reports whether buggify Fault sites may fire.
func (sys *System) FaultsOn() bool { return !sys.faults.Quiet() }

// OnStep runs fn after every queue pop: after the entry ran and before the
// state fold and the rules. fn may draw from the run stream and act on the
// System (Crash, Stop). Steps of a halted run do not call it. Add every
// OnStep before the run starts; they run in the order added.
func (sys *System) OnStep(fn func()) { sys.onStep = append(sys.onStep, fn) }

// cancel retires a pending queue entry. An entry that already ran, or an
// unknown id, is ignored.
func (sys *System) cancel(id entryID) { sys.q.cancel(id) }

// AddRule adds a rule. Add every rule before the run starts.
func (sys *System) AddRule(r Rule) { sys.rules = append(sys.rules, r) }

// Fail records a failure of property. Only the first failure of one property
// is kept; later ones raise its Repeats. With Config.HaltOnFirst the run halts.
func (sys *System) Fail(kind FailKind, property, detail string) {
	for i := range sys.failures {
		if sys.failures[i].Property == property {
			sys.failures[i].Repeats++
			return
		}
	}
	sys.failures = append(sys.failures, Failure{
		Kind: kind, Property: property, Detail: detail, Step: sys.steps, At: sys.q.now,
	})
	sys.logMsg("fail "+property+": "+detail, nil)
	if sys.cfg.HaltOnFirst {
		sys.halted = true
	}
}

// At runs fn as a host task at time t. Host code (clients, nemesis, phase
// changes) uses host tasks. A panic in fn halts the run.
func (sys *System) At(t Time, fn func()) { sys.at(t, noLane, fn) }

// After runs fn as a host task after d.
func (sys *System) After(d actor.Duration, fn func()) { sys.after(d, fn) }

// at queues fn as a host task at time t on lane and returns its entry id.
func (sys *System) at(t Time, lane laneID, fn func()) entryID {
	e := &entry{at: t, lane: lane, kind: kindHost, node: -1}
	e.run = func() {
		if !sys.guard("host task", fn) {
			sys.halted = true
		}
	}
	return sys.q.add(e)
}

// after queues fn as a host task after d, with no lane.
func (sys *System) after(d actor.Duration, fn func()) entryID {
	return sys.at(sys.q.now+Time(max(d, 0)), noLane, fn)
}

// Start boots every spawned actor: first the actors that are not workloads,
// in spawn order, then the workloads. Spawn every actor before Start; an
// actor spawned later stays down until Boot. RunUntil calls Start if it was not
// called.
func (sys *System) Start() {
	if sys.started {
		return
	}
	sys.started = true
	for _, s := range sys.actors {
		if !s.base().workload {
			s.boot(sys)
		}
	}
	for _, s := range sys.actors {
		if s.base().workload {
			s.boot(sys)
		}
	}
}

// RunUntil takes queue entries until stop returns true (checked before each
// entry; stop may be nil), the queue is empty, the next entry is after
// Config.End, the step limit is hit, or the run halted.
func (sys *System) RunUntil(stop func() bool) {
	sys.Start()
	for {
		if sys.halted {
			return
		}
		if stop != nil && stop() {
			return
		}
		at, ok := sys.q.peek()
		if !ok {
			return
		}
		if sys.cfg.End > 0 && at > sys.cfg.End {
			return
		}
		if sys.steps >= sys.cfg.MaxSteps {
			sys.Fail(FailMaxSteps, "max_steps", fmt.Sprintf("the run took more than %d steps", sys.cfg.MaxSteps))
			sys.halted = true
			return
		}
		e, _ := sys.q.pop()
		sys.steps++
		sys.foldEntry(e)
		e.run()
		for _, fn := range sys.onStep {
			if sys.halted {
				break
			}
			if !sys.guard("OnStep", fn) {
				sys.halted = true
			}
		}
		sys.foldState()
		sys.observe()
	}
}

// Run is Start, RunUntil(nil) and Finish.
func (sys *System) Run() Result {
	sys.RunUntil(nil)
	return sys.Finish()
}

// Finish runs Rule.AtEnd of every rule (unless the run halted) and returns
// the result. Calling it again returns the same result.
func (sys *System) Finish() Result {
	if sys.finished {
		return sys.result
	}
	sys.finished = true
	for _, r := range sys.rules {
		if sys.halted {
			break
		}
		if !sys.guard("rule AtEnd "+r.Name(), func() { r.AtEnd(view{sys}) }) {
			sys.halted = true
		}
	}
	res := Result{
		Seed: sys.cfg.Seed, Steps: sys.steps, RandCalls: sys.src.Calls(), Hash: sys.hash.Sum(),
		Crashes: sys.crashes, Trips: sys.trips, Exits: sys.exits,
		Failures: append([]Failure(nil), sys.failures...),
		Coverage: sys.asserts.Coverage(),
		Sites:    sys.faults.Sites(),
	}
	for _, l := range sys.lines {
		res.Log = append(res.Log, l.entry())
	}
	if len(sys.failures) > 0 && sys.tail != nil {
		for _, l := range sys.tail.tail() {
			res.Tail = append(res.Tail, l.entry())
		}
	}
	sys.result = res
	return res
}

// guard runs fn and turns a panic into a FailPanic. It reports whether fn
// returned normally.
func (sys *System) guard(what string, fn func()) bool {
	done := false
	defer func() {
		if !done {
			sys.Fail(FailPanic, "panic", what+": "+fmt.Sprint(recover()))
		}
	}()
	fn()
	done = true
	return true
}

// appendFact puts f on the timeline and folds it into the trace hash.
func (sys *System) appendFact(source actor.Incarnation, f actor.Fact) {
	sys.tl.Append(source, sys.q.now, f)
	f.Fold(&sys.hash)
}

// systemFact appends a fact of the System itself (source zero).
func (sys *System) systemFact(name string, fields ...actor.Field) {
	sys.appendFact(actor.Incarnation{}, actor.Fact{Name: name, Fields: fields})
}

// foldEntry folds one queue entry: time, kind, lane, target, then the body.
func (sys *System) foldEntry(e *entry) {
	h := &sys.hash
	h.Int64(int64(e.at))
	h.Uint64(uint64(e.kind))
	h.Uint64(uint64(e.lane))
	name := ""
	if e.node >= 0 {
		name = sys.actors[e.node].base().name
	}
	h.String(name)
	h.Bytes(e.inc[:])
	if e.body != nil {
		e.body.Fold(h)
	}
}

// foldState folds, for every actor, up or down and (if up) its state hash,
// inbox lengths, busy-until time and the number of its timers, awaited
// answers and watches; then the state hash of every Tracked. A cached
// hash is recomputed only when the System called into its owner.
func (sys *System) foldState() {
	h := &sys.hash
	for _, s := range sys.actors {
		b := s.base()
		h.Bool(b.up)
		if !b.up {
			continue
		}
		if !sys.cachedHash("actor "+b.name, &b.hashCache, s.stateHash) {
			return
		}
		c, d := s.inboxLens()
		h.Uint64(b.sum)
		h.Uint64(uint64(c))
		h.Uint64(uint64(d))
		h.Int64(int64(b.busy))
		timers, answers := b.counts()
		h.Uint64(uint64(timers))
		h.Uint64(uint64(answers))
		h.Uint64(uint64(sys.watchCount(b.idx, b.inc)))
	}
	for _, t := range sys.tracked {
		if !sys.cachedHash("tracked "+t.name, &t.hashCache, t.hash) {
			return
		}
		h.Uint64(t.sum)
	}
}

// hashCache is a cached state hash. It is valid while hashValid.
type hashCache struct {
	sum       uint64
	hashValid bool
}

// invalidateHash marks the cached StateHash stale so the next fold recomputes it.
func (c *hashCache) invalidateHash() {
	c.hashValid = false
}

// cachedHash brings c up to date. With Config.VerifyHashes it also checks a
// valid cache against a fresh hash. It reports false when the run halted.
func (sys *System) cachedHash(what string, c *hashCache, full func() uint64) bool {
	if c.hashValid && !sys.cfg.VerifyHashes {
		return true
	}
	var sum uint64
	if !sys.guard("StateHash "+what, func() { sum = full() }) {
		sys.halted = true
		return false
	}
	if c.hashValid && sum != c.sum {
		sys.Fail(FailProtocol, "stale_state_hash", what+": the state changed while the System did not call it")
		sys.halted = true
		return false
	}
	c.sum, c.hashValid = sum, true
	return true
}

// view is the View of a System.
type view struct{ sys *System }

func (v view) Step() uint64             { return v.sys.steps }
func (v view) Now() Time                { return v.sys.q.now }
func (v view) Timeline() *Timeline      { return v.sys.tl }
func (v view) Assert() actor.Assertions { return simAsserts{v.sys.asserts} }

// observe gives every rule the view after one queue pop.
func (sys *System) observe() {
	for _, r := range sys.rules {
		if sys.halted {
			return
		}
		if !sys.guard("rule Observe "+r.Name(), func() { r.Observe(view{sys}) }) {
			sys.halted = true
		}
	}
}
