// Package sim runs actors in a deterministic simulation: one seed, one
// random stream, one event queue, virtual time. It carries out the effects
// actors record on their Outbox (package actor), simulates outside systems
// as Services and product IO with Executors, and folds every step into a
// trace hash, so one seed always gives one hash.
//
// Test code only: production runs the same actors on dst/runtime/live.
package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
	"github.com/manifesterio/mds/dst/core/rng"
	"github.com/manifesterio/mds/dst/core/trace"
)

// Time is global virtual time in nanoseconds since the run started. Actors
// never see it directly: they see actor.Now, with the clock skew of their
// node.
type Time int64

// Lane is an ordering group of the event queue. Entries on one lane at one
// time run in the order they were added (FIFO).
type Lane uint64

// EntryID names one queue entry, so that it can be cancelled.
type EntryID uint64

// The lanes the System gives a meaning to.
const (
	// NoLane is no FIFO group: every entry gets a fresh random tie.
	NoLane Lane = 0
	// HostLane is the lane of the System itself (restarts, kills, phases).
	// Lanes at or above it belong to the System: it keeps one wake-up lane
	// per actor there. Pair lanes (one per sender and receiver) are just
	// below it; host code may use the lanes below those.
	HostLane Lane = 1 << 62
)

const (
	defaultMaxSteps  uint64 = 10_000_000
	defaultStopGrace        = 5 * actor.Second
)

// Config configures one run.
type Config struct {
	Seed         uint64
	WallStart    actor.Wall     // UTC at time zero
	End          Time           // stop before the first entry after End; 0: no end
	MaxSteps     uint64         // 0 means 10_000_000; more steps is FailMaxSteps
	ClockSkewMax actor.Duration // per-node clock skew in [0, ClockSkewMax], drawn once at Spawn
	Attrition    AttritionPlan
	Log          bool // keep every step line in Result.Log
	Tail         int  // keep the last Tail step lines for Result.Tail of a failed run
	HaltOnFirst  bool // stop the run at the first failure
	VerifyHashes bool // recompute cached state hashes every step; a mismatch fails the seed
}

// Stopped says why RunUntil returned.
type Stopped uint8

// The reasons RunUntil returns.
const (
	StopAsked Stopped = iota // the stop function returned true
	Drained                  // the queue is empty
	TimeUp                   // the next entry is after Config.End
	StepLimit                // Config.MaxSteps was reached (a FailMaxSteps)
	Halted                   // the run halted (HaltOnFirst, or a panic outside a node)
)

// FailKind says what kind of failure a seed had. It is not folded into the
// trace hash.
type FailKind uint8

// The kinds of failure.
const (
	FailViolation     FailKind = iota // an Always, AlwaysOrUnreachable or Unreachable broke
	FailProtocol                      // the API was used the wrong way (effect after Stop, zero address, ...)
	FailPanic                         // a panic in a Receive, New, an executor, a service, a host task or a rule
	FailMaxSteps                      // the run took more than Config.MaxSteps steps
	FailNotRepeatable                 // the same seed gave another run (set by the campaign canary)
)

// Failure is the first failure of one property in a run. Later failures of
// the same property only raise Repeats.
type Failure struct {
	Seed     uint64
	Kind     FailKind
	Property string
	Detail   string
	Step     uint64
	At       Time
	Repeats  int
}

// Counter is one run statistic.
type Counter struct {
	Name string
	N    uint64
}

// LogEntry is one step line. Its text is never hashed.
type LogEntry struct {
	Step uint64
	At   Time
	Text string
}

// Result is one run. (Steps, RandCalls, Hash, Stats) is what a golden pin holds.
type Result struct {
	Seed, Steps, RandCalls, Hash uint64
	Crashes, Trips, Exits        int
	Stats                        []Counter
	Failures                     []Failure
	Coverage                     map[string]assert.Coverage
	Sites                        map[string]fault.SiteCount
	Log                          []LogEntry // every step, only with Config.Log
	Tail                         []LogEntry // the last Config.Tail steps, only on failure
}

// OK reports a run without failures.
func (r Result) OK() bool { return len(r.Failures) == 0 }

// Counter indexes. The names and their order are the ones of the old broker
// kernel (without "freezes": freeze is deleted).
const (
	countNodeSteps = iota
	countServiceSteps
	countKernelSteps
	countDeliveries
	countDroppedStale
	countDroppedByCrash
	countTimersSet
	countTimersCancelled
	countFaults
	countWakes
	countRestarts
	countGracefulStops
	countDeadlineKills
	counterCount
)

func counterNames() [counterCount]string {
	return [counterCount]string{
		countNodeSteps:       "node_steps",
		countServiceSteps:    "engine_steps",
		countKernelSteps:     "sim_steps",
		countDeliveries:      "deliveries",
		countDroppedStale:    "dropped_stale",
		countDroppedByCrash:  "dropped_by_crash",
		countTimersSet:       "timers_set",
		countTimersCancelled: "timers_cancelled",
		countFaults:          "faults",
		countWakes:           "wakes",
		countRestarts:        "restarts",
		countGracefulStops:   "graceful_stops",
		countDeadlineKills:   "deadline_kills",
	}
}

// System is one simulation run: one seed, one random stream, one event queue,
// the actors, the services and the rules.
// It is not safe for concurrent use. Use one System per run.
type System struct {
	cfg     Config
	rand    *rng.Rand
	q       *queue
	faults  *fault.Injector
	asserts *assert.Log
	hash    trace.Hash
	tl      *Timeline

	owner    *actor.Owner
	actors   []slot
	names    map[string]bool
	services []serviceSlot
	svcNames map[string]bool
	rules    []Rule

	pairLanes    map[laneKey2]Lane
	nextPairLane Lane
	watches      []watch
	outside      map[uint64]*outsideAsk
	outsideAsks  uint64

	// plantKeepFiredTimers is a planted bug, set only by tests.
	plantKeepFiredTimers bool

	phase    Phase
	steps    uint64
	attr     int // attrition rolls to make after this queue pop
	halted   bool
	started  bool
	finished bool
	result   Result

	counters              [counterCount]uint64
	crashes, trips, exits int
	failures              []Failure
	lines                 []logLine
	tail                  *trace.Ring[logLine]
}

// NewSystem makes a System for cfg.
func NewSystem(cfg Config) *System {
	if cfg.MaxSteps == 0 {
		cfg.MaxSteps = defaultMaxSteps
	}
	sys := &System{
		cfg:       cfg,
		rand:      rng.New(cfg.Seed),
		hash:      trace.New(),
		tl:        NewTimeline(),
		owner:     actor.NewOwner(),
		names:     map[string]bool{},
		svcNames:  map[string]bool{},
		pairLanes: map[laneKey2]Lane{},
		outside:   map[uint64]*outsideAsk{},
	}
	sys.q = newQueue(sys.rand)
	sys.faults = fault.New(sys.rand)
	sys.asserts = assert.New(func(property, detail string) { sys.Fail(FailViolation, property, detail) })
	sys.asserts.SetStepCounter(sys.Steps)
	if cfg.Tail > 0 {
		sys.tail = trace.NewRing[logLine](cfg.Tail)
	}
	return sys
}

// Now is the current virtual time.
func (sys *System) Now() Time { return sys.q.now }

// Wall is the UTC time now: Config.WallStart plus Now.
func (sys *System) Wall() actor.Wall { return sys.cfg.WallStart + actor.Wall(sys.q.now) }

// Steps is the number of queue entries taken so far.
func (sys *System) Steps() uint64 { return sys.steps }

// Rand is the one random stream of the run.
func (sys *System) Rand() *rng.Rand { return sys.rand }

// Env is the run's Rand, Faults and Assertions for host code. Its Self is
// the zero Incarnation.
func (sys *System) Env() actor.Env { return env{sys: sys} }

// Faults is the buggify injector of the run.
func (sys *System) Faults() *fault.Injector { return sys.faults }

// Assert is the assertion log of the run.
func (sys *System) Assert() *assert.Log { return sys.asserts }

// Logging reports Config.Log.
func (sys *System) Logging() bool { return sys.cfg.Log }

// Timeline is the fact timeline of the run.
func (sys *System) Timeline() *Timeline { return sys.tl }

// Phase is the current phase.
func (sys *System) Phase() Phase { return sys.phase }

// Note adds a log line. It is never hashed.
func (sys *System) Note(text string) { sys.logMsg(text, nil) }

// Fact appends a host fact (source zero) to the timeline.
func (sys *System) Fact(f actor.Fact) { sys.appendFact(actor.Incarnation{}, f) }

// Cancel retires a pending queue entry. An entry that already ran, or an
// unknown id, is ignored.
func (sys *System) Cancel(id EntryID) { sys.q.cancel(id) }

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
		Seed: sys.cfg.Seed, Kind: kind, Property: property, Detail: detail, Step: sys.steps, At: sys.q.now,
	})
	sys.logMsg("fail "+property+": "+detail, nil)
	if sys.cfg.HaltOnFirst {
		sys.halted = true
	}
}

// At runs fn as a host task at time t on lane. Host code (clients, nemesis,
// phase changes) uses host tasks. A panic in fn halts the run.
func (sys *System) At(t Time, lane Lane, fn func()) EntryID {
	e := &entry{at: t, lane: lane, kind: kindHost, node: -1}
	e.run = func() {
		sys.counters[countKernelSteps]++
		if !sys.guard("host task", fn) {
			sys.halted = true
		}
	}
	return sys.q.add(e)
}

// After runs fn as a host task after d, with no lane.
func (sys *System) After(d actor.Duration, fn func()) EntryID {
	return sys.At(sys.q.now+Time(max(d, 0)), NoLane, fn)
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
			s.boot()
		}
	}
	for _, s := range sys.actors {
		if s.base().workload {
			s.boot()
		}
	}
}

// RunUntil takes queue entries until stop returns true (checked before each
// entry; stop may be nil), the queue is empty, the next entry is after
// Config.End, the step limit is hit, or the run halted.
func (sys *System) RunUntil(stop func() bool) Stopped {
	sys.Start()
	for {
		if sys.halted {
			return Halted
		}
		if stop != nil && stop() {
			return StopAsked
		}
		at, ok := sys.q.peek()
		if !ok {
			return Drained
		}
		if sys.cfg.End > 0 && at > sys.cfg.End {
			return TimeUp
		}
		if sys.steps >= sys.cfg.MaxSteps {
			sys.Fail(FailMaxSteps, "max_steps", fmt.Sprintf("the run took more than %d steps", sys.cfg.MaxSteps))
			sys.halted = true
			return StepLimit
		}
		e, _ := sys.q.pop()
		sys.steps++
		sys.foldEntry(e)
		e.run()
		for sys.attr > 0 && !sys.halted {
			sys.attr--
			sys.attrition()
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
		Seed: sys.cfg.Seed, Steps: sys.steps, RandCalls: sys.rand.Calls(), Hash: sys.hash.Sum(),
		Crashes: sys.crashes, Trips: sys.trips, Exits: sys.exits,
		Failures: append([]Failure(nil), sys.failures...),
		Coverage: sys.asserts.Coverage(),
		Sites:    sys.faults.Sites(),
	}
	for i, name := range counterNames() {
		res.Stats = append(res.Stats, Counter{Name: name, N: sys.counters[i]})
	}
	for _, l := range sys.lines {
		res.Log = append(res.Log, l.entry())
	}
	if len(sys.failures) > 0 && sys.tail != nil {
		for _, l := range sys.tail.Tail() {
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
	h.String(e.service)
	if e.body != nil {
		e.body.Fold(h)
	}
}

// foldState folds, for every actor, up or down and (if up) its state hash,
// inbox lengths, busy-until time and the number of its timers, awaited
// answers and watches; then the state hash of every service. A cached
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
	for _, s := range sys.services {
		b := s.base()
		if !sys.cachedHash("service "+b.name, &b.hashCache, s.stateHash) {
			return
		}
		h.Uint64(b.sum)
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
