// Package faulty turns a fault-free spec into a simulated service (a
// sim.Service).
//
// A spec is a small program with two methods: Apply runs one op and gives the
// right answer, and StateHash hashes its whole state. The spec never fails and
// never waits. Faulty puts timing and faults around it, so that the code under
// test meets a service that is slow, that refuses calls, that times out and that
// loses answers, as a real outside system does.
//
// The faults are exactly these, and nothing else:
//
//   - Slow reply: the apply time and the respond time are drawn from a delay that
//     is mostly fast and has a rare slow tail (Faults.ApplyLatency, RespondLatency).
//   - Error, nothing applied (the "503"): the call is refused, the spec never sees
//     it, and the caller gets Unavailable (Faults.Unavailable503).
//   - Timeout: the caller gets TimedOut after CallerTimeout. The op is applied or
//     not, by a fair coin drawn when it lands (Faults.Timeout).
//   - Applied, but the reply is lost: the op is applied, and the caller gets
//     TimedOut after CallerTimeout (Faults.LostResponse).
//   - Outage window: for a time, every call is refused as in the "503" case
//     (Faults.Outage).
//   - Optional, set in Shape: a limit that refuses some ops before the spec sees
//     them (Shape.Limit), and a bit flip in the answer of some ops
//     (Shape.Corrupt).
//
// All coins and delays are drawn when the call is issued, in a fixed order (see
// Service.Handle), so one seed always gives the same run. After EnterRecovery no
// new fault is drawn. A call that was planned before stays as planned.
//
// This package has no product words. The product gives it the spec, the fault
// numbers, and a Shape that says how to read a request and how to build an answer.
package faulty

import (
	"slices"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/dst/sim"
)

// Spec is the fault-free program that gives the right answer. It has the same
// two methods as a check/spectest.Spec, so one spec serves both.
type Spec[Op, Res any] interface {
	Apply(op Op) Res
	StateHash() uint64
}

// Failure says how a call failed. The product turns it into its own error value.
type Failure uint8

const (
	// Unavailable means that the service refused the call and applied nothing.
	Unavailable Failure = iota + 1
	// TimedOut means that the caller gave up. The op may or may not have been applied.
	TimedOut
)

// Outage is a window in which every call is refused. A window starts by a coin
// with chance Prob when a call is issued and no window is open. It lasts a time
// drawn from Dur (the end is not included). Windows never overlap.
type Outage struct {
	Prob float64
	Dur  [2]actor.Duration
}

// Faults are the numbers of the fault kinds. A chance of 0 turns a kind off.
type Faults struct {
	ApplyLatency   sim.FastWithRareSlow // from issue to the effect
	RespondLatency sim.FastWithRareSlow // from the effect to the reply
	Unavailable503 float64              // chance that a call is refused, nothing applied
	Timeout        float64              // chance of a timeout; the op may or may not be applied
	LostResponse   float64              // chance that the op is applied and the reply is lost
	Outage         Outage
	CallerTimeout  actor.Duration // when the caller sees TimedOut
}

// Asserts are the assertions of the fault kinds. The product declares them at
// package level (the assertion catalog needs that), with names it chooses --
// or with NamedAsserts for the usual naming slots of a Prefix.
type Asserts struct {
	Outage      assert.Reachable // an outage window started
	Unavailable assert.Reachable // a call was refused
	Timeout     assert.Reachable // a timeout was injected
	Lost        assert.Reachable // a reply was lost
	Applied     assert.Sometimes // a timed-out op was applied
	NotApplied  assert.Sometimes // a timed-out op was not applied
}

// NamedAsserts declares the six Asserts handles with the usual names for a
// fault Prefix. Call it only in a package-level var block, like
// assert.NewReachable. The names are "{prefix} outage started",
// "{prefix} 503 injected", "{prefix} timeout injected",
// "{prefix} lost response injected", "{prefix} timed-out op applied" and
// "{prefix} timed-out op not applied".
func NamedAsserts(prefix string) Asserts {
	return Asserts{
		Outage:      assert.NewReachable(prefix + " outage started"),
		Unavailable: assert.NewReachable(prefix + " 503 injected"),
		Timeout:     assert.NewReachable(prefix + " timeout injected"),
		Lost:        assert.NewReachable(prefix + " lost response injected"),
		Applied:     assert.NewSometimes(prefix + " timed-out op applied"),
		NotApplied:  assert.NewSometimes(prefix + " timed-out op not applied"),
	}
}

// Corruption flips a bit in the answer of some ops, once in a while. The spec
// state stays whole: only the copy that goes to the caller changes.
type Corruption[Op, Res any] struct {
	Prob    float64          // chance, drawn for each op that Targets accepts
	Targets func(op Op) bool // which ops can be corrupted (reads, for example)
	// Flip flips the bit with number bits (taken modulo the size of the data) in
	// res. It reports the bit it flipped, or false when res has nothing to flip.
	// The answer of Apply must be a copy, so Flip may change it in place.
	Flip    func(res Res, bits uint64) (out Res, bit uint64, ok bool)
	Kind    string           // the name of the fault fact, for example "obj_get_corrupt"
	Reached assert.Reachable // recorded when a bit was flipped
}

// Shape tells Faulty how to read a request and how to build an answer. It is
// the only place where the product types meet the spec types.
type Shape[Req, Rep actor.Payload, Op, Res any] struct {
	// Name is the service name. It is also the name in fault facts.
	Name string
	// Prefix starts the names of the fault facts: Prefix+"_outage",
	// Prefix+"_503", Prefix+"_timeout" and Prefix+"_lost_response".
	Prefix  string
	Asserts Asserts
	// Op returns the spec op inside a request.
	Op func(req Req) Op
	// Mutating reports whether op may change the spec state. A read needs no
	// landing when its answer is dropped.
	Mutating func(op Op) bool
	// Changed reports whether the answer res says that the spec state changed.
	Changed func(res Res) bool
	// Reply builds the normal answer.
	Reply func(req Req, res Res) Rep
	// Fail builds the answer for a failure.
	Fail func(req Req, f Failure) Rep
	// Limit is optional. It runs when an op lands, before the spec. When it
	// returns true, the answer is the returned one and the spec never sees op.
	Limit func(op Op) (Res, bool)
	// Corrupt is optional.
	Corrupt *Corruption[Op, Res]
}

// ShapeOf returns a Shape with Name, Prefix and Asserts set. The product fills
// Op, Mutating, Changed, Reply and Fail (and optionally Limit and Corrupt).
func ShapeOf[Req, Rep actor.Payload, Op, Res any](name, prefix string, a Asserts) Shape[Req, Rep, Op, Res] {
	return Shape[Req, Rep, Op, Res]{Name: name, Prefix: prefix, Asserts: a}
}

// How an issued call ends.
type kind uint8

const (
	replyNormal      kind = iota // apply at t1, reply at t2
	replyUnavailable             // refused, nothing applied
	replyTimeout                 // the caller sees TimedOut; applied or not, drawn at landing
	replyLost                    // applied at t1; the caller sees TimedOut
)

// plan is one call in flight.
type plan[Req, Rep actor.Payload] struct {
	id     uint64
	reply  sim.Responder[Rep]
	req    Req
	kind   kind
	t1, t2 actor.Mono
	flag   bool   // the corruption coin came up
	bits   uint64 // which bit, before the modulo
}

// Service is the spec with faults around it. It is a sim.Service.
type Service[Req, Rep actor.Payload, Op, Res any] struct {
	shape  Shape[Req, Rep, Op, Res]
	faults Faults
	spec   Spec[Op, Res]

	nextID   uint64
	plans    []plan[Req, Rep] // in issue order
	until    actor.Mono       // the end of the outage window
	recovery bool
	gen      uint64
	// specHash caches spec.StateHash(). It is valid only while specOK. Every path
	// that can change the spec clears specOK (landing a mutating op, Bump).
	specHash uint64
	specOK   bool
}

// New returns a service around spec. Add it to a System with sim.AddService.
func New[Req, Rep actor.Payload, Op, Res any](spec Spec[Op, Res], f Faults, s Shape[Req, Rep, Op, Res]) *Service[Req, Rep, Op, Res] {
	return &Service[Req, Rep, Op, Res]{shape: s, faults: f, spec: spec}
}

// Gen counts the changes that were applied. Bump adds a change made outside a
// call, for example a setup step on the spec.
func (s *Service[Req, Rep, Op, Res]) Gen() uint64 { return s.gen }

// Bump records a change of the spec made outside a call. It also clears the
// cached spec hash.
func (s *Service[Req, Rep, Op, Res]) Bump() { s.gen++; s.specOK = false }

// specStateHash is the spec hash, computed again only after a change.
func (s *Service[Req, Rep, Op, Res]) specStateHash() uint64 {
	if !s.specOK {
		s.specHash, s.specOK = s.spec.StateHash(), true
	}
	return s.specHash
}

// SpecStateHash is the cached spec hash. A test can compare it with the spec's
// own StateHash: a stale cache shows as a difference.
func (s *Service[Req, Rep, Op, Res]) SpecStateHash() uint64 { return s.specStateHash() }

// Name returns the service name.
func (s *Service[Req, Rep, Op, Res]) Name() string { return s.shape.Name }

// EnterRecovery stops new faults for the calls issued after it.
func (s *Service[Req, Rep, Op, Res]) EnterRecovery() { s.recovery = true }

// between returns a value in [lo, hi). It draws once when hi > lo, even when
// there is only one value, and never when hi <= lo.
func between(r actor.Rand, lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo)))
}

// draw draws the coins of the boundary faults: the outage window, then the
// three coins "503", timeout and lost, always all three and in this order. It
// records the fault that wins. A refusal beats a timeout, and a timeout beats a
// lost reply.
func (s *Service[Req, Rep, Op, Res]) draw(ctx *sim.ServiceCtx, request actor.Field) kind {
	r, now, f, pre, a := ctx, ctx.Now(), s.faults, s.shape.Prefix, s.shape.Asserts
	if now >= s.until && r.Chance(f.Outage.Prob) {
		d := f.Outage.Dur[0]
		if f.Outage.Dur[1] > d {
			d = actor.Duration(between(r, int64(f.Outage.Dur[0]), int64(f.Outage.Dur[1])))
		}
		s.until = now + actor.Mono(d)
		ctx.RecordFault(pre+"_outage", actor.Field{Key: "dur", Value: actor.Int64Value(d)})
		ctx.Reachable(a.Outage)
	}
	inOutage := now < s.until
	is503, isTimeout, isLost := r.Chance(f.Unavailable503), r.Chance(f.Timeout), r.Chance(f.LostResponse)
	switch {
	case inOutage || is503:
		cause := "coin"
		if inOutage {
			cause = "outage"
		}
		ctx.RecordFault(pre+"_503", request, actor.Field{Key: "cause", Value: actor.StringValue(cause)})
		ctx.Reachable(a.Unavailable)
		return replyUnavailable
	case isTimeout:
		ctx.RecordFault(pre+"_timeout", request)
		ctx.Reachable(a.Timeout)
		return replyTimeout
	case isLost:
		ctx.RecordFault(pre+"_lost_response", request)
		ctx.Reachable(a.Lost)
		return replyLost
	}
	return replyNormal
}

// Handle plans one call. It draws, in this order: the outage coin (and the
// outage length), the three coins "503", timeout and lost, the apply delay,
// the respond delay, and the corruption coin (and the bit). After
// EnterRecovery, only the two delays are drawn. Fault facts name the call by
// its number in this service ("req").
func (s *Service[Req, Rep, Op, Res]) Handle(ctx *sim.ServiceCtx, req Req, reply sim.Responder[Rep]) {
	r, now, f := ctx, ctx.Now(), s.faults
	op := s.shape.Op(req)
	s.nextID++
	id := s.nextID
	request := actor.Field{Key: "req", Value: actor.Uint64Value(id)}
	k := replyNormal
	if !s.recovery {
		k = s.draw(ctx, request)
	}
	t1 := now + actor.Mono(f.ApplyLatency.Sample(r))
	t2 := t1 + actor.Mono(f.RespondLatency.Sample(r))
	var flag bool
	var bits uint64
	if c := s.shape.Corrupt; !s.recovery && c != nil && c.Targets(op) && r.Chance(c.Prob) {
		flag, bits = true, r.Uint64()
	}
	pl := plan[Req, Rep]{id: id, reply: reply, req: req, kind: k, t1: t1, t2: t2, flag: flag, bits: bits}
	switch k {
	case replyUnavailable:
		reply.After(actor.Duration(t2-now), s.shape.Fail(req, Unavailable))
		return
	case replyTimeout, replyLost:
		reply.After(f.CallerTimeout, s.shape.Fail(req, TimedOut))
		if !s.shape.Mutating(op) {
			return // nothing to apply, and the caller already has its answer
		}
	case replyNormal:
	}
	ctx.Later(actor.Duration(t1-now), func(ctx *sim.ServiceCtx) { s.land(ctx, id) })
	s.plans = append(s.plans, pl)
}

// land lands one planned call: it runs the op on the spec and sends the reply.
func (s *Service[Req, Rep, Op, Res]) land(ctx *sim.ServiceCtx, id uint64) {
	i := slices.IndexFunc(s.plans, func(p plan[Req, Rep]) bool { return p.id == id })
	if i < 0 {
		return
	}
	pl := s.plans[i]
	s.plans = slices.Delete(s.plans, i, i+1)
	apply := true
	if pl.kind == replyTimeout {
		apply = ctx.Chance(0.5)
		ctx.Sometimes(apply, s.shape.Asserts.Applied)
		ctx.Sometimes(!apply, s.shape.Asserts.NotApplied)
	}
	if !apply {
		return
	}
	res := s.apply(s.shape.Op(pl.req))
	if pl.kind != replyNormal || !pl.reply.Wanted() {
		return
	}
	if c := s.shape.Corrupt; c != nil && pl.flag {
		if out, bit, ok := c.Flip(res, pl.bits); ok {
			res = out
			ctx.RecordFault(c.Kind, actor.Field{Key: "req", Value: actor.Uint64Value(pl.id)},
				actor.Field{Key: "bit", Value: actor.Uint64Value(bit)})
			ctx.Reachable(c.Reached)
		}
	}
	pl.reply.After(actor.Duration(max(pl.t2, ctx.Now())-ctx.Now()), s.shape.Reply(pl.req, res))
}

// apply runs op on the spec (or the limit) and keeps the change count.
func (s *Service[Req, Rep, Op, Res]) apply(op Op) Res {
	var res Res
	refused := false
	if s.shape.Limit != nil {
		res, refused = s.shape.Limit(op)
	}
	if !refused {
		res = s.spec.Apply(op)
	}
	if s.shape.Mutating(op) {
		s.specOK = false // whatever Changed says, the cache must not outlive a mutating call
	}
	if s.shape.Changed(res) {
		s.gen++
		s.specOK = false // a change that Mutating did not name must not leave a stale hash
	}
	return res
}

// StateHash folds the spec hash, the outage window, the recovery flag and the
// calls in flight, in issue order.
func (s *Service[Req, Rep, Op, Res]) StateHash() uint64 {
	h := trace.New()
	mix := h.Uint64
	mix(s.specStateHash())
	mix(uint64(s.until))
	h.Bool(s.recovery)
	mix(uint64(len(s.plans)))
	for _, p := range s.plans {
		mix(p.id)
		mix(uint64(p.kind))
		mix(uint64(p.t1))
		mix(uint64(p.t2))
		mix(p.bits)
	}
	return h.Sum()
}

var _ sim.Service[actor.Payload, actor.Payload] = (*Service[actor.Payload, actor.Payload, int, int])(nil)
