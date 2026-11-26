// Package actortest is unit-test helpers for actor.Env and one-Receive outbox
// stepping. Product harnesses keep their IO answer sources as callbacks.
package actortest

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
)

// Config selects the random stream of NewEnv. Counter gives the sequence
// 1, 2, 3, ...; otherwise the stream is the SplitMix64 stream of Seed, which
// is the run stream of a System with that seed.
type Config struct {
	Self    actor.Incarnation
	Seed    uint64
	Stream  bool // no effect: the seeded stream is the run stream
	Counter bool
	Faults  actor.Faults // Maybe/MaybeP; nil means never
}

// Env is a test Env: seeded or counter random stream, optional FaultAlways,
// and recorded assertion names (Viol, Reach). Pass the embedded actor.Env to
// the code under test.
type Env struct {
	actor.Env

	seeded *rng.Rand // the stream unless Counter
	n      uint64    // the counter stream
	faults actor.Faults

	// FaultAlways makes Fault return true whenever p > 0.
	FaultAlways bool

	Viol  []string
	Reach []string
}

// counter is the Source 1, 2, 3, ...
type counter struct{ n *uint64 }

func (c counter) Uint64() uint64 {
	*c.n++
	return *c.n
}

// NewEnv builds a test Env from c.
func NewEnv(c Config) *Env {
	e := &Env{faults: c.Faults}
	var src actor.Source
	if c.Counter {
		src = counter{&e.n}
	} else {
		e.seeded = rng.New(c.Seed)
		src = e.seeded
	}
	e.Env = actor.NewEnv(c.Self, actor.NewRand(src), e, e)
	return e
}

// Reseed restarts the seeded stream from seed. It has no effect on a counter.
func (e *Env) Reseed(seed uint64) {
	if e.seeded != nil {
		*e.seeded = *rng.New(seed)
	}
}

// Maybe implements actor.Faults.
func (e *Env) Maybe(s actor.Site) bool {
	return e.faults != nil && e.faults.Maybe(s)
}

// MaybeP implements actor.Faults.
func (e *Env) MaybeP(s actor.Site, p float64) bool {
	return e.faults != nil && e.faults.MaybeP(s, p)
}

// Fault implements actor.Faults.
func (e *Env) Fault(_ actor.Site, p float64) bool { return e.FaultAlways && p > 0 }

// Always implements actor.Assertions.
func (e *Env) Always(c bool, p actor.Always, _ ...actor.Pair) {
	if !c {
		e.Viol = append(e.Viol, p.Name())
	}
}

// AlwaysOrUnreachable implements actor.Assertions.
func (e *Env) AlwaysOrUnreachable(c bool, p actor.AlwaysOrUnreachable, _ ...actor.Pair) {
	if !c {
		e.Viol = append(e.Viol, p.Name())
	}
}

// Sometimes implements actor.Assertions.
func (*Env) Sometimes(bool, actor.Sometimes) {}

// Reachable implements actor.Assertions.
func (e *Env) Reachable(p actor.Reachable) { e.Reach = append(e.Reach, p.Name()) }

// Unreachable implements actor.Assertions.
func (e *Env) Unreachable(p actor.Unreachable, _ ...actor.Pair) {
	e.Viol = append(e.Viol, p.Name())
}

var (
	_ actor.Faults     = (*Env)(nil)
	_ actor.Assertions = (*Env)(nil)
)
