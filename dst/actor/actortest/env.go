// Package actortest is unit-test helpers for actor.Env and one-Receive outbox
// stepping. Product harnesses keep their IO answer sources as callbacks.
package actortest

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
)

// Env is a test Env: the SplitMix64 stream of its seed (the run stream of a
// System with that seed), optional FaultAlways, and recorded assertion names
// (Viol, Reach). Pass the embedded actor.Env to the code under test.
type Env struct {
	actor.Env

	rand   *rng.Rand
	faults actor.Faults

	// FaultAlways makes Fault return true whenever p > 0.
	FaultAlways bool

	Viol  []string
	Reach []string
}

// Option configures NewEnv.
type Option func(*Env, *actor.Incarnation)

// WithSelf sets the incarnation of the Env.
func WithSelf(self actor.Incarnation) Option {
	return func(_ *Env, s *actor.Incarnation) { *s = self }
}

// WithFaults makes Maybe and MaybeP answer from f; without it they never fire.
func WithFaults(f actor.Faults) Option {
	return func(e *Env, _ *actor.Incarnation) { e.faults = f }
}

// NewEnv builds a test Env on the stream of seed.
func NewEnv(seed uint64, opts ...Option) *Env {
	e := &Env{rand: rng.New(seed)}
	var self actor.Incarnation
	for _, o := range opts {
		o(e, &self)
	}
	e.Env = actor.NewEnv(self, actor.NewRand(e.rand), e, e)
	return e
}

// Reseed restarts the stream from seed.
func (e *Env) Reseed(seed uint64) { *e.rand = *rng.New(seed) }

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
