// Package actortest is unit-test helpers for actor.Env and one-Receive outbox
// stepping. Product harnesses keep their IO answer sources as callbacks.
package actortest

import (
	"encoding/binary"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
)

// Config selects the RNG of NewEnv. Exactly one mode applies: Stream, else
// Counter, else SplitMix with Seed (plain modulo Intn, for harnesses that pin
// the stream).
type Config struct {
	Self    actor.Incarnation
	Seed    uint64 // run stream seed when Stream, SplitMix state when !Counter
	Stream  bool   // draw from the run stream of a System with Seed
	Counter bool
	Faults  actor.Faults // Maybe/MaybeP; nil means never
}

// Env is a test actor.Env: seeded or counter RNG, optional FaultAlways, and
// recorded assertion names (Viol, Reach).
type Env struct {
	self    actor.Incarnation
	State   uint64 // SplitMix state; tests may reseed
	counter bool
	n       uint64
	rand    *rng.Rand
	faults  actor.Faults

	// FaultAlways makes Fault return true whenever p > 0.
	FaultAlways bool

	Viol  []string
	Reach []string
}

// NewEnv builds a test Env from c.
func NewEnv(c Config) *Env {
	e := &Env{self: c.Self, faults: c.Faults}
	switch {
	case c.Stream:
		e.rand = rng.New(c.Seed)
	case c.Counter:
		e.counter = true
	default:
		e.State = c.Seed
	}
	return e
}

// Self implements actor.Env.
func (e *Env) Self() actor.Incarnation { return e.self }

// Uint64 implements actor.Rand.
func (e *Env) Uint64() uint64 {
	switch {
	case e.rand != nil:
		return e.rand.Uint64()
	case e.counter:
		e.n++
		return e.n
	default:
		e.State += 0x9e3779b97f4a7c15
		z := e.State
		z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
		z = (z ^ z>>27) * 0x94d049bb133111eb
		return z ^ z>>31
	}
}

// Intn implements actor.Rand.
func (e *Env) Intn(n int) int {
	if e.rand != nil {
		return e.rand.Intn(n)
	}
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}

// Between implements actor.Rand.
func (e *Env) Between(lo, hi int64) int64 {
	if e.rand != nil {
		return e.rand.Between(lo, hi)
	}
	if hi <= lo {
		return lo
	}
	return lo + int64(e.Intn(int(hi-lo+1)))
}

// Float64 implements actor.Rand.
func (e *Env) Float64() float64 {
	if e.rand != nil {
		return e.rand.Float64()
	}
	return float64(e.Uint64()>>11) / (1 << 53)
}

// Chance implements actor.Rand.
func (e *Env) Chance(p float64) bool {
	if e.rand != nil {
		return e.rand.Chance(p)
	}
	return p > 0 && float64(e.Uint64()%1000)/1000 < p
}

// Duration implements actor.Rand.
func (e *Env) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(e.Between(int64(lo), int64(hi)))
}

// NewID implements actor.Rand.
func (e *Env) NewID() actor.ID {
	var id actor.ID
	switch {
	case e.rand != nil:
		e.rand.Bytes(id[:])
	case e.counter:
		u := e.Uint64()
		for i := range id {
			id[i] = byte(u >> (8 * (i % 8)))
		}
	default:
		binary.LittleEndian.PutUint64(id[:8], e.Uint64())
		binary.LittleEndian.PutUint64(id[8:], e.Uint64())
	}
	return id
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

var _ actor.Env = (*Env)(nil)
