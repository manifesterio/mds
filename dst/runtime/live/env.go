package live

import (
	"cmp"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"time"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
)

// NewEnv makes a production Env: a random stream seeded from crypto/rand,
// buggify sites that never fire, and assertions that call onFail (when set)
// for a broken Always, AlwaysOrUnreachable or Unreachable. An actor uses its
// Env only on its own goroutine.
func NewEnv(self actor.Incarnation, onFail func(property, detail string)) actor.Env {
	return &chachaEnv{
		self: self,
		src:  newChaCha8(),
		onViol: func(_, prop string, d []assert.Pair) {
			if onFail != nil {
				onFail(prop, fmt.Sprint(d))
			}
		},
	}
}

// NewLoggingEnv is a production Env like NewEnv that logs assertion violations
// with slog (kind, prop, and detail key/value pairs) and continues. If log is
// nil, slog.Default is used.
func NewLoggingEnv(self actor.Incarnation, log *slog.Logger) actor.Env {
	log = cmp.Or(log, slog.Default())
	return &chachaEnv{
		self: self,
		src:  newChaCha8(),
		onViol: func(kind, prop string, d []assert.Pair) {
			args := []any{"kind", kind, "prop", prop}
			for _, x := range d {
				args = append(args, x.Key, x.Value)
			}
			log.Error("assertion violated", args...)
		},
	}
}

// NewCounterEnv is a production Env with a sequential counter for random
// numbers (replicas that draw only for fault injection, which is off) and
// inert buggify and asserts.
func NewCounterEnv(self actor.Incarnation) actor.Env {
	return &counterEnv{self: self}
}

func newChaCha8() *mrand.ChaCha8 {
	var seed [32]byte
	_, _ = rand.Read(seed[:])
	return mrand.NewChaCha8(seed)
}

// between returns a value in [lo, hi] from a drawn uint64. Callers must not
// call it when hi <= lo. span == 0 covers the full int64 range without a
// divide-by-zero.
func between(u uint64, lo, hi int64) int64 {
	span := uint64(hi-lo) + 1
	if span == 0 {
		return int64(u)
	}
	return lo + int64(u%span)
}

type chachaEnv struct {
	self   actor.Incarnation
	src    *mrand.ChaCha8
	onViol func(kind, prop string, d []assert.Pair)
}

func (e *chachaEnv) Self() actor.Incarnation { return e.self }
func (e *chachaEnv) Uint64() uint64          { return e.src.Uint64() }

func (e *chachaEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}

func (e *chachaEnv) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return between(e.Uint64(), lo, hi)
}

func (e *chachaEnv) Float64() float64 { return float64(e.Uint64()>>11) / (1 << 53) }

func (e *chachaEnv) Chance(p float64) bool {
	if p <= 0 {
		return false
	}
	return float64(e.Uint64()>>11)/(1<<53) < p
}

func (e *chachaEnv) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(e.Between(int64(lo), int64(hi)))
}

func (e *chachaEnv) NewID() actor.ID {
	var id actor.ID
	binary.LittleEndian.PutUint64(id[:8], e.Uint64())
	binary.LittleEndian.PutUint64(id[8:], e.Uint64())
	return id
}

func (*chachaEnv) Maybe(fault.Site) bool            { return false }
func (*chachaEnv) MaybeP(fault.Site, float64) bool  { return false }
func (*chachaEnv) Fault(fault.Site, float64) bool   { return false }
func (*chachaEnv) Sometimes(bool, assert.Sometimes) {}
func (*chachaEnv) Reachable(assert.Reachable)       {}

func (e *chachaEnv) Unreachable(p assert.Unreachable, d ...assert.Pair) {
	e.viol(string(assert.KindUnreachable), p.Name(), d)
}

func (e *chachaEnv) Always(c bool, p assert.Always, d ...assert.Pair) {
	if !c {
		e.viol(string(assert.KindAlways), p.Name(), d)
	}
}

func (e *chachaEnv) AlwaysOrUnreachable(c bool, p assert.AlwaysOrUnreachable, d ...assert.Pair) {
	if !c {
		e.viol(string(assert.KindAlwaysOrUnreachable), p.Name(), d)
	}
}

func (e *chachaEnv) viol(kind, name string, d []assert.Pair) {
	if e.onViol != nil {
		e.onViol(kind, name, d)
	}
}

// counterEnv is sequential-counter RNG with inert faults and asserts.
type counterEnv struct {
	self actor.Incarnation
	n    uint64
}

func (e *counterEnv) Self() actor.Incarnation { return e.self }

func (e *counterEnv) Uint64() uint64 {
	e.n++
	return e.n
}

func (e *counterEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}

func (e *counterEnv) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(e.Intn(int(hi-lo+1)))
}

func (e *counterEnv) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(e.Between(int64(lo), int64(hi)))
}

func (e *counterEnv) Float64() float64 { return float64(e.Uint64()>>11) / (1 << 53) }

func (e *counterEnv) Chance(p float64) bool {
	return p > 0 && float64(e.Uint64()%1000) < p*1000
}

func (e *counterEnv) NewID() actor.ID {
	var id actor.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}

func (*counterEnv) Maybe(fault.Site) bool                      { return false }
func (*counterEnv) MaybeP(fault.Site, float64) bool            { return false }
func (*counterEnv) Fault(fault.Site, float64) bool             { return false }
func (*counterEnv) Always(bool, assert.Always, ...assert.Pair) {}
func (*counterEnv) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {
}
func (*counterEnv) Sometimes(bool, assert.Sometimes)               {}
func (*counterEnv) Reachable(assert.Reachable)                     {}
func (*counterEnv) Unreachable(assert.Unreachable, ...assert.Pair) {}

// RealClock is the Clock of the process: time.Now and time.AfterFunc.
func RealClock() Clock { return realClock{start: time.Now()} }

type realClock struct{ start time.Time }

func (c realClock) Mono() actor.Mono { return actor.Mono(time.Since(c.start)) }
func (realClock) Wall() actor.Wall   { return actor.Wall(time.Now().UnixNano()) }

func (realClock) AfterFunc(d actor.Duration, f func()) Timer {
	return time.AfterFunc(time.Duration(d), f)
}

var (
	_ actor.Env = (*chachaEnv)(nil)
	_ actor.Env = (*counterEnv)(nil)
)
