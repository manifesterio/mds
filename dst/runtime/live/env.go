package live

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
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
	var seed [32]byte
	_, _ = rand.Read(seed[:])
	return &prodEnv{self: self, r: mrand.New(mrand.NewChaCha8(seed)), onFail: onFail} //nolint:gosec // ChaCha8 seeded from crypto/rand; not used for secrets
}

type prodEnv struct {
	self   actor.Incarnation
	r      *mrand.Rand
	onFail func(property, detail string)
}

func (e *prodEnv) Self() actor.Incarnation { return e.self }
func (e *prodEnv) Uint64() uint64          { return e.r.Uint64() }

func (e *prodEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return e.r.IntN(n)
}

func (e *prodEnv) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + e.r.Int64N(hi-lo+1)
}

func (e *prodEnv) Chance(p float64) bool { return p > 0 && e.r.Float64() < p }

func (e *prodEnv) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(e.Between(int64(lo), int64(hi)))
}

func (e *prodEnv) NewID() actor.ID {
	var id actor.ID
	binary.LittleEndian.PutUint64(id[:8], e.r.Uint64())
	binary.LittleEndian.PutUint64(id[8:], e.r.Uint64())
	return id
}

func (*prodEnv) Maybe(fault.Site) bool                                { return false }
func (*prodEnv) MaybeP(fault.Site, float64) bool                      { return false }
func (*prodEnv) Fault(fault.Site, float64) bool                       { return false }
func (*prodEnv) Sometimes(bool, assert.Sometimes)                     {}
func (*prodEnv) Reachable(assert.Reachable)                           {}
func (e *prodEnv) Unreachable(p assert.Unreachable, d ...assert.Pair) { e.fail(p.Name(), d) }

func (e *prodEnv) Always(c bool, p assert.Always, d ...assert.Pair) {
	if !c {
		e.fail(p.Name(), d)
	}
}

func (e *prodEnv) AlwaysOrUnreachable(c bool, p assert.AlwaysOrUnreachable, d ...assert.Pair) {
	if !c {
		e.fail(p.Name(), d)
	}
}

func (e *prodEnv) fail(name string, details []assert.Pair) {
	if e.onFail != nil {
		e.onFail(name, fmt.Sprint(details))
	}
}

// RealClock is the Clock of the process: time.Now and time.AfterFunc.
func RealClock() Clock { return realClock{start: time.Now()} }

type realClock struct{ start time.Time }

func (c realClock) Mono() actor.Mono { return actor.Mono(time.Since(c.start)) }
func (realClock) Wall() actor.Wall   { return actor.Wall(time.Now().UnixNano()) }

func (realClock) AfterFunc(d actor.Duration, f func()) Timer {
	return time.AfterFunc(time.Duration(d), f)
}

var _ actor.Env = (*prodEnv)(nil)
