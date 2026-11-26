// Package clock has the two implementations of live.Clock that the broker and
// the control plane share outside the simulator:
//
//   - Real is the production clock. It only converts types over shared/clock/internal/realclock.
//     It is nondeterministic by design and is not in the determinism gate.
//   - Fake is the test clock. It only converts types: the timers, the lock and
//     Advance are shared/clock/internal/syncclock. Time moves only when the caller calls Advance.
//     Do not use it inside the simulator, which has its own single-goroutine clock.
package clock

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
	dstreal "github.com/manifesterio/mds/shared/clock/internal/realclock"
	"github.com/manifesterio/mds/shared/clock/internal/sched"
)

// Real is the process clock.
type Real struct{ dstreal.Clock }

var _ live.Clock = Real{}

// NewReal returns a Real clock anchored at the current time.
func NewReal() Real { return Real{dstreal.New()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Real) Mono() actor.Mono { return actor.Mono(c.Clock.Mono()) }

// Wall returns the current wall-clock time in nanoseconds.
func (c Real) Wall() actor.Wall { return actor.Wall(c.Clock.Wall()) }

// AfterFunc calls f once after d, on its own goroutine.
func (c Real) AfterFunc(d actor.Duration, f func()) live.Timer {
	return c.Clock.AfterFunc(sched.TimeNs(d), f)
}
