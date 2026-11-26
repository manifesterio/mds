package clock

import (
	"time"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/runtime/live"
	"github.com/manifesterio/mds/shared/clock/internal/sched"
	"github.com/manifesterio/mds/shared/clock/internal/syncclock"
)

// Fake is a live.Clock that moves only on Advance.
type Fake struct{ *syncclock.Sync }

var _ live.Clock = Fake{}

// NewFake returns a Fake clock whose Wall time starts at wall. Mono starts at one
// second, so that zero can mean "not set" in the code under test.
func NewFake(wall time.Time) Fake {
	return Fake{syncclock.NewSync(sched.TimeNs(wall.UnixNano()), sched.TimeNs(time.Second))}
}

// Mono returns the node-local time.
func (c Fake) Mono() actor.Mono { return actor.Mono(c.Sync.Mono()) }

// Wall returns the UTC time.
func (c Fake) Wall() actor.Wall { return actor.Wall(c.Sync.Wall()) }

// AfterFunc schedules f for d after now. It runs inside a later Advance.
func (c Fake) AfterFunc(d actor.Duration, f func()) live.Timer {
	return c.Sync.AfterFunc(sched.TimeNs(d), f)
}

// Advance moves time forward by d and fires every timer that is due.
func (c Fake) Advance(d time.Duration) { c.Sync.Advance(sched.TimeNs(d)) }
