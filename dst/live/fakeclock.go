package live

import (
	"sync"
	"time"

	"github.com/manifesterio/mds/dst/actor"
)

// FakeClock is a Clock that moves only on Advance, for tests of code that
// starts timers from several goroutines. Timers fire inside Advance, on the
// goroutine that calls it, in time order. Do not use it in a simulator run,
// which has its own single-goroutine clock.
type FakeClock struct {
	mu     sync.Mutex
	mono   actor.Mono
	offset int64 // Wall is Mono + offset
	seq    uint64
	timers []*fakeTimer // armed, not fired, not stopped
}

type fakeTimer struct {
	c    *FakeClock
	at   actor.Mono
	seq  uint64
	f    func()
	done bool
}

var _ Clock = (*FakeClock)(nil)

// NewFakeClock returns a FakeClock whose Wall time starts at wall. Mono starts
// at one second, so that zero can mean "not set" in the code under test.
func NewFakeClock(wall time.Time) *FakeClock {
	start := actor.Mono(time.Second)
	return &FakeClock{mono: start, offset: wall.UnixNano() - int64(start)}
}

// Mono returns the virtual node-local time.
func (c *FakeClock) Mono() actor.Mono {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// Wall returns the virtual UTC time.
func (c *FakeClock) Wall() actor.Wall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return actor.Wall(int64(c.mono) + c.offset)
}

// AfterFunc schedules f for d after now (d < 0 counts as 0). It runs inside a
// later Advance, without the lock held, so f may call AfterFunc and Stop.
func (c *FakeClock) AfterFunc(d actor.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	t := &fakeTimer{c: c, at: c.mono + actor.Mono(max(d, 0)), seq: c.seq, f: f}
	c.timers = append(c.timers, t)
	return t
}

// Stop stops the timer. It reports false if it already fired or was stopped.
func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if t.done {
		return false
	}
	t.done = true
	for i, x := range t.c.timers {
		if x == t {
			t.c.timers = append(t.c.timers[:i], t.c.timers[i+1:]...)
			break
		}
	}
	return true
}

// Advance moves time forward by d and fires every timer that is due, in time
// order. A timer may start new timers; they fire in the same call if due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.mono + actor.Mono(max(d, 0))
	for {
		next := -1
		for i, t := range c.timers {
			if t.at <= target && (next < 0 || t.at < c.timers[next].at ||
				t.at == c.timers[next].at && t.seq < c.timers[next].seq) {
				next = i
			}
		}
		if next < 0 {
			c.mono = target
			c.mu.Unlock()
			return
		}
		t := c.timers[next]
		c.timers = append(c.timers[:next], c.timers[next+1:]...)
		t.done = true
		c.mono = max(c.mono, t.at)
		c.mu.Unlock()
		t.f()
		c.mu.Lock()
	}
}

// Pending counts the timers that did not fire and were not stopped.
func (c *FakeClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
