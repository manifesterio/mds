package live_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
)

func TestFakeClockTimersFireInTimeOrderOnAdvance(t *testing.T) {
	c := live.NewFakeClock(time.Unix(1000, 0))
	var order []string
	c.AfterFunc(actor.Duration(3*time.Second), func() { order = append(order, "c") })
	c.AfterFunc(actor.Duration(time.Second), func() {
		order = append(order, "a")
		// a timer started by a timer fires in the same Advance if it is due
		c.AfterFunc(actor.Duration(time.Second), func() { order = append(order, "b") })
	})
	if c.Pending() != 2 {
		t.Fatalf("pending %d", c.Pending())
	}
	c.Advance(2500 * time.Millisecond)
	if !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("order %v", order)
	}
	c.Advance(time.Second)
	if !slices.Equal(order, []string{"a", "b", "c"}) || c.Pending() != 0 {
		t.Fatalf("order %v pending %d", order, c.Pending())
	}
}

func TestFakeClockStopAndTimeMoves(t *testing.T) {
	c := live.NewFakeClock(time.Unix(1000, 0))
	mono, wall := c.Mono(), c.Wall()
	if mono != actor.Mono(time.Second) || wall != actor.Wall(time.Unix(1000, 0).UnixNano()) {
		t.Fatalf("start mono %d wall %d", mono, wall)
	}
	tm := c.AfterFunc(actor.Duration(time.Second), func() { t.Error("a stopped timer fired") })
	if !tm.Stop() || tm.Stop() || c.Pending() != 0 {
		t.Fatal("Stop must report true once and drop the pending count")
	}
	fired := c.AfterFunc(1, func() {})
	c.Advance(time.Minute)
	if fired.Stop() {
		t.Fatal("Stop of a fired timer must report false")
	}
	if c.Mono()-mono != actor.Mono(time.Minute) || c.Wall()-wall != actor.Wall(time.Minute) {
		t.Fatalf("mono moved %d, wall moved %d", c.Mono()-mono, c.Wall()-wall)
	}
}

func TestFakeClockSafeFromManyGoroutines(t *testing.T) {
	c := live.NewFakeClock(time.Unix(0, 0))
	var wg sync.WaitGroup
	var mu sync.Mutex
	n := 0
	for range 8 {
		wg.Go(func() {
			for range 100 {
				c.AfterFunc(3, func() { mu.Lock(); n++; mu.Unlock() })
				_ = c.Mono()
			}
		})
	}
	wg.Wait()
	c.Advance(5)
	if n != 800 || c.Pending() != 0 {
		t.Fatalf("fired %d pending %d", n, c.Pending())
	}
}
