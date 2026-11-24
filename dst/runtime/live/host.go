package live

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/manifesterio/mds/dst/core/actor"
)

// The ways Host.Run, Ask, Tell and OnLoop can end.
var (
	// ErrTripped: an actor ended with Trip (its memory is not trusted). The
	// text has the actor and the cause.
	ErrTripped = errors.New("tripped")
	// ErrHostStopped: the Host is not running (any more).
	ErrHostStopped = errors.New("live: the host has stopped")
	// ErrProtocol: an actor used the API the wrong way (a service in
	// production, an effect after Stop, a zero handle, ...).
	ErrProtocol = errors.New("live: protocol failure")
)

// Clock is real time for the Host: monotonic node time, UTC, and timers.
type Clock interface {
	Mono() actor.Mono
	Wall() actor.Wall
	AfterFunc(d actor.Duration, f func()) Timer
}

// Timer is a timer made by Clock.AfterFunc.
type Timer interface {
	// Stop stops the timer. It reports false if it already fired or was
	// already stopped.
	Stop() bool
}

// HostConfig is what a Host needs.
type HostConfig struct {
	Clock Clock
	// Env makes the Env of each actor incarnation. Nil: NewEnv with no
	// violation hook.
	Env func(self actor.Incarnation) actor.Env
	// Shutdown, when closed, asks for a graceful stop: every actor with a
	// StopMsg gets it, and every actor still running after StopGrace is
	// killed.
	Shutdown <-chan struct{}
	// StopGrace is the time a graceful stop may take. 0 means 5 s.
	StopGrace actor.Duration
	// Facts, when set, receives every Fact an actor records.
	Facts func(source actor.Incarnation, f actor.Fact)
}

const defaultStopGrace = 5 * actor.Second

// Host runs actors in production: one goroutine per actor, real timers,
// executors for product IO. One Host per process; it may hold several
// actors that talk to each other.
type Host struct {
	cfg   HostConfig
	owner *actor.Owner

	mu       sync.Mutex
	actors   []hosted
	names    map[string]bool
	running  bool
	stopped  bool
	firstErr error
	ended    chan struct{} // closed when the first actor ends, or the Host stops
	endOnce  sync.Once

	watches     []watch
	outside     map[uint64]*outsideCall
	outsideAsks uint64
}

// NewHost makes a Host. Spawn the actors, then Run it.
func NewHost(cfg HostConfig) *Host {
	if cfg.StopGrace <= 0 {
		cfg.StopGrace = defaultStopGrace
	}
	if cfg.Env == nil {
		cfg.Env = func(self actor.Incarnation) actor.Env { return NewEnv(self, nil) }
	}
	return &Host{
		cfg: cfg, owner: actor.NewOwner(), names: map[string]bool{},
		outside: map[uint64]*outsideCall{}, ended: make(chan struct{}),
	}
}

// hosted is one actor of the Host, whatever its types.
type hosted interface {
	name() string
	run(ctx context.Context) // the actor's goroutine
	push(it item)            // from any goroutine
	stopRequest(deadline actor.Mono)
	kill()
	alive(boot actor.ID) bool
	onLoop(fn func()) bool
}

// item is one thing for an actor's goroutine: a message (with the owned
// number it carries, 0 for none), an answer to adapt, or a function to run
// between two Receives.
type item struct {
	msg    actor.Payload
	n      uint64
	answer bool
	boot   actor.ID // answers and replies: the incarnation they are for
	fn     func()
}

// Spawn adds an actor before Run. exec runs product IO on the actor's
// goroutine and must not block: it starts the IO and answers later with
// Answer, from any goroutine. exec may be nil for an actor with actor.NoIO.
// An error from exec ends the Host.
func Spawn[In, IO actor.Payload](h *Host, p actor.Props[In, IO], exec func(ctx *ExecCtx, io IO) error,
	opts ...SpawnOption) actor.EntityRef[In] {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.running || p.Name == "" || p.New == nil || h.names[p.Name] {
		h.failLocked(fmt.Errorf("%w: Spawn %q: before Run, with a unique Name and New", ErrProtocol, p.Name))
		return actor.EntityRef[In]{}
	}
	var o spawnOpts
	for _, opt := range opts {
		opt.applySpawn(&o)
	}
	a := newActorRunner(h, len(h.actors), p, exec, o.after)
	h.names[p.Name] = true
	h.actors = append(h.actors, a)
	return actor.NewEntityRef[In](a.entityID())
}

// SpawnOption configures a live actor.
type SpawnOption interface{ applySpawn(o *spawnOpts) }

type spawnOpts struct{ after func() }

type optionFunc func(o *spawnOpts)

func (f optionFunc) applySpawn(o *spawnOpts) { f(o) }

// AfterReceive runs f on the actor's goroutine after every Receive and its
// effects (host metrics: epoch, role).
func AfterReceive(f func()) SpawnOption { return optionFunc(func(o *spawnOpts) { o.after = f }) }

// Run boots every actor and blocks until the Host stops: ctx ends
// (ErrStopped... returned as ctx.Err() wrapped), Shutdown is closed and every
// actor has stopped (nil, or ErrKilled), or an actor's incarnation ends
// (that end's error: nil for Exit, ErrTripped for Trip, the panic or exec
// error for a crash). When one actor ends, the others are stopped
// gracefully.
func (h *Host) Run(ctx context.Context) error {
	h.mu.Lock()
	if h.running || h.stopped {
		h.mu.Unlock()
		return fmt.Errorf("%w: Run called twice", ErrProtocol)
	}
	h.running = true
	actors := h.actors
	err := h.firstErr
	h.mu.Unlock()
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, a := range actors {
		wg.Go(func() { a.run(runCtx) })
	}
	killed := h.supervise(ctx, actors)
	cancel()
	wg.Wait()
	h.finish()
	return h.result(ctx, killed)
}

// supervise waits for the reason to stop, then stops every actor: StopMsg,
// then a kill after StopGrace. It reports whether a kill was needed.
func (h *Host) supervise(ctx context.Context, actors []hosted) bool {
	select {
	case <-ctx.Done():
		return false
	case <-h.cfg.Shutdown:
	case <-h.ended:
	}
	deadline := h.cfg.Clock.Mono() + actor.Mono(h.cfg.StopGrace)
	for _, a := range actors {
		a.stopRequest(deadline)
	}
	grace := make(chan struct{})
	t := h.cfg.Clock.AfterFunc(h.cfg.StopGrace, func() { close(grace) })
	defer t.Stop()
	for _, a := range actors {
		if !h.waitStopped(ctx, a, grace) {
			for _, b := range actors {
				b.kill()
			}
			return true
		}
	}
	return false
}

func (h *Host) waitStopped(ctx context.Context, a hosted, grace <-chan struct{}) bool {
	ar, ok := a.(interface{ doneCh() <-chan struct{} })
	if !ok {
		return true
	}
	select {
	case <-ar.doneCh():
		return true
	case <-grace:
		return false
	case <-ctx.Done():
		return false
	}
}

func (h *Host) result(ctx context.Context, killed bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case h.firstErr != nil:
		return h.firstErr
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrStopped, ctx.Err())
	case killed:
		return ErrKilled
	}
	return nil
}

// actorEnded records the end of an actor's incarnation and starts stopping
// the Host. err is nil for Exit.
func (h *Host) actorEnded(err error) {
	h.mu.Lock()
	if err != nil && h.firstErr == nil {
		h.firstErr = err
	}
	h.mu.Unlock()
	h.endOnce.Do(func() { close(h.ended) })
}

func (h *Host) failLocked(err error) {
	if h.firstErr == nil {
		h.firstErr = err
	}
}

// fail ends the Host with a protocol failure.
func (h *Host) fail(err error) {
	h.mu.Lock()
	h.failLocked(err)
	h.mu.Unlock()
	h.endOnce.Do(func() { close(h.ended) })
}

// finish marks the Host stopped and fails every outside call still waiting.
func (h *Host) finish() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = true
	for n, c := range h.outside { //determinismcheck:ok production: order does not matter
		delete(h.outside, n)
		close(c.stopped)
	}
	h.watches = nil
}

func (h *Host) isStopped() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stopped
}

// actorOf resolves an EntityID of this Host.
func (h *Host) actorOf(id actor.EntityID) (hosted, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case id.Runtime == nil:
		return nil, fmt.Errorf("%w: a zero EntityRef was used", ErrProtocol)
	case id.Runtime != h.owner || int(id.Slot) >= len(h.actors):
		return nil, fmt.Errorf("%w: an EntityRef of another Host was used", ErrProtocol)
	}
	return h.actors[id.Slot], nil
}

// tell delivers msg to an actor.
func (h *Host) tell(to actor.EntityID, msg actor.Payload) error {
	a, err := h.actorOf(to)
	if err != nil {
		return err
	}
	a.push(item{msg: msg})
	return nil
}
