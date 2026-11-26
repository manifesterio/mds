package live

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"

	"github.com/manifesterio/mds/dst/actor"
)

// owned is one timer or awaited answer of the actor. It is checked when the
// message is taken from the inbox, so a cancelled, replaced or forgotten one
// is dropped even if it already arrived.
type owned struct {
	scope string
	key   actor.TimerKey // keyed timers only
	timer Timer          // the real timer, while it has not fired
	adapt func(actor.Payload) actor.Payload
}

// actorRunner is one actor with its types. Its fields below mu are touched
// only on the actor's goroutine.
type actorRunner[In, IO actor.Payload] struct {
	h     *Host
	idx   int
	props actor.Props[In, IO]
	exec  func(ctx *ExecCtx, io IO) error
	after func()
	boot  actor.ID

	mu      sync.Mutex
	control []item
	data    []item
	wake    chan struct{} // capacity 1
	up      bool
	done    chan struct{} // closed when the goroutine has ended
	killed  chan struct{}
	stopMsg chan actor.Mono // the graceful stop request, at most once

	a       actor.Actor[In, IO]
	numbers uint64
	own     map[uint64]*owned
	keyed   map[actor.TimerKey]uint64
}

func newActorRunner[In, IO actor.Payload](h *Host, idx int, p actor.Props[In, IO],
	exec func(*ExecCtx, IO) error, after func()) *actorRunner[In, IO] {
	var boot actor.ID
	_, _ = rand.Read(boot[:])
	return &actorRunner[In, IO]{
		h: h, idx: idx, props: p, exec: exec, after: after, boot: boot,
		wake: make(chan struct{}, 1), done: make(chan struct{}), killed: make(chan struct{}),
		stopMsg: make(chan actor.Mono, 1),
		own:     map[uint64]*owned{}, keyed: map[actor.TimerKey]uint64{},
	}
}

func (r *actorRunner[In, IO]) name() string { return r.props.Name }

func (r *actorRunner[In, IO]) doneCh() <-chan struct{} { return r.done }

func (r *actorRunner[In, IO]) self() actor.Incarnation {
	return actor.Incarnation{Name: r.props.Name, Boot: r.boot}
}

func (r *actorRunner[In, IO]) entityID() actor.EntityID {
	return actor.EntityID{Runtime: r.h.owner, Slot: uint32(r.idx), Name: r.props.Name}
}

func (r *actorRunner[In, IO]) asker() actor.Asker {
	return actor.Asker{Kind: actor.ActorAsker, Runtime: r.h.owner, Slot: uint32(r.idx), Name: r.props.Name, Boot: r.boot}
}

// alive reports whether the actor's incarnation boot is running.
func (r *actorRunner[In, IO]) alive(boot actor.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.up && r.boot == boot
}

// push puts an item in the inbox, from any goroutine. Props.Priority runs
// here, so it must be a pure function of the message.
func (r *actorRunner[In, IO]) push(it item) {
	prio := actor.Data
	if it.fn == nil && r.props.Priority != nil {
		if m, ok := actor.As[In](it.msg); ok && !it.answer {
			prio = r.props.Priority(m)
		}
	}
	r.mu.Lock()
	if prio == actor.Control || it.fn != nil {
		r.control = append(r.control, it)
	} else {
		r.data = append(r.data, it)
	}
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *actorRunner[In, IO]) pop() (item, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, q := range []*[]item{&r.control, &r.data} {
		if len(*q) > 0 {
			it := (*q)[0]
			(*q)[0] = item{}
			*q = (*q)[1:]
			return it, true
		}
	}
	return item{}, false
}

func (r *actorRunner[In, IO]) stopRequest(deadline actor.Mono) {
	select {
	case r.stopMsg <- deadline:
	default:
	}
}

func (r *actorRunner[In, IO]) kill() {
	select {
	case <-r.killed:
	default:
		close(r.killed)
	}
}

// onLoop runs fn on the actor's goroutine between two Receives. It reports
// false when the actor is not running.
func (r *actorRunner[In, IO]) onLoop(fn func()) bool {
	r.mu.Lock()
	up := r.up
	r.mu.Unlock()
	if !up {
		return false
	}
	r.push(item{fn: fn})
	return true
}

// run is the actor's goroutine: boot, then one item at a time until the
// incarnation ends, ctx ends or the actor is killed.
func (r *actorRunner[In, IO]) run(ctx context.Context) {
	defer close(r.done)
	defer r.stopTimers()
	r.a = r.props.New(r.h.cfg.Env(r.self()))
	r.mu.Lock()
	r.up = true
	r.mu.Unlock()
	defer r.down()
	if r.props.Boot != nil && !r.receive(r.props.Boot(r.self())) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.killed:
			r.h.actorEnded(nil)
			return
		case deadline := <-r.stopMsg:
			if r.props.StopMsg == nil {
				return // no graceful stop: the actor is stopped at once
			}
			r.push(item{msg: r.props.StopMsg(deadline)})
			continue
		default:
		}
		it, ok := r.pop()
		if !ok {
			r.idle(ctx)
			continue
		}
		if !r.handle(it) {
			return
		}
	}
}

func (r *actorRunner[In, IO]) idle(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-r.killed:
	case <-r.wake:
	case deadline := <-r.stopMsg:
		r.stopMsg <- deadline // handled by the loop
	}
}

func (r *actorRunner[In, IO]) down() {
	r.mu.Lock()
	r.up = false
	r.mu.Unlock()
	r.h.incarnationEnded(r.idx, r.boot)
}

// handle takes one item. It reports false when the incarnation ended.
func (r *actorRunner[In, IO]) handle(it item) bool {
	switch {
	case it.fn != nil:
		it.fn()
		return true
	case !r.take(it.n):
		return true // a cancelled, replaced or forgotten timer or answer
	}
	msg := it.msg
	if it.answer {
		msg = r.own[it.n].adaptOr(msg)
		delete(r.own, it.n)
	}
	m, ok := actor.As[In](msg)
	if !ok {
		r.h.fail(fmt.Errorf("%w: %s got %T, which is not its message type", ErrProtocol, r.name(), msg))
		return false
	}
	return r.receive(m)
}

// take reports whether an item that carries owned number n may be received.
// Timers are forgotten here; answers in handle, after their adapter ran.
func (r *actorRunner[In, IO]) take(n uint64) bool {
	if n == 0 {
		return true
	}
	o, ok := r.own[n]
	if !ok {
		return false
	}
	if o.adapt == nil {
		delete(r.own, n)
		if o.key != "" && r.keyed[o.key] == n {
			delete(r.keyed, o.key)
		}
	}
	return true
}

func (o *owned) adaptOr(msg actor.Payload) actor.Payload {
	if o == nil || o.adapt == nil {
		return msg
	}
	return o.adapt(msg)
}

// receive runs one Receive and carries out its effects. It reports false
// when the incarnation ended (Stop, panic, executor error).
func (r *actorRunner[In, IO]) receive(msg In) bool {
	o := actor.NewOutbox[In, IO](actor.Binding{
		Self:  r.entityID(),
		Asker: r.asker(),
		Next:  func() uint64 { r.numbers++; return r.numbers },
		Misuse: func(what string) {
			r.h.fail(fmt.Errorf("%w: %s: %s", ErrProtocol, r.name(), what))
		},
	})
	now := actor.Now{Mono: r.h.cfg.Clock.Mono(), Wall: r.h.cfg.Clock.Wall()}
	if err := guard(func() { r.a.Receive(now, msg, o) }); err != nil {
		actor.Close(o)
		r.h.actorEnded(fmt.Errorf("live: %s crashed: %w", r.name(), err))
		return false
	}
	alive := r.apply(actor.Close(o))
	if r.after != nil {
		r.after()
	}
	return alive
}

// guard runs fn and turns a panic into an error.
func guard(fn func()) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	fn()
	return nil
}

func (r *actorRunner[In, IO]) nextNumber() uint64 {
	r.numbers++
	return r.numbers
}

// forget drops every timer and awaited answer in scope or below.
func (r *actorRunner[In, IO]) forget(scope string) {
	for n, o := range r.own { //determinismcheck:ok production: deletes only
		if scope == "" || o.scope == scope || strings.HasPrefix(o.scope, scope+"/") {
			r.drop(n, o)
		}
	}
}

// drop forgets owned number n and stops its timer.
func (r *actorRunner[In, IO]) drop(n uint64, o *owned) {
	if o.timer != nil {
		o.timer.Stop()
	}
	delete(r.own, n)
	if o.key != "" && r.keyed[o.key] == n {
		delete(r.keyed, o.key)
	}
}

func (r *actorRunner[In, IO]) cancelKey(key actor.TimerKey) {
	if n, ok := r.keyed[key]; ok {
		r.drop(n, r.own[n])
	}
}

func (r *actorRunner[In, IO]) stopTimers() {
	for n, o := range r.own { //determinismcheck:ok production: deletes only
		r.drop(n, o)
	}
}
