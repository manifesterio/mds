package live

import (
	"context"
	"fmt"

	"github.com/manifesterio/mds/dst/core/actor"
)

// apply carries out the effects of one Receive in record order. It
// reports false when the incarnation ended.
func (r *actorRunner[In, IO]) apply(effects []actor.Effect) bool {
	for i, e := range effects {
		if e.Kind == actor.EffectStop {
			if i != len(effects)-1 {
				r.h.fail(fmt.Errorf("%w: %s: an effect was recorded after Stop", ErrProtocol, r.name()))
				return false
			}
			r.h.actorEnded(endError(r.name(), e.How, e.Cause))
			return false
		}
		if err := r.applyOne(e); err != nil {
			r.h.actorEnded(err)
			return false
		}
	}
	return true
}

func endError(name string, how actor.End, cause string) error {
	switch how {
	case actor.Exit:
		return nil
	case actor.Trip:
		return fmt.Errorf("%w: %s: %s", ErrTripped, name, cause)
	case actor.Crash:
	}
	return fmt.Errorf("%w: %s ended with Crash (%s): a crash comes from outside", ErrProtocol, name, cause)
}

func (r *actorRunner[In, IO]) applyOne(e actor.Effect) error {
	switch e.Kind {
	case actor.EffectTellSelf, actor.EffectAfter, actor.EffectStartTimer:
		r.startTimer(e)
	case actor.EffectCancelTimer:
		r.cancelKey(e.StoredKey())
	case actor.EffectForgetPending:
		r.forget(e.Scope)
	case actor.EffectFact:
		if r.h.cfg.Facts != nil {
			r.h.cfg.Facts(r.self(), e.Fact)
		}
	case actor.EffectTell:
		return r.h.tell(e.To, e.Msg)
	case actor.EffectAsk:
		r.own[e.Number] = &owned{scope: e.Scope, adapt: e.Adapt}
		return r.h.tell(e.To, e.Msg)
	case actor.EffectReply:
		return r.h.reply(r.idx, r.boot, e.Reply, e.Msg)
	case actor.EffectWatch:
		r.h.watch(r.idx, r.boot, e.Reply, e.Msg)
	case actor.EffectPerform:
		r.own[e.Number] = &owned{scope: e.Scope, adapt: e.Adapt}
		return r.execute(e.Msg, e.Number)
	case actor.EffectDo:
		return r.execute(e.Msg, 0)
	case actor.EffectTellService, actor.EffectCall:
		return fmt.Errorf("%w: %s: services exist only in simulation", ErrProtocol, r.name())
	case actor.EffectStop:
	}
	return nil
}

// startTimer arms a self message. A zero delay goes straight to the inbox,
// behind what is there, in record order.
func (r *actorRunner[In, IO]) startTimer(e actor.Effect) {
	n := r.nextNumber()
	o := &owned{scope: e.Scope}
	if e.Kind == actor.EffectStartTimer {
		key := e.StoredKey()
		r.cancelKey(key)
		o.key = key
		r.keyed[key] = n
	}
	r.own[n] = o
	it := item{msg: e.Msg, n: n}
	if e.D <= 0 {
		r.push(it)
		return
	}
	o.timer = r.h.cfg.Clock.AfterFunc(e.D, func() { r.push(it) })
}

// execute hands io to the executor. n is the Perform's number, 0 for Do.
func (r *actorRunner[In, IO]) execute(io actor.Payload, n uint64) error {
	if r.exec == nil {
		return fmt.Errorf("%w: %s: Do or Perform without an exec function", ErrProtocol, r.name())
	}
	v, ok := actor.As[IO](io)
	if !ok {
		return fmt.Errorf("%w: %s: the IO value is not of the actor's IO type", ErrProtocol, r.name())
	}
	ctx := &ExecCtx{h: r.h, to: r, boot: r.boot, n: n, self: r.self()}
	if err := r.exec(ctx, v); err != nil {
		return fmt.Errorf("live: %s: exec: %w", r.name(), err)
	}
	return nil
}

// ExecCtx is lent to one exec call. Answer may be called from any goroutine.
type ExecCtx struct {
	h    *Host
	to   hosted
	boot actor.ID
	n    uint64 // 0 for a Do
	self actor.Incarnation
}

// Host is the Host, for exec functions that Tell actors.
func (c *ExecCtx) Host() *Host { return c.h }

// Issuer is the incarnation that recorded the IO.
func (c *ExecCtx) Issuer() actor.Incarnation { return c.self }

// Answer answers the Perform that issued req. It is typed by the same Request
// constraint as Perform. The answer of a Do, a second answer, or an answer
// after the actor ended is dropped.
func Answer[IO, Rep actor.Payload, Req actor.Request[IO, Rep]](c *ExecCtx, _ Req, answer Rep) {
	if c.n == 0 || !c.to.alive(c.boot) {
		return
	}
	c.to.push(item{msg: answer, n: c.n, answer: true, boot: c.boot})
}

// ---- watches and outside callers ----

type watch struct {
	idx   int
	boot  actor.ID
	reply actor.ReplyID
	gone  actor.Payload
}

// outsideCall is one live.Ask that is waiting.
type outsideCall struct {
	answer  chan actor.Payload // capacity 1
	stopped chan struct{}      // closed when the Host stops
}

// reply delivers answer to the asker behind r, if it still waits, and
// removes the replier's watches on r.
func (h *Host) reply(idx int, boot actor.ID, r actor.ReplyID, answer actor.Payload) error {
	h.mu.Lock()
	h.watches = removeWatches(h.watches, func(w watch) bool { return w.idx == idx && w.boot == boot && w.reply == r })
	switch r.Asker.Kind {
	case actor.LiveOutsideAsker:
		c, ok := h.outside[r.Number]
		if ok {
			delete(h.outside, r.Number)
			c.answer <- answer
		}
		h.mu.Unlock()
		return nil
	case actor.ActorAsker:
		h.mu.Unlock()
		return h.replyToActor(r, answer)
	}
	h.mu.Unlock()
	return fmt.Errorf("%w: Reply to a zero or foreign ReplyTo", ErrProtocol)
}

func (h *Host) replyToActor(r actor.ReplyID, answer actor.Payload) error {
	a, err := h.actorOf(actor.EntityID{Runtime: r.Asker.Runtime, Slot: r.Asker.Slot})
	if err != nil {
		return err
	}
	if a.alive(r.Asker.Boot) {
		a.push(item{msg: answer, n: r.Number, answer: true, boot: r.Asker.Boot})
	}
	return nil
}

// watch registers a Watch; if the asker is already gone, gone comes at once.
func (h *Host) watch(idx int, boot actor.ID, r actor.ReplyID, gone actor.Payload) {
	w := watch{idx: idx, boot: boot, reply: r, gone: gone}
	if !h.askerAlive(r) {
		h.fire(w)
		return
	}
	h.mu.Lock()
	h.watches = append(h.watches, w)
	h.mu.Unlock()
}

func (h *Host) askerAlive(r actor.ReplyID) bool {
	switch r.Asker.Kind {
	case actor.LiveOutsideAsker:
		h.mu.Lock()
		defer h.mu.Unlock()
		_, ok := h.outside[r.Number]
		return ok
	case actor.ActorAsker:
		a, err := h.actorOf(actor.EntityID{Runtime: r.Asker.Runtime, Slot: r.Asker.Slot})
		return err == nil && a.alive(r.Asker.Boot)
	}
	return false
}

// askerEnded fires every watch on asks of an asker that ended.
func (h *Host) askerEnded(ended func(actor.ReplyID) bool) {
	var fire []watch
	h.mu.Lock()
	h.watches = removeWatches(h.watches, func(w watch) bool {
		if ended(w.reply) {
			fire = append(fire, w)
			return true
		}
		return false
	})
	h.mu.Unlock()
	for _, w := range fire {
		h.fire(w)
	}
}

func (h *Host) fire(w watch) {
	h.mu.Lock()
	a := h.actors[w.idx]
	h.mu.Unlock()
	if a.alive(w.boot) {
		a.push(item{msg: w.gone})
	}
}

// incarnationEnded drops the watches an actor held and fires those on its
// asks.
func (h *Host) incarnationEnded(idx int, boot actor.ID) {
	h.mu.Lock()
	h.watches = removeWatches(h.watches, func(w watch) bool { return w.idx == idx && w.boot == boot })
	h.mu.Unlock()
	h.askerEnded(func(r actor.ReplyID) bool {
		return r.Asker.Kind == actor.ActorAsker && int(r.Asker.Slot) == idx && r.Asker.Boot == boot
	})
}

func removeWatches(ws []watch, drop func(watch) bool) []watch {
	out := ws[:0]
	for _, w := range ws {
		if !drop(w) {
			out = append(out, w)
		}
	}
	return out
}

// Ask asks an actor from outside the actors: the gRPC handler. It blocks
// until the answer (nil error), ctx ends (ctx.Err(); the asker is gone and
// the actor's Watch fires) or the Host stops (ErrHostStopped).
func Ask[Req, Rep actor.Payload](ctx context.Context, h *Host, to actor.EntityRef[Req],
	makeReq func(actor.ReplyTo[Rep]) Req) (Rep, error) {
	var zero Rep
	c := &outsideCall{answer: make(chan actor.Payload, 1), stopped: make(chan struct{})}
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return zero, ErrHostStopped
	}
	h.outsideAsks++
	n := h.outsideAsks
	h.outside[n] = c
	h.mu.Unlock()
	id := actor.ReplyID{Asker: actor.Asker{Kind: actor.LiveOutsideAsker, Runtime: h.owner}, Number: n}
	if err := h.tell(to.EntityID(), makeReq(actor.NewReplyTo[Rep](id))); err != nil {
		h.forgetOutside(n)
		return zero, err
	}
	select {
	case a := <-c.answer:
		rep, ok := actor.As[Rep](a)
		if !ok {
			return zero, fmt.Errorf("%w: the answer is a %T", ErrProtocol, a)
		}
		return rep, nil
	case <-c.stopped:
		return zero, ErrHostStopped
	case <-ctx.Done():
		if !h.forgetOutside(n) {
			// The answer won the race: it is already in the channel.
			if a, ok := actor.As[Rep](<-c.answer); ok {
				return a, nil
			}
		}
		h.askerEnded(func(r actor.ReplyID) bool { return r == id })
		return zero, ctx.Err()
	}
}

// forgetOutside removes a waiting outside call. It reports false when the
// call was already answered.
func (h *Host) forgetOutside(n uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.outside[n]; !ok {
		return false
	}
	delete(h.outside, n)
	return true
}

// Tell sends msg to an actor from outside (a socket reader, a hook). Safe
// from any goroutine.
func Tell[P actor.Payload](h *Host, to actor.EntityRef[P], msg P) error {
	if h.isStopped() {
		return ErrHostStopped
	}
	return h.tell(to.EntityID(), msg)
}

// OnLoop runs fn on the actor's goroutine between two Receives and waits for
// it: for reads of process state that must not race with Receive. fn must
// not block.
func OnLoop[P actor.Payload](ctx context.Context, h *Host, on actor.EntityRef[P], fn func()) error {
	a, err := h.actorOf(on.EntityID())
	if err != nil {
		return err
	}
	done := make(chan struct{})
	if h.isStopped() || !a.onLoop(func() { fn(); close(done) }) {
		return ErrHostStopped
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-h.ended:
		select {
		case <-done:
			return nil
		default:
			return ErrHostStopped
		}
	}
}
