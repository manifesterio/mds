package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
)

// executor simulates product IO. One executor per IO sum, shared by every
// actor of that sum. It runs right after the Receive that recorded the IO,
// once per Do or Perform, in record order. It must not block: it answers a
// Perform with Answer or Hold, and sends unsolicited results with Tell.
type executor[IO actor.Payload] func(ctx *ExecCtx, io IO)

// SpawnOption configures what only a simulation needs.
type SpawnOption interface{ applySpawn(o *spawnOpts) }

type spawnOpts struct {
	workload bool
	cost     func(r *actor.Rand) actor.Duration
	exec     any // executor[IO], checked in Spawn
	after    any // func(actor.Incarnation, In, []actor.Effect), checked in Spawn
	onEnd    func(self actor.Incarnation, how actor.End, cause string)
}

type optionFunc func(o *spawnOpts)

func (f optionFunc) applySpawn(o *spawnOpts) { f(o) }

// WithExecutor runs the actor's product IO. Required unless IO is NoIO.
func WithExecutor[IO actor.Payload](x executor[IO]) SpawnOption {
	return optionFunc(func(o *spawnOpts) { o.exec = x })
}

// Workload marks a test client. A workload boots after every other actor
// and never ends: an End, a Crash, a Stop or a panic stops the run with a
// failure. Host code reaches it with Tell.
func Workload() SpawnOption { return optionFunc(func(o *spawnOpts) { o.workload = true }) }

// OnEnd runs when an incarnation of the actor ends, after its entries were
// dropped and before the restart policy runs. Host code uses it to stop
// what depends on the actor.
func OnEnd(f func(self actor.Incarnation, how actor.End, cause string)) SpawnOption {
	return optionFunc(func(o *spawnOpts) { o.onEnd = f })
}

// Cost is how long the actor is busy after one Receive: f draws it from the
// run stream (a nil f, or a draw below zero, costs nothing). While the actor
// is busy its inputs wait in its inbox.
func Cost(f func(r *actor.Rand) actor.Duration) SpawnOption {
	return optionFunc(func(o *spawnOpts) { o.cost = f })
}

// AfterReceive runs after each Receive and its effects, with the message and
// the effects it recorded. Host code uses it for checks on one actor.
func AfterReceive[In actor.Payload](f func(self actor.Incarnation, msg In, effects []actor.Effect)) SpawnOption {
	return optionFunc(func(o *spawnOpts) { o.after = f })
}

// Spawn adds an actor and returns its stable handle. Start (or Boot) boots
// it. Props without New, a Name used twice, or an option of the wrong types
// is a protocol failure and gives the zero EntityRef.
func Spawn[In, IO actor.Payload](sys *System, p actor.Props[In, IO], opts ...SpawnOption) actor.EntityRef[In] {
	var o spawnOpts
	for _, opt := range opts {
		opt.applySpawn(&o)
	}
	switch {
	case p.Name == "" || p.New == nil:
		sys.Fail(FailProtocol, "spawn", "an actor needs a Name and New")
		return actor.EntityRef[In]{}
	case sys.names[p.Name]:
		sys.Fail(FailProtocol, "spawn", fmt.Sprintf("actor %s: the name is used twice", p.Name))
		return actor.EntityRef[In]{}
	}
	h := &host[In, IO]{
		actorBase: actorBase{idx: len(sys.actors), name: p.Name,
			workload: o.workload, restart: p.Restart},
		props: p, cost: o.cost, onEnd: o.onEnd,
	}
	if !typedOptions(sys, h, o) {
		return actor.EntityRef[In]{}
	}
	if limit := sys.cfg.ClockSkewMax; limit > 0 {
		h.skew = actor.Duration(sys.rand.Between(0, int64(limit)))
	}
	sys.names[p.Name] = true
	sys.actors = append(sys.actors, h)
	return core.NewEntityRef[In](h.entityID(sys.owner))
}

// typedOptions checks the typed options against the actor's types.
func typedOptions[In, IO actor.Payload](sys *System, h *host[In, IO], o spawnOpts) bool {
	ok := true
	check := func(set, matches bool, what string) {
		if set && !matches {
			sys.Fail(FailProtocol, "spawn", fmt.Sprintf("actor %s: %s does not match its types", h.name, what))
			ok = false
		}
	}
	var good bool
	h.exec, good = o.exec.(executor[IO])
	check(o.exec != nil, good, "WithExecutor")
	h.afterRcv, good = o.after.(func(actor.Incarnation, In, []actor.Effect))
	check(o.after != nil, good, "AfterReceive")
	return ok
}
