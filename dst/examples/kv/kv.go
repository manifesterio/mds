// Package kv is the example of the dst actor API: a client plays a script of
// puts and gets against a server, and the server keeps its values in a store,
// which is a simulated outside system behind the server's executor.
//
//	client --putValue / getValue--> server --storePut / storeGet--> store
//
// Files: server.go (the system under test), store.go (the simulated outside
// system), client.go (the test workload), kv.go (wiring).
//
// Naming rule: an actor or an IO that others call has <name>Req (what it
// accepts) and <name>Reply (what it answers). Messages are named for what
// they mean: putValue, getValue, storeAnswered, timeForNextOp.
//
// The ways to send, in one line each:
//   - server.Tell(outbox, msg): send to an actor, expect no answer.
//   - outbox.Do(io): hand IO to the executor, expect no answer.
//   - actor.Ask(outbox, server, makeReq, adapt): ask an actor; the answer
//     comes back as one of my own messages, built by adapt.
//   - actor.Perform(outbox, req, adapt): hand IO to the executor; its answer
//     comes back as one of my own messages, built by adapt.
//   - actor.Reply(outbox, replyTo, answer): answer an Ask I received.
//
// Other effects: outbox.After(delay, msg) sends msg to myself later. A
// ReplyTo dies with the asking incarnation, so a late answer to a crashed
// asker is dropped. actor.EntityRef[P] is the stable handle that survives
// restarts.
package kv

//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
)

const ms = actor.Millisecond

// Run builds the system for one seed, plays the script, and returns the
// result.
func Run(seed uint64, script []Step) sim.Result {
	return run(seed, script, &store{values: map[string]string{}})
}

func run(seed uint64, script []Step, theStore *store) sim.Result {
	system := sim.NewSystem(sim.Config{Seed: seed})

	// The store is outside state: its hash is folded into the trace, and
	// every call into it marks it changed.
	tracked := system.Track("store", theStore.StateHash)
	serverRef := sim.Spawn(system, actor.Props[serverReq, storeReq]{
		Name: "server",
		New: func(actor.Env) actor.Actor[serverReq, storeReq] {
			return &server{}
		},
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, req storeReq) {
		tracked.Touch()
		theStore.handle(system.Env(), ctx, req)
	}))

	sim.Spawn(system, actor.Props[clientMsg, actor.NoIO]{
		Name: "client",
		Boot: func(actor.Incarnation) clientMsg { return start{} },
		New: func(env actor.Env) actor.Actor[clientMsg, actor.NoIO] {
			return &client{env: env, server: serverRef, script: script}
		},
	}, sim.Workload())

	return system.Run()
}
