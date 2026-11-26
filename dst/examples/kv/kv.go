// Package kv is the example of the dst actor API: a client plays a script of
// puts and gets against a server, and the server keeps its values in a store,
// which is a simulated outside service.
//
//	client --putValue / getValue--> server --storePut / storeGet--> store
//
// Files: server.go (the system under test), store.go (the simulated outside
// service), client.go (the test workload), kv.go (wiring).
//
// Naming rule: an actor or service that others call has <name>Req (what it
// accepts) and <name>Reply (what it answers). Messages are named for what
// they mean: putValue, getValue, storeAnswered, timeForNextOp.
//
// The ways to send, in one line each:
//   - server.Tell(outbox, msg): send to an actor, expect no answer.
//   - store.Tell(outbox, req): the same, to a service.
//   - actor.Ask(outbox, server, makeReq, adapt): ask an actor; the answer
//     comes back as one of my own messages, built by adapt.
//   - actor.Call(outbox, store.Request(req), adapt): ask a service (it gets
//     its reply handle from Handle, so the request needs no ReplyTo).
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

	// Go cannot infer Req and Rep from a concrete type, so they are spelled out.
	storeRef := sim.AddService[storeReq, storeReply](system, "store", theStore)

	serverRef := sim.Spawn(system, actor.Props[serverReq, actor.NoIO]{
		Name: "server",
		New: func(actor.Env) actor.Actor[serverReq, actor.NoIO] {
			return &server{store: storeRef}
		},
	})

	sim.Spawn(system, actor.Props[clientMsg, actor.NoIO]{
		Name: "client",
		Boot: func(actor.Incarnation) clientMsg { return start{} },
		New: func(env actor.Env) actor.Actor[clientMsg, actor.NoIO] {
			return &client{env: env, server: serverRef, script: script}
		},
	}, sim.Workload())

	return system.Run()
}
