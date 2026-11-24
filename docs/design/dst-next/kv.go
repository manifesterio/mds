//go:build ignore

// DESIGN SKETCH, NOT COMPILED. The kv example rewritten for the proposed dst API:
// actors, built-in effects, typed refs, ask with a reply adapter, and dstgen for
// the sum-type boilerplate. Compare with dst/examples/kv/kv.go (about 280 lines).
//
// The story: a client plays a script against a server. The server keeps its
// values in a store, which is a simulated outside service.
//
//	client --putValue / getValue--> server --storePut / storeGet--> store
//
// Files: server.go (the system under test), store.go (the simulated outside
// service), client.go (the test workload), kv.go (wiring).
//
// Naming rule: an actor or service that others call has <name>Req (what it
// accepts) and <name>Reply (what it answers). Messages are named for what they
// mean: putValue, getValue, storeAnswered, timeForNextOp.
//
// The five ways to send, in one line each:
//   - actor.Tell(outbox, actor, msg): send to an actor, expect no answer.
//   - actor.TellService(outbox, service, req): the same, to a service.
//   - actor.Ask(outbox, actor, makeReq, adapt): ask an actor; the answer comes back as
//     one of my own messages, built by adapt.
//   - actor.Call(outbox, service, req, adapt): ask a service (it gets its
//     reply handle from Handle, so the request needs no ReplyTo).
//   - actor.Reply(outbox, replyTo, answer): answer an Ask I received.
//
// Other effects: outbox.After(delay, msg) sends msg to myself later. A ReplyTo dies
// with the asking incarnation, so a late answer to a crashed asker is dropped.
// actor.EntityRef[P] is the stable handle that survives restarts.
package kv

import (
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/sim"
)

const ms = actor.Millisecond

// Run builds the system for one seed, plays the script, and returns the result.
func Run(seed uint64, script []step) sim.Result {
	system := sim.NewSystem(sim.Config{Seed: seed})

	// Go cannot infer Req and Rep from a concrete type, so they are spelled out.
	theStore := sim.AddService[storeReq, storeReply](system, "store", &store{values: map[string]string{}})

	theServer := sim.Spawn(system, actor.Props[serverReq, actor.NoIO]{
		Name: "server",
		New: func(actor.Env) actor.Actor[serverReq, actor.NoIO] {
			return &server{store: theStore}
		},
	})

	sim.Spawn(system, actor.Props[clientMsg, actor.NoIO]{
		Name: "client",
		Boot: func(actor.Incarnation) clientMsg { return start{} },
		New: func(env actor.Env) actor.Actor[clientMsg, actor.NoIO] {
			return &client{env: env, server: theServer, script: script}
		},
	})

	return system.Run()
}
