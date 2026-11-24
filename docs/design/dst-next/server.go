//go:build ignore

// DESIGN SKETCH, NOT COMPILED.
//
// The server is the system under test: the state machine that a gRPC server
// would wrap. It writes values to the store, reads them back for its callers,
// and answers each caller once the store has answered.
package kv

import "github.com/manifesterio/mds/dst/core/actor"

// ---- what the server accepts, and what it answers ----

//dst:sum
type serverReq interface {
	actor.Payload
	isServerReq()
}

//dst:cases serverReq
type (
	putValue struct{ Key, Value string } // store this value; no answer
	getValue struct {                    // read this key; answered with a serverReply
		Key     string
		ReplyTo actor.ReplyTo[serverReply]
	}
	storeAnswered struct { // internal: the store answered a read
		Answer storeReply
		Asker  actor.ReplyTo[serverReply]
	}
)

//dst:payload
type serverReply struct{ Key, Value string }

// ---- the server ----

type server struct {
	store actor.ServiceRef[storeReq, storeReply]
}

// The server keeps no state of its own: the store holds every value.
func (*server) StateHash() uint64 { return 0 }

func (s *server) Receive(_ actor.Now, msg serverReq, outbox *actor.Outbox[serverReq, actor.NoIO]) {
	switch m := msg.(type) {
	case putValue:
		actor.TellService(outbox, s.store, storePut{Key: m.Key, Value: m.Value})
	case getValue:
		actor.Call(outbox, s.store, storeGet{Key: m.Key}, thenAnswer(m.ReplyTo))
	case storeAnswered:
		actor.Reply(outbox, m.Asker, serverReply(m.Answer))
	}
}

// thenAnswer turns the store's answer into a message to the server itself, and
// remembers who asked, so the server needs no table of pending reads.
func thenAnswer(asker actor.ReplyTo[serverReply]) func(storeReply) serverReq {
	return func(answer storeReply) serverReq {
		return storeAnswered{Answer: answer, Asker: asker}
	}
}
