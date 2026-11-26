package kv

import (
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/dst/sim"
)

// The store is a simulated outside service (think S3 or DynamoDB). It keeps
// a map from keys to values, and answers each read after a short random
// delay.

var putIsStored = assert.NewAlways("kv: a put is stored")

// ---- what the store accepts, and what it answers ----

// storeReq is every request the store takes.
//
//sumtype:decl
//dst:sum
type storeReq interface {
	actor.Payload
	isStoreReq()
}

//dst:cases storeReq
type (
	storePut struct{ Key, Value string } // store this value; no answer
	storeGet struct{ Key string }        // read this key; answered with a storeReply
)

// storeReply answers a storeGet.
//
//dst:payload
type storeReply struct{ Key, Value string }

// ---- the store ----

type store struct {
	values map[string]string
	// loseWrites is a planted bug for the test that proves the client's
	// check works: every put is dropped.
	loseWrites bool
}

func (s *store) StateHash() uint64 { return trace.StringMap(s.values) }

func (s *store) Handle(ctx *sim.ServiceCtx, req storeReq, reply sim.Responder[storeReply]) {
	switch r := req.(type) {
	case storePut:
		if s.loseWrites {
			return
		}
		s.values[r.Key] = r.Value
		ctx.Env().Always(s.values[r.Key] == r.Value, putIsStored, assert.Pair{Key: "key", Value: r.Key})
	case storeGet:
		delay := ctx.Env().Duration(1*ms, 5*ms)
		reply.After(delay, storeReply{Key: r.Key, Value: s.values[r.Key]})
	}
}
