package kv

import (
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

// The store is a simulated outside system (think S3 or DynamoDB) behind the
// server's executor. It keeps a map from keys to values, and answers each
// read after a short random delay.

var putIsStored = actor.NewAlways("kv: a put is stored")

// ---- what the store accepts, and what it answers ----

// storeReq is every request the store takes: the server's IO.
//
//sumtype:decl
//dst:sum
type storeReq interface {
	actor.Payload
	isStoreReq()
}

// storePut stores a value. The server hands it over with Do: no answer.
//
//dst:cases storeReq
type storePut struct{ Key, Value string }

// storeGet reads a key.
//
//dst:cases storeReq
//dst:answer storeReply
type storeGet struct{ Key string }

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

// handle runs one request for the executor. Draw every delay here, so draws
// happen in issue order.
func (s *store) handle(env actor.Env, ctx *sim.ExecCtx, req storeReq) {
	switch r := req.(type) {
	case storePut:
		if s.loseWrites {
			return
		}
		s.values[r.Key] = r.Value
		env.Always(s.values[r.Key] == r.Value, putIsStored, actor.Pair{Key: "key", Value: r.Key})
	case storeGet:
		delay := env.Duration(1*ms, 5*ms)
		sim.Answer(ctx, r, delay, storeReply{Key: r.Key, Value: s.values[r.Key]})
	}
}
