//go:build ignore

// DESIGN SKETCH, NOT COMPILED.
//
// The client is the test workload. It plays a script of puts and gets against
// the server, pauses a little between steps, and checks that every read sees
// the value the script expects.
package kv

import (
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
)

var readSeesLastWrite = assert.NewAlways("kv: a read sees the last write")

// ---- what the client receives ----

//dst:sum
type clientMsg interface{ isClientMsg() }

//dst:cases clientMsg
type (
	start          struct{} // the first message after boot
	timeForNextOp  struct{} // the pause between two steps is over
	serverAnswered struct { // the server answered a read
		Answer   serverReply
		Expected string
	}
)

// ---- the client ----

// step is one line of the script: put a value, or get a key and expect a value.
type step struct {
	key, value string
	isPut      bool
}

type client struct {
	env    actor.Env
	server actor.EntityRef[serverReq]
	script []step
	played int // how many steps have been played
}

func (c *client) StateHash() uint64 { return uint64(c.played) }

func (c *client) Receive(_ actor.Now, msg clientMsg, out *actor.Out[clientMsg, actor.NoIO]) {
	switch m := msg.(type) {
	case start, timeForNextOp:
		c.playNextStep(out)
	case serverAnswered:
		c.env.Always(m.Answer.Value == m.Expected, readSeesLastWrite,
			assert.Pair{Key: "key", Value: m.Answer.Key},
			assert.Pair{Key: "got", Value: m.Answer.Value},
			assert.Pair{Key: "expected", Value: m.Expected})
	}
}

func (c *client) playNextStep(out *actor.Out[clientMsg, actor.NoIO]) {
	if c.finished() {
		return
	}
	s := c.script[c.played]
	c.played++

	if s.isPut {
		actor.Tell(out, c.server, putValue{Key: s.key, Value: s.value})
	} else {
		actor.Ask(out, c.server, getValueOf(s.key), expecting(s.value))
	}

	if !c.finished() {
		out.After(c.env.Duration(1*ms, 20*ms), timeForNextOp{})
	}
}

func (c *client) finished() bool { return c.played == len(c.script) }

// getValueOf builds the read request; Ask hands it the ReplyTo to put inside.
func getValueOf(key string) func(actor.ReplyTo[serverReply]) serverReq {
	return func(replyTo actor.ReplyTo[serverReply]) serverReq {
		return getValue{Key: key, ReplyTo: replyTo}
	}
}

// expecting turns the server's answer into a client message that carries the
// value the script expects, so the client needs no table of pending reads.
func expecting(value string) func(serverReply) clientMsg {
	return func(answer serverReply) clientMsg {
		return serverAnswered{Answer: answer, Expected: value}
	}
}
