package sim_test

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/dst/sim"
)

// ---- the test protocol ----

type msg interface {
	actor.Payload
	isMsg()
}

type (
	start struct{}           // the boot message
	note  struct{ S string } // a plain message
	tick  struct{ N int }    // a timer message
	asked struct {           // a request that carries a ReplyTo
		Q       string
		ReplyTo actor.ReplyTo[answer]
	}
	got       struct{ V string } // an adapted answer
	goneMsg   struct{ Who string }
	fromComp  struct{ C compMsg }
	replyLate struct { // reply to ReplyTo later
		V       string
		ReplyTo actor.ReplyTo[answer]
	}
)

func (start) isMsg()     {}
func (note) isMsg()      {}
func (tick) isMsg()      {}
func (asked) isMsg()     {}
func (got) isMsg()       {}
func (goneMsg) isMsg()   {}
func (fromComp) isMsg()  {}
func (replyLate) isMsg() {}

func (start) Fold(h *trace.Hash)       { h.Uint64(1) }
func (m note) Fold(h *trace.Hash)      { h.Uint64(2); h.String(m.S) }
func (m tick) Fold(h *trace.Hash)      { h.Uint64(3); h.Int64(int64(m.N)) }
func (m asked) Fold(h *trace.Hash)     { h.Uint64(4); h.String(m.Q); m.ReplyTo.Fold(h) }
func (m got) Fold(h *trace.Hash)       { h.Uint64(5); h.String(m.V) }
func (m goneMsg) Fold(h *trace.Hash)   { h.Uint64(6); h.String(m.Who) }
func (m fromComp) Fold(h *trace.Hash)  { h.Uint64(7); m.C.Fold(h) }
func (m replyLate) Fold(h *trace.Hash) { h.Uint64(8); h.String(m.V); m.ReplyTo.Fold(h) }

type answer struct{ V string }

func (m answer) Fold(h *trace.Hash) { h.Uint64(9); h.String(m.V) }

type compMsg struct{ S string }

func (m compMsg) Fold(h *trace.Hash) { h.Uint64(10); h.String(m.S) }

// io is the product IO of the test actors.
type io interface {
	actor.Payload
	isIO()
}

type (
	read  struct{ Key string } // answered by readDone
	write struct{ Key string } // no answer
)

type readDone struct{ V string }

func (read) isIO()  {}
func (write) isIO() {}

func (m read) Fold(h *trace.Hash)     { h.Uint64(20); h.String(m.Key) }
func (m write) Fold(h *trace.Hash)    { h.Uint64(21); h.String(m.Key) }
func (m readDone) Fold(h *trace.Hash) { h.Uint64(22); h.String(m.V) }

func (m read) AsRequest() (io, readDone) { return m, readDone{} }

type outbox = actor.Outbox[msg, io]

// ---- a scripted actor: what it does is a function; what it saw is its state ----

type script func(a *scripted, now actor.Now, m msg, o *outbox)

type scripted struct {
	name string
	fn   script
	seen []string // every message it received, as text
	log  *[]string
}

func (a *scripted) Receive(now actor.Now, m msg, o *outbox) {
	line := fmt.Sprintf("%s@%d %s", a.name, now.Mono, describe(m))
	a.seen = append(a.seen, line)
	*a.log = append(*a.log, line)
	if a.fn != nil {
		a.fn(a, now, m, o)
	}
}

func (a *scripted) StateHash() uint64 {
	h := trace.New()
	for _, s := range a.seen {
		h.String(s)
	}
	return h.Sum()
}

func describe(m msg) string {
	switch m := m.(type) {
	case asked:
		return "asked " + m.Q
	case replyLate:
		return "replyLate " + m.V
	default:
		return strings.TrimPrefix(fmt.Sprintf("%T%+v", m, m), "sim_test.")
	}
}

// world is a System with a shared receive log.
type world struct {
	sys *sim.System
	log []string
}

func newWorld(seed uint64) *world {
	return &world{sys: sim.NewSystem(sim.Config{Seed: seed})}
}

func (w *world) props(name string, fn script) actor.Props[msg, io] {
	return actor.Props[msg, io]{
		Name: name,
		Boot: func(actor.Incarnation) msg { return start{} },
		New: func(actor.Env) actor.Actor[msg, io] {
			return &scripted{name: name, fn: fn, log: &w.log}
		},
	}
}

func (w *world) spawn(name string, fn script, opts ...sim.SpawnOption) actor.EntityRef[msg] {
	return sim.Spawn(w.sys, w.props(name, fn), opts...)
}

// lines returns the log lines of one actor, without the actor name and time.
func (w *world) lines(name string) []string {
	var out []string
	for _, l := range w.log {
		if rest, ok := strings.CutPrefix(l, name+"@"); ok {
			_, text, _ := strings.Cut(rest, " ")
			out = append(out, text)
		}
	}
	return out
}

// executor answers read after 10 ms with "v:"+key; write has no answer.
func executor(ctx *sim.ExecCtx, x io) {
	if r, ok := x.(read); ok {
		sim.Answer(ctx, r, 10*actor.Millisecond, readDone{V: "v:" + r.Key})
	}
}

const ms = actor.Millisecond

func toGot(d readDone) msg { return got(d) }
