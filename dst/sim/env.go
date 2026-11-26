package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/fault"
	"github.com/manifesterio/mds/dst/internal/assertlog"
)

// env is the actor.Env of a System: the run stream, buggify and assertions,
// shared by every actor, service and rule of the run, plus the incarnation
// it was made for (zero for host code and services).
type env struct {
	sys  *System
	self actor.Incarnation
}

func (e env) Self() actor.Incarnation { return e.self }

func (e env) Duration(lo, hi actor.Duration) actor.Duration {
	return actor.Duration(e.sys.rand.Between(int64(lo), int64(hi)))
}

func (e env) Uint64() uint64               { return e.sys.rand.Uint64() }
func (e env) Intn(n int) int               { return e.sys.rand.Intn(n) }
func (e env) Between(lo, hi int64) int64   { return e.sys.rand.Between(lo, hi) }
func (e env) Float64() float64             { return e.sys.rand.Float64() }
func (e env) Chance(p float64) bool        { return e.sys.rand.Chance(p) }
func (e env) Maybe(s fault.Site) bool      { return e.sys.faults.Maybe(s) }
func (e env) Reachable(p assert.Reachable) { e.sys.asserts.Reachable(assertlog.Property(p.Name())) }
func (e env) Sometimes(c bool, p assert.Sometimes) {
	e.sys.asserts.Sometimes(c, assertlog.Property(p.Name()))
}

// NewID draws two Uint64 values and puts them in the id, little-endian.
func (e env) NewID() actor.ID {
	var id actor.ID
	e.sys.rand.Bytes(id[:])
	return id
}

func (e env) MaybeP(s fault.Site, p float64) bool {
	return e.sys.faults.MaybeP(s, p)
}

func (e env) Fault(s fault.Site, p float64) bool {
	return e.sys.faults.Fault(s, p)
}

func (e env) Always(c bool, p assert.Always, d ...assert.Pair) {
	e.sys.asserts.Always(c, assertlog.Property(p.Name()), d...)
}

func (e env) AlwaysOrUnreachable(c bool, p assert.AlwaysOrUnreachable, d ...assert.Pair) {
	e.sys.asserts.AlwaysOrUnreachable(c, assertlog.Property(p.Name()), d...)
}

func (e env) Unreachable(p assert.Unreachable, d ...assert.Pair) {
	e.sys.asserts.Unreachable(assertlog.Property(p.Name()), d...)
}

// logLine is one step line. The message is kept and turned into text only
// when the Result is built. Log text is never hashed.
type logLine struct {
	step uint64
	at   Time
	text string
	msg  actor.Payload
}

func (l logLine) entry() LogEntry {
	text := l.text
	if l.msg != nil {
		s := fmt.Sprintf("%T %+v", l.msg, l.msg)
		if len(s) > 160 {
			s = s[:160] + "..."
		}
		text += " " + s
	}
	return LogEntry{Step: l.step, At: l.at, Text: text}
}

// logMsg keeps a log line when Config.Log or Config.Tail asks for lines.
func (sys *System) logMsg(text string, msg actor.Payload) {
	if !sys.cfg.Log && sys.tail == nil {
		return
	}
	l := logLine{step: sys.steps, at: sys.q.now, text: text, msg: msg}
	if sys.cfg.Log {
		sys.lines = append(sys.lines, l)
	}
	if sys.tail != nil {
		sys.tail.Add(l)
	}
}

var _ actor.Env = env{}
