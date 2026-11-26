package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/actor"
)

// env is the actor.Env of self: the run stream, buggify and assertions,
// shared by every actor, service and rule of the run. Self is zero for host
// code and services.
func (sys *System) env(self actor.Incarnation) actor.Env {
	return actor.NewEnv(self, sys.rand, sys.faults, simAsserts{sys.asserts})
}

// simAsserts is the assertLog as actor.Assertions.
type simAsserts struct{ l *assertLog }

func (a simAsserts) Always(c bool, p actor.Always, d ...actor.Pair) {
	a.l.Always(c, assertProp(p.Name()), d...)
}

func (a simAsserts) AlwaysOrUnreachable(c bool, p actor.AlwaysOrUnreachable, d ...actor.Pair) {
	a.l.AlwaysOrUnreachable(c, assertProp(p.Name()), d...)
}

func (a simAsserts) Sometimes(c bool, p actor.Sometimes) {
	a.l.Sometimes(c, assertProp(p.Name()))
}

func (a simAsserts) Reachable(p actor.Reachable) { a.l.Reachable(assertProp(p.Name())) }

func (a simAsserts) Unreachable(p actor.Unreachable, d ...actor.Pair) {
	a.l.Unreachable(assertProp(p.Name()), d...)
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
		sys.tail.add(l)
	}
}
