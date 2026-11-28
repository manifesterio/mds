// assertLog records properties (Always, Sometimes, Reachable, ...) without
// panicking, so a run keeps going after a failure. The run owns the log;
// products record through actor.Env with the typed handles of dst/actor.

package sim

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/dst/actor"
)

// assertProp is a stable assertion id: the catalog name of a handle.
type assertProp string

// isSafety reports whether a kind fails the run when it is false (Always,
// AlwaysOrUnreachable, Unreachable). The other kinds count coverage.
func isSafety(k actor.AssertKind) bool {
	return k == actor.KindAlways || k == actor.KindAlwaysOrUnreachable || k == actor.KindUnreachable
}

// assertLog records and continues. A false Always calls onFail and the run keeps going.
// Invariant: assertions never panic.
type assertLog struct {
	onFail func(property, detail string)
	step   func() uint64
	stats  map[assertProp]*coverage
}

// Coverage counts evaluations of one property.
//
// FirstStep is the step of the first interesting evaluation: a safety check
// that failed, or a coverage check that passed. It is 0 when there was none,
// or when it happened before the first step.
type Coverage struct {
	Kind      actor.AssertKind
	Checks    uint64
	Passes    uint64
	FirstStep uint64
}

// coverage is Coverage plus a flag that says FirstStep is set.
type coverage struct {
	Coverage
	hit bool
}

// newAssertLog returns a log that reports failures through onFail. onFail may be nil.
func newAssertLog(onFail func(property, detail string)) *assertLog {
	return &assertLog{onFail: onFail, stats: map[assertProp]*coverage{}}
}

// SetStepCounter gives the log the step counter of the run, for
// Coverage.FirstStep. Without it, FirstStep stays 0.
func (l *assertLog) SetStepCounter(step func() uint64) { l.step = step }

func (l *assertLog) fail(property, detail string) {
	if l.onFail != nil {
		l.onFail(property, detail)
	}
}

// record counts one evaluation. ok is the result of the check.
func (l *assertLog) record(p assertProp, kind actor.AssertKind, ok bool) {
	s := l.stats[p]
	if s == nil {
		s = &coverage{Coverage: Coverage{Kind: kind}}
		l.stats[p] = s
	}
	s.Checks++
	if ok {
		s.Passes++
	}
	if ok != isSafety(kind) && !s.hit {
		s.hit = true
		if l.step != nil {
			s.FirstStep = l.step()
		}
	}
}

func detailText(details []actor.Pair) string {
	var b strings.Builder
	for _, d := range details {
		fmt.Fprintf(&b, " %s=%v", d.Key, d.Value)
	}
	return strings.TrimSpace(b.String())
}

// Always records cond under p. A false cond calls onFail.
func (l *assertLog) Always(cond bool, p assertProp, details ...actor.Pair) {
	l.record(p, actor.KindAlways, cond)
	if !cond {
		l.fail(string(p), detailText(details))
	}
}

// AlwaysOrUnreachable is like Always, but it is recorded with its own kind:
// it is fine if the code never reaches it.
func (l *assertLog) AlwaysOrUnreachable(cond bool, p assertProp, details ...actor.Pair) {
	l.record(p, actor.KindAlwaysOrUnreachable, cond)
	if !cond {
		l.fail(string(p), detailText(details))
	}
}

// Sometimes records whether cond held without failing the run.
func (l *assertLog) Sometimes(cond bool, p assertProp) {
	l.record(p, actor.KindSometimes, cond)
}

// Reachable records that p was reached.
func (l *assertLog) Reachable(p assertProp) {
	l.record(p, actor.KindReachable, true)
}

// Unreachable reports that an unreachable point was hit. It is counted as a
// check that did not pass.
func (l *assertLog) Unreachable(p assertProp, details ...actor.Pair) {
	l.record(p, actor.KindUnreachable, false)
	l.fail(string(p), "reached an Unreachable point "+detailText(details))
}

// Coverage returns a copy of the per-property stats.
func (l *assertLog) Coverage() map[string]Coverage {
	out := make(map[string]Coverage, len(l.stats))
	//determinismcheck:ok copies into another map; iteration order does not escape
	for p, s := range l.stats {
		out[string(p)] = s.Coverage
	}
	return out
}
