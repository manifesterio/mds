// Package assertlog records properties (Always, Sometimes, Reachable, ...)
// without panicking, so a run keeps going after a failure. The run owns the
// Log; products record through actor.Env with the typed handles of
// dst/core/assert.
package assertlog

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/dst/core/assert"
)

// Property is a stable assertion id: the catalog name of a handle.
type Property string

// isSafety reports whether a kind fails the run when it is false (Always,
// AlwaysOrUnreachable, Unreachable). The other kinds count coverage.
func isSafety(k assert.Kind) bool {
	return k == assert.KindAlways || k == assert.KindAlwaysOrUnreachable || k == assert.KindUnreachable
}

// Log records and continues. A false Always calls onFail and the run keeps going.
// Invariant: assertions never panic.
type Log struct {
	onFail func(property, detail string)
	step   func() uint64
	stats  map[Property]*coverage
}

// coverage is Coverage plus a flag that says FirstStep is set.
type coverage struct {
	assert.Coverage
	hit bool
}

// New returns a log that reports failures through onFail. onFail may be nil.
func New(onFail func(property, detail string)) *Log {
	return &Log{onFail: onFail, stats: map[Property]*coverage{}}
}

// SetStepCounter gives the log the step counter of the run, for
// Coverage.FirstStep. Without it, FirstStep stays 0.
func (l *Log) SetStepCounter(step func() uint64) { l.step = step }

func (l *Log) fail(property, detail string) {
	if l.onFail != nil {
		l.onFail(property, detail)
	}
}

// record counts one evaluation. ok is the result of the check.
func (l *Log) record(p Property, kind assert.Kind, ok bool) {
	s := l.stats[p]
	if s == nil {
		s = &coverage{Coverage: assert.Coverage{Kind: kind}}
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

func detailText(details []assert.Pair) string {
	var b strings.Builder
	for _, d := range details {
		fmt.Fprintf(&b, " %s=%v", d.Key, d.Value)
	}
	return strings.TrimSpace(b.String())
}

// Always records cond under p. A false cond calls onFail.
func (l *Log) Always(cond bool, p Property, details ...assert.Pair) {
	l.record(p, assert.KindAlways, cond)
	if !cond {
		l.fail(string(p), detailText(details))
	}
}

// AlwaysOrUnreachable is like Always, but it is recorded with its own kind:
// it is fine if the code never reaches it.
func (l *Log) AlwaysOrUnreachable(cond bool, p Property, details ...assert.Pair) {
	l.record(p, assert.KindAlwaysOrUnreachable, cond)
	if !cond {
		l.fail(string(p), detailText(details))
	}
}

// Sometimes records whether cond held without failing the run.
func (l *Log) Sometimes(cond bool, p Property) {
	l.record(p, assert.KindSometimes, cond)
}

// Reachable records that p was reached.
func (l *Log) Reachable(p Property) {
	l.record(p, assert.KindReachable, true)
}

// Unreachable reports that an unreachable point was hit. It is counted as a
// check that did not pass.
func (l *Log) Unreachable(p Property, details ...assert.Pair) {
	l.record(p, assert.KindUnreachable, false)
	l.fail(string(p), "reached an Unreachable point "+detailText(details))
}

// Coverage returns a copy of the per-property stats.
func (l *Log) Coverage() map[string]assert.Coverage {
	out := make(map[string]assert.Coverage, len(l.stats))
	//determinismcheck:ok copies into another map; iteration order does not escape
	for p, s := range l.stats {
		out[string(p)] = s.Coverage
	}
	return out
}
