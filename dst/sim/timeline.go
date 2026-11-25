package sim

import (
	"slices"

	"github.com/manifesterio/mds/dst/core/actor"
)

// Kernel fact names written by the System (and host RecordFault).
const (
	factPhase   = "sim_phase"
	factFault   = "sim_fault"
	factCrashed = "sim_crashed"
	factExited  = "sim_exited"
	factQuiesce = "sim_quiesce"
)

// Timeline is the append-only list of facts of a run: one log in append order
// plus an index per name, so a rule that reads one fact name never scans the
// others. In a run only the System appends.
type Timeline struct {
	seq    uint64
	n      int
	byName map[string][]TimedFact
}

// Cursor remembers how far a reader has read one fact name. Use one Cursor
// per name.
type Cursor struct{ next int }

// TimedFact is one fact on the timeline.
type TimedFact struct {
	Seq    uint64
	At     Time
	Source actor.Incarnation // zero for System, service and host facts
	Fact   actor.Fact
}

// NewTimeline returns an empty timeline.
func NewTimeline() *Timeline { return &Timeline{byName: map[string][]TimedFact{}} }

// Append stamps and records a fact and returns its sequence number (from 1).
// In a run only the System calls it; tests call it to build a timeline by
// hand.
func (t *Timeline) Append(source actor.Incarnation, at Time, f actor.Fact) uint64 {
	t.seq++
	t.n++
	t.byName[f.Name] = append(t.byName[f.Name], TimedFact{Seq: t.seq, At: at, Source: source, Fact: f})
	return t.seq
}

// Len is the number of facts of every name.
func (t *Timeline) Len() int { return t.n }

// Since returns the facts named name that were appended after c, and moves c
// to the end.
func (t *Timeline) Since(name string, c *Cursor) []TimedFact {
	l := t.byName[name]
	if c.next >= len(l) {
		return nil
	}
	out := l[c.next:len(l):len(l)]
	c.next = len(l)
	return out
}

// Snapshot returns every fact named name so far. Do not change it.
func (t *Timeline) Snapshot(name string) []TimedFact {
	return slices.Clip(t.byName[name])
}

// PhaseStart reports when the System announced phase p, from its sim_phase fact.
func (t *Timeline) PhaseStart(p Phase) (Time, bool) {
	for _, tf := range t.Snapshot(factPhase) {
		if ph, ok := tf.Fact.Field("phase").(actor.Uint64Value); ok && Phase(ph) == p {
			return tf.At, true
		}
	}
	return 0, false
}

// HasFact reports that a fact named name was recorded. When key is non-empty,
// one of those facts must also have that string field equal to val.
func (t *Timeline) HasFact(name, key, val string) bool {
	for _, tf := range t.Snapshot(name) {
		if key == "" {
			return true
		}
		if s, ok := tf.Fact.Field(key).(actor.StringValue); ok && string(s) == val {
			return true
		}
	}
	return false
}

// HasCrashCause reports a sim_crashed fact with the given cause field.
func (t *Timeline) HasCrashCause(cause string) bool {
	return t.HasFact(factCrashed, "cause", cause)
}

// HasExited reports a sim_exited fact (graceful stop).
func (t *Timeline) HasExited() bool {
	return t.HasFact(factExited, "", "")
}
