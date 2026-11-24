package campaign

import (
	"fmt"
	"time"

	"github.com/manifesterio/mds/dst/sim"
)

// simKindNames are the words that name a kind of failure of a sim run.
var simKindNames = map[sim.FailKind]string{
	sim.FailViolation:     "violation",
	sim.FailProtocol:      "protocol",
	sim.FailPanic:         "panic",
	sim.FailMaxSteps:      "max_steps",
	sim.FailNotRepeatable: KindNotRepeatable,
}

// KindNotRepeatable is the failure kind of a seed whose second run gave
// another result than the first (see Options.CheckEvery).
const KindNotRepeatable = "nondeterminism"

// FromSim turns the result of a sim run into a campaign result. The
// counter "steps" is set. A scenario may add its own counters to the
// returned value. When reproduce is empty, the campaign makes the replay
// command from Options.Tool.
func FromSim(r sim.Result, reproduce string) Result {
	out := Result{
		Seed: r.Seed, Hash: r.Hash, Steps: r.Steps, RandCalls: r.RandCalls,
		OK: r.OK(), Reproduce: reproduce,
		Coverage: make(map[string]Coverage, len(r.Coverage)),
		Counters: map[string]uint64{"steps": r.Steps},
		Sites:    r.Sites,
		Tail:     simLogLines(r.Tail),
		Log:      simLogLines(r.Log),
	}
	for name, c := range r.Coverage {
		out.Coverage[name] = Coverage{Kind: string(c.Kind), Checks: c.Checks, Passes: c.Passes}
	}
	for _, f := range r.Failures {
		kind, ok := simKindNames[f.Kind]
		if !ok {
			kind = fmt.Sprintf("kind(%d)", f.Kind)
		}
		out.Failures = append(out.Failures, Failure{
			Kind: kind, Property: f.Property, Detail: f.Detail, Step: f.Step,
			AtMs: int64(f.At) / int64(time.Millisecond), Repeats: f.Repeats,
		})
	}
	return out
}

// simLogLines formats step lines as "step time text".
func simLogLines(in []sim.LogEntry) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, l := range in {
		out = append(out, fmt.Sprintf("%6d %12v  %s", l.Step, time.Duration(l.At), l.Text))
	}
	return out
}
