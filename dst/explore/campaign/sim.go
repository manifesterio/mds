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
// counter "steps" is set. Each extra map is merged into Counters after
// that. When reproduce is empty, the campaign makes the replay command
// from Options.Tool.
func FromSim(r sim.Result, reproduce string, extras ...map[string]uint64) Result {
	out := Result{
		Seed: r.Seed, Hash: r.Hash, Steps: r.Steps, RandCalls: r.RandCalls,
		OK: r.OK(), Reproduce: reproduce,
		Coverage: make(map[string]Coverage, len(r.Coverage)),
		Counters: map[string]uint64{"steps": r.Steps},
		Sites:    r.Sites,
		Tail:     simLogLines(r.Tail),
		Log:      simLogLines(r.Log),
	}
	for _, extra := range extras {
		for k, v := range extra {
			out.Counters[k] = v
		}
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

// CrashExitCounters reports crashes and exits of a sim run for campaign summaries.
func CrashExitCounters(r sim.Result) map[string]uint64 {
	return map[string]uint64{
		"crashes": uint64(r.Crashes),
		"exits":   uint64(r.Exits),
	}
}

// RegisterSim adds a scenario whose Run returns a sim.Result. reproduceFmt is
// fmt.Sprintf'd with the scenario name and the seed. When counters is non-nil,
// its map is merged into the campaign result (after steps).
func RegisterSim(reg *Registry, name, detail, reproduceFmt string, run func(uint64, RunOptions) sim.Result, counters func(sim.Result) map[string]uint64) {
	reg.Add(Scenario{
		Name:   name,
		Detail: detail,
		Run: func(seed uint64, o RunOptions) Result {
			r := run(seed, o)
			var extra map[string]uint64
			if counters != nil {
				extra = counters(r)
			}
			return FromSim(r, fmt.Sprintf(reproduceFmt, name, r.Seed), extra)
		},
	})
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
