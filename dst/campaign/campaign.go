// Package campaign runs many independent seeds in parallel, folds the results
// and prints one report. A product registers its scenarios in a Registry and
// calls Main. The standard flags, the environment variables, the report, the
// until-stable mode and the determinism re-run are all here.
//
// Files:
//
//   - campaign.go: Outcome, Summary and RunSeeds (the parallel fold)
//   - registry.go: Scenario, RunOptions and Registry
//   - options.go: Config, Options and ParseOptions (flags and variables)
//   - run.go: RunCampaign, Replay and Main
//   - report.go: Report, its text and JSON
package campaign

import (
	goruntime "runtime"
	"slices"
	"sync"
	"time"

	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

// SiteTotal counts one Maybe site over many seeds: in how many runs it was
// asked (Reached), in how many it was switched on (On), and the sums of
// calls and fires.
type SiteTotal struct {
	Reached uint64
	On      uint64
	Calls   uint64
	Fired   uint64
}

// Outcome is one seed of a scenario: the sim result plus what the product adds.
type Outcome struct {
	sim.Result
	// Counters are optional named totals of the run (writes, ...). RunSeeds
	// sums them. The counter "steps" is added from Result.Steps.
	Counters map[string]uint64
	// Text is the own text of the product for this seed. When it is set, a
	// failed seed prints it in place of the standard failure lines, and Replay
	// prints it after the step lines in place of the standard last lines.
	Text string
}

// SetupFailure is the Outcome of a run that failed before any System existed
// (unknown scenario, bad flags): one protocol failure of property.
func SetupFailure(seed uint64, property, detail string) Outcome {
	return Outcome{Result: sim.Result{
		Seed: seed, Failures: []sim.Failure{{Kind: sim.FailProtocol, Property: property, Detail: detail}},
	}}
}

// Counts summarizes how many seeds ran and how many failed.
type Counts struct {
	Seeds  int
	OK     int
	Failed int
}

// Summary is the outcome of many seeds of one scenario.
type Summary struct {
	Failed   []Outcome
	Hash     uint64
	Coverage map[string]sim.Coverage
	Counters map[string]uint64
	Sites    map[string]SiteTotal
	Elapsed  time.Duration
	Counts   Counts
}

// Merge adds the coverage, counters and sites of o into s, for a total over
// several campaigns. Hash, Elapsed, Counts and Failed are not touched.
func (s *Summary) Merge(o Summary) {
	if s.Coverage == nil {
		s.Coverage = map[string]sim.Coverage{}
	}
	addCoverage(s.Coverage, o.Coverage)
	if s.Sites == nil {
		s.Sites = map[string]SiteTotal{}
	}
	for k, v := range o.Sites {
		t := s.Sites[k]
		t.Reached += v.Reached
		t.On += v.On
		t.Calls += v.Calls
		t.Fired += v.Fired
		s.Sites[k] = t
	}
	if s.Counters == nil {
		s.Counters = map[string]uint64{}
	}
	for k, v := range o.Counters {
		s.Counters[k] += v
	}
}

// Missing returns the sorted names in expected that no seed passed: they have
// no coverage entry or zero Passes.
func Missing(cov map[string]sim.Coverage, expected []string) []string {
	var out []string
	for _, name := range expected {
		if cov[name].Passes == 0 {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// NeverFired returns the sorted names of the sites that did not fire in any
// run: sites in sites with Fired 0, and names in expected that no run reached.
func NeverFired(sites map[string]SiteTotal, expected []string) []string {
	var out []string
	for name, t := range sites {
		if t.Fired == 0 {
			out = append(out, name)
		}
	}
	for _, name := range expected {
		if sites[name].Fired == 0 && !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// RunSeeds executes seeds first, first+1, ... for seeds count, using workers goroutines.
// When workers <= 0, GOMAXPROCS is used. Each run is independent; results are
// folded in seed order. Coverage maps are merged by summing Checks and Passes.
func RunSeeds(first uint64, seeds int, workers int, run func(seed uint64) Outcome) Summary {
	if seeds <= 0 {
		return Summary{Coverage: map[string]sim.Coverage{}, Counters: map[string]uint64{}, Sites: map[string]SiteTotal{}}
	}
	if workers <= 0 {
		workers = goruntime.GOMAXPROCS(0)
	}
	workers = min(workers, seeds)

	start := time.Now()
	results := make([]Outcome, seeds)
	var wg sync.WaitGroup
	jobs := make(chan int)
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				seed := first + uint64(i)
				r := run(seed)
				r.Seed = seed
				results[i] = r
			}
		})
	}
	for i := range seeds {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	out := Summary{
		Coverage: map[string]sim.Coverage{},
		Counters: map[string]uint64{},
		Sites:    map[string]SiteTotal{},
		Counts:   Counts{Seeds: seeds},
	}
	h := trace.New()
	for _, r := range results {
		h.Uint64(r.Hash)
		addCoverage(out.Coverage, r.Coverage)
		out.Counters["steps"] += r.Steps
		for k, v := range r.Counters {
			out.Counters[k] += v
		}
		for k, v := range r.Sites {
			t := out.Sites[k]
			t.Reached++
			if v.On {
				t.On++
			}
			t.Calls += v.Calls
			t.Fired += v.Fired
			out.Sites[k] = t
		}
		if r.OK() {
			out.Counts.OK++
			continue
		}
		out.Failed = append(out.Failed, r)
		out.Counts.Failed++
	}
	out.Hash = h.Sum()
	out.Elapsed = time.Since(start)
	return out
}

func addCoverage(into, from map[string]sim.Coverage) {
	for k, c := range from {
		t := into[k]
		t.Kind = c.Kind
		t.Checks += c.Checks
		t.Passes += c.Passes
		into[k] = t
	}
}
