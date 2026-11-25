// Command cpsim runs the deterministic simulation of the replicated control plane.
// It is a thin main over dst/explore/campaign: the scenarios, the flags, the report and
// the determinism check come from there. The flags of the control plane itself
// are -nodes, -steps and -bug.
//
//	go run ./cmd/cpsim -seeds 300                         # all scenarios, 300 seeds each
//	go run ./cmd/cpsim -scenario chaos -seed 17           # one seed, with a step trace
package main

import (
	"flag"
	"io"
	"os"

	"github.com/manifesterio/mds/controlplane/internal/verify/simulator"
	"github.com/manifesterio/mds/dst/explore/campaign"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// checkEvery is the determinism check: every Nth seed runs twice, and the two
// runs must give the same hash, steps and random calls.
const checkEvery = 10

// product holds the flags of the control plane. A scenario reads them when it runs.
type product struct {
	nodes int
	steps int
	bug   string
}

func run(args []string, stdout, stderr io.Writer) int {
	var p product
	return campaign.Main(config(&p), registry(&p), args, stdout, stderr)
}

// config is what cpsim gives the standard main.
func config(p *product) campaign.Config {
	return campaign.Config{
		Tool: "cpsim",
		Defaults: campaign.Options{
			CheckEvery: checkEvery,
			SummaryCounters: []campaign.SummaryCounter{
				{Key: "slots", Format: "%7d slots"},
				{Key: "acked", Format: "%6d acked writes"},
			},
		},
		ExtraFlags: func(fs *flag.FlagSet) {
			fs.IntVar(&p.nodes, "nodes", 3, "number of replicas")
			fs.IntVar(&p.steps, "steps", simulator.DefaultSteps, "scheduler events in the fault phase of a run")
			fs.StringVar(&p.bug, "bug", "", "plant a bug to see the simulator find it: writer-offsets, log-overwrite, claim-always-wins")
		},
	}
}

// registry has one campaign scenario for each scenario of the simulator.
func registry(p *product) *campaign.Registry {
	var reg campaign.Registry
	for _, name := range simulator.ScenarioNames() {
		sc, _ := simulator.ScenarioByName(name)
		campaign.RegisterSim(&reg, sc.Name, sc.Detail,
			"go run ./cmd/cpsim -scenario %s -seed %d",
			runner(sc.Name, p),
		)
	}
	return &reg
}

// runner keeps product Stats and Report text; ExtraFlags stay on Config.
func runner(scenario string, p *product) func(uint64, campaign.RunOptions) campaign.SimSeed {
	return func(seed uint64, o campaign.RunOptions) campaign.SimSeed {
		res, err := simulator.Run(simulator.Config{
			Seed: seed, Scenario: scenario, Nodes: p.nodes, Steps: p.steps, Bug: simulator.Bug(p.bug),
			Log: o.Log, HaltOnFirst: o.HaltOnFirst,
		})
		if err != nil {
			return campaign.FailSimSeed(seed, "scenario", err.Error())
		}
		out := campaign.SimSeed{
			Result: res.Result,
			Counters: map[string]uint64{
				"slots": uint64(res.Stats.LogSlots),
				"acked": uint64(res.Stats.Acked),
			},
		}
		if !res.OK() || o.Log {
			out.Text = simulator.Report(res)
		}
		return out
	}
}
