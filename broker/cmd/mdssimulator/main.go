// Command mdssimulator runs a scenario campaign (the Kafka broker) through the
// deterministic simulator. It is a thin main over dst/campaign: the flags, the environment variables (MDS_SEEDS, MDS_SEED,
// MDS_CAMPAIGN), the report and the determinism check come from there. It reads
// flags and the clock and runs seeds on many goroutines, so it is not a
// deterministic package.
//
//	go run ./cmd/mdssimulator -scenario kafka -seeds 300   # a campaign
//	go run ./cmd/mdssimulator -scenario kafka -seed 17     # one seed, with a step log
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/manifesterio/mds/broker/internal/verify/simulator/kafka"
	"github.com/manifesterio/mds/dst/campaign"
	"github.com/manifesterio/mds/dst/kernel"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	return campaign.Main(config(), registry(), args, stdout, stderr)
}

// checkEvery is the determinism check: every Nth seed runs twice, and the two
// runs must give the same hash, steps and random calls.
const checkEvery = 10

// minimumCoverage is the campaign size from which a path that no seed reached
// counts as a failure (a smaller campaign cannot be expected to reach it).
const minimumCoverage = 100

// config is what mdssimulator gives the standard main. The default scenario is
// kafka.
func config() campaign.Config {
	return campaign.Config{
		Tool:      "mdssimulator",
		EnvPrefix: "MDS",
		Defaults: campaign.Options{
			Scenarios:           []string{"kafka"},
			CheckEvery:          checkEvery,
			MinSeedsForCoverage: minimumCoverage,
		},
	}
}

// registry has one scenario: kafka (the broker under a Kafka workload).
func registry() *campaign.Registry {
	var reg campaign.Registry
	add(&reg, "kafka", "the broker under a Kafka workload",
		func(seed uint64, o campaign.RunOptions) kernel.Result {
			return kafka.Run(kafka.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst})
		})
	return &reg
}

// add registers a scenario that runs one seed with run.
func add(reg *campaign.Registry, name, detail string, run func(uint64, campaign.RunOptions) kernel.Result) {
	reg.Add(campaign.Scenario{
		Name:   name,
		Detail: detail,
		Run: func(seed uint64, o campaign.RunOptions) campaign.Result {
			return fromKernel(name, run(seed, o))
		},
	})
}

// fromKernel turns the result of one seed into a campaign result. It adds the
// replay command and the two counters (crashes and exits) that the old tool
// printed for each seed.
func fromKernel(scenario string, r kernel.Result) campaign.Result {
	out := campaign.FromKernel(r, fmt.Sprintf("go run ./cmd/mdssimulator -scenario %s -seed %d", scenario, r.Seed))
	out.Counters["crashes"] = uint64(r.Crashes)
	out.Counters["exits"] = uint64(r.Exits)
	return out
}
