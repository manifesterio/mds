package kafka

import (
	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/preset"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/sim"
)

// brokerConfig is the sim-friendly broker. A short flush window keeps commits
// frequent, and a small buffer engages backpressure. The segment size is pinned
// to 4 MiB and the batch cap to 8. A seed's swarm overrides the segment knobs
// (see swarmBroker).
func brokerConfig(bug broker.InjectedBug) broker.Config {
	return broker.Config{BatchWindow: 5 * actor.Millisecond, MaxSegmentBytes: 4 << 20, MaxSegmentBatches: 8,
		MaxBufferedBytes: 3 << 10, ResumeBufferedBytes: 1 << 10, InjectedBug: bug}
}

// Settings are the knobs of one seed after the swarm: the knobs of the cluster
// and the configuration of the broker.
type Settings struct {
	cluster.Settings
	Broker broker.Config
}

// Config says how to run one seed of the Kafka scenario: 3 brokers, 3
// at-least-once producer actors and 2 consumer actors over 2 topics x 3 partitions, all
// three fake services, attrition and a swarm over the fault families. A zero
// Config is the scenario as the campaign runs it.
type Config struct {
	Seed uint64
	// Log keeps every step line. HaltOnFirst stops at the first failure.
	// VerifyHashes recomputes every cached state hash at each step.
	Log, HaltOnFirst, VerifyHashes bool
	// Bug plants a bug in the broker (a test proves that the rules catch it).
	Bug broker.InjectedBug
	// Opts is the workload. A zero Opts is DefaultOpts.
	Opts Opts
	// Tune, when it is set, changes the knobs of the seed after the swarm.
	Tune func(s *Settings)
	// ExtraRules, when it is set, gives more rules. They run after the rules of
	// the scenario. A test uses them to count what a campaign did.
	ExtraRules func(c *cluster.Cluster, h *History) []sim.Rule
}

// ambiguousProduceConfig is a dedicated campaign preset that forces maybe-applied
// produce outcomes without wall-clock waits: a short virtual request timeout so
// RequestDue can fire while the broker may still apply the produce, and a raised
// ConnResetProb so in-flight frames reset. Client giveUp still resolves via
// history.ResultUnknown.
func ambiguousProduceConfig(seed uint64) Config {
	opts := DefaultOpts()
	opts.ReqTimeout = 50 * actor.Millisecond
	return Config{
		Seed: seed,
		Opts: opts,
		Tune: func(s *Settings) { s.ConnResetProb = 0.05 },
	}
}

// Run builds and runs one seed of the Kafka scenario.
func Run(cfg Config) sim.Result {
	opts := cfg.Opts
	if opts.Producers+opts.Consumers == 0 {
		opts = DefaultOpts()
	}
	p := preset.New()
	p.Settings.ControlPlane.MaxCommitItems = fakeservices.DynamoMaxCommitItems
	set := Settings{Settings: p.Settings, Broker: brokerConfig(cfg.Bug)}
	swarm(sim.DeriveRand(cfg.Seed, "swarm"), &set)
	if cfg.Tune != nil {
		cfg.Tune(&set)
	}
	c := cluster.New(cluster.Plan{
		Seed: cfg.Seed, Settings: set.Settings, Phases: p.Phases, QuiesceEvery: p.QuiesceEvery,
		Log: cfg.Log, HaltOnFirst: cfg.HaltOnFirst, VerifyHashes: cfg.VerifyHashes,
	})
	for _, t := range opts.Topics {
		c.ControlPlane.CreateTopic(t, opts.Partitions)
	}
	c.AddBrokers(3, broker.Props(set.Broker))
	w := c.System
	h := history.New[Op, Res](w.Now, func(msg string) { w.Fail(sim.FailProtocol, "history", msg) })
	for id := range opts.Producers + opts.Consumers {
		cluster.AddClient(c, clientSpec(id, h, opts))
	}
	w.AddRule(newOracle(opts, h, c.Objects, c.ControlPlane))
	w.AddRule(&progressCheck{hist: h})
	w.AddRule(sim.FaultCoverageRule(preset.KafkaFaultCoverage))
	w.AddRule(newSegmentCaps(set.Broker, set.ControlPlane.MaxCommitItems, opts.topicPartitions(), c.ControlPlane))
	w.AddRule(&segmentTiming{})
	if cfg.ExtraRules != nil {
		for _, r := range cfg.ExtraRules(c, h) {
			w.AddRule(r)
		}
	}
	return c.Run()
}

// swarm draws the knobs of one seed from the swarm stream of the seed.
func swarm(r actor.Rand, s *Settings) {
	preset.Swarm(r, &s.Settings)
	swarmBroker(r, s)
}

func swarmBroker(r actor.Rand, s *Settings) {
	bc := s.Broker
	pick := func(menu ...int) int { return menu[r.Intn(len(menu))] }
	window := func(menu ...int) actor.Duration { return actor.Duration(pick(menu...)) * actor.Millisecond }
	d := broker.DefaultConfig()
	switch x := r.Float64(); {
	case x < 0.2:
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 6<<10, 8<<10), window(20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(16, 32, 256), pick(12, 24, 96)
	case x < 0.4:
		bc.MaxSegmentBytes, bc.BatchWindow = pick(256<<10, 4<<20, 16<<20), window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 3, 4), pick(12, 24, 96)
	case x < 0.55:
		bc.MaxSegmentBytes, bc.BatchWindow = d.MaxSegmentBytes, d.BatchWindow
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = d.MaxSegmentBatches, d.MaxSegmentPartitions
	case x < 0.75:
		bc.MaxSegmentBytes, bc.BatchWindow = 16<<20, window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = 256, pick(2, 3, 4, 5, 6)
	default:
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 16<<10, 64<<10, 256<<10, 4<<20, 16<<20), window(5, 20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 4, 8, 32, 256), pick(3, 6, 12, 24, 96)
	}
	bc.MaxBufferedBytes = pick(3<<10, 3<<10, 16<<10, 64<<10, 1<<20)
	bc.ResumeBufferedBytes = bc.MaxBufferedBytes / 3
	if r.Chance(0.5) {
		s.ControlPlane.MaxCommitItems = min(1+bc.MaxSegmentPartitions, fakeservices.DynamoMaxCommitItems)
	}
	s.Broker = bc
}
