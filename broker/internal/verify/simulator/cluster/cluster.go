// Package cluster is the host of one broker simulation run on dst/sim. It
// builds the System of a seed: the two fake outside systems (object store and
// control plane, as sim services), the connection table (network.go), the
// broker actors, the client actors (test workloads), the phases, the broker
// attrition and the quiesce task. A scenario (kafka) gives it the broker
// Props, the clients and the rules.
//
// The brokers and the clients share one executor: it forwards object store
// and control-plane calls to their services, and runs the connection table
// for the socket IO.
package cluster

import (
	"fmt"
	"slices"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/sim"
)

// wallStart is the virtual UTC time at the start of every run (2023-11-14).
const wallStart actor.Wall = 1_700_000_000 * 1_000_000_000

// tailLines is how many of the last step lines a failed run keeps.
const tailLines = 48

// Settings are the knobs of one run. A scenario starts from the defaults of
// internal/preset, and its swarm changes them for each seed.
type Settings struct {
	ObjectStore  fakeservices.ObjectStoreFaults
	ControlPlane fakeservices.ControlPlaneFaults
	// NetDelay bounds the one-way delay of one frame on a connection.
	NetDelay [2]actor.Duration
	// ConnResetProb is the chance that one write resets its connection while
	// faults are on.
	ConnResetProb float64
	// ClockSkewMax is the largest fixed clock skew of one actor. It is drawn
	// once for each actor when it is spawned.
	ClockSkewMax actor.Duration
	// Attrition kills and restarts brokers during the fault phase.
	Attrition sim.AttritionPlan
	// RecoveryDelay is the time from the end of a broker incarnation to its
	// next boot.
	RecoveryDelay [2]actor.Duration
	// Cost is the CPU model of every actor: how long it is busy after a Receive.
	Cost sim.CostParams
}

// Plan says how to build one run.
type Plan struct {
	Seed     uint64
	Settings Settings
	Phases   sim.Phases
	// QuiesceEvery is the time between two quiesce points. Zero: no quiesce point.
	QuiesceEvery actor.Duration
	// MaxSteps is the step limit of the run. Zero: the System default.
	MaxSteps uint64
	// Log keeps every step line. HaltOnFirst stops the run at the first failure.
	// VerifyHashes recomputes every cached state hash at each step.
	Log, HaltOnFirst, VerifyHashes bool
}

// Client is a test client actor. It records every operation in a history and
// does not judge.
type Client[In actor.Payload] interface {
	actor.Actor[In, core.IO]
	// Quiescent reports that no operation is in flight.
	Quiescent() bool
}

// ClientSpec says how to run one client.
type ClientSpec[In actor.Payload] struct {
	// New builds the client. brokers are the broker actor names, in order;
	// a client dials them by name (core.Connect.Addr).
	New func(env actor.Env, brokers []string) Client[In]
	// Boot makes the first message of the client.
	Boot func(self actor.Incarnation) In
	// OnPhase makes the message the client gets when the phase changes.
	// Nil: none.
	OnPhase func(p sim.Phase) (In, bool)
	// FromNet turns a connection event (core.ConnOpened is never sent to a
	// client; core.BytesReceived and core.ConnClosed are) into a message.
	FromNet func(ev core.EventData) In
}

// Cluster is the System of one run and the handles a scenario needs.
type Cluster struct {
	// System is the simulation. A scenario adds its rules to it.
	System *sim.System
	// Objects and ControlPlane are the fake services. A rule reads the
	// ground truth from them (see package truth).
	Objects      *fakeservices.ObjectStore
	ControlPlane *fakeservices.ControlPlane

	plan    Plan
	net     *network
	brokers []string
	clients []*clientSlot
}

// clientSlot holds the client built by the last boot of one client actor.
type clientSlot struct {
	live interface{ Quiescent() bool }
}

// New builds the System and the two services. Add the brokers, then the
// clients, then the rules, then call Run.
func New(p Plan) *Cluster {
	sys := sim.NewSystem(sim.Config{
		Seed: p.Seed, WallStart: wallStart, MaxSteps: p.MaxSteps,
		ClockSkewMax: p.Settings.ClockSkewMax, Attrition: p.Settings.Attrition,
		Log: p.Log, Tail: tailLines, HaltOnFirst: p.HaltOnFirst, VerifyHashes: p.VerifyHashes,
	})
	c := &Cluster{System: sys, plan: p}
	c.Objects = fakeservices.NewObjectStore(p.Settings.ObjectStore)
	c.ControlPlane = fakeservices.NewControlPlane(p.Settings.ControlPlane)
	c.net = newNetwork(sys, p.Settings.NetDelay, p.Settings.ConnResetProb,
		sim.AddService[core.ObjectCall, core.ObjectDone](sys, c.Objects.Name(), c.Objects),
		sim.AddService[core.ControlPlaneCall, core.ControlPlaneDone](sys, c.ControlPlane.Name(), c.ControlPlane))
	return c
}

// restart is the restart policy of a broker: every end (crash, trip or exit)
// is followed by a boot after a delay in RecoveryDelay.
func (c *Cluster) restart(_ actor.End, r actor.Rand) (actor.Duration, bool) {
	d := c.plan.Settings.RecoveryDelay
	return r.Duration(d[0], d[1]), true
}

// AddBrokers spawns n brokers named broker-0, broker-1, ... from props (its
// Name and Restart are replaced). Clients dial them by name, and the System
// may kill them.
func (c *Cluster) AddBrokers(n int, props actor.Props[core.EventData, core.IO]) {
	for i := range n {
		p := props
		p.Name = fmt.Sprintf("broker-%d", i)
		p.Restart = c.restart
		ref := sim.Spawn(c.System, p,
			sim.WithExecutor(c.net.exec),
			sim.Cost(c.plan.Settings.Cost),
			sim.Attrition(),
			sim.OnEnd(c.net.ended))
		c.net.add(p.Name, ref, func(ev core.EventData) { sim.Tell(c.System, ref, 0, ev) })
		c.brokers = append(c.brokers, p.Name)
	}
}

// AddClient spawns a client named client-0, client-1, ... as a workload: it
// boots after every broker and never ends. Add the brokers first: the client
// gets their names.
func AddClient[In actor.Payload](c *Cluster, s ClientSpec[In]) actor.EntityRef[In] {
	slot := &clientSlot{}
	name := fmt.Sprintf("client-%d", len(c.clients))
	c.clients = append(c.clients, slot)
	brokers := slices.Clone(c.brokers)
	opts := []sim.SpawnOption{sim.WithExecutor(c.net.exec), sim.Workload(), sim.Cost(c.plan.Settings.Cost)}
	if s.OnPhase != nil {
		opts = append(opts, sim.OnPhase(s.OnPhase))
	}
	ref := sim.Spawn(c.System, actor.Props[In, core.IO]{
		Name: name,
		New: func(env actor.Env) actor.Actor[In, core.IO] {
			cl := s.New(env, brokers)
			slot.live = cl
			return cl
		},
		Boot: s.Boot,
	}, opts...)
	c.net.add(name, ref, func(ev core.EventData) { sim.Tell(c.System, ref, 0, s.FromNet(ev)) })
	return ref
}

// quiesce appends a "sim_quiesce" fact that says whether no client has an
// operation in flight, then queues the next one.
func (c *Cluster) quiesce() {
	q := true
	for _, s := range c.clients {
		if s.live != nil && !s.live.Quiescent() {
			q = false
		}
	}
	c.System.Fact(actor.Fact{Name: "sim_quiesce", Fields: []actor.Field{{Key: "quiescent", Value: actor.Bool(q)}}})
	c.System.After(c.plan.QuiesceEvery, c.quiesce)
}

// Run schedules the phases and the quiesce points, boots the actors, runs the
// System to the end and returns the result.
func (c *Cluster) Run() sim.Result {
	sim.SchedulePhases(c.System, c.plan.Phases)
	if c.plan.QuiesceEvery > 0 {
		c.System.After(c.plan.QuiesceEvery, c.quiesce)
	}
	return c.System.Run()
}
