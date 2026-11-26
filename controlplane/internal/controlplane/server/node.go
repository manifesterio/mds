package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/broker"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
)

// Config wires a Node.
type Config struct {
	Machine replica.Config
	Clock   live.Clock
	Log     executor.Log
	// Objects is the checkpoint store. Nil: in memory.
	Objects executor.ObjectStore
	// StopGrace is how long the actors get after Shutdown to exit.
	StopGrace actor.Duration
	// BrokerLeaseTTL is how long a broker stays live after its last heartbeat
	// (default 5s). Brokers send one every interval, so keep it 3x the interval.
	BrokerLeaseTTL time.Duration
	// GroupInitialRebalanceDelay is how long a new consumer group waits for
	// more members before its first generation (default 3s).
	GroupInitialRebalanceDelay time.Duration
}

// Node serves ControlPlane RPCs. It runs one live Host with two actors: the
// replica, and the group coordinator (rebalance), which asks the replica
// directly. gRPC handlers reach them with live.Ask and live.OnLoop.
type Node struct {
	cpv1.UnimplementedControlPlaneServer

	config  Config
	host    *live.Host
	replica actor.EntityRef[runtime.EventData]
	groups  actor.EntityRef[rebalance.Input]
	m       atomic.Pointer[replica.Replica]
	health  *health.Server
	leases  *broker.Leases // replica goroutine only (OnLoop); empty until brokers heartbeat

	// active is the role bit: the replica is the active appender and Ready.
	// Only the replica's AfterReceive hook writes it, and rebalance hears of
	// every change as a RoleChanged.
	active atomic.Bool

	mu        sync.Mutex
	shutdown  chan struct{}
	started   bool          // Start launched the Host (guarded by mu)
	loopDone  chan struct{} // closed when the Host has stopped
	loopErr   error         // the result of the Host; set before loopDone is closed
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error

	// epoch and stepDowns are copies of the replica's write role, stored
	// after every Receive, so a metrics scrape never waits on the actor.
	epoch     atomic.Uint64
	stepDowns atomic.Uint64
}

// New builds a Node. Call Start then serve gRPC with Register.
func New(config Config) *Node {
	if config.Clock == nil {
		config.Clock = live.RealClock()
	}
	if config.Log == nil {
		config.Log = executor.NewMemoryLog()
	}
	if config.StopGrace <= 0 {
		config.StopGrace = 15 * actor.Second
	}
	if config.BrokerLeaseTTL <= 0 {
		config.BrokerLeaseTTL = time.Duration(broker.DefaultTTL)
	}
	return &Node{
		config:   config,
		health:   health.NewServer(),
		leases:   broker.New(actor.Duration(config.BrokerLeaseTTL)),
		shutdown: make(chan struct{}),
		loopDone: make(chan struct{}),
	}
}

// Health is the gRPC health server: SERVING once the replica is Ready.
func (n *Node) Health() *health.Server { return n.health }

// Start builds the Host, starts it, and waits until the replica is Ready.
func (n *Node) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.cancel = cancel
	ex := executor.New(runCtx, executor.Config{}, n.config.Log, n.config.Objects)
	n.host = live.NewHost(live.HostConfig{
		Clock: n.config.Clock, Shutdown: n.shutdown, StopGrace: n.config.StopGrace,
	})
	n.replica = live.Spawn(n.host, replica.Props(n.config.Machine, n.m.Store), func(ctx *live.ExecCtx, io runtime.IO) error {
		return runIO(ex, ctx, io)
	},
		live.AfterReceive(n.afterReplica))
	n.groups = live.Spawn(n.host, rebalance.Props(n.groupsConfig(), n.replica), nil)
	n.mu.Lock()
	n.started = true
	n.mu.Unlock()
	go func() {
		err := n.host.Run(runCtx)
		n.setRole(false)
		n.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		n.loopErr = err
		close(n.loopDone)
	}()
	return n.waitReady(ctx, cancel)
}

func (n *Node) groupsConfig() rebalance.Config {
	return rebalance.Config{InitialDelay: actor.Duration(n.config.GroupInitialRebalanceDelay)}
}

// afterReplica runs on the replica's goroutine after every Receive: it
// copies the write role for metrics and tells rebalance when the role flips.
func (n *Node) afterReplica() {
	m := n.m.Load()
	if m == nil {
		return
	}
	n.epoch.Store(m.Epoch())
	n.stepDowns.Store(m.StepDowns())
	n.setRole(m.IsActive() && m.Ready())
}

// setRole tells rebalance about a change of the role bit, and only about a
// change.
func (n *Node) setRole(active bool) {
	if n.roleEdge(active) && n.host != nil {
		_ = live.Tell(n.host, n.groups, rebalance.Input(rebalance.RoleChanged{Active: active}))
	}
}

// roleEdge stores the role bit and reports whether it changed.
func (n *Node) roleEdge(active bool) bool { return n.active.Swap(active) != active }

// LogRole is the replica's writer epoch and how often it stepped down.
func (n *Node) LogRole() (uint64, uint64) {
	return n.epoch.Load(), n.stepDowns.Load()
}

// waitReady polls the replica until it is Ready, the context ends, the Host
// stops, or two minutes pass.
func (n *Node) waitReady(ctx context.Context, cancel context.CancelFunc) error {
	fail := func(err error) error {
		cancel()
		<-n.loopDone
		if err != nil {
			return err
		}
		return n.loopErr
	}
	timedOut := make(chan struct{})
	deadline := n.config.Clock.AfterFunc(actor.Duration(2*time.Minute), func() { close(timedOut) })
	defer deadline.Stop()
	for {
		var ready bool
		err := n.onLoop(ctx, func(m *replica.Replica) { ready = m.Ready() })
		switch {
		case err == nil && ready:
			n.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
			return nil
		case ctx.Err() != nil:
			return fail(ctx.Err())
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-n.loopDone:
			if n.loopErr != nil {
				return n.loopErr
			}
			return errors.New("controlplane/server: run exited before Ready")
		case <-timedOut:
			return fail(errors.New("controlplane/server: timed out waiting for Ready"))
		case <-n.tick():
		}
	}
}

// tick is a channel closed after one millisecond of the configured clock.
func (n *Node) tick() <-chan struct{} {
	c := make(chan struct{})
	n.config.Clock.AfterFunc(actor.Millisecond, func() { close(c) })
	return c
}

// Close asks the actors to stop and waits for the Host. It is safe to call
// more than once, and before Start.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		select {
		case <-n.shutdown:
		default:
			close(n.shutdown)
		}
		n.mu.Lock()
		started := n.started
		n.mu.Unlock()
		if started {
			if n.cancel != nil {
				n.cancel()
			}
			<-n.loopDone
			n.closeErr = n.loopErr
			if errors.Is(n.closeErr, live.ErrStopped) {
				n.closeErr = nil
			}
		}
	})
	return n.closeErr
}

// onLoop runs fn with the replica on the replica's goroutine, between two
// Receives, and waits for it.
func (n *Node) onLoop(ctx context.Context, fn func(m *replica.Replica)) error {
	if n.host == nil {
		return errors.New("controlplane/server: machine not started")
	}
	var missing bool
	err := live.OnLoop(ctx, n.host, n.replica, func() {
		m := n.m.Load()
		if m == nil {
			missing = true
			return
		}
		fn(m)
	})
	if err == nil && missing {
		return errors.New("controlplane/server: machine not started")
	}
	return err
}

// ask sends a request that carries a ReplyTo to the replica and waits for
// the answer. When ctx ends first it returns ErrTimeout with MaybeApplied for
// a write (maybeApplied) and NotApplied for a read, and ctx's error.
func (n *Node) ask(ctx context.Context, maybeApplied bool,
	body func(to actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData) (runtime.ControlPlaneDone, error) {
	if n.host == nil {
		return runtime.ControlPlaneDone{}, errors.New("controlplane/server: the run loop has ended")
	}
	d, err := live.Ask(ctx, n.host, n.replica, body)
	switch {
	case err == nil:
		return d, nil
	case ctx.Err() != nil:
		outcome := runtime.NotApplied
		if maybeApplied {
			outcome = runtime.MaybeApplied
		}
		return runtime.ControlPlaneDone{Err: runtime.ErrTimeout, Outcome: outcome}, ctx.Err()
	}
	return runtime.ControlPlaneDone{}, errors.New("controlplane/server: the run loop has ended")
}

// Register registers the ControlPlane and health services on s.
func (n *Node) Register(s *grpc.Server) {
	cpv1.RegisterControlPlaneServer(s, n)
	healthpb.RegisterHealthServer(s, n.health)
}
