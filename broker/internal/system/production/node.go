package production

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/runtime/live"
	"github.com/manifesterio/mds/shared/objectstore"
)

// NodeConfig is everything one broker process needs.
type NodeConfig struct {
	NodeID   string
	Broker   broker.Config // Host/Port are the advertised address; Port 0 means the listener's
	Listener net.Listener

	Objects      system.ObjectStore
	ObjClassify  objectstore.Classifier
	ControlPlane system.ControlPlane
	CPClassify   system.ErrorClassifier

	Shutdown  <-chan struct{} // optional; closing it is Node.Stop
	StopGrace time.Duration   // time the broker gets to finish in-flight work (10s)
	// Lease, if set, sends a heartbeat every LeaseInterval and feeds the live
	// broker set to Metadata. Nil: no heartbeat, Metadata lists this broker only.
	Lease         LeaseClient
	LeaseInterval time.Duration // heartbeat interval (1s)
	FlushGrace    time.Duration // time writers get to flush after a graceful exit (5s)
	Net           NetConfig
	Exec          ExecConfig
	// Facts, if set, gets every fact the broker records, on the broker
	// actor's goroutine (tracing and metrics).
	Facts func(actor.Fact)
	// OnIO, if set, sees every IO the broker issues, on the broker actor's
	// goroutine, before it executes (used to pair segment keys with
	// segment_sealed facts, see SegmentLog).
	OnIO func(core.IO)
	Log  *slog.Logger
}

// Node is a running broker.
type Node struct {
	addr     net.Addr
	net      *Net
	stop     chan struct{}
	stopOnce func()
	done     chan struct{}
	err      error
	kill     context.CancelFunc
}

// Start wires the broker to its adapters and runs it until Stop, Kill or a
// failure. The returned Node's Wait gives the Host's error.
func Start(ctx context.Context, config NodeConfig) (*Node, error) {
	if config.Listener == nil || config.Objects == nil || config.ControlPlane == nil {
		return nil, errors.New("prod.Start: Listener, Objects and ControlPlane are required")
	}
	config = withDefaults(config)
	bc, err := advertised(config.Broker, config.Listener)
	if err != nil {
		return nil, err
	}
	self := core.BrokerAddr{NodeID: bc.BrokerID, Host: bc.Host, Port: bc.Port}
	return start(ctx, config, broker.Props(bc), self), nil
}

// start runs the actor of props on a live Host. The exec function is the
// Executor over the real connections, object store and control plane; the
// connections and the heartbeat tell the actor what happens outside. self is
// the address the heartbeat renews. config has its defaults.
func start(ctx context.Context, config NodeConfig, props actor.Props[core.EventData, core.IO], self core.BrokerAddr) *Node {
	rctx, kill := context.WithCancel(ctx)
	stop := make(chan struct{})
	n := &Node{addr: config.Listener.Addr(), stop: stop, stopOnce: sync.OnceFunc(func() { close(stop) }),
		done: make(chan struct{}), kill: kill}
	host := live.NewHost(live.HostConfig{
		Clock:     live.RealClock(),
		Env:       func(self actor.Incarnation) actor.Env { return NewEnv(self, config.Log) },
		Shutdown:  stop,
		StopGrace: actor.Duration(config.StopGrace),
		Facts:     factsHook(config.Facts),
	})
	// The executor needs the connections, and the connections need the
	// broker's handle, which Spawn gives. exec is set before the Host runs.
	var exec func(*live.ExecCtx, core.IO) error
	ref := live.Spawn(host, props, func(c *live.ExecCtx, io core.IO) error { return exec(c, io) })
	in := hostInbox{host: host, to: ref}
	n.net = NewNet(config.Listener, in, config.Net)
	ex := NewExecutor(rctx, config.Exec, config.Objects, config.ControlPlane, n.net,
		Classifier{Objects: config.ObjClassify, CP: config.CPClassify})
	exec = ex.Exec
	if config.OnIO != nil {
		exec = func(c *live.ExecCtx, io core.IO) error {
			config.OnIO(io)
			return ex.Exec(c, io)
		}
	}
	if config.Lease != nil {
		go leaseLoop(rctx, in, config.Lease, self, config.LeaseInterval, config.Log)
	}
	if config.Shutdown != nil {
		go n.stopOn(config.Shutdown)
	}
	go n.run(rctx, host, config.FlushGrace)
	return n
}

// factsHook adapts NodeConfig.Facts to live.HostConfig.Facts.
func factsHook(facts func(actor.Fact)) func(actor.Incarnation, actor.Fact) {
	if facts == nil {
		return nil
	}
	return func(_ actor.Incarnation, f actor.Fact) { facts(f) }
}

// hostInbox tells the broker actor on its Host.
type hostInbox struct {
	host *live.Host
	to   actor.EntityRef[core.EventData]
}

var _ system.Inbox = hostInbox{}

func (i hostInbox) Tell(msg core.EventData) error { return live.Tell(i.host, i.to, msg) }

// withDefaults fills the zero grace periods and the heartbeat interval.
func withDefaults(config NodeConfig) NodeConfig {
	if config.StopGrace <= 0 {
		config.StopGrace = 10 * time.Second
	}
	if config.FlushGrace <= 0 {
		config.FlushGrace = 5 * time.Second
	}
	if config.LeaseInterval <= 0 {
		config.LeaseInterval = time.Second
	}
	return config
}

// advertised fills the broker's advertised host and port from the listener.
func advertised(bc broker.Config, ln net.Listener) (broker.Config, error) {
	if bc.Port == 0 {
		_, p, err := net.SplitHostPort(ln.Addr().String())
		if err != nil {
			return bc, err
		}
		port, err := strconv.Atoi(p)
		if err != nil {
			return bc, err
		}
		bc.Port, err = checkedInt32(port)
		if err != nil {
			return bc, err
		}
	}
	bc.Host = cmp.Or(bc.Host, "127.0.0.1")
	return bc, nil
}

// checkedInt32 converts n to int32 after rejecting values that would overflow.
func checkedInt32(n int) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %d out of int32 range", n)
	}
	return int32(n), nil
}

// stopOn stops the node when shutdown closes, or returns once it is done.
func (n *Node) stopOn(shutdown <-chan struct{}) {
	select {
	case <-shutdown:
		n.Stop()
	case <-n.done:
	}
}

// run drives the broker to its end, then closes the network and marks done.
func (n *Node) run(ctx context.Context, host *live.Host, flush time.Duration) {
	defer close(n.done)
	err := host.Run(ctx)
	grace := time.Duration(0)
	if err == nil {
		grace = flush // graceful exit: let the last answers reach the clients
	}
	n.net.Shutdown(grace)
	n.kill()
	n.err = err
}

// Addr is the address the broker listens on.
func (n *Node) Addr() net.Addr { return n.addr }

// Stop asks for a graceful stop: in-flight produces finish and are answered.
func (n *Node) Stop() { n.stopOnce() }

// Kill stops the broker at once, resetting every connection (a crash).
func (n *Node) Kill() { n.kill() }

// Done is closed when the broker has fully stopped.
func (n *Node) Done() <-chan struct{} { return n.done }

// Wait blocks until the broker has stopped and returns the Host's result: nil
// after a graceful exit, live.ErrStopped after Kill, live.ErrTripped when the
// broker asked for a restart, live.ErrKilled when a graceful stop missed
// StopGrace, or the error of a crash.
func (n *Node) Wait() error {
	<-n.done
	return n.err
}

// String names the node for logs.
func (n *Node) String() string { return fmt.Sprintf("broker(%s)", n.addr) }

// OpenConns is the number of open client connections.
func (n *Node) OpenConns() int { return n.net.OpenConns() }
