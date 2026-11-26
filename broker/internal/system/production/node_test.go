package production

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specsystem"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func startNode(t *testing.T, mod func(*NodeConfig)) (*Node, NodeConfig) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := NodeConfig{
		NodeID: "n1", Broker: broker.DefaultConfig(), Listener: ln,
		Objects: specsystem.NewObjectStore(), ObjClassify: MemoryErrorClassifier{},
		ControlPlane: specsystem.NewControlPlane(), CPClassify: memoryCP{},
		StopGrace: time.Second, FlushGrace: 50 * time.Millisecond,
	}
	if mod != nil {
		mod(&config)
	}
	n, err := Start(t.Context(), config)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Kill(); <-n.Done() })
	return n, config
}

func waitDone(t *testing.T, n *Node) error {
	t.Helper()
	select {
	case <-n.Done():
		return n.Wait()
	case <-time.After(10 * time.Second):
		t.Fatal("node did not stop")
		return nil
	}
}

func TestStartRequiresPieces(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	full := NodeConfig{Listener: ln, Objects: specsystem.NewObjectStore(), ControlPlane: specsystem.NewControlPlane()}
	for name, mod := range map[string]func(*NodeConfig){
		"listener": func(c *NodeConfig) { c.Listener = nil },
		"objects":  func(c *NodeConfig) { c.Objects = nil },
		"cp":       func(c *NodeConfig) { c.ControlPlane = nil },
	} {
		c := full
		mod(&c)
		if _, err := Start(t.Context(), c); err == nil || !strings.Contains(err.Error(), "required") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestStartGracefulStop(t *testing.T) {
	var mu sync.Mutex
	var seen []core.IO
	n, _ := startNode(t, func(c *NodeConfig) {
		c.OnIO = func(a core.IO) { mu.Lock(); seen = append(seen, a); mu.Unlock() }
	})
	if n.Addr() == nil || !strings.HasPrefix(n.String(), "broker(127.0.0.1:") {
		t.Errorf("addr %v string %q", n.Addr(), n.String())
	}
	if n.OpenConns() != 0 {
		t.Errorf("open conns %d", n.OpenConns())
	}
	// A list-all-topics Metadata request makes the broker call the control
	// plane; the tap sees that IO.
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", n.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	request := kmsg.NewPtrMetadataRequest()
	request.Version, request.Topics = 1, nil
	body := request.AppendTo(nil)
	frame := binary.BigEndian.AppendUint32(nil, uint32(8+2+len(body)))
	frame = binary.BigEndian.AppendUint16(frame, uint16(request.Key()))
	frame = binary.BigEndian.AppendUint16(frame, uint16(request.Version))
	frame = binary.BigEndian.AppendUint32(frame, 7)
	frame = binary.BigEndian.AppendUint16(frame, 0xffff) // null client id
	if _, err := conn.Write(append(frame, body...)); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		if got > 0 {
			break
		}
	}
	mu.Lock()
	if len(seen) == 0 {
		t.Error("OnIO saw no IO")
	}
	mu.Unlock()
	n.Stop()
	n.Stop() // idempotent
	if err := waitDone(t, n); err != nil {
		t.Errorf("graceful stop: %v", err)
	}
}

func TestStartKill(t *testing.T) {
	n, _ := startNode(t, nil)
	n.Kill()
	if err := waitDone(t, n); !errors.Is(err, live.ErrStopped) && !errors.Is(err, live.ErrKilled) {
		t.Errorf("kill: %v", err)
	}
}

func TestStartShutdownChannel(t *testing.T) {
	sd := make(chan struct{})
	n, _ := startNode(t, func(c *NodeConfig) { c.Shutdown = sd })
	close(sd)
	if err := waitDone(t, n); err != nil {
		t.Errorf("shutdown channel: %v", err)
	}
}

func TestAdvertisedAndDefaults(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	bc, err := advertised(broker.Config{}, ln)
	if err != nil || bc.Host != "127.0.0.1" || bc.Port != int32(ln.Addr().(*net.TCPAddr).Port) {
		t.Errorf("advertised %+v %v", bc, err)
	}
	bc, _ = advertised(broker.Config{Host: "h", Port: 9}, ln)
	if bc.Host != "h" || bc.Port != 9 {
		t.Errorf("explicit kept: %+v", bc)
	}
	if _, err := advertised(broker.Config{}, badAddrListener{ln}); err == nil {
		t.Error("bad listener address accepted")
	}
	d := withDefaults(NodeConfig{})
	if d.StopGrace != 10*time.Second || d.FlushGrace != 5*time.Second || d.LeaseInterval != time.Second {
		t.Errorf("defaults %+v", d)
	}
	d = withDefaults(NodeConfig{StopGrace: 1, FlushGrace: 2, LeaseInterval: 3})
	if d.StopGrace != 1 || d.FlushGrace != 2 || d.LeaseInterval != 3 {
		t.Errorf("explicit kept %+v", d)
	}
}

type badAddr struct{}

func (badAddr) Network() string { return "x" }
func (badAddr) String() string  { return "no-port" }

type badAddrListener struct{ net.Listener }

func (badAddrListener) Addr() net.Addr { return badAddr{} }

// scripted is a test actor whose Receive is on.
type scripted struct {
	on func(msg core.EventData, o *actor.Outbox[core.EventData, core.IO])
}

func (s scripted) Receive(_ actor.Now, msg core.EventData, o *actor.Outbox[core.EventData, core.IO]) {
	s.on(msg, o)
}

func (scripted) StateHash() uint64 { return 0 }

// startScripted runs a scripted actor as the broker of a Node, with the
// broker's boot, stop and priority functions.
func startScripted(t *testing.T, mod func(*NodeConfig),
	on func(msg core.EventData, o *actor.Outbox[core.EventData, core.IO])) *Node {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := NodeConfig{
		NodeID: "n1", Broker: broker.DefaultConfig(), Listener: ln,
		Objects: specsystem.NewObjectStore(), ObjClassify: MemoryErrorClassifier{},
		ControlPlane: specsystem.NewControlPlane(), CPClassify: memoryCP{},
		StopGrace: time.Second, FlushGrace: 50 * time.Millisecond,
	}
	if mod != nil {
		mod(&config)
	}
	props := broker.Props(config.Broker)
	props.New = func(actor.Env) actor.Actor[core.EventData, core.IO] { return scripted{on: on} }
	n := start(t.Context(), withDefaults(config), props, core.BrokerAddr{NodeID: 1})
	t.Cleanup(func() { n.Kill(); <-n.Done() })
	return n
}

// The IO recorded before a Trip is carried out, timers fire, facts reach the
// Facts hook, and the Trip ends the Node with live.ErrTripped and its cause.
func TestNodeTripAfterIOTimerAndFact(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	add := func(s string) { mu.Lock(); seen = append(seen, s); mu.Unlock() }
	n := startScripted(t, func(c *NodeConfig) {
		c.OnIO = func(io core.IO) { add(fmt.Sprintf("%T", io)) }
		c.Facts = func(f actor.Fact) { add("fact " + f.Name) }
	}, func(msg core.EventData, o *actor.Outbox[core.EventData, core.IO]) {
		if _, ok := msg.(core.NodeStarted); ok {
			o.Do(core.PauseReads{Conn: 2})
			o.Fact(actor.Fact{Name: "booted"})
			o.After(actor.Millisecond, core.FlushDue{})
		}
		if _, ok := msg.(core.FlushDue); ok {
			o.Do(core.ResumeReads{Conn: 2})
			o.Stop(actor.Trip, "x")
		}
	})
	err := waitDone(t, n)
	if !errors.Is(err, live.ErrTripped) || !strings.Contains(err.Error(), "x") {
		t.Fatalf("err = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(seen, ","); got != "core.PauseReads,fact booted,core.ResumeReads" {
		t.Fatalf("seen %s", got)
	}
}

func TestNodeExitIsClean(t *testing.T) {
	n := startScripted(t, nil, func(_ core.EventData, o *actor.Outbox[core.EventData, core.IO]) {
		o.Stop(actor.Exit, "")
	})
	if err := waitDone(t, n); err != nil {
		t.Fatalf("err = %v", err)
	}
}

// A broker that ignores the stop request is killed after StopGrace.
func TestNodeStopIgnoredIsKilled(t *testing.T) {
	n := startScripted(t, func(c *NodeConfig) { c.StopGrace = 50 * time.Millisecond },
		func(core.EventData, *actor.Outbox[core.EventData, core.IO]) {})
	start := time.Now()
	n.Stop()
	if err := waitDone(t, n); !errors.Is(err, live.ErrKilled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("killed before the grace ended")
	}
}

// Accepted connections, their bytes and the live broker set reach the actor.
func TestNodeTellsTheBroker(t *testing.T) {
	got := make(chan core.EventData, 16)
	lease := &fakeLease{live: []core.BrokerAddr{{NodeID: 1}}, okFor: 1 << 30}
	n := startScripted(t, func(c *NodeConfig) { c.Lease, c.LeaseInterval = lease, time.Hour },
		func(msg core.EventData, _ *actor.Outbox[core.EventData, core.IO]) {
			select {
			case got <- msg:
			default:
			}
		})
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", n.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	var opened, bytes, brokers bool
	for end := time.After(5 * time.Second); !opened || !bytes || !brokers; {
		select {
		case m := <-got:
			if _, ok := m.(core.ConnOpened); ok {
				opened = true
			}
			if b, ok := m.(core.BytesReceived); ok {
				bytes = bytes || string(b.Data) == "hi"
			}
			if l, ok := m.(core.LiveBrokers); ok {
				brokers = len(l.Brokers) == 1
			}
		case <-end:
			t.Fatalf("opened %v bytes %v brokers %v", opened, bytes, brokers)
		}
	}
}
