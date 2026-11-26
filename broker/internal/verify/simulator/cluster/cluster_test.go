package cluster

import (
	"fmt"
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/shared/objectstore"
)

type outbox = actor.Outbox[core.EventData, core.IO]

// seen is one message a stub actor received.
type seen struct {
	who string
	at  sim.Time
	msg core.EventData
}

// stub is a broker or client for tests. script records the effects of a
// Receive.
type stub struct {
	name   string
	log    *[]seen
	now    func() sim.Time
	script func(msg core.EventData, o *outbox)
	busy   bool
}

func (s *stub) Receive(_ actor.Now, msg core.EventData, o *outbox) {
	*s.log = append(*s.log, seen{who: s.name, at: s.now(), msg: msg})
	if s.script != nil {
		s.script(msg, o)
	}
}

func (s *stub) StateHash() uint64 { return uint64(len(*s.log)) }

func (s *stub) Quiescent() bool { return !s.busy }

func quickPlan(seed uint64) Plan {
	return Plan{
		Seed:         seed,
		Phases:       sim.Phases{Faults: 2 * actor.Second, Heal: actor.Second, Drain: actor.Second},
		QuiesceEvery: 500 * actor.Millisecond,
		Settings: Settings{
			RecoveryDelay: [2]actor.Duration{100 * actor.Millisecond, 200 * actor.Millisecond},
			NetDelay:      [2]actor.Duration{actor.Millisecond, 3 * actor.Millisecond},
		},
	}
}

// brokerProps is a stub broker. Every boot shares log.
func brokerProps(c *Cluster, log *[]seen, script func(self string, msg core.EventData, o *outbox)) actor.Props[core.EventData, core.IO] {
	return actor.Props[core.EventData, core.IO]{
		New: func(env actor.Env) actor.Actor[core.EventData, core.IO] {
			self := env.Self().Name
			s := &stub{name: self, log: log, now: c.System.Now}
			if script != nil {
				s.script = func(msg core.EventData, o *outbox) { script(self, msg, o) }
			}
			return s
		},
		Boot:     func(self actor.Incarnation) core.EventData { return core.NodeStarted{Self: self} },
		Priority: core.PriorityOf,
	}
}

// phaseKey is the ClientDue key that carries a phase change to a stub client.
func phaseKey(p sim.Phase) string { return fmt.Sprintf("phase/%d", p) }

// addStubClient adds a client whose messages are core.EventData.
func addStubClient(c *Cluster, log *[]seen, busy bool, script func(msg core.EventData, o *outbox)) {
	AddClient(c, ClientSpec[core.EventData]{
		New: func(env actor.Env, _ []string) Client[core.EventData] {
			return &stub{name: env.Self().Name, log: log, now: c.System.Now, script: script, busy: busy}
		},
		Boot:    func(self actor.Incarnation) core.EventData { return core.NodeStarted{Self: self} },
		OnPhase: func(p sim.Phase) (core.EventData, bool) { return core.ClientDue{Key: phaseKey(p)}, true },
		FromNet: func(ev core.EventData) core.EventData { return ev },
	})
}

func TestRestartDelayIsInRecoveryDelay(t *testing.T) {
	c := New(quickPlan(1))
	r := sim.NewSystem(sim.Config{Seed: 9}).Env()
	for range 200 {
		d, ok := c.restart(actor.Crash, r)
		if !ok || d < 100*actor.Millisecond || d > 200*actor.Millisecond {
			t.Fatalf("restart delay %v ok=%v", d, ok)
		}
	}
}

func TestRunBootsBrokersThenClientsAndTellsPhases(t *testing.T) {
	var log []seen
	c := New(quickPlan(3))
	c.AddBrokers(2, brokerProps(c, &log, nil))
	addStubClient(c, &log, false, nil)
	res := c.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	var order []string
	var phases []string
	for _, s := range log {
		if _, ok := s.msg.(core.NodeStarted); ok {
			order = append(order, s.who)
		}
		if m, ok := s.msg.(core.ClientDue); ok {
			phases = append(phases, m.Key)
		}
	}
	if len(order) != 3 || order[2] != "client-0" {
		t.Errorf("boot order %v", order)
	}
	if want := []string{phaseKey(sim.PhaseHeal), phaseKey(sim.PhaseDrain)}; !slices.Equal(phases, want) {
		t.Errorf("client heard %v, want %v", phases, want)
	}
	quiet := c.System.Timeline().Snapshot("sim_quiesce")
	if len(quiet) != 8 { // every 500 ms from 0.5 s to the end at 4 s
		t.Errorf("%d quiesce facts", len(quiet))
	}
	for _, q := range quiet {
		if v, _ := q.Fact.Field("quiescent").(actor.Bool); !bool(v) {
			t.Errorf("a quiesce fact says not quiescent: %+v", q)
		}
	}
}

func TestQuiesceFactSaysWhenAClientIsBusy(t *testing.T) {
	var log []seen
	c := New(quickPlan(4))
	c.AddBrokers(1, brokerProps(c, &log, nil))
	addStubClient(c, &log, true, nil)
	c.Run()
	for _, q := range c.System.Timeline().Snapshot("sim_quiesce") {
		if v, _ := q.Fact.Field("quiescent").(actor.Bool); bool(v) {
			t.Fatalf("a busy client was reported quiescent: %+v", q)
		}
	}
}

// A broker that trips is booted again by the restart policy.
func TestBrokerTripRestarts(t *testing.T) {
	var log []seen
	c := New(quickPlan(5))
	boots := 0
	c.AddBrokers(1, brokerProps(c, &log, func(_ string, msg core.EventData, o *outbox) {
		if _, ok := msg.(core.NodeStarted); ok {
			boots++
			if boots == 1 {
				o.Stop(actor.Trip, "test")
			}
		}
	}))
	res := c.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if boots != 2 || res.Trips != 1 {
		t.Errorf("boots=%d trips=%d, want 2 and 1", boots, res.Trips)
	}
}

// The executor forwards the object store and control-plane calls to their
// services, and the services answer.
func TestExecutorForwardsServiceCalls(t *testing.T) {
	var log []seen
	c := New(quickPlan(7))
	var got []string
	c.AddBrokers(1, brokerProps(c, &log, func(_ string, msg core.EventData, o *outbox) {
		if _, ok := msg.(core.NodeStarted); !ok {
			return
		}
		actor.Perform(o, core.ObjectCall{Call: objectstore.Put{Key: "k", Data: []byte("v")}},
			func(d core.ObjectDone) core.EventData {
				got = append(got, fmt.Sprintf("object %v", d.Result.(objectstore.PutDone).Err))
				return d
			})
		actor.Perform(o, core.ControlPlaneCall{Op: contract.DescribeTopics{}},
			func(d core.ControlPlaneDone) core.EventData {
				got = append(got, fmt.Sprintf("control plane %v", d.Err))
				return d
			})
	}))
	if res := c.Run(); !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	slices.Sort(got)
	if want := []string{"control plane OK", "object OK"}; !slices.Equal(got, want) {
		t.Fatalf("answers %v, want %v", got, want)
	}
	if _, ok := c.Objects.Object("k"); !ok {
		t.Fatal("the put did not land")
	}
}

// echo is a broker that answers every frame with the same bytes, and pauses
// reads on the frame "pause" and resumes them on its timer.
func echo(_ string, msg core.EventData, o *outbox) {
	if m, ok := msg.(core.BytesReceived); ok {
		if string(m.Data) == "pause" {
			o.Do(core.PauseReads{Conn: m.Conn})
			o.StartTimer(actor.TimerKey(fmt.Sprint(m.Conn)), 100*actor.Millisecond, core.OpDue{Op: uint64(m.Conn)})
		}
		o.Do(core.Send(m))
	}
	if m, ok := msg.(core.OpDue); ok {
		o.Do(core.ResumeReads{Conn: core.ConnID(m.Op)})
	}
}

// client dials broker-0 at boot and, once connected, writes frames.
func dialAndWrite(frames ...string) func(msg core.EventData, o *outbox) {
	return func(msg core.EventData, o *outbox) {
		if _, ok := msg.(core.NodeStarted); ok {
			actor.Perform(o, core.Connect{Addr: "broker-0"}, func(d core.ConnectDone) core.EventData { return d })
		}
		if m, ok := msg.(core.ConnectDone); ok && m.Err == core.OK {
			for _, f := range frames {
				o.Do(core.Send{Conn: m.Conn, Data: []byte(f)})
			}
		}
	}
}

// frames lists what `who` received as BytesReceived, in order.
func frames(log []seen, who string) []string {
	var out []string
	for _, s := range log {
		if b, ok := s.msg.(core.BytesReceived); ok && s.who == who {
			out = append(out, string(b.Data))
		}
	}
	return out
}

// Frames arrive whole and in order both ways, ConnOpened comes before the
// first frame, and a paused connection holds its frames until it resumes.
func TestConnectionsDeliverWholeFramesInOrder(t *testing.T) {
	var log []seen
	c := New(quickPlan(11))
	c.AddBrokers(1, brokerProps(c, &log, echo))
	want := []string{"a", "bb", "pause", "ccc", "dddd"}
	addStubClient(c, &log, false, func(msg core.EventData, o *outbox) {
		dialAndWrite(want[:3]...)(msg, o)
		if m, ok := msg.(core.BytesReceived); ok && string(m.Data) == "pause" {
			for _, f := range want[3:] {
				o.Do(core.Send{Conn: m.Conn, Data: []byte(f)})
			}
		}
	})
	if res := c.Run(); !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if got := frames(log, "broker-0"); !slices.Equal(got, want) {
		t.Fatalf("broker got %v, want %v", got, want)
	}
	if got := frames(log, "client-0"); !slices.Equal(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
	var opened, paused, after sim.Time
	for _, s := range log {
		if _, ok := s.msg.(core.ConnOpened); ok {
			opened = s.at
		}
		if m, ok := s.msg.(core.BytesReceived); ok && s.who == "broker-0" {
			switch string(m.Data) {
			case "pause":
				paused = s.at
			case "ccc":
				after = s.at
			}
		}
	}
	if opened == 0 || opened > paused {
		t.Fatalf("ConnOpened at %d, first frames at %d", opened, paused)
	}
	if after-paused < sim.Time(100*actor.Millisecond) {
		t.Fatalf("a paused connection delivered after %d ns", after-paused)
	}
}

// A dial to a broker that is not up fails with ErrConnClosed.
func TestConnectToADownBrokerFails(t *testing.T) {
	var log []seen
	c := New(quickPlan(12))
	addStubClient(c, &log, false, dialAndWrite("x"))
	if res := c.Run(); !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	for _, s := range log {
		if d, ok := s.msg.(core.ConnectDone); ok && d.Err == core.ErrConnClosed {
			return
		}
	}
	t.Fatal("no failed dial")
}

// closedAt returns the ConnClosed that `who` got, if any.
func closedAt(log []seen, who string) (core.ConnClosed, bool) {
	for _, s := range log {
		if m, ok := s.msg.(core.ConnClosed); ok && s.who == who {
			return m, true
		}
	}
	return core.ConnClosed{}, false
}

// A graceful close reaches the other end after the frames already sent; an
// abort drops them.
func TestCloseConn(t *testing.T) {
	for _, abort := range []bool{false, true} {
		var log []seen
		c := New(quickPlan(13))
		c.AddBrokers(1, brokerProps(c, &log, nil))
		addStubClient(c, &log, false, func(msg core.EventData, o *outbox) {
			dialAndWrite("a", "b")(msg, o)
			if m, ok := msg.(core.ConnectDone); ok {
				o.Do(core.CloseConn{Conn: m.Conn, Abort: abort})
				o.Do(core.Send{Conn: m.Conn, Data: []byte("late")})
			}
		})
		if res := c.Run(); !res.OK() {
			t.Fatalf("failures: %+v", res.Failures)
		}
		m, ok := closedAt(log, "broker-0")
		if !ok || m.Abort != abort {
			t.Fatalf("abort=%v: broker got close %+v %v", abort, m, ok)
		}
		want := []string{"a", "b"}
		if abort {
			want = nil
		}
		if got := frames(log, "broker-0"); !slices.Equal(got, want) {
			t.Fatalf("abort=%v: broker got %v, want %v", abort, got, want)
		}
	}
}

// When a broker crashes, its client gets ConnClosed{Abort: true}.
func TestCrashResetsTheClient(t *testing.T) {
	var log []seen
	c := New(quickPlan(14))
	c.AddBrokers(1, brokerProps(c, &log, nil))
	addStubClient(c, &log, false, dialAndWrite("a"))
	broker := c.net.ends["broker-0"].entity
	c.System.After(500*actor.Millisecond, func() { c.System.Crash(broker, "test") })
	if res := c.Run(); !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	if m, ok := closedAt(log, "client-0"); !ok || !m.Abort {
		t.Fatalf("client got close %+v %v", m, ok)
	}
}

// With ConnResetProb 1, the first write resets the connection: both ends get
// ConnClosed{Abort: true} and the frame is lost.
func TestConnReset(t *testing.T) {
	var log []seen
	p := quickPlan(15)
	p.Settings.ConnResetProb = 1
	c := New(p)
	c.AddBrokers(1, brokerProps(c, &log, nil))
	addStubClient(c, &log, false, dialAndWrite("a"))
	if res := c.Run(); !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	for _, who := range []string{"broker-0", "client-0"} {
		if m, ok := closedAt(log, who); !ok || !m.Abort {
			t.Fatalf("%s got close %+v %v", who, m, ok)
		}
	}
	if got := frames(log, "broker-0"); len(got) != 0 {
		t.Fatalf("broker got %v", got)
	}
	if n := len(c.System.Timeline().Snapshot("sim_fault")); n != 1 {
		t.Fatalf("%d faults recorded", n)
	}
}
