package production

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specsystem"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/shared/objectstore"
	"github.com/manifesterio/mds/shared/objectstore/s3"
)

type slowObjs struct {
	system.ObjectStore
	block chan struct{}
	err   error
}

func (s slowObjs) Put(ctx context.Context, key string, data []byte) error {
	select {
	case <-s.block:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.err
}

// memoryCP classifies the errors of the in-memory control plane (specsystem).
type memoryCP struct{}

func (memoryCP) Classify(_ core.IO, err error) (core.ErrCode, core.Outcome) {
	return memoryCode(err)
}

// fakeConns records what the executor does to connections.
type fakeConns struct {
	mu  sync.Mutex
	log []string
}

func (c *fakeConns) Write(_ core.ConnID, data []byte) error {
	c.add("write " + string(data))
	return ErrNoConn
}
func (c *fakeConns) Close(_ core.ConnID, abort bool) error {
	if abort {
		c.add("abort")
	} else {
		c.add("close")
	}
	return nil
}
func (c *fakeConns) PauseReads(core.ConnID)  { c.add("pause") }
func (c *fakeConns) ResumeReads(core.ConnID) { c.add("resume") }
func (c *fakeConns) add(s string)            { c.mu.Lock(); c.log = append(c.log, s); c.mu.Unlock() }

func (c *fakeConns) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.log)
}

// probe is a test actor: at boot it hands each IO of script to the exec
// function, in order, and sends every answer to got. An object store answer
// comes as OpReadDone and a control plane answer as OpCallDone, with the
// index of the IO in script as Op.
type probe struct {
	script []core.IO
	got    chan core.EventData
}

func (p *probe) Receive(_ actor.Now, msg core.EventData, o *actor.Outbox[core.EventData, core.IO]) {
	if _, ok := msg.(core.NodeStarted); !ok {
		p.got <- msg
		return
	}
	for i, io := range p.script {
		op := uint64(i)
		switch r := io.(type) {
		case core.ObjectCall:
			actor.Perform(o, r, func(d core.ObjectDone) core.EventData { return core.OpReadDone{Op: op, Done: d} })
		case core.ControlPlaneCall:
			actor.Perform(o, r, func(d core.ControlPlaneDone) core.EventData { return core.OpCallDone{Op: op, Done: d} })
		case core.Connect:
			actor.Perform(o, r, func(d core.ConnectDone) core.EventData { return d })
		case core.Send, core.CloseConn, core.PauseReads, core.ResumeReads:
			o.Do(io)
		}
	}
}

func (*probe) StateHash() uint64 { return 0 }

// runProbe runs a probe with script on a Host whose exec function is e. The
// Host stops when the test ends.
func runProbe(t *testing.T, e *Executor, script ...core.IO) chan core.EventData {
	t.Helper()
	return runProbeExec(t, e.Exec, script...)
}

// runProbeExec is runProbe with any exec function.
func runProbeExec(t *testing.T, exec func(*live.ExecCtx, core.IO) error, script ...core.IO) chan core.EventData {
	t.Helper()
	got := make(chan core.EventData, 16)
	host := live.NewHost(live.HostConfig{Clock: live.RealClock()})
	live.Spawn(host, actor.Props[core.EventData, core.IO]{
		Name: "probe",
		New:  func(actor.Env) actor.Actor[core.EventData, core.IO] { return &probe{script: script, got: got} },
		Boot: func(self actor.Incarnation) core.EventData { return core.NodeStarted{Self: self} },
	}, exec)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { _ = host.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return got
}

func wait(t *testing.T, got chan core.EventData) core.EventData {
	t.Helper()
	select {
	case m := <-got:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("no answer")
		return nil
	}
}

func TestExecutorAnswersOnceAndNeverBlocks(t *testing.T) {
	objs := slowObjs{ObjectStore: specsystem.NewObjectStore(), block: make(chan struct{})}
	cls := Classifier{Objects: MemoryErrorClassifier{}, CP: memoryCP{}}
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{ObjTimeout: 50 * time.Millisecond}, objs, nil, conns, cls)

	got := runProbe(t, e,
		core.ObjectCall{Call: objectstore.Put{Key: "k"}}, // blocked in the store
		core.ObjectCall{Call: objectstore.Get{Ref: core.SegmentRef{Key: "missing"}}},
		core.Connect{Addr: "x"})
	answers := map[uint64]objectstore.Result{}
	var order []uint64
	var connect *core.ConnectDone
	for range 3 {
		m := wait(t, got)
		if d, ok := m.(core.OpReadDone); ok {
			answers[d.Op] = d.Done.Result
			order = append(order, d.Op)
		} else if d, ok := m.(core.ConnectDone); ok {
			connect = &d
		} else {
			t.Fatalf("unexpected %T", m)
		}
	}
	if !slices.Equal(order, []uint64{1, 0}) {
		t.Fatalf("answers in order %v: the blocked Put held up the Get", order)
	}
	if put, _ := answers[0].(objectstore.PutDone); put.Err != core.ErrIO || put.Outcome != core.MaybeApplied { // memory classifier: unknown error
		t.Errorf("put %+v", answers[0])
	}
	if g, _ := answers[1].(objectstore.GetDone); g.Err != core.ErrNotFound {
		t.Errorf("get %+v", answers[1])
	}
	if connect == nil || connect.Err != core.ErrInvalid {
		t.Errorf("connect %+v", connect)
	}
	select {
	case m := <-got:
		t.Fatalf("more than one answer per call: %+v", m)
	case <-time.After(30 * time.Millisecond):
	}
}

// Socket IO goes to the connections, in Seq order per connection, with no answer.
func TestExecutorSocketIO(t *testing.T) {
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
	got := runProbe(t, e,
		core.Send{Conn: 7, Data: []byte("x"), Seq: 1}, core.PauseReads{Conn: 7, Seq: 2}, core.ResumeReads{Conn: 7, Seq: 3},
		core.CloseConn{Conn: 7, Seq: 4}, core.CloseConn{Conn: 8, Abort: true, Seq: 1},
		core.Connect{})
	if _, ok := wait(t, got).(core.ConnectDone); !ok {
		t.Fatal("no ConnectDone")
	}
	waitSeen(conns, 5, 2*time.Second) // the pool runs the Connect beside the socket IO
	// Connection 7 is in Seq order. Connection 8 is a different connection:
	// nothing orders its abort against the others.
	seen := conns.seen()
	if !slices.Contains(seen, "abort") {
		t.Fatalf("no abort in %v", seen)
	}
	seen = slices.DeleteFunc(seen, func(s string) bool { return s == "abort" })
	if s := strings.Join(seen, ","); s != "write x,pause,resume,close" {
		t.Fatalf("connections saw %s", s)
	}
}

// An answer that comes after the actor ended is dropped.
func TestExecutorAnswerAfterEndDropped(t *testing.T) {
	block := make(chan struct{})
	objs := slowObjs{ObjectStore: specsystem.NewObjectStore(), block: block}
	e := NewExecutor(t.Context(), ExecConfig{}, objs, nil, &fakeConns{}, Classifier{Objects: MemoryErrorClassifier{}})
	got := make(chan core.EventData, 1)
	host := live.NewHost(live.HostConfig{Clock: live.RealClock()})
	live.Spawn(host, actor.Props[core.EventData, core.IO]{
		Name: "probe",
		New: func(actor.Env) actor.Actor[core.EventData, core.IO] {
			return exitAfterPut{got: got}
		},
		Boot: func(self actor.Incarnation) core.EventData { return core.NodeStarted{Self: self} },
	}, e.Exec)
	if err := host.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(block)
	select {
	case m := <-got:
		t.Fatalf("answered after the end: %+v", m)
	case <-time.After(30 * time.Millisecond):
	}
}

// exitAfterPut issues a Put and exits in the same Receive.
type exitAfterPut struct{ got chan core.EventData }

func (p exitAfterPut) Receive(_ actor.Now, msg core.EventData, o *actor.Outbox[core.EventData, core.IO]) {
	if _, ok := msg.(core.NodeStarted); !ok {
		p.got <- msg
		return
	}
	actor.Perform(o, core.ObjectCall{Call: objectstore.Put{Key: "k"}},
		func(d core.ObjectDone) core.EventData { return d })
	o.Stop(actor.Exit, "")
}

func (exitAfterPut) StateHash() uint64 { return 0 }

func TestExecutorUnknownIOIsAnError(t *testing.T) {
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, &fakeConns{}, Classifier{})
	if err := e.Exec(nil, nil); err == nil || !strings.Contains(err.Error(), "unknown IO") {
		t.Fatalf("err = %v", err)
	}
}

func TestClassifierRoutes(t *testing.T) {
	c := Classifier{Objects: s3.Classifier{}, CP: memoryCP{}}
	if code, out := c.Classify(core.ObjectCall{Call: objectstore.Put{}}, context.DeadlineExceeded); code != core.ErrTimeout || out != core.MaybeApplied {
		t.Errorf("obj: %v %v", code, out)
	}
	if code, _ := c.Classify(core.Send{}, errors.New("x")); code != core.ErrIO {
		t.Errorf("other: %v", code)
	}
}

// slowCP answers after d, or with the call's context error.
type slowCP struct{ d time.Duration }

func (s slowCP) Call(ctx context.Context, _ contract.Operation) (contract.Response, error) {
	select {
	case <-time.After(s.d):
		return contract.GroupLeaveResp{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A control plane call takes its own Timeout when it has one (group joins wait
// for a rebalance), and the executor's default otherwise.
func TestControlPlaneCallOwnTimeout(t *testing.T) {
	cls := Classifier{Objects: MemoryErrorClassifier{}, CP: memoryCP{}}
	e := NewExecutor(t.Context(), ExecConfig{CPTimeout: 10 * time.Millisecond}, nil, slowCP{d: 60 * time.Millisecond}, &fakeConns{}, cls)
	got := runProbe(t, e,
		core.ControlPlaneCall{Op: contract.GroupLeave{Group: "g"}}, // default 10 ms: too slow
		core.ControlPlaneCall{Op: contract.GroupLeave{Group: "g"}, Timeout: actor.Duration(time.Second)})
	for range 2 {
		d := wait(t, got).(core.OpCallDone)
		switch d.Op {
		case 0:
			if d.Done.Err == core.OK {
				t.Errorf("default deadline: %+v, want a failure", d.Done)
			}
		case 1:
			if d.Done.Err != core.OK {
				t.Errorf("own deadline: %+v, want OK", d.Done)
			}
		}
	}
}

// Socket IO that arrives out of Seq order is applied in Seq order, once the
// gap is filled; Exec never waits for the missing IO.
func TestExecutorReordersSocketIO(t *testing.T) {
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
	for _, io := range []core.IO{
		core.Send{Conn: 7, Data: []byte("c"), Seq: 3},
		core.Send{Conn: 7, Data: []byte("a"), Seq: 1},
	} {
		if err := e.Exec(nil, io); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(conns.seen(), ","); got != "write a" {
		t.Fatalf("before the gap is filled: %s", got)
	}
	if err := e.Exec(nil, core.Send{Conn: 7, Data: []byte("b"), Seq: 2}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(conns.seen(), ","); got != "write a,write b,write c" {
		t.Fatalf("connections saw %s", got)
	}
}

// Connections are ordered on their own: a gap on one holds nothing on another.
func TestExecutorConnectionsOrderIndependently(t *testing.T) {
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
	for _, io := range []core.IO{
		core.Send{Conn: 1, Data: []byte("1b"), Seq: 2},
		core.Send{Conn: 2, Data: []byte("2a"), Seq: 1},
		core.Send{Conn: 2, Data: []byte("2b"), Seq: 2},
	} {
		if err := e.Exec(nil, io); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(conns.seen(), ","); got != "write 2a,write 2b" {
		t.Fatalf("connection 1 is waiting for Seq 1, saw %s", got)
	}
	if err := e.Exec(nil, core.Send{Conn: 1, Data: []byte("1a"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(conns.seen(), ","); got != "write 2a,write 2b,write 1a,write 1b" {
		t.Fatalf("connections saw %s", got)
	}
}

// A connection's entry is dropped once its CloseConn is applied, so a later
// connection with the same id starts again at Seq 1.
func TestExecutorForgetsClosedConnection(t *testing.T) {
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
	for _, io := range []core.IO{
		core.CloseConn{Conn: 7, Seq: 2}, // held: not removed yet
		core.Send{Conn: 7, Data: []byte("a"), Seq: 1},
	} {
		if err := e.Exec(nil, io); err != nil {
			t.Fatal(err)
		}
	}
	e.orderMu.Lock()
	n := len(e.order)
	e.orderMu.Unlock()
	if n != 0 {
		t.Fatalf("%d connection entries after the close", n)
	}
	if err := e.Exec(nil, core.Send{Conn: 7, Data: []byte("again"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(conns.seen(), ","); got != "write a,close,write again" {
		t.Fatalf("connections saw %s", got)
	}
}

// The actor numbers socket IO from 1: Seq 0 is a bug, and ends the Host.
func TestExecutorSeqZeroIsAnError(t *testing.T) {
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
	for _, io := range []core.IO{core.Send{Conn: 7}, core.CloseConn{Conn: 7}, core.PauseReads{Conn: 7}, core.ResumeReads{Conn: 7}} {
		if err := e.Exec(nil, io); err == nil || !strings.Contains(err.Error(), "Seq 0") {
			t.Errorf("%T: err = %v", io, err)
		}
	}
	if got := conns.seen(); len(got) != 0 {
		t.Fatalf("connections saw %v", got)
	}
}
