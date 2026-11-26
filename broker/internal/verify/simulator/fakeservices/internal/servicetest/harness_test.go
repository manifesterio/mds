package servicetest

import (
	"fmt"
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/shared/objectstore"
)

const ms = actor.Duration(1_000_000)

var objectAnswered = assert.NewReachable("servicetest object answered")

// objects answers every object call after one millisecond with the data
// "obj", and records a "slow" fault for each call.
type objects struct{ calls int }

func (s *objects) Handle(ctx *sim.ServiceCtx, _ core.ObjectCall, reply sim.Responder[core.ObjectDone]) {
	s.calls++
	ctx.RecordFault("slow")
	ctx.Env().Reachable(objectAnswered)
	reply.After(ms, core.ObjectDone{Result: objectstore.GetDone{Data: []byte("obj")}})
}

func (s *objects) StateHash() uint64 { return uint64(s.calls) }

// plane answers every control-plane call after two milliseconds with no
// topics.
type plane struct{ calls int }

func (s *plane) Handle(_ *sim.ServiceCtx, _ core.ControlPlaneCall, reply sim.Responder[core.ControlPlaneDone]) {
	s.calls++
	reply.After(2*ms, core.ControlPlaneDone{Resp: contract.DescribeTopicsResp{}})
}

func (s *plane) StateHash() uint64 { return uint64(s.calls) }

// setup makes a harness with callers a and b and both services.
func setup(t TB, seed uint64) (*Harness, *objects, *plane) {
	h := New(t, seed, "a", "b")
	o, p := &objects{}, &plane{}
	h.Objects = sim.AddService[core.ObjectCall, core.ObjectDone](h.Sys, "objects", o)
	h.ControlPlane = sim.AddService[core.ControlPlaneCall, core.ControlPlaneDone](h.Sys, "plane", p)
	return h, o, p
}

var (
	getCall   = core.ObjectCall{Call: objectstore.Get{}}
	topicCall = core.ControlPlaneCall{Op: contract.DescribeTopics{}}
)

func TestHarnessDeliversAnswersOfBothServices(t *testing.T) {
	h, o, p := setup(t, 1)
	h.Issue("a", 1, getCall)
	if o.calls != 1 || len(h.Delivered) != 0 {
		t.Fatalf("after Issue: %d object calls, %d deliveries", o.calls, len(h.Delivered))
	}
	h.Do("b", 2, topicCall)
	h.Run()
	if p.calls != 1 || len(h.Delivered) != 2 {
		t.Fatalf("after Run: %d control-plane calls, deliveries %+v", p.calls, h.Delivered)
	}
	first, second := h.Delivered[0], h.Delivered[1]
	if first.To != "a" || first.Label != 1 || first.At != sim.Time(ms) {
		t.Errorf("object answer %+v", first)
	}
	if second.To != "b" || second.Label != 2 || second.At != sim.Time(2*ms) {
		t.Errorf("control-plane answer %+v", second)
	}
	if d, ok := h.Find(1).(core.ObjectDone); !ok || string(d.Result.(objectstore.GetDone).Data) != "obj" {
		t.Errorf("Find(1) = %#v", h.Find(1))
	}
	if d, ok := h.Find(2).(core.ControlPlaneDone); !ok || d.Err != core.OK {
		t.Errorf("Find(2) = %#v", h.Find(2))
	}
	if h.Find(99) != nil {
		t.Error("Find of an unknown label is not nil")
	}
	if got := h.Faults(); !slices.Equal(got, []string{"slow"}) {
		t.Errorf("faults %v", got)
	}
	if !h.Hit(objectAnswered.Name()) || h.Hit("servicetest never declared") {
		t.Error("Hit")
	}
}

func TestHarnessTraceIsTheSameForTheSameSeed(t *testing.T) {
	run := func() string {
		h, _, _ := setup(t, 7)
		h.Do("a", 1, topicCall)
		h.Do("b", 2, getCall)
		h.Run()
		return h.Trace()
	}
	a, b := run(), run()
	want := fmt.Sprintf("%d b 2 ", ms)
	if a != b || len(a) == 0 || a[:len(want)] != want {
		t.Fatalf("traces differ or start wrong:\n%s\n%s", a, b)
	}
}

func TestHarnessRunForAndStep(t *testing.T) {
	h, o, _ := setup(t, 1)
	h.DoAfter(5*ms, "a", 1, getCall)
	h.RunFor(3 * ms)
	if h.Now() != sim.Time(3*ms) || o.calls != 0 {
		t.Fatalf("at %d: %d calls", h.Now(), o.calls)
	}
	for h.Step() {
	}
	if h.Now() != sim.Time(6*ms) || len(h.Delivered) != 1 {
		t.Fatalf("at %d: deliveries %+v", h.Now(), h.Delivered)
	}
	if h.Step() {
		t.Fatal("Step on an empty queue")
	}
}

func TestHarnessCrashDropsTheAnswerAndBootTakesNewCommands(t *testing.T) {
	h, _, _ := setup(t, 1)
	h.Issue("a", 1, getCall)
	h.Crash("a")
	h.Run()
	if h.Find(1) != nil {
		t.Fatalf("a crashed caller got %#v", h.Find(1))
	}
	h.Boot("a")
	h.Recover()
	h.Issue("a", 2, topicCall)
	h.Run()
	if _, ok := h.Find(2).(core.ControlPlaneDone); !ok {
		t.Fatalf("after Boot: %+v", h.Delivered)
	}
}

// recordingTB is a TB that keeps what fails instead of ending the test.
type recordingTB struct {
	fatals   []string
	errors   []string
	cleanups []func()
}

func (r *recordingTB) Helper() {}
func (r *recordingTB) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}
func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recordingTB) Cleanup(fn func()) { r.cleanups = append(r.cleanups, fn) }

// A socket IO has no service in this harness: the caller performs it and the
// harness reports it.
func TestHarnessRefusesSocketIO(t *testing.T) {
	tb := &recordingTB{}
	h, _, _ := setup(tb, 1)
	h.Do("a", 1, core.CloseConn{})
	h.Run()
	for _, fn := range tb.cleanups {
		fn()
	}
	if len(tb.fatals) != 1 || len(h.Delivered) != 0 {
		t.Fatalf("fatals %q, deliveries %+v", tb.fatals, h.Delivered)
	}
}
