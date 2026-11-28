package service_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/simulator/service"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

const ms = actor.Millisecond

// ask is the request (and the IO); tell is its answer and also an input of
// the caller actor, as is started.
type ask struct{ N uint64 }
type tell struct{ N uint64 }
type started struct{}

func (a ask) Fold(h *trace.Hash)     { h.Uint64(1); h.Uint64(a.N) }
func (t tell) Fold(h *trace.Hash)    { h.Uint64(2); h.Uint64(t.N) }
func (started) Fold(h *trace.Hash)   { h.Uint64(3) }
func (a ask) AsRequest() (ask, tell) { return a, tell{} }

type input interface {
	actor.Payload
	isInput()
}

func (tell) isInput()    {}
func (started) isInput() {}

// echo answers N+1 after a delay, and counts every call and every Later step.
type echo struct {
	calls, laters uint64
	recovered     bool
	wanted        []bool
}

func (e *echo) StateHash() uint64 { return e.calls*1000 + e.laters*10 + btou(e.recovered) }

func btou(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func (e *echo) Handle(ctx *service.Ctx, req ask, reply sim.Reply[tell]) {
	e.calls++
	ctx.Later(ctx.Env().Duration(ms, 5*ms), func(ctx *service.Ctx) {
		e.laters++
		e.wanted = append(e.wanted, reply.Wanted())
		ctx.RecordFault("echo_fault", actor.Field{Key: "n", Value: actor.Uint64Value(req.N)})
		reply.After(ms, tell{req.N + 1})
	})
}

func (e *echo) EnterRecovery() { e.recovered = true }

type world struct {
	sys *sim.System
	e   *echo
	ep  *service.Endpoint[ask, tell]
	got []uint64
}

type perfActor struct{ w *world }

func (a perfActor) StateHash() uint64 { return 0 }
func (a perfActor) Receive(_ actor.Now, in input, o *actor.Outbox[input, ask]) {
	switch m := in.(type) {
	case started:
		for n := range uint64(3) {
			actor.Perform(o, ask{n}, func(t tell) input { return t })
		}
	case tell:
		a.w.got = append(a.w.got, m.N)
	}
}

func newWorld(seed uint64, verify bool) *world {
	w := &world{sys: sim.NewSystem(sim.Config{Seed: seed, VerifyHashes: verify}), e: &echo{}}
	w.ep = service.Add[ask, tell](w.sys, "echo", w.e)
	sim.Spawn(w.sys, actor.Props[input, ask]{
		Name: "a",
		Boot: func(actor.Incarnation) input { return started{} },
		New:  func(actor.Env) actor.Actor[input, ask] { return perfActor{w} },
	}, sim.WithExecutor(func(ctx *sim.ExecCtx, r ask) { service.Forward(ctx, r, w.ep) }))
	return w
}

func TestForwardAnswersThroughLaterAndTracksState(t *testing.T) {
	w := newWorld(1, true) // VerifyHashes: a missed Touch fails the run
	res := w.sys.Run()
	if !res.OK() {
		t.Fatalf("failures: %+v", res.Failures)
	}
	slices.Sort(w.got)
	if !slices.Equal(w.got, []uint64{1, 2, 3}) || w.e.calls != 3 || w.e.laters != 3 {
		t.Errorf("got %v calls %d laters %d", w.got, w.e.calls, w.e.laters)
	}
	if !slices.Equal(w.e.wanted, []bool{true, true, true}) {
		t.Errorf("wanted %v", w.e.wanted)
	}
	if n := len(w.sys.Timeline().Snapshot("sim_fault")); n != 3 {
		t.Errorf("fault facts %d", n)
	}
	f := w.sys.Timeline().Snapshot("sim_fault")[0].Fact
	if s, ok := f.Field("service").(actor.StringValue); !ok || s != "echo" {
		t.Errorf("service field %v", f.Field("service"))
	}
}

func TestStateIsPartOfTheTraceHash(t *testing.T) {
	a, b := newWorld(2, false).sys.Run(), newWorld(2, false).sys.Run()
	if a.Hash != b.Hash {
		t.Fatalf("seed 2 twice: %x %x", a.Hash, b.Hash)
	}
	w := newWorld(2, false)
	w.sys.After(100*ms, func() { w.ep.EnterRecovery() })
	if r := w.sys.Run(); r.Hash == a.Hash || !w.e.recovered {
		t.Errorf("EnterRecovery did not reach the service or the hash: %x %x", r.Hash, a.Hash)
	}
}

func TestAddProtocolFailures(t *testing.T) {
	sys := sim.NewSystem(sim.Config{Seed: 1})
	service.Add[ask, tell](sys, "x", &echo{})
	service.Add[ask, tell](sys, "x", &echo{})
	service.Add[ask, tell](sys, "y", nil)
	r := sys.Run()
	seen := 0
	for _, f := range r.Failures {
		if f.Kind == sim.FailProtocol {
			seen++
		}
	}
	if seen != 2 {
		t.Errorf("failures: %+v", r.Failures)
	}
}
