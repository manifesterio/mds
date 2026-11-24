package actor_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
)

// ---- a tiny protocol: the actor's messages, a component's messages, an IO sum ----

type msg interface {
	actor.Payload
	isMsg()
}

type (
	ping     struct{ N int }
	fromComp struct{ C compMsg }
	answered struct{ V string }
)

func (ping) isMsg()     {}
func (fromComp) isMsg() {}
func (answered) isMsg() {}

func (m ping) Fold(h *trace.Hash)     { h.Uint64(1); h.Int64(int64(m.N)) }
func (m fromComp) Fold(h *trace.Hash) { h.Uint64(2); m.C.Fold(h) }
func (m answered) Fold(h *trace.Hash) { h.Uint64(3); h.String(m.V) }

type compMsg struct{ S string }

func (m compMsg) Fold(h *trace.Hash) { h.Uint64(10); h.String(m.S) }

type io interface {
	actor.Payload
	isIO()
}

type (
	read     struct{ Key string }
	readDone struct{ Value string }
	write    struct{ Key string }
)

func (read) isIO()  {}
func (write) isIO() {}

func (m read) Fold(h *trace.Hash)     { h.Uint64(20); h.String(m.Key) }
func (m write) Fold(h *trace.Hash)    { h.Uint64(21); h.String(m.Key) }
func (m readDone) Fold(h *trace.Hash) { h.Uint64(22); h.String(m.Value) }

// AsRequest ties read to its answer readDone (dstgen writes this).
func (m read) AsRequest() (io, readDone) { return m, readDone{} }

type request struct {
	Q       string
	ReplyTo actor.ReplyTo[answered]
}

func (m request) Fold(h *trace.Hash) { h.Uint64(30); h.String(m.Q); m.ReplyTo.Fold(h) }

// ---- helpers ----

type rig struct {
	n      uint64
	misuse []string
}

func (r *rig) outbox() *actor.Outbox[msg, io] {
	return actor.NewOutbox[msg, io](actor.Binding{
		Self:   actor.EntityID{Runtime: owner, Slot: 2, Name: "me"},
		Asker:  actor.Asker{Kind: actor.ActorAsker, Runtime: owner, Slot: 2, Name: "me"},
		Next:   func() uint64 { r.n++; return r.n },
		Misuse: func(what string) { r.misuse = append(r.misuse, what) },
	})
}

func kinds(effects []actor.Effect) []actor.EffectKind {
	out := make([]actor.EffectKind, len(effects))
	for i, e := range effects {
		out[i] = e.Kind
	}
	return out
}

var owner = actor.NewOwner()

var (
	other   = actor.NewEntityRef[request](actor.EntityID{Runtime: owner, Slot: 3, Name: "other"})
	service = actor.NewServiceRef[write, readDone](actor.ServiceID{Runtime: owner, Slot: 1, Name: "store"})
)

// ---- tests ----

func TestOutboxRecordsInOrder(t *testing.T) {
	r := &rig{}
	o := r.outbox()
	o.TellSelf(ping{1})
	o.After(-5, ping{2})
	o.StartTimer("k", 10, ping{3})
	o.CancelTimer("k")
	o.Do(write{"a"})
	o.Fact(actor.Fact{Name: "f"})
	o.ForgetPending()
	other.Tell(o, request{Q: "q"})
	service.Tell(o, write{"b"})
	actor.Call(o, service.Request(write{"c"}), func(d readDone) msg { return answered{d.Value} })
	actor.Perform(o, read{"d"}, func(d readDone) msg { return answered{d.Value} })
	actor.Reply(o, actor.NewReplyTo[answered](actor.ReplyID{Asker: actor.Asker{Kind: actor.ActorAsker, Name: "x"}, Number: 9}), answered{"r"})
	o.Stop(actor.Trip, "bad")
	effects := actor.Close(o)

	want := []actor.EffectKind{
		actor.EffectTellSelf, actor.EffectAfter, actor.EffectStartTimer, actor.EffectCancelTimer,
		actor.EffectDo, actor.EffectFact, actor.EffectForgetPending, actor.EffectTell,
		actor.EffectTellService, actor.EffectCall, actor.EffectPerform, actor.EffectReply, actor.EffectStop,
	}
	if got := kinds(effects); !slices.Equal(got, want) {
		t.Fatalf("kinds: got %v want %v", got, want)
	}
	if effects[1].D != 0 {
		t.Errorf("After(-5): D = %d, want 0", effects[1].D)
	}
	if effects[9].Number != 1 || effects[10].Number != 2 {
		t.Errorf("pending numbers: Call %d, Perform %d; want 1, 2", effects[9].Number, effects[10].Number)
	}
	if got := effects[10].Msg; got != (read{"d"}) {
		t.Errorf("Perform records the request as IO: got %v", got)
	}
	if got := effects[10].Adapt(readDone{"v"}); got != (answered{"v"}) {
		t.Errorf("Perform adapt: got %v", got)
	}
	if len(r.misuse) != 0 {
		t.Errorf("misuse: %v", r.misuse)
	}
}

func TestAskMintsReplyTo(t *testing.T) {
	r := &rig{n: 41}
	o := r.outbox()
	var seen actor.ReplyTo[answered]
	actor.Ask(o, other,
		func(to actor.ReplyTo[answered]) request { seen = to; return request{Q: "q", ReplyTo: to} },
		func(a answered) msg { return a })
	e := actor.Close(o)[0]
	if e.Number != 42 || e.Reply.Number != 42 || seen.ID() != e.Reply {
		t.Fatalf("ask number/reply: %+v, seen %+v", e, seen.ID())
	}
	if e.Reply.Asker.Name != "me" || e.Reply.Asker.Kind != actor.ActorAsker {
		t.Errorf("asker: %+v", e.Reply.Asker)
	}
	if req, ok := actor.As[request](e.Msg); !ok || req.ReplyTo != seen {
		t.Errorf("request carries the ReplyTo: %+v", e.Msg)
	}
	if e.To != other.EntityID() {
		t.Errorf("to: %+v", e.To)
	}
}

func TestSubWrapsAndScopes(t *testing.T) {
	r := &rig{}
	o := r.outbox()
	comp := actor.Sub(o, "session", func(c compMsg) msg { return fromComp{c} })
	inner := actor.Sub(comp, "retry", func(s compMsg) compMsg { return compMsg{"inner:" + s.S} })

	comp.TellSelf(compMsg{"a"})
	comp.StartTimer("t", 5, compMsg{"b"})
	inner.After(1, compMsg{"c"})
	inner.ForgetPending()
	actor.Perform(comp, read{"k"}, func(d readDone) compMsg { return compMsg{d.Value} })
	inner.Watch(actor.ReplyTo[answered]{}, compMsg{"gone"})
	comp.Stop(actor.Exit, "") // not allowed through a Sub
	if !comp.Self().IsZero() {
		t.Error("a component has no Self")
	}
	effects := actor.Close(o)

	if got := effects[0].Msg; got != (fromComp{compMsg{"a"}}) {
		t.Errorf("TellSelf through Sub: %v", got)
	}
	if got := effects[1].StoredKey(); got != "session/t" {
		t.Errorf("stored key: %q", got)
	}
	if got := effects[2].Msg; got != (fromComp{compMsg{"inner:c"}}) {
		t.Errorf("nested wrap: %v", got)
	}
	if effects[3].Kind != actor.EffectForgetPending || effects[3].Scope != "session/retry" {
		t.Errorf("forget scope: %+v", effects[3])
	}
	if got := effects[4].Adapt(readDone{"v"}); got != (fromComp{compMsg{"v"}}) {
		t.Errorf("Perform answer through Sub: %v", got)
	}
	if got := effects[5].Msg; got != (fromComp{compMsg{"inner:gone"}}) {
		t.Errorf("Watch gone through Sub: %v", got)
	}
	if len(effects) != 6 || len(r.misuse) != 1 {
		t.Errorf("Stop through Sub must be refused: %d effects, misuse %v", len(effects), r.misuse)
	}
}

func TestUseAfterCloseIsMisuse(t *testing.T) {
	r := &rig{}
	o := r.outbox()
	actor.Close(o)
	o.TellSelf(ping{1})
	if len(r.misuse) != 1 {
		t.Fatalf("misuse: %v", r.misuse)
	}
	if got := actor.Close(o); len(got) != 0 {
		t.Errorf("effects after close: %v", got)
	}
}

func TestSelf(t *testing.T) {
	o := (&rig{}).outbox()
	if got := o.Self().Name(); got != "me" {
		t.Errorf("Self: %q", got)
	}
}

func sum(fold func(h *trace.Hash)) uint64 {
	h := trace.New()
	fold(&h)
	return h.Sum()
}

func TestRefFolds(t *testing.T) {
	ref := actor.NewEntityRef[ping](actor.EntityID{Runtime: owner, Slot: 1, Name: "a"})
	want := sum(func(h *trace.Hash) { h.Uint64(2101); h.String("a") })
	if got := sum(ref.Fold); got != want {
		t.Errorf("EntityRef fold: %x want %x", got, want)
	}
	boot := actor.ID{1, 2, 3}
	reply := actor.NewReplyTo[ping](actor.ReplyID{Asker: actor.Asker{Kind: actor.ActorAsker, Name: "b", Boot: boot}, Number: 5})
	want = sum(func(h *trace.Hash) {
		h.Uint64(2102)
		h.Uint64(1)
		h.String("b")
		h.Bytes(boot[:])
		h.Uint64(5)
	})
	if got := sum(reply.Fold); got != want {
		t.Errorf("ReplyTo fold: %x want %x", got, want)
	}
	svc := actor.NewServiceRef[ping, ping](actor.ServiceID{Runtime: owner, Name: "s"})
	want = sum(func(h *trace.Hash) { h.Uint64(2103); h.String("s") })
	if got := sum(svc.Fold); got != want {
		t.Errorf("ServiceRef fold: %x want %x", got, want)
	}
	var zero actor.ReplyTo[ping]
	if !zero.IsZero() || !(actor.EntityRef[ping]{}).IsZero() {
		t.Error("zero refs")
	}
}

func TestEffectFoldsDiffer(t *testing.T) {
	o := (&rig{}).outbox()
	o.StartTimer("k", 5, ping{1})
	o.StartTimer("k", 6, ping{1})
	sub := actor.Sub(o, "s", func(c compMsg) msg { return fromComp{c} })
	sub.CancelTimer("k")
	o.CancelTimer("k")
	e := actor.Close(o)
	seen := map[uint64]bool{}
	for _, x := range e {
		seen[sum(x.Fold)] = true
	}
	if len(seen) != len(e) {
		t.Errorf("distinct effects fold alike: %d distinct of %d", len(seen), len(e))
	}
}
