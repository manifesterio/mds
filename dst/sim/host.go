package sim

import (
	"fmt"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/core"
)

// slot is one actor of the System, whatever its types. It is the one seam
// between the untyped System and a typed host, so it is wide on purpose.
type slot interface { //nolint:interfacebloat // see above
	base() *actorBase
	boot()
	pump()
	stateHash() uint64
	inboxLens() (int, int)
	drop()
	// ended runs the OnEnd hook of the incarnation that just ended.
	ended(who actor.Incarnation, how actor.End, cause string)
	stopRequest(deadline actor.Mono) bool
	// arrive puts a message that reached the actor in its inbox. n is the
	// owned number it carries (a timer or an answer), 0 for none.
	arrive(msg actor.Payload, inc actor.ID, toInc bool, n uint64)
	// answerArrives handles the answer for awaited number n of incarnation inc.
	answerArrives(answer actor.Payload, inc actor.ID, n uint64)
	phaseStep(p Phase)
}

// item is one waiting message. n is its owned number (0: none).
type item[In actor.Payload] struct {
	msg  In
	boot bool
	n    uint64
}

// host is one actor with its types.
type host[In, IO actor.Payload] struct {
	actorBase
	props    actor.Props[In, IO]
	exec     executor[IO]
	onPhase  func(Phase) (In, bool)
	afterRcv func(self actor.Incarnation, msg In, effects []actor.Effect)
	onEnd    func(self actor.Incarnation, how actor.End, cause string)
	cost     CostParams
	a        actor.Actor[In, IO]
	control  []item[In]
	data     []item[In]
}

func (h *host[In, IO]) base() *actorBase { return &h.actorBase }

func (h *host[In, IO]) stateHash() uint64 { return h.a.StateHash() }

func (h *host[In, IO]) inboxLens() (int, int) { return len(h.control), len(h.data) }

func (h *host[In, IO]) empty() bool { return len(h.control) == 0 && len(h.data) == 0 }

func (h *host[In, IO]) drop() {
	var zero actor.Actor[In, IO]
	h.a = zero
	h.control, h.data = nil, nil
	h.owned, h.keyed = nil, nil
	h.busy = 0
}

// push puts msg in its inbox. It reports false when Priority panicked (the
// run halts).
func (h *host[In, IO]) push(it item[In]) bool {
	p := actor.Data
	if h.props.Priority != nil && !h.sys.guard("Priority "+h.name, func() { p = h.props.Priority(it.msg) }) {
		h.sys.halted = true
		return false
	}
	if p == actor.Control {
		h.control = append(h.control, it)
	} else {
		h.data = append(h.data, it)
	}
	return true
}

// next takes the next message: Control first, then Data.
func (h *host[In, IO]) next() (item[In], bool) {
	var zero item[In]
	for _, q := range []*[]item[In]{&h.control, &h.data} {
		if len(*q) > 0 {
			it := (*q)[0]
			(*q)[0] = zero
			*q = (*q)[1:]
			return it, true
		}
	}
	return zero, false
}

func (h *host[In, IO]) arrive(msg actor.Payload, inc actor.ID, toInc bool, n uint64) {
	sys := h.sys
	if !h.up || (toInc && h.inc != inc) {
		sys.dropped(dropDeadTarget, h.name)
		sys.logMsg("drop stale "+h.name, msg)
		return
	}
	m, ok := core.As[In](msg)
	if !ok {
		sys.Fail(FailProtocol, "wrong_message_type", fmt.Sprintf("%s: got %T, which is not its message type", h.name, msg))
		return
	}
	sys.logMsg("deliver "+h.name+" <-", msg)
	if h.push(item[In]{msg: m, n: n}) {
		h.pump()
	}
}

func (h *host[In, IO]) stopRequest(deadline actor.Mono) bool {
	if h.props.StopMsg == nil {
		return false
	}
	var msg In
	if !h.sys.guard("StopMsg "+h.name, func() { msg = h.props.StopMsg(deadline) }) {
		h.sys.halted = true
		return true
	}
	h.control = append(h.control, item[In]{msg: msg})
	return true
}

// boot starts a new incarnation: a new boot number from the run stream, a
// fresh actor from Props.New, the boot message first, and a wake-up.
func (h *host[In, IO]) boot() {
	sys := h.sys
	if h.up || h.broken {
		return
	}
	h.inc = env{sys: sys}.NewID()
	h.control, h.data = nil, nil
	h.resetIncarnation()
	h.boots++
	self := h.self()
	var a actor.Actor[In, IO]
	ok := sys.guard("New "+h.name, func() { a = h.props.New(env{sys: sys, self: self}) })
	if ok && a == nil {
		sys.Fail(FailProtocol, "factory", h.name+": Props.New returned nil")
	}
	if !ok || a == nil {
		h.broken = true
		if h.workload {
			sys.halted = true
		}
		return
	}
	h.a, h.up = a, true
	h.invalidateHash()
	if h.boots > 1 {
		sys.systemFact("sim_restarted", actor.Field{Key: "node", Value: actor.StringValue(h.name)})
	}
	if h.props.Boot != nil {
		var msg In
		if !sys.guard("Boot "+h.name, func() { msg = h.props.Boot(self) }) {
			sys.halted = true
			return
		}
		h.control = append(h.control, item[In]{msg: msg, boot: true})
	}
	if !h.empty() {
		sys.ensureWake(&h.actorBase, sys.q.now)
	}
}

// pump gives the actor one Receive if it is up, not paused, not busy, and
// has a message waiting. A busy actor gets a wake-up at the end of its busy
// time.
func (h *host[In, IO]) pump() {
	sys := h.sys
	if !h.up || h.paused || sys.halted {
		return
	}
	now := sys.q.now
	if h.busy > now {
		if !h.empty() {
			sys.ensureWake(&h.actorBase, h.busy)
		}
		return
	}
	it, ok := h.next()
	for ok && !h.take(it.n) {
		sys.dropped(dropStaleTimer, h.name)
		sys.logMsg("drop stale "+h.name, it.msg)
		it, ok = h.next()
	}
	if !ok {
		return
	}
	if h.received == 0 && h.props.Boot != nil && !it.boot {
		sys.Fail(FailProtocol, "boot_first", h.name+": the first Receive did not take the boot message")
	}
	h.received++
	sys.hash.String(h.name)
	it.msg.Fold(&sys.hash)
	sys.logMsg("receive "+h.name, it.msg)
	if !h.receive(it.msg) || !h.up {
		return
	}
	cost := max(h.cost.sample(sys.Env()), 0)
	h.busy = now + Time(cost)
	if !h.empty() {
		sys.ensureWake(&h.actorBase, h.busy)
	}
	sys.attr++
}

// receive runs one Receive with msg and carries out its effects. It reports
// false when Receive panicked: then a workload halts the run and any other
// actor crashes: no effect of the panicking Receive is applied.
func (h *host[In, IO]) receive(msg In) bool {
	sys := h.sys
	h.invalidateHash()
	inc := h.inc
	o := core.NewOutbox[In, IO](core.Binding{
		Self:   h.entityID(),
		Asker:  h.asker(),
		Next:   h.nextNumber,
		Misuse: func(what string) { sys.Fail(FailProtocol, "outbox_misuse", h.name+": "+what) },
	})
	now := actor.Now{Mono: h.local(), Wall: sys.wall()}
	if !sys.guard("Receive "+h.name, func() { h.a.Receive(now, msg, o) }) {
		core.Close(o)
		if h.workload {
			sys.halted = true
		} else {
			sys.end(h, actor.Crash, "panic")
		}
		return false
	}
	effects := core.Close(o)
	h.apply(effects)
	if h.afterRcv != nil && h.alive(inc) && !sys.halted {
		if !sys.guard("AfterReceive "+h.name, func() { h.afterRcv(h.self(), msg, effects) }) {
			sys.halted = true
		}
	}
	return true
}

// phaseStep gives a live workload its OnPhase message as one Receive. It
// takes no node time and skips the inbox.
func (h *host[In, IO]) phaseStep(p Phase) {
	sys := h.sys
	if h.onPhase == nil || !h.up || sys.halted {
		return
	}
	sys.hash.String(h.name)
	sys.hash.Uint64(tagOnPhase)
	sys.hash.Uint64(uint64(p))
	var msg In
	var ok bool
	if !sys.guard("OnPhase "+h.name, func() { msg, ok = h.onPhase(p) }) {
		sys.halted = true
		return
	}
	if !ok {
		return
	}
	sys.logMsg(fmt.Sprintf("on_phase %s %s", h.name, p), msg)
	h.receive(msg)
}

func (h *host[In, IO]) ended(who actor.Incarnation, how actor.End, cause string) {
	if h.onEnd != nil && !h.sys.guard("OnEnd "+h.name, func() { h.onEnd(who, how, cause) }) {
		h.sys.halted = true
	}
}
