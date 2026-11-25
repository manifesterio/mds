package cluster

import (
	"bytes"
	"slices"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/sim"
)

// The assertions of the connection table. Each one is declared once here, and
// the code uses the value. A name is never built at run time, and it must not
// change: reports and replays use it.
var (
	connResetInjected = assert.NewReachable("net: conn reset injected") //determinismcheck:ok catalog handle, set once at start
	crashResetPeer    = assert.NewSometimes("net: crash reset a peer")  //determinismcheck:ok catalog handle, set once at start
)

// network is the executor of the brokers and the clients. Object store and
// control-plane calls go to their services. The socket IO runs on a small
// connection table:
//
//   - A client's Connect to a broker that is up opens a connection: the
//     client gets ConnectDone with its ConnID, the broker gets ConnOpened, both
//     after one delay. A broker that is down answers ErrConnClosed.
//   - A Send arrives at the other end as one whole core.BytesReceived, after a
//     delay drawn from the run's Env. Frames of one direction keep their order.
//   - CloseConn closes both ends. The other end gets ConnClosed: a graceful
//     close after the frames already sent, an abort at once (frames still in
//     flight are dropped).
//   - PauseReads holds the frames that reach the broker on that connection;
//     ResumeReads releases them in order. An abort is not held.
//   - One fault: while faults are on, a Send resets its connection with
//     ConnResetProb. Both ends get ConnClosed{Abort: true}; the frames in
//     flight and the frame of the Send are dropped.
//   - When a broker incarnation ends, each of its connections closes: the
//     client gets ConnClosed{Abort: true} after the frames the broker had sent.
//
// A frame is delivered only to the incarnation that owns the end.
type network struct {
	sys          *sim.System
	delay        [2]actor.Duration
	resetProb    float64
	objects      actor.ServiceRef[core.ObjectCall, core.ObjectDone]
	controlPlane actor.ServiceRef[core.ControlPlaneCall, core.ControlPlaneDone]

	ends   map[string]*endpoint // by actor name; looked up by name only
	conns  []*connection        // open connections, in id order
	nextID uint64
}

// endpoint is one actor that can own connection ends.
type endpoint struct {
	name   string
	entity actor.Entity
	tell   func(ev core.EventData)
	last   core.ConnID // the last ConnID given out at this actor; never reused
}

// end is one side of a connection.
type end struct {
	ep     *endpoint
	boot   actor.ID // the incarnation that owns the end
	id     core.ConnID
	paused bool
	held   []core.EventData // arrived while paused
}

// frame is one event on its way to the other end.
type frame struct {
	at sim.Time
	ev core.EventData
}

// direction is the frames from one end to the other, in order.
type direction struct {
	last   sim.Time // the arrival time of the newest frame
	flight []frame
}

// connection is a pair of ends. dirs[d] carries frames from end[d] to
// end[1-d]. end[0] is the client that dialed, end[1] the broker.
type connection struct {
	id   uint64
	end  [2]end
	dirs [2]direction
}

func newNetwork(sys *sim.System, delay [2]actor.Duration, resetProb float64,
	objects actor.ServiceRef[core.ObjectCall, core.ObjectDone],
	controlPlane actor.ServiceRef[core.ControlPlaneCall, core.ControlPlaneDone]) *network {
	return &network{sys: sys, delay: delay, resetProb: resetProb, objects: objects, controlPlane: controlPlane,
		ends: map[string]*endpoint{}}
}

// add registers an actor that can own connection ends. tell delivers a
// connection event to it.
func (n *network) add(name string, entity actor.Entity, tell func(ev core.EventData)) {
	n.ends[name] = &endpoint{name: name, entity: entity, tell: tell}
}

// exec is the executor of every actor of the cluster.
func (n *network) exec(ctx *sim.ExecCtx, io core.IO) {
	from := ctx.Issuer()
	switch x := io.(type) {
	case core.ObjectCall:
		sim.Forward(ctx, x, n.objects)
	case core.ControlPlaneCall:
		sim.Forward(ctx, x, n.controlPlane)
	case core.Connect:
		n.connect(ctx, from, x)
	case core.Send:
		n.send(from, x)
	case core.CloseConn:
		if c, s := n.lookup(from, x.Conn); c != nil {
			n.close(c, s, x.Abort)
		}
	case core.PauseReads:
		if c, s := n.lookup(from, x.Conn); c != nil {
			c.end[s].paused = true
		}
	case core.ResumeReads:
		if c, s := n.lookup(from, x.Conn); c != nil {
			n.resume(&c.end[s])
		}
	}
}

// draw is the delay of one frame.
func (n *network) draw() actor.Duration {
	return n.sys.Env().Duration(n.delay[0], n.delay[1])
}

// lookup finds the open connection end id of incarnation who.
func (n *network) lookup(who actor.Incarnation, id core.ConnID) (*connection, int) {
	for _, c := range n.conns {
		for s := range 2 {
			if e := &c.end[s]; e.ep.name == who.Name && e.boot == who.Boot && e.id == id {
				return c, s
			}
		}
	}
	return nil, 0
}

func (n *network) connect(ctx *sim.ExecCtx, from actor.Incarnation, x core.Connect) {
	d := n.draw()
	target, known := n.ends[x.Addr]
	var cur actor.Incarnation
	up := false
	if known {
		cur, up = n.sys.Current(target.entity)
	}
	client, ok := n.ends[from.Name]
	if !up || !ok {
		sim.Answer(ctx, x, d, core.ConnectDone{Err: core.ErrConnClosed})
		return
	}
	n.nextID++
	client.last++
	target.last++
	c := &connection{id: n.nextID}
	c.end[0] = end{ep: client, boot: from.Boot, id: client.last}
	c.end[1] = end{ep: target, boot: cur.Boot, id: target.last}
	n.conns = append(n.conns, c)
	sim.Answer(ctx, x, d, core.ConnectDone{Conn: c.end[0].id})
	n.push(c, 0, d, core.ConnOpened{Conn: c.end[1].id, Peer: from.Name})
}

func (n *network) send(from actor.Incarnation, x core.Send) {
	c, s := n.lookup(from, x.Conn)
	if c == nil {
		return // closed: the frame is lost, as on a reset socket
	}
	if n.sys.Phase() == sim.PhaseFaults && n.sys.Env().Chance(n.resetProb) {
		n.reset(c)
		return
	}
	n.push(c, s, n.draw(), core.BytesReceived{Conn: c.end[1-s].id, Data: bytes.Clone(x.Data)})
}

// push puts ev on the direction from end s, to arrive after d but never
// before the frames already on it.
func (n *network) push(c *connection, s int, d actor.Duration, ev core.EventData) {
	dir := &c.dirs[s]
	at := max(n.sys.Now()+sim.Time(d), dir.last)
	dir.last = at
	dir.flight = append(dir.flight, frame{at: at, ev: ev})
	n.sys.At(at, sim.NoLane, func() { n.land(c, s) })
}

// land delivers every frame of the direction from end s that has arrived.
func (n *network) land(c *connection, s int) {
	dir := &c.dirs[s]
	to := &c.end[1-s]
	for len(dir.flight) > 0 && dir.flight[0].at <= n.sys.Now() {
		ev := dir.flight[0].ev
		dir.flight = dir.flight[1:]
		if to.paused {
			to.held = append(to.held, ev)
			continue
		}
		n.deliver(to, ev)
	}
}

// deliver gives ev to the incarnation that owns e, if it is still up.
func (n *network) deliver(e *end, ev core.EventData) {
	if cur, up := n.sys.Current(e.ep.entity); up && cur.Boot == e.boot {
		e.ep.tell(ev)
	}
}

// resume releases the held frames of e, in order.
func (n *network) resume(e *end) {
	e.paused = false
	held := e.held
	e.held = nil
	for _, ev := range held {
		n.deliver(e, ev)
	}
}

// remove takes c out of the table: no end can use it any more. Frames
// already on their way still land.
func (n *network) remove(c *connection) {
	n.conns = slices.DeleteFunc(n.conns, func(x *connection) bool { return x == c })
}

// drop forgets the frames on their way to end s.
func (c *connection) drop(s int) {
	c.dirs[1-s].flight = nil
	c.end[s].held = nil
}

// close closes c from end s.
func (n *network) close(c *connection, s int, abort bool) {
	n.remove(c)
	c.drop(s)
	if abort {
		c.drop(1 - s)
		c.end[1-s].paused = false // a reset is not held by a pause
	}
	n.push(c, s, n.draw(), core.ConnClosed{Conn: c.end[1-s].id, Abort: abort})
}

// reset aborts c from the network: both ends get ConnClosed{Abort: true}.
func (n *network) reset(c *connection) {
	n.remove(c)
	n.sys.RecordFault("conn_reset",
		actor.Field{Key: "service", Value: actor.StringValue("network")},
		actor.Field{Key: "a", Value: actor.StringValue(c.end[0].ep.name)},
		actor.Field{Key: "b", Value: actor.StringValue(c.end[1].ep.name)},
	)
	n.sys.Env().Reachable(connResetInjected)
	for s := range 2 {
		c.drop(s)
		c.end[s].paused = false
	}
	for s := range 2 {
		n.push(c, 1-s, n.draw(), core.ConnClosed{Conn: c.end[s].id, Abort: true})
	}
}

// ended closes every connection of an incarnation that ended: the other end
// gets ConnClosed{Abort: true} after the frames already sent to it.
func (n *network) ended(who actor.Incarnation, _ actor.End, _ string) {
	for _, c := range slices.Clone(n.conns) {
		for s := range 2 {
			if e := &c.end[s]; e.ep.name != who.Name || e.boot != who.Boot {
				continue
			}
			n.remove(c)
			c.drop(s)
			n.push(c, s, n.draw(), core.ConnClosed{Conn: c.end[1-s].id, Abort: true})
			n.sys.Env().Sometimes(true, crashResetPeer)
		}
	}
}
