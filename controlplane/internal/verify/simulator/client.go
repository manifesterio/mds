package simulator

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/verify/check"
	"github.com/manifesterio/mds/dst/core/actor"
	dstsim "github.com/manifesterio/mds/dst/sim"
)

const (
	clientTimeout = 2 * actor.Second
	maxAttempts   = 8
	finalAttempts = 40
)

// opState is the operation a client is working on right now.
type opState struct {
	op       runtime.ControlPlaneOperation
	write    bool
	attempts int // the attempts so far; the number of the current one
	node     int // node of the current attempt
	snap     *snapshot
	final    bool   // a read of the final check: one node, retried there
	then     func() // what the client does when the operation is over
}

// client sends one operation at a time to a random node, and retries it as a
// new ask when the node says unavailable or does not answer. The same
// operation is sent every time, so a commit keeps its segment key.
type client struct {
	pool *clientPool
	cur  *opState
	idle bool
}

// clientPool is the set of clients of a run.
type clientPool struct {
	s   *sim
	w   *workload
	all []*client
}

func newClientPool(s *sim, count int) *clientPool {
	p := &clientPool{s: s, w: &workload{rand: s.rand}}
	for range count {
		p.add()
	}
	return p
}

func (p *clientPool) add() *client {
	c := &client{pool: p, idle: true}
	p.all = append(p.all, c)
	return c
}

func (p *clientPool) start() {
	for _, c := range p.all {
		d := actor.Duration(p.s.rand.Between(int64(20*actor.Millisecond), int64(200*actor.Millisecond)))
		p.s.after(d, c.issue)
	}
}

// busy reports that a client still works on an operation.
func (p *clientPool) busy() bool {
	for _, c := range p.all {
		if !c.idle {
			return true
		}
	}
	return false
}

// startFinal makes one client per node. Each reads all the state from its node.
func (p *clientPool) startFinal() {
	for _, n := range p.s.nodes {
		c := p.add()
		reads := finalReads(p.s.consumers != nil)
		var next func()
		next = func() {
			if len(reads) == 0 {
				c.idle = true
				return
			}
			op := reads[0]
			reads = reads[1:]
			c.begin(&opState{op: op, final: true, node: n.idx, then: next})
		}
		next()
	}
}

func (p *clientPool) pickNode(last int) int {
	n := len(p.s.nodes)
	if last < 0 || n == 1 {
		return p.s.rand.Intn(n)
	}
	return (last + 1 + p.s.rand.Intn(n-1)) % n
}

// issue starts the next random operation, unless the quiet tail has started.
func (c *client) issue() {
	s := c.pool.s
	if s.quiet {
		c.idle = true
		return
	}
	nowMs := int64(s.mono()) / int64(actor.Millisecond)
	op, write := c.pool.w.next(nowMs)
	c.begin(&opState{op: op, write: write, node: -1, then: func() {
		think := actor.Duration(s.rand.Between(int64(10*actor.Millisecond), int64(120*actor.Millisecond)))
		s.after(think, c.issue)
	}})
}

func (c *client) begin(op *opState) {
	c.idle = false
	c.cur = op
	c.pool.s.stats.Ops++
	c.attempt()
}

// attempt asks a node to run the operation. The ask reaches the node after a
// network delay, and the answer comes back to the client after another. The
// client gives up on the attempt clientTimeout after it started. prefix is how
// many log slots the node had applied when it answered.
func (c *client) attempt() {
	p, op := c.pool, c.cur
	op.attempts++
	try := op.attempts
	if !op.final {
		op.node = p.pickNode(op.node)
	}
	op.snap = p.s.oracle.snapshotFor(op.op)
	s, node := p.s, p.s.nodes[op.node]
	current := func() bool { return c.cur == op && op.attempts == try }
	there := s.store.netDelay(s.rand)
	var prefix runtime.LogPosition
	dstsim.AskDelayed(s.w, node.ref, there, func(to actor.ReplyTo[runtime.ControlPlaneDone]) runtime.EventData {
		return runtime.ControlPlaneDeliver{ReplyTo: to, Op: op.op}
	}, clientTimeout-there, func(d runtime.ControlPlaneDone) actor.Duration {
		prefix = node.m.Applied()
		return s.store.netDelay(s.rand)
	}, func(d runtime.ControlPlaneDone, ok bool) {
		if !ok {
			return // the timeout below answers the client
		}
		if current() {
			c.answered(d, prefix)
		}
	})
	s.after(clientTimeout, func() {
		if current() {
			c.answered(runtime.ControlPlaneDone{Err: runtime.ErrTimeout, Outcome: runtime.MaybeApplied}, 0)
		}
	})
}

func retriable(code runtime.ErrCode) bool {
	switch code {
	case runtime.ErrUnavailable, runtime.ErrTimeout, runtime.ErrIO, runtime.ErrConnClosed:
		return true
	}
	return false
}

func (c *client) answered(d runtime.ControlPlaneDone, prefix runtime.LogPosition) {
	s, op := c.pool.s, c.cur
	if !retriable(d.Err) {
		s.oracle.record(op, d, prefix)
		c.finish()
		return
	}
	limit := maxAttempts
	if op.final {
		limit = finalAttempts
	}
	if op.attempts >= limit {
		s.stats.GaveUp++
		if op.final {
			check.Violation(s.env, finalReadsAnswered, "node cp-%d did not answer %T in the quiet tail (last error %v)", op.node, op.op, d.Err)
		}
		c.finish()
		return
	}
	s.stats.Retries++
	backoff := actor.Duration(op.attempts) * 60 * actor.Millisecond
	s.after(backoff, func() {
		if c.cur == op {
			c.attempt()
		}
	})
}

func (c *client) finish() {
	op := c.cur
	c.cur = nil
	op.then()
}
