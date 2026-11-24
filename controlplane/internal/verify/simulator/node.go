package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/core/actor"
	dstsim "github.com/manifesterio/mds/dst/sim"
)

// simNode is one replica of the simulation. It lives for the whole run. It is
// the actor "cp-<idx>" of the System, built from replica.Props as production
// builds it: each boot gives it a new Replica and a new boot number, so
// nothing of the old boot reaches the new one. Its IO goes to the storage
// executor (services.go). It can crash, pause and restart.
type simNode struct {
	s   *sim
	idx int
	ref actor.EntityRef[runtime.EventData]
	m   *replica.Replica // the replica of the last boot (see replica)

	paused bool // the host paused the node (the System holds its inbox)
	cut    bool // cut off from the log and the object store

	boots int

	// groups hosts the rebalance actor of the node and its group calls
	// (groups.go). It is nil in a run with no consumer groups.
	groups *groupHost
}

func nodeName(idx int) string { return fmt.Sprintf("cp-%d", idx) }

// tripRestartMin and tripRestartMax bound the time a replica that tripped
// stays down before its next boot.
const (
	tripRestartMin = 100 * actor.Millisecond
	tripRestartMax = 500 * actor.Millisecond
)

// newNode spawns the node in the System of s. The node is down until boot.
func newNode(s *sim, idx int, cfg replica.Config) *simNode {
	n := &simNode{s: s, idx: idx}
	p := replica.Props(cfg, n.started)
	p.Name = nodeName(idx)
	p.Restart = restartAfterTrip
	n.ref = dstsim.Spawn(s.w, p,
		dstsim.WithExecutor(s.store.exec),
		dstsim.AfterReceive(n.afterReceive),
		dstsim.OnEnd(n.ended))
	return n
}

// restartAfterTrip boots a replica that tripped (its memory is not trusted)
// again after a random time. A replica that crashed or exited stays down
// until the host boots it.
func restartAfterTrip(how actor.End, r actor.Rand) (actor.Duration, bool) {
	if how != actor.Trip {
		return 0, false
	}
	return r.Duration(tripRestartMin, tripRestartMax), true
}

func (n *simNode) name() string { return nodeName(n.idx) }

// up reports that the node has an incarnation that runs.
func (n *simNode) up() bool { return n.s.w.Up(n.ref) }

// replica is the replica, or nil while the node is down.
func (n *simNode) replica() *replica.Replica {
	if !n.up() {
		return nil
	}
	return n.m
}

// started runs at each boot with the new Replica. The rebalance actor boots
// with it.
func (n *simNode) started(m *replica.Replica) {
	n.boots++
	n.paused = false
	n.m = m
	n.s.noteBoot(n)
	if n.groups != nil {
		n.s.w.Boot(n.groups.ref)
	}
}

// boot starts a new incarnation of the node (and so of its rebalance actor),
// unless the node is up.
func (n *simNode) boot() {
	if !n.up() {
		n.s.w.Boot(n.ref)
	}
}

// ended runs when an incarnation of the replica ends: a crash from outside,
// a trip (the replica asked for a restart) or an exit. The rebalance actor
// goes down with it.
func (n *simNode) ended(_ actor.Incarnation, how actor.End, cause string) {
	switch how {
	case actor.Trip:
		n.s.stats.Trips++
		n.down("stop for restart: " + cause)
	case actor.Exit:
		n.down("shutdown")
	case actor.Crash:
		n.down(cause)
	}
}

// down notes that the incarnation is gone and crashes the rebalance actor.
// Messages that are on their way to the old incarnation are dropped by the
// System when they arrive.
func (n *simNode) down(why string) {
	n.s.stats.Crashes++
	n.s.tracef("%s crashes (%s)", n.name(), why)
	n.paused = false
	if n.groups != nil {
		n.groups.stop()
		n.s.w.Crash(n.groups.ref, "the replica stopped")
	}
}

// crash kills the node (the nemesis) and starts it again after d.
func (n *simNode) crash(why string, d actor.Duration) {
	if !n.up() {
		return
	}
	n.s.w.Crash(n.ref, why)
	n.s.w.After(d, n.boot)
}

// pause stops the node from taking messages. They wait in its inbox and run
// in order when it resumes.
func (n *simNode) pause() {
	if n.up() && !n.paused {
		n.s.stats.Pauses++
		n.paused = true
		n.s.w.Pause(n.ref)
		if n.groups != nil {
			n.s.w.Pause(n.groups.ref)
		}
	}
}

func (n *simNode) resume() {
	if !n.paused {
		return
	}
	n.paused = false
	n.s.w.Resume(n.ref)
	if n.groups != nil {
		n.s.w.Resume(n.groups.ref)
	}
}

// afterReceive runs after each Receive of the replica and its effects. It
// checks the properties of one node, and gives the group host the effects
// (the role it tells the rebalance actor, the answers to its writes).
func (n *simNode) afterReceive(_ actor.Incarnation, _ runtime.EventData, effects []actor.Effect) {
	n.s.checkNode(n)
	if n.groups != nil {
		n.groups.afterReplica(effects)
	}
}
