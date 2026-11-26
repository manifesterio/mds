package production

import (
	"context"
	"fmt"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ExecConfig bounds each kind of I/O. Zero fields take the defaults.
type ExecConfig struct {
	// ObjTimeout is the base budget of an object-store call (30s). A PUT or GET
	// of a large body gets ObjTransferRate-worth of extra time on top, so a
	// 16 MiB segment on a slow link is not cut off by the base alone.
	ObjTimeout      time.Duration
	ObjTransferRate int64         // assumed minimum bytes/s for the extra time (2 MiB/s)
	CPTimeout       time.Duration // per control-plane call (10s)
}

// objTimeout is the budget of a call that moves n bytes: the base plus n at
// the assumed minimum transfer rate.
func (c ExecConfig) objTimeout(n int64) time.Duration {
	return c.ObjTimeout + time.Duration(n)*time.Second/time.Duration(c.ObjTransferRate)
}

func (c ExecConfig) withDefaults() ExecConfig {
	if c.ObjTimeout <= 0 {
		c.ObjTimeout = 30 * time.Second
	}
	if c.ObjTransferRate <= 0 {
		c.ObjTransferRate = 2 << 20
	}
	if c.CPTimeout <= 0 {
		c.CPTimeout = 10 * time.Second
	}
	return c
}

// Executor carries out the broker's IO: it is the exec function of the
// broker actor on the live Host. Socket IO (Send, CloseConn, PauseReads,
// ResumeReads) goes to the connections; a Send is queued behind earlier
// writes on its connection. Each object store and control plane call runs on
// its own goroutine under a timeout and is answered exactly once with
// live.Answer.
type Executor struct {
	ctx    context.Context
	config ExecConfig
	objs   system.ObjectStore
	cp     system.ControlPlane
	conns  system.Connections
	class  system.ErrorClassifier
}

// NewExecutor builds an Executor; cancelling ctx aborts calls in flight.
func NewExecutor(ctx context.Context, config ExecConfig, objs system.ObjectStore, cp system.ControlPlane,
	conns system.Connections, class system.ErrorClassifier) *Executor {
	return &Executor{ctx: ctx, config: config.withDefaults(), objs: objs, cp: cp, conns: conns, class: class}
}

// Exec is the exec function of live.Spawn. It never blocks: it queues a write,
// changes a connection, or starts a goroutine. An IO case it does not know is
// an error, which ends the Host.
func (e *Executor) Exec(ctx *live.ExecCtx, io core.IO) error {
	switch a := io.(type) {
	case core.Send:
		// A write to a connection that is gone is dropped: its reader reports
		// the close.
		_ = e.conns.Write(a.Conn, a.Data)
	case core.CloseConn:
		_ = e.conns.Close(a.Conn, a.Abort)
	case core.PauseReads:
		e.conns.PauseReads(a.Conn)
	case core.ResumeReads:
		e.conns.ResumeReads(a.Conn)
	case core.Connect:
		// Brokers reach the control plane through ControlPlaneCall, not raw dials.
		go live.Answer(ctx, a, core.ConnectDone{Err: core.ErrInvalid})
	case core.ObjectCall:
		go func() { live.Answer(ctx, a, core.ObjectDone{Result: e.object(a)}) }()
	case core.ControlPlaneCall:
		go func() { live.Answer(ctx, a, e.control(a)) }()
	default:
		return fmt.Errorf("production: unknown IO %T", io)
	}
	return nil
}

func mutateOutcome(err error, fail func(error) (core.ErrCode, core.Outcome)) (core.ErrCode, core.Outcome) {
	if err != nil {
		return fail(err)
	}
	return core.OK, core.Applied
}

// failCode classifies err. The bool is false when err is nil.
func failCode(err error, fail func(error) (core.ErrCode, core.Outcome)) (core.ErrCode, bool) {
	if err == nil {
		return core.OK, false
	}
	c, _ := fail(err)
	return c, true
}

func (e *Executor) object(a core.ObjectCall) objectstore.Result {
	var n int64
	switch c := a.Call.(type) {
	case objectstore.Put:
		n = int64(len(c.Data))
	case objectstore.Get:
		n = c.Ref.Len
	case objectstore.Delete, objectstore.List:
	}
	ctx, cancel := context.WithTimeout(e.ctx, e.config.objTimeout(n))
	defer cancel()
	// The classifier gets the whole action, so it routes by action type.
	fail := func(err error) (core.ErrCode, core.Outcome) { return e.class.Classify(a, err) }
	switch c := a.Call.(type) {
	case objectstore.Put:
		code, outcome := mutateOutcome(e.objs.Put(ctx, c.Key, c.Data), fail)
		return objectstore.PutDone{Key: c.Key, Err: code, Outcome: outcome}
	case objectstore.Get:
		data, err := e.objs.Get(ctx, c.Ref)
		if code, bad := failCode(err, fail); bad {
			return objectstore.GetDone{Err: code}
		}
		return objectstore.GetDone{Data: data}
	case objectstore.Delete:
		code, outcome := mutateOutcome(e.objs.Delete(ctx, c.Key), fail)
		return objectstore.DeleteDone{Err: code, Outcome: outcome}
	case objectstore.List:
		keys, err := e.objs.List(ctx, c.Prefix)
		code, bad := failCode(err, fail)
		if !bad {
			return objectstore.ListDone{Keys: keys}
		}
		return objectstore.ListDone{Err: code}
	}
	//nolint:forbidigo // impossible: Call is a sealed sum and the switch lists every case
	panic("production: unknown object store call")
}

func (e *Executor) control(a core.ControlPlaneCall) core.ControlPlaneDone {
	limit := e.config.CPTimeout
	if a.Timeout > 0 { // group joins and syncs wait for a rebalance (core.ControlPlaneCall)
		limit = time.Duration(a.Timeout)
	}
	ctx, cancel := context.WithTimeout(e.ctx, limit)
	defer cancel()
	response, err := e.cp.Call(ctx, a.Op)
	if err != nil {
		c, o := e.class.Classify(a, err)
		return core.ControlPlaneDone{Err: c, Outcome: o}
	}
	return core.ControlPlaneDone{Resp: response, Outcome: core.Applied}
}
