package executor

import (
	"context"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// Config bounds each kind of I/O. Zero fields take the defaults.
type Config struct {
	LogTimeout time.Duration
	ObjTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.LogTimeout <= 0 {
		c.LogTimeout = 10 * time.Second
	}
	if c.ObjTimeout <= 0 {
		c.ObjTimeout = 30 * time.Second
	}
	return c
}

// Executor runs the replica's IO on the log and the object store. Every
// call gets exactly one answer: an error becomes an answer with an ErrCode and
// an Outcome, never silence.
type Executor struct {
	ctx    context.Context
	config Config
	log    Log
	objs   ObjectStore
}

// New makes an Executor. A nil object store is an in-memory one.
func New(ctx context.Context, config Config, log Log, objs ObjectStore) *Executor {
	if objs == nil {
		objs = NewMemoryObjects()
	}
	return &Executor{ctx: ctx, config: config.withDefaults(), log: log, objs: objs}
}

// Do runs io and returns its answer. It blocks; tests and tools use it.
func (e *Executor) Do(io runtime.IO) actor.Payload {
	switch r := io.(type) {
	case runtime.LogAppend:
		return e.LogAppend(r)
	case runtime.LogRead:
		return e.LogRead(r)
	case runtime.LogGetEnd:
		return e.LogGetEnd(r)
	case runtime.LogDeleteOld:
		return e.LogDeleteOld(r)
	case runtime.LogClaimEpoch:
		return e.LogClaimEpoch(r)
	case runtime.ObjectPut:
		return e.ObjPut(r)
	case runtime.ObjectGet:
		return e.ObjGet(r)
	case runtime.ObjectDelete:
		return e.ObjDelete(r)
	case runtime.ObjectList:
		return e.ObjList(r)
	}
	return nil
}

func (e *Executor) logCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(e.ctx, e.config.LogTimeout)
}

func (e *Executor) objCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(e.ctx, e.config.ObjTimeout)
}

// LogAppend puts a.Data at a.Slot if the slot is free.
func (e *Executor) LogAppend(a runtime.LogAppend) runtime.LogAppendDone {
	ctx, cancel := e.logCtx()
	defer cancel()
	existing, taken, err := e.log.Append(ctx, a.Slot, a.Data)
	switch {
	case err != nil:
		c, o := Classify(a, err)
		return runtime.LogAppendDone{Slot: a.Slot, Err: c, Outcome: o}
	case taken && len(existing) == 0: // at or below the floor: not a commit
		return runtime.LogAppendDone{Slot: a.Slot, Taken: true, Err: runtime.ErrConflict, Outcome: runtime.NotApplied}
	case taken:
		return runtime.LogAppendDone{Slot: a.Slot, Taken: true, Existing: existing, Err: runtime.OK, Outcome: runtime.Applied}
	}
	return runtime.LogAppendDone{Slot: a.Slot, Err: runtime.OK, Outcome: runtime.Applied}
}

// LogRead reads entries at or after a.From.
func (e *Executor) LogRead(a runtime.LogRead) runtime.LogReadDone {
	ctx, cancel := e.logCtx()
	defer cancel()
	ents, err := e.log.Read(ctx, a.From, int(a.Limit))
	if err != nil {
		c, o := Classify(a, err)
		return runtime.LogReadDone{Err: c, Outcome: o}
	}
	return runtime.LogReadDone{Entries: ents, Err: runtime.OK, Outcome: runtime.Applied}
}

// LogGetEnd reads the log tail.
func (e *Executor) LogGetEnd(a runtime.LogGetEnd) runtime.LogGetEndDone {
	ctx, cancel := e.logCtx()
	defer cancel()
	tail, err := e.log.CheckTail(ctx)
	if err != nil {
		c, o := Classify(a, err)
		return runtime.LogGetEndDone{Err: c, Outcome: o}
	}
	return runtime.LogGetEndDone{Tail: tail, Err: runtime.OK, Outcome: runtime.Applied}
}

// LogDeleteOld removes the slots at or below a.Through.
func (e *Executor) LogDeleteOld(a runtime.LogDeleteOld) runtime.LogDeleteOldDone {
	ctx, cancel := e.logCtx()
	defer cancel()
	if err := e.log.TrimThrough(ctx, a.Through); err != nil {
		c, o := Classify(a, err)
		return runtime.LogDeleteOldDone{Err: c, Outcome: o}
	}
	return runtime.LogDeleteOldDone{Err: runtime.OK, Outcome: runtime.Applied}
}

// LogClaimEpoch tries to make a.Owner the one writer.
func (e *Executor) LogClaimEpoch(a runtime.LogClaimEpoch) runtime.LogClaimEpochDone {
	ctx, cancel := e.logCtx()
	defer cancel()
	epoch, owner, won, err := e.log.ClaimEpoch(ctx, a.Expect, a.Owner)
	if err != nil {
		c, o := Classify(a, err)
		return runtime.LogClaimEpochDone{Err: c, Outcome: o}
	}
	return runtime.LogClaimEpochDone{Epoch: epoch, Owner: owner, Won: won, Err: runtime.OK, Outcome: runtime.Applied}
}

// ObjPut uploads a.Data under a.Key.
func (e *Executor) ObjPut(a runtime.ObjectPut) runtime.ObjectPutDone {
	ctx, cancel := e.objCtx()
	defer cancel()
	c, o := Classify(a, e.objs.Put(ctx, a.Key, a.Data))
	return runtime.ObjectPutDone{Key: a.Key, Err: c, Outcome: o}
}

// ObjGet reads the byte range a.Ref.
func (e *Executor) ObjGet(a runtime.ObjectGet) runtime.ObjectGetDone { //nolint:dupl // the same error shape as objList, another answer type
	ctx, cancel := e.objCtx()
	defer cancel()
	data, err := e.objs.Get(ctx, a.Ref)
	if err != nil {
		c, _ := Classify(a, err)
		return runtime.ObjectGetDone{Err: c}
	}
	return runtime.ObjectGetDone{Data: data}
}

// ObjDelete removes the object a.Key.
func (e *Executor) ObjDelete(a runtime.ObjectDelete) runtime.ObjectDeleteDone {
	ctx, cancel := e.objCtx()
	defer cancel()
	c, o := Classify(a, e.objs.Delete(ctx, a.Key))
	return runtime.ObjectDeleteDone{Err: c, Outcome: o}
}

// ObjList lists the keys under a.Prefix.
func (e *Executor) ObjList(a runtime.ObjectList) runtime.ObjectListDone { //nolint:dupl // the same error shape as objGet, another answer type
	ctx, cancel := e.objCtx()
	defer cancel()
	keys, err := e.objs.List(ctx, a.Prefix)
	if err != nil {
		c, _ := Classify(a, err)
		return runtime.ObjectListDone{Err: c}
	}
	return runtime.ObjectListDone{Keys: keys}
}
