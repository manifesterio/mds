package server

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/runtime/live"
)

// runIO is the replica's exec function for live.Spawn: it starts io on a
// goroutine and answers the actor when it is done.
func runIO(e *executor.Executor, ctx *live.ExecCtx, io runtime.IO) error {
	go answerIO(e, ctx, io)
	return nil
}

func answerIO(e *executor.Executor, ctx *live.ExecCtx, io runtime.IO) {
	switch r := io.(type) {
	case runtime.LogAppend:
		live.Answer(ctx, r, e.LogAppend(r))
	case runtime.LogRead:
		live.Answer(ctx, r, e.LogRead(r))
	case runtime.LogGetEnd:
		live.Answer(ctx, r, e.LogGetEnd(r))
	case runtime.LogDeleteOld:
		live.Answer(ctx, r, e.LogDeleteOld(r))
	case runtime.LogClaimEpoch:
		live.Answer(ctx, r, e.LogClaimEpoch(r))
	case runtime.ObjectPut:
		live.Answer(ctx, r, e.ObjPut(r))
	case runtime.ObjectGet:
		live.Answer(ctx, r, e.ObjGet(r))
	case runtime.ObjectDelete:
		live.Answer(ctx, r, e.ObjDelete(r))
	case runtime.ObjectList:
		live.Answer(ctx, r, e.ObjList(r))
	}
}
