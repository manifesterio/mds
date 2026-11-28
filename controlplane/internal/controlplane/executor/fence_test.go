package executor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor/logtest"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func TestClassifyLogActions(t *testing.T) {
	cases := []struct {
		name string
		a    runtime.IO
		err  error
		code runtime.ErrCode
		out  runtime.Outcome
	}{
		{"claim timeout", runtime.LogClaimEpoch{}, context.DeadlineExceeded, runtime.ErrTimeout, runtime.MaybeApplied},
		{"claim io", runtime.LogClaimEpoch{}, errors.New("boom"), runtime.ErrIO, runtime.MaybeApplied},
		{"read timeout", runtime.LogRead{}, context.DeadlineExceeded, runtime.ErrTimeout, runtime.NotApplied},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, out := executor.Classify(c.a, c.err)
			if code != c.code || out != c.out {
				t.Fatalf("got %v %v, want %v %v", code, out, c.code, c.out)
			}
		})
	}
}

// slowClaimLog is a Log whose ClaimEpoch waits until the call context ends.
type slowClaimLog struct {
	executor.Log
}

func (slowClaimLog) ClaimEpoch(ctx context.Context, _ uint64, _ string) (uint64, string, bool, error) {
	<-ctx.Done()
	return 0, "", false, ctx.Err()
}

func TestExecutorAppendBelowFloorIsConflict(t *testing.T) {
	ctx := t.Context()
	l := executor.NewMemoryLog()
	for slot := range 2 {
		if _, _, err := l.Append(ctx, runtime.LogPosition(slot), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.TrimThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second}, l, executor.NewMemoryObjects())

	got, ok := ex.Do(runtime.LogAppend{Slot: 0, Data: []byte("late")}).(runtime.LogAppendDone)
	if !ok || !got.Taken || got.Existing != nil || got.Err != runtime.ErrConflict || got.Outcome != runtime.NotApplied {
		t.Fatalf("below floor: got %+v, want taken, no existing, ErrConflict, NotApplied", got)
	}
}

func TestExecutorAppendLiveTakenKeepsExisting(t *testing.T) {
	ctx := t.Context()
	l := executor.NewMemoryLog()
	if _, _, err := l.Append(ctx, 4, []byte("live")); err != nil {
		t.Fatal(err)
	}
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second}, l, executor.NewMemoryObjects())

	got, ok := ex.Do(runtime.LogAppend{Slot: 4, Data: []byte("other")}).(runtime.LogAppendDone)
	if !ok || !got.Taken || string(got.Existing) != "live" || got.Err != runtime.OK || got.Outcome != runtime.Applied {
		t.Fatalf("live taken: %+v", got)
	}
}

func TestExecutorClaimEpoch(t *testing.T) {
	ctx := t.Context()
	ex := executor.New(ctx, executor.Config{LogTimeout: time.Second}, executor.NewMemoryLog(), executor.NewMemoryObjects())

	got := ex.Do(runtime.LogClaimEpoch{Expect: 0, Owner: "cp-0"})
	want := runtime.LogClaimEpochDone{Epoch: 1, Owner: "cp-0", Won: true, Err: runtime.OK, Outcome: runtime.Applied}
	if got != want {
		t.Fatalf("claim: got %+v, want %+v", got, want)
	}

	got = ex.Do(runtime.LogClaimEpoch{Expect: 0, Owner: "cp-1"})
	want = runtime.LogClaimEpochDone{Epoch: 1, Owner: "cp-0", Won: false, Err: runtime.OK, Outcome: runtime.Applied}
	if got != want {
		t.Fatalf("lost claim: got %+v, want %+v", got, want)
	}
}

func TestExecutorClaimEpochTimeoutIsMaybeApplied(t *testing.T) {
	ctx := t.Context()
	log := slowClaimLog{Log: executor.NewMemoryLog()}
	ex := executor.New(ctx, executor.Config{LogTimeout: 20 * time.Millisecond}, log, executor.NewMemoryObjects())

	got := ex.Do(runtime.LogClaimEpoch{Expect: 0, Owner: "cp-0"})
	want := runtime.LogClaimEpochDone{Err: runtime.ErrTimeout, Outcome: runtime.MaybeApplied}
	if got != want {
		t.Fatalf("timed-out claim: got %+v, want %+v", got, want)
	}
}

func TestMemoryLogConformance(t *testing.T) {
	logtest.Run(t, func(*testing.T) executor.Log { return executor.NewMemoryLog() })
}
