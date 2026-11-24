package executor_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func TestMemLogRoundTrip(t *testing.T) {
	ctx := t.Context()
	l := executor.NewMemoryLog()
	if _, taken, err := l.Append(ctx, 0, []byte("a")); err != nil || taken {
		t.Fatalf("append0: taken=%v err=%v", taken, err)
	}
	exist, taken, err := l.Append(ctx, 0, []byte("b"))
	if err != nil || !taken || string(exist) != "a" {
		t.Fatalf("taken: %q taken=%v err=%v", exist, taken, err)
	}
	if _, _, err := l.Append(ctx, -1, nil); !errors.Is(err, executor.ErrInvalidSlot) {
		t.Fatalf("neg slot: %v", err)
	}
	ents, err := l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 1 || string(ents[0].Data) != "a" {
		t.Fatalf("read: %+v %v", ents, err)
	}
	if _, err := l.Read(ctx, -1, 1); !errors.Is(err, executor.ErrInvalidSlot) {
		t.Fatalf("bad read: %v", err)
	}
	tail, err := l.CheckTail(ctx)
	if err != nil || tail != 1 {
		t.Fatalf("tail=%d err=%v", tail, err)
	}
	if err := l.TrimThrough(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if err := l.TrimThrough(ctx, -1); !errors.Is(err, executor.ErrInvalidSlot) {
		t.Fatalf("bad trim: %v", err)
	}
	ents, err = l.Read(ctx, 0, 10)
	if err != nil || len(ents) != 0 {
		t.Fatalf("after trim: %+v %v", ents, err)
	}
}

func TestMemObjects(t *testing.T) {
	ctx := t.Context()
	o := executor.NewMemoryObjects()
	if err := o.Put(ctx, "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := o.Get(ctx, runtime.SegmentRef{Key: "k", Off: 1, Len: 3})
	if err != nil || string(got) != "ell" {
		t.Fatalf("get slice: %q %v", got, err)
	}
	got, err = o.Get(ctx, runtime.SegmentRef{Key: "k", Off: 2, Len: 0})
	if err != nil || string(got) != "llo" {
		t.Fatalf("get rest: %q %v", got, err)
	}
	if _, err := o.Get(ctx, runtime.SegmentRef{Key: "missing"}); !errors.Is(err, executor.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := o.Get(ctx, runtime.SegmentRef{Key: "k", Off: 10, Len: 1}); !errors.Is(err, executor.ErrNotFound) {
		t.Fatalf("oob: %v", err)
	}
	keys, err := o.List(ctx, "k")
	if err != nil || len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("list: %v %v", keys, err)
	}
	if err := o.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	keys, err = o.List(ctx, "")
	if err != nil || len(keys) != 0 {
		t.Fatalf("empty list: %v %v", keys, err)
	}
}

func TestClassify(t *testing.T) {
	code, out := executor.Classify(runtime.LogAppend{}, nil)
	if code != runtime.OK || out != runtime.Applied {
		t.Fatalf("nil: %v %v", code, out)
	}
	code, out = executor.Classify(runtime.LogAppend{}, context.Canceled)
	if code != runtime.ErrTimeout || out != runtime.MaybeApplied {
		t.Fatalf("cancel append: %v %v", code, out)
	}
	code, out = executor.Classify(runtime.LogRead{}, context.DeadlineExceeded)
	if code != runtime.ErrTimeout || out != runtime.NotApplied {
		t.Fatalf("deadline read: %v %v", code, out)
	}
	code, out = executor.Classify(runtime.ObjectGet{}, executor.ErrNotFound)
	if code != runtime.ErrNotFound || out != runtime.NotApplied {
		t.Fatalf("not found: %v %v", code, out)
	}
	code, out = executor.Classify(runtime.LogAppend{}, executor.ErrInvalidSlot)
	if code != runtime.ErrInvalid || out != runtime.NotApplied {
		t.Fatalf("invalid: %v %v", code, out)
	}
	code, out = executor.Classify(runtime.ObjectPut{}, errors.New("boom"))
	if code != runtime.ErrIO || out != runtime.MaybeApplied {
		t.Fatalf("io: %v %v", code, out)
	}
	var ne net.Error = timeoutError{}
	code, out = executor.Classify(runtime.LogDeleteOld{}, ne)
	if code != runtime.ErrTimeout || out != runtime.MaybeApplied {
		t.Fatalf("net timeout: %v %v", code, out)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string { return "timeout" }

func (timeoutError) Timeout() bool { return true }

func (timeoutError) Temporary() bool { return true }

func TestExecutorIOResults(t *testing.T) {
	ex := executor.New(t.Context(), executor.Config{LogTimeout: time.Second, ObjTimeout: time.Second}, executor.NewMemoryLog(), executor.NewMemoryObjects())
	for _, io := range []runtime.IO{
		runtime.LogAppend{Slot: 0, Data: []byte("a")},
		runtime.ObjectPut{Key: "k", Data: []byte("v")},
		runtime.LogGetEnd{},
		runtime.LogRead{From: 0, Limit: 1},
		runtime.ObjectGet{Ref: runtime.SegmentRef{Key: "k", Len: 1}},
		runtime.ObjectList{Prefix: "k"},
		runtime.LogDeleteOld{Through: 0},
		runtime.ObjectDelete{Key: "k"},
	} {
		if ex.Do(io) == nil {
			t.Fatalf("%T: no answer", io)
		}
	}
}
