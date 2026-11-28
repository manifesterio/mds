package replica_test

// A property test of crash recovery, after Bornholt et al., "Using Lightweight
// Formal Methods to Validate a Key-Value Storage Node in Amazon S3" (SOSP 2021),
// section 5. Rapid draws a list of topic and commit operations mixed with
// crashes, lost checkpoints and storage failures. Each operation goes to the
// real replica (through server.Node, on the in-memory log and object store) and
// to spec.ControlPlane, and the answers must be equal. After every crash:
//
//   - persistence: the whole state (topics and every partition's runs) equals
//     the model, so no answered write is lost and no unanswered one appears;
//   - idempotent retry: every commit sent before the crash, sent again, is a
//     replay with the same offsets.
//
// The test sticks to topics and segment commits, the data a broker cannot
// rebuild. Group state is checked by the domain unit tests and the simulator.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/dst/actor"
)

// Kinds of injected storage failure.
const (
	recFailAppend     = iota // an append fails and nothing is stored
	recFailAppendLate        // an append is stored, then fails: the answer is lost
	recFailRead              // a log read fails
	recFailPut               // a checkpoint upload fails
	recFailKinds
	recNever = -1 // a call that is never made to fail
)

const recCheckpointPrefix = "checkpoint/"

var errRecInjected = errors.New("injected failure")

// recProcess is what one run of the node sees of the disk (the shared log and
// objects). A crash kills it: from then on its calls fail and change nothing,
// so nothing it had in flight lands later.
type recProcess struct {
	log   *executor.MemoryLog
	objs  *executor.MemoryObjects
	dead  atomic.Bool
	mu    sync.Mutex
	armed [recFailKinds]int
}

var (
	_ executor.Log         = (*recProcess)(nil)
	_ executor.ObjectStore = (*recProcess)(nil)
)

func (p *recProcess) fail(kind int) error {
	if p.dead.Load() {
		return errRecInjected
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind != recNever && p.armed[kind] > 0 {
		p.armed[kind]--
		return errRecInjected
	}
	return nil
}

func (p *recProcess) Append(ctx context.Context, slot runtime.LogPosition, data []byte) ([]byte, bool, error) {
	if err := p.fail(recFailAppend); err != nil {
		return nil, false, err
	}
	existing, taken, err := p.log.Append(ctx, slot, data)
	if err == nil && p.fail(recFailAppendLate) != nil {
		return nil, false, errRecInjected
	}
	return existing, taken, err
}

func (p *recProcess) Read(ctx context.Context, from runtime.LogPosition, limit int) ([]runtime.LogEntry, error) {
	if err := p.fail(recFailRead); err != nil {
		return nil, err
	}
	return p.log.Read(ctx, from, limit)
}

func (p *recProcess) CheckTail(ctx context.Context) (runtime.LogPosition, error) {
	if err := p.fail(recFailRead); err != nil {
		return 0, err
	}
	return p.log.CheckTail(ctx)
}

func (p *recProcess) TrimThrough(ctx context.Context, through runtime.LogPosition) error {
	if err := p.fail(recNever); err != nil {
		return err
	}
	return p.log.TrimThrough(ctx, through)
}

func (p *recProcess) ClaimEpoch(ctx context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	if err := p.fail(recNever); err != nil {
		return 0, "", false, err
	}
	return p.log.ClaimEpoch(ctx, expect, owner)
}

func (p *recProcess) Put(ctx context.Context, key string, data []byte) error {
	if err := p.fail(recFailPut); err != nil {
		return err
	}
	return p.objs.Put(ctx, key, data)
}

func (p *recProcess) Get(ctx context.Context, ref runtime.SegmentRef) ([]byte, error) {
	if err := p.fail(recNever); err != nil {
		return nil, err
	}
	return p.objs.Get(ctx, ref)
}

func (p *recProcess) Delete(ctx context.Context, key string) error {
	if err := p.fail(recNever); err != nil {
		return err
	}
	return p.objs.Delete(ctx, key)
}

func (p *recProcess) List(ctx context.Context, prefix string) ([]string, error) {
	if err := p.fail(recNever); err != nil {
		return nil, err
	}
	return p.objs.List(ctx, prefix)
}

// recWorld is one check: a node restarted on the same disk, and the model.
type recWorld struct {
	t       *rapid.T
	cfg     replica.Config
	log     *executor.MemoryLog
	objs    *executor.MemoryObjects
	proc    *recProcess
	node    *server.Node
	model   *spec.ControlPlane
	commits []contract.CommitSegment // every fresh commit, to send again
	failed  bool                     // a failure was injected
	step    int
	phase   string
}

func newRecWorld(t *rapid.T, trim bool) *recWorld {
	w := &recWorld{t: t, log: executor.NewMemoryLog(), objs: executor.NewMemoryObjects(), model: spec.New(), cfg: replica.Config{
		Rank: 0, Writer: "cp-0", CatchUpEvery: actor.Duration(time.Millisecond), Heartbeat: actor.Duration(time.Second),
		TakeoverAfter:   actor.Duration(20 * time.Millisecond),
		CheckpointEvery: actor.Duration(3 * time.Millisecond), CheckpointMinSlots: 1, CheckpointPrefix: recCheckpointPrefix,
	}}
	if trim {
		w.cfg.KeepSlots = 2 // the log is cut behind each checkpoint
	}
	w.start()
	t.Cleanup(func() {
		if w.node != nil {
			_ = w.node.Close()
		}
	})
	return w
}

func (w *recWorld) start() {
	w.proc = &recProcess{log: w.log, objs: w.objs}
	w.node = server.New(server.Config{Machine: w.cfg, Log: w.proc, Objects: w.proc,
		StopGrace: actor.Duration(50 * time.Millisecond)})
	ctx, cancel := context.WithTimeout(w.t.Context(), 10*time.Second)
	defer cancel()
	if err := w.node.Start(ctx); err != nil {
		w.node = nil // Start already stopped it
		w.t.Fatalf("the node did not start: %v", err)
	}
}

func (w *recWorld) checkpoints() []string {
	keys, _ := w.objs.List(w.t.Context(), recCheckpointPrefix)
	slices.Sort(keys)
	return keys
}

// crash kills the process and drops checkpoints: keep 0 drops none, 1 the
// newest (its upload had not finished), 2 all. The log is never touched: an
// answered write is in it. A trimmed log cannot rebuild the state alone, so
// then every checkpoint stays.
func (w *recWorld) crash(keep int) {
	w.proc.dead.Store(true)
	_ = w.node.Close()
	if w.cfg.KeepSlots > 0 {
		keep = 0
	}
	keys := w.checkpoints()
	switch keep {
	case 0:
		keys = nil
	case 1:
		keys = keys[max(0, len(keys)-1):]
	}
	for _, k := range keys {
		_ = w.objs.Delete(w.t.Context(), k)
	}
	w.start()
}

// waitCheckpoint waits (a little) until a checkpoint covers the whole log.
func (w *recWorld) waitCheckpoint() {
	tail, _ := w.log.CheckTail(w.t.Context())
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if keys := w.checkpoints(); len(keys) > 0 {
			slot, err := strconv.ParseInt(strings.TrimPrefix(keys[len(keys)-1], recCheckpointPrefix), 10, 64)
			if err == nil && slot >= int64(tail) {
				return
			}
		}
	}
}

func (w *recWorld) inject(kind int) {
	w.proc.mu.Lock()
	defer w.proc.mu.Unlock()
	if kind < 0 {
		w.proc.armed = [recFailKinds]int{}
		return
	}
	w.proc.armed[kind]++
	w.failed = true
}

func recTransient(d runtime.ControlPlaneDone, err error) bool {
	return err != nil || d.Err == runtime.ErrUnavailable || d.Err == runtime.ErrTimeout || d.Err == runtime.ErrIO
}

// ask sends op until it gets a real answer. ErrUnavailable (not the writer
// yet) is always retried. After an injected failure an error answer is
// allowed: the failures stop and op is sent again. A wrong answer never is.
func (w *recWorld) ask(op runtime.ControlPlaneOperation) contract.Done {
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(w.t.Context(), 3*time.Second)
		d, err := w.node.Exec(ctx, op)
		cancel()
		retry := (err == nil && d.Err == runtime.ErrUnavailable) || (w.failed && recTransient(d, err))
		if !retry || time.Now().After(deadline) {
			if err != nil {
				w.t.Fatalf("step %d: %T%+v: no answer: %v", w.step, op, op, err)
			}
			return d.Answer()
		}
		if w.failed {
			w.inject(-1)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// do sends op to the model and the node and requires the same answer.
func (w *recWorld) do(op runtime.ControlPlaneOperation) {
	want := w.model.Apply(op)
	got := w.ask(op)
	if got.Err != want.Err || fmt.Sprintf("%+v", got.Resp) != fmt.Sprintf("%+v", want.Resp) {
		w.t.Fatalf("step %d%s: %T%+v\n  model: err=%v resp=%+v\n  node:  err=%v resp=%+v",
			w.step, w.phase, op, op, want.Err, want.Resp, got.Err, got.Resp)
	}
}

var (
	recTopics = []string{"a", "b"}
	recKeys   = []string{"k0", "k1", "k2", "k3"}
)

// afterCrash checks persistence and idempotent retry.
func (w *recWorld) afterCrash() {
	w.phase = " (after a crash)"
	defer func() { w.phase = "" }()
	w.do(contract.DescribeTopics{})
	for _, topic := range recTopics {
		for p := range int32(3) {
			w.do(contract.FindSegments{TopicPartition: contract.NewTopicPartition(topic, p)})
		}
	}
	for _, c := range w.commits {
		w.do(c)
	}
}

// drawCommit draws a fresh commit, or 1 time in 2 resends an earlier one. Keys
// come from a small set so that replays and key conflicts happen.
func (w *recWorld) drawCommit() contract.CommitSegment {
	if len(w.commits) > 0 && rapid.Bool().Draw(w.t, "resend") {
		return w.commits[rapid.IntRange(0, len(w.commits)-1).Draw(w.t, "which")]
	}
	var known []contract.TopicPartition
	for _, tp := range w.model.Apply(contract.DescribeTopics{}).Resp.(contract.DescribeTopicsResp).Topics {
		for p := range tp.Partitions {
			known = append(known, contract.NewTopicPartition(tp.Name, p))
		}
	}
	batch := rapid.Custom(func(t *rapid.T) contract.PendingBatch {
		tp := contract.NewTopicPartition(rapid.SampledFrom(recTopics).Draw(t, "topic"), int32(rapid.IntRange(0, 2).Draw(t, "partition")))
		if len(known) > 0 && rapid.IntRange(0, 3).Draw(t, "existing") < 3 {
			tp = rapid.SampledFrom(known).Draw(t, "existing partition")
		}
		return contract.PendingBatch{TopicPartition: tp, Records: int64(rapid.IntRange(1, 4).Draw(t, "records")),
			Loc: runtime.SegmentRef{Key: "seg", Off: int64(rapid.IntRange(0, 50).Draw(t, "off")), Len: int64(rapid.IntRange(1, 12).Draw(t, "len"))}}
	})
	c := contract.CommitSegment{SegmentKey: rapid.SampledFrom(recKeys).Draw(w.t, "key"), Batches: rapid.SliceOfN(batch, 1, 3).Draw(w.t, "batches")}
	w.commits = append(w.commits, c)
	return c
}

// TestCrashRecovery runs 30 checks of 20 steps by default (it waits on real
// takeovers and checkpoints); -rapid.checks and -rapid.steps raise it.
func TestCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("waits on real takeovers and checkpoints")
	}
	recFewerChecks(t, 30, 20)
	for _, trim := range []bool{false, true} {
		t.Run(fmt.Sprintf("trim=%t", trim), func(t *testing.T) {
			t.Parallel()
			rapid.Check(t, func(rt *rapid.T) {
				w := newRecWorld(rt, trim)
				// Simple operations first: the shrinker moves the index toward 0.
				ops := []func(){
					func() {
						w.do(contract.FindSegments{TopicPartition: contract.NewTopicPartition(
							rapid.SampledFrom(recTopics).Draw(rt, "topic"), int32(rapid.IntRange(0, 2).Draw(rt, "partition")))})
					},
					func() {
						w.do(contract.CreateTopic{Name: rapid.SampledFrom(recTopics).Draw(rt, "name"),
							Partitions: int32(rapid.IntRange(1, 3).Draw(rt, "partitions"))})
					},
					func() { w.do(w.drawCommit()) },
					func() { w.waitCheckpoint() },
					func() { w.inject(rapid.IntRange(0, recFailKinds-1).Draw(rt, "failure")) },
					func() {
						w.crash(rapid.IntRange(0, 2).Draw(rt, "checkpoints dropped"))
						w.afterCrash()
					},
				}
				rt.Repeat(map[string]func(*rapid.T){"step": func(*rapid.T) {
					ops[rapid.IntRange(0, len(ops)-1).Draw(rt, "op")]()
					w.step++
				}})
				// Every check ends with a crash, so each one tests recovery.
				w.crash(0)
				w.afterCrash()
			})
		})
	}
}

// recFewerChecks lowers rapid's defaults (100 checks, 30 steps) unless the
// flags were set on the command line.
func recFewerChecks(t *testing.T, checks, steps int) {
	t.Helper()
	asked := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { asked[f.Name] = true })
	for name, value := range map[string]int{"rapid.checks": checks, "rapid.steps": steps} {
		if asked[name] {
			continue
		}
		old := flag.Lookup(name).Value.String()
		if err := flag.Set(name, strconv.Itoa(value)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = flag.Set(name, old) })
	}
}
