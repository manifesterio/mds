package production

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specsystem"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
	"github.com/manifesterio/mds/shared/objectstore"
)

// sealer is a test actor: at boot it seals two segments the way the broker
// does, a segment_sealed fact and then the PUT of that segment, for k1 and
// then k2. Then it keeps recording facts, so Observe runs on the actor
// goroutine while the PUTs are on the pool.
type sealer struct{ left int }

func (s *sealer) Receive(_ actor.Now, msg core.EventData, o *actor.Outbox[core.EventData, core.IO]) {
	if _, ok := msg.(core.NodeStarted); ok {
		for _, k := range []struct {
			key, reason string
			bytes       int64
		}{{"k1", "size", 1000}, {"k2", "timer", 10}} {
			o.Fact(actor.Fact{Name: "segment_sealed", Fields: []actor.Field{
				{Key: "segment", Value: actor.StringValue(k.key)},
				{Key: "reason", Value: actor.StringValue(k.reason)}, {Key: "bytes", Value: actor.Int64Value(k.bytes)}}})
			actor.Perform(o, core.ObjectCall{Call: objectstore.Put{Key: k.key}},
				func(core.ObjectDone) core.EventData { return core.FlushDue{} })
		}
	}
	if s.left > 0 {
		s.left--
		o.Fact(actor.Fact{Name: "segment_sealed", Fields: []actor.Field{
			{Key: "segment", Value: actor.StringValue("busy-" + strconv.Itoa(s.left))},
			{Key: "reason", Value: actor.StringValue("busy")}}})
		o.TellSelf(core.FlushDue{})
	}
}

func (*sealer) StateHash() uint64 { return 0 }

// runSealer runs a sealer on a live Host wired like cmd/mdsbroker: facts go to
// SegmentLog.Observe, IO through the Executor. hold, if set,
// runs on the worker before Exec.
func runSealer(t *testing.T, sl *SegmentLog, busy int, hold func(core.IO)) {
	t.Helper()
	e := NewExecutor(t.Context(), ExecConfig{}, specsystem.NewObjectStore(), nil, &fakeConns{}, Classifier{Objects: MemoryErrorClassifier{}})
	host := live.NewHost(live.HostConfig{
		Clock: live.RealClock(),
		Facts: func(_ actor.Incarnation, f actor.Fact) { sl.Observe(f) },
	})
	live.Spawn(host, actor.Props[core.EventData, core.IO]{
		Name: "sealer",
		New:  func(actor.Env) actor.Actor[core.EventData, core.IO] { return &sealer{left: busy} },
		Boot: func(self actor.Incarnation) core.EventData { return core.NodeStarted{Self: self} },
	}, func(c *live.ExecCtx, io core.IO) error {
		if hold != nil {
			hold(io)
		}
		return e.Exec(c, io)
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { _ = host.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// The sealed line of a segment carries that segment's fact. PUTs need not run
// in record order on the live pool: here the PUT of k1 is held until k2's has
// been logged (or holdFirst passed). SegmentLog pairs by the fact's segment
// key, so the order does not matter.
func TestSegmentLogPairsSealOnPool(t *testing.T) {
	var buf syncBuffer
	sl := NewSegmentLog(slog.New(slog.NewJSONHandler(&buf, nil)))
	hold := func(io core.IO) {
		if c, ok := io.(core.ObjectCall); ok {
			if p, ok := c.Call.(objectstore.Put); ok && p.Key == "k1" {
				waitFor(func() bool { return strings.Contains(buf.String(), `"segment":"k2"`) }, holdFirst)
			}
		}
	}
	runSealer(t, sl, 0, hold)
	waitFor(func() bool { return strings.Count(buf.String(), "segment_sealed") == 2 }, 2*time.Second)
	var b bytes.Buffer
	b.WriteString(buf.String())
	reason := map[any]any{}
	for _, l := range lines(t, &b) {
		if l["event"] == "segment_sealed" {
			reason[l["segment"]] = l["reason"]
		}
	}
	if reason["k1"] != "size" || reason["k2"] != "timer" {
		t.Fatalf("sealed lines paired wrong: k1 reason %v (want size), k2 reason %v (want timer)", reason["k1"], reason["k2"])
	}
}

// Observe runs on the actor goroutine (the Facts hook) while the PUTs run on
// the pool, and SegmentLog has no lock. Run with -race: SegmentLog must not be
// touched from the pool.
func TestSegmentLogRaceOnPool(t *testing.T) {
	var buf syncBuffer
	sl := NewSegmentLog(slog.New(slog.NewJSONHandler(&buf, nil)))
	runSealer(t, sl, 20000, nil)
	waitFor(func() bool { return strings.Contains(buf.String(), `"segment":"k2"`) }, 2*time.Second)
	time.Sleep(200 * time.Millisecond) // let the busy facts run alongside
}

func waitFor(cond func() bool, d time.Duration) {
	deadline := time.Now().Add(d)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

// syncBuffer is a bytes.Buffer safe for the log handler and the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
