package broker

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/actor/actortest"
	"github.com/manifesterio/mds/shared/objectstore"
)

// testT is the part of testing.TB that the harness uses. The conformance test
// runs the harness inside a rapid run, where the test handle is a *rapid.T and
// not a *testing.T.
type testT = actortest.TB

// client is the test's view of one connection.
type client struct {
	id   core.ConnID
	want []kafkawire.RequestHeader
	out  [][]byte // response frames (length prefix stripped) not yet decoded
	fr   *kafkawire.Framer
	corr int32
}

// testControlPlane is the control plane spec, called the way the executor calls it.
type testControlPlane struct{ *spec.ControlPlane }

// exec answers one control plane call from the spec.
func (c testControlPlane) exec(op contract.Operation) core.ControlPlaneDone {
	done := c.Apply(op)
	return core.ControlPlaneDone{Resp: done.Resp, Err: done.Err, Outcome: done.Outcome}
}

// CreateTopic makes a topic (or grows it) and returns the error code.
func (c testControlPlane) CreateTopic(name string, partitions int32) core.ErrCode {
	return c.Apply(contract.CreateTopic{Name: name, Partitions: partitions}).Err
}

// harness runs a Broker one Receive at a time, on a fresh Outbox each time, and
// acts as its executor and runtime: it answers every Perform from the specs, keeps
// the armed timers, and plays the client side of the sockets. An answer is computed
// when the Perform is recorded, adapted with the Perform's adapter, and delivered
// through a queue, so tests can hold it back (holdCP, holdPut) or fault it (cpHook,
// objHook).
type harness struct {
	t      testT
	b      *Broker
	env    *actortest.Env
	book   *actortest.Book
	store  *objectstore.Spec
	cp     testControlPlane
	config Config
	now    actor.Mono

	// cpHook/objHook may replace the answer of a call; exec runs the spec.
	cpHook  func(a core.ControlPlaneCall, exec func() core.ControlPlaneDone) core.ControlPlaneDone
	objHook func(a core.ObjectCall, exec func() core.ObjectDone) core.ObjectDone
	holdCP  bool
	holdPut bool
	held    []core.EventData

	number  uint64                          // last pending number handed out
	queue   []core.EventData                // adapted answers not yet received
	timers  map[actor.TimerKey]actor.Effect // mirror of book.Timers for tests
	due     map[actor.TimerKey]actor.Mono   // when each armed timer is due (fireNext)
	clients map[core.ConnID]*client
	all     []actor.Effect // every effect recorded, in order
	paused  map[core.ConnID]bool
	closed  map[core.ConnID]bool
	exited  bool
	cpCalls []contract.Operation
	puts    []string
}

func newHarness(t testT, config Config) *harness {
	env := actortest.NewEnv(actortest.Config{Self: testSelf, Seed: 7})
	h := &harness{t: t, env: env, book: &actortest.Book{Timers: map[actor.TimerKey]actortest.Record{}},
		store: objectstore.NewSpec(), cp: testControlPlane{spec.New()},
		config: config, timers: map[actor.TimerKey]actor.Effect{}, due: map[actor.TimerKey]actor.Mono{},
		clients: map[core.ConnID]*client{},
		paused:  map[core.ConnID]bool{}, closed: map[core.ConnID]bool{}}
	h.cp.CreateTopic("t", 2)
	h.b = New(config)(h.env)
	return h
}

// self is the incarnation the harness boots.
var testSelf = actor.Incarnation{Name: "broker", Boot: actor.ID{1}}

// startedHarness boots the broker.
func startedHarness(t *testing.T, config Config) *harness {
	h := newHarness(t, config)
	h.do(core.NodeStarted{Self: testSelf})
	return h
}

// step gives msg to the broker one millisecond after the last message, carries
// out the recorded effects and returns them.
func (h *harness) step(msg core.EventData) []actor.Effect {
	h.now += actor.Mono(actor.Millisecond)
	effects := actortest.Run(h.t, &h.number, actor.Binding{}, func(o *actor.Outbox[core.EventData, core.IO]) {
		h.b.Receive(actor.Now{Mono: h.now, Wall: actor.Wall(h.now)}, msg, o)
	})
	h.all = append(h.all, effects...)
	actortest.Drain(h.t, effects, h.book, actortest.Handler{
		Strict: true,
		AfterStart: func(e actor.Effect) {
			h.due[e.StoredKey()] = h.now + actor.Mono(e.D)
		},
		Perform: h.perform,
		Do:      func(e actor.Effect) { h.do1(e.Msg) },
		Stop:    func(actor.Effect) { h.exited = true },
	})
	h.syncTimers()
	return effects
}

func (h *harness) syncTimers() {
	clear(h.timers)
	for k, r := range h.book.Timers {
		h.timers[k] = r.Effect
	}
}

// do steps msg and then delivers queued answers until none is left.
func (h *harness) do(msg core.EventData) []actor.Effect {
	effects := h.step(msg)
	h.run()
	return effects
}

func (h *harness) run() {
	for len(h.queue) > 0 {
		c := h.queue[0]
		h.queue = h.queue[1:]
		h.step(c)
	}
}

// deliver queues the answer of the Perform e, adapted to the broker's message,
// or holds it back.
func (h *harness) deliver(e actor.Effect, answer actor.Payload) {
	_, isCP := answer.(core.ControlPlaneDone)
	isPut := false
	if d, ok := answer.(core.ObjectDone); ok {
		_, isPut = d.Result.(objectstore.PutDone)
	}
	msg := actortest.Adapt[core.EventData](h.t, e, answer)
	if (isCP && h.holdCP) || (isPut && h.holdPut) {
		h.held = append(h.held, msg)
		return
	}
	h.queue = append(h.queue, msg)
}

// releaseHeld delivers held answers and runs to quiescence.
func (h *harness) releaseHeld() {
	held := h.held
	h.held = nil
	h.queue = append(h.queue, held...)
	h.run()
}

// perform answers one Perform from the specs, through the hooks.
func (h *harness) perform(e actor.Effect) {
	switch a := e.Msg.(type) {
	case core.ObjectCall:
		if put, ok := a.Call.(objectstore.Put); ok {
			h.puts = append(h.puts, put.Key)
		}
		run := func() core.ObjectDone { return core.ObjectDone{Result: h.store.Exec(a.Call)} }
		if h.objHook != nil {
			h.deliver(e, h.objHook(a, run))
		} else {
			h.deliver(e, run())
		}
	case core.ControlPlaneCall:
		h.cpCalls = append(h.cpCalls, a.Op)
		run := func() core.ControlPlaneDone { return h.cp.exec(a.Op) }
		if h.cpHook != nil {
			h.deliver(e, h.cpHook(a, run))
		} else {
			h.deliver(e, run())
		}
	default:
		h.t.Fatalf("unexpected Perform of %T", e.Msg)
	}
}

// do1 carries out one socket IO.
func (h *harness) do1(io actor.Payload) {
	switch a := io.(type) {
	case core.Send:
		c := h.clients[a.Conn]
		if c == nil {
			h.t.Fatalf("send to unknown conn %d", a.Conn)
		}
		frames, err := c.fr.Push(a.Data)
		if err != nil {
			h.t.Fatal(err)
		}
		c.out = append(c.out, frames...)
	case core.CloseConn:
		h.closed[a.Conn] = true
	case core.PauseReads:
		h.paused[a.Conn] = true
	case core.ResumeReads:
		h.paused[a.Conn] = false
	default:
		h.t.Fatalf("unexpected Do of %T", io)
	}
}

// performs returns the IO values of the Performs among effects, in order.
func performs(effects []actor.Effect) []actor.Payload {
	var out []actor.Payload
	for _, e := range effects {
		if e.Kind == actor.EffectPerform {
			out = append(out, e.Msg)
		}
	}
	return out
}

// getDone returns the GetDone inside an answer, if the answer is an object store GetDone.
func getDone(d core.ObjectDone) (objectstore.GetDone, bool) {
	g, isGet := d.Result.(objectstore.GetDone)
	return g, isGet
}

// timerOrder sorts timer keys the way the broker made them: the flush timer
// first, then the segment and operation timers by their id.
func timerOrder(key actor.TimerKey) uint64 {
	_, id, ok := strings.Cut(string(key), "/")
	if !ok {
		return 0
	}
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		panic("timer key " + string(key))
	}
	return n
}

// fire delivers the timer under key.
func (h *harness) fire(key actor.TimerKey) {
	e, ok := h.book.TakeTimer(key)
	if !ok {
		h.t.Fatalf("timer %q not armed", key)
	}
	delete(h.timers, key)
	msg, isMsg := e.Msg.(core.EventData)
	if !isMsg {
		h.t.Fatalf("timer %q carries %T", key, e.Msg)
	}
	h.do(msg)
}

// armedKeys returns the armed timer keys in timerOrder.
func (h *harness) armedKeys() []actor.TimerKey {
	keys := slices.Collect(maps.Keys(h.timers))
	slices.SortFunc(keys, func(x, y actor.TimerKey) int { return cmp.Compare(timerOrder(x), timerOrder(y)) })
	return keys
}

// fireAll fires every armed timer, flush first, then by id, until none is armed.
func (h *harness) fireAll() {
	for n := 0; len(h.timers) > 0; n++ {
		if n > 1000 {
			h.t.Fatal("timers never settle")
		}
		h.fire(h.armedKeys()[0])
	}
}

// fireNext moves the clock to the earliest armed timer and fires it. Ties go
// in timerOrder, so the order is the same on every run.
func (h *harness) fireNext() {
	keys := h.armedKeys()
	if len(keys) == 0 {
		h.t.Fatal("no timer armed")
	}
	first := keys[0]
	for _, k := range keys[1:] {
		if h.due[k] < h.due[first] {
			first = k
		}
	}
	h.now = max(h.now, h.due[first])
	h.fire(first)
}

func (h *harness) flush() { h.fire(timerFlush) }

// ---- client side ----

func (h *harness) open(id core.ConnID) *client {
	c := &client{id: id, fr: kafkawire.NewFramer(1 << 26)}
	h.clients[id] = c
	h.do(core.ConnOpened{Conn: id, Peer: "test"})
	return c
}

// frame encodes a request without sending it.
func (h *harness) frame(c *client, ver int16, request kmsg.Request) []byte {
	c.corr++
	hd := kafkawire.RequestHeader{APIKey: request.Key(), APIVersion: ver, CorrelationID: c.corr, ClientID: "test"}
	b, err := kafkawire.Codec{}.EncodeRequest(hd, request)
	if err != nil {
		h.t.Fatal(err)
	}
	c.want = append(c.want, hd)
	return b
}

// send writes requests to the broker in one chunk and runs to quiescence.
func (h *harness) send(c *client, ver int16, request kmsg.Request) {
	h.do(core.BytesReceived{Conn: c.id, Data: h.frame(c, ver, request)})
}

// next decodes the oldest undelivered response on c.
func (h *harness) next(c *client) kmsg.Response {
	h.t.Helper()
	if len(c.out) == 0 {
		h.t.Fatalf("conn %d: no response", c.id)
	}
	hd := c.want[0]
	response, err := kafkawire.Codec{}.DecodeResponse(hd, c.out[0])
	if err != nil {
		h.t.Fatalf("conn %d: decode %d: %v", c.id, hd.APIKey, err)
	}
	c.want, c.out = c.want[1:], c.out[1:]
	return response.(kmsg.Response)
}

func (h *harness) call(c *client, ver int16, request kmsg.Request) kmsg.Response {
	h.t.Helper()
	h.send(c, ver, request)
	return h.next(c)
}

// ---- request builders ----

const (
	verProduce  = 7
	verFetch    = 11
	verList     = 5
	verMetadata = 9
)

var params = kafkawire.BatchParams{FirstTimestamp: 1000, MaxTimestamp: 1000}

// mkBatch builds a batch of len(vals) records; pid -1 means non-idempotent.
func mkBatch(t testT, pid int64, epoch int16, seq int32, vals ...string) []byte {
	t.Helper()
	var recs []kmsg.Record
	for _, v := range vals {
		recs = append(recs, kmsg.Record{Value: []byte(v)})
	}
	p := params
	p.ProducerID, p.ProducerEpoch, p.BaseSequence = pid, epoch, seq
	b, err := kafkawire.BuildBatch(p, recs)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func plain(t testT, vals ...string) []byte { return mkBatch(t, -1, -1, -1, vals...) }

type ent struct {
	topicPartition core.TopicPartition
	rec            []byte
}

func e(topic string, part int32, rec []byte) ent {
	return ent{topicPartition: contract.NewTopicPartition(topic, part), rec: rec}
}

func prodReq(acks int16, es ...ent) *kmsg.ProduceRequest {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = acks, 1000
	for _, e := range es {
		r.Topics = append(r.Topics, kmsg.ProduceRequestTopic{Topic: e.topicPartition.Topic,
			Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: e.topicPartition.Partition, Records: e.rec}}})
	}
	return r
}

// produced flattens a produce response's partitions in request order.
func produced(r kmsg.Response) []kmsg.ProduceResponseTopicPartition {
	var out []kmsg.ProduceResponseTopicPartition
	for _, t := range r.(*kmsg.ProduceResponse).Topics {
		out = append(out, t.Partitions...)
	}
	return out
}

func fetchReq(topicPartition core.TopicPartition, off int64, max int32) *kmsg.FetchRequest {
	r := kmsg.NewPtrFetchRequest()
	r.MaxBytes, r.MinBytes, r.MaxWaitMillis = 1<<20, 1, 100
	r.Topics = []kmsg.FetchRequestTopic{{Topic: topicPartition.Topic, Partitions: []kmsg.FetchRequestTopicPartition{
		{Partition: topicPartition.Partition, FetchOffset: off, PartitionMaxBytes: max, CurrentLeaderEpoch: -1}}}}
	return r
}

func fetched(r kmsg.Response) kmsg.FetchResponseTopicPartition {
	return r.(*kmsg.FetchResponse).Topics[0].Partitions[0]
}

// produceOne produces one entry, flushes and returns its partition result.
func (h *harness) produceOne(c *client, e ent) kmsg.ProduceResponseTopicPartition {
	h.t.Helper()
	h.send(c, verProduce, prodReq(-1, e))
	if _, ok := h.timers[timerFlush]; ok {
		h.flush()
	}
	return produced(h.next(c))[0]
}

func factsNamed(all []actor.Effect, name string) []actor.Fact {
	var out []actor.Fact
	for _, e := range all {
		if e.Kind == actor.EffectFact && e.Fact.Name == name {
			out = append(out, e.Fact)
		}
	}
	return out
}

func (h *harness) noViolations() {
	h.t.Helper()
	if len(h.env.Viol) > 0 {
		h.t.Fatalf("assertion violations: %v", h.env.Viol)
	}
}
