// Package kafka is the Kafka scenario: sans-I/O Kafka clients (at-least-once
// producers, no idempotence, and consumers over kafkawire), an oracle that rebuilds each
// partition's committed log from the control plane and object store truth, the
// invariants over it, and the campaign preset. See docs/architecture.md.
package kafka

//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/session"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/dst/sim"
)

// Opts configures the workload.
type Opts struct {
	Producers, Consumers int
	Topics               []string
	Partitions           int32
	// MaxAttempts caps client produce sends before abandon. Zero means 3.
	MaxAttempts int
	// ReqTimeout is how long a non-fetch request waits before the client drops
	// the connection. Zero means 1500 ms.
	ReqTimeout actor.Duration
}

// DefaultOpts is 3 at-least-once producers and 2 consumers over 2 topics x 3
// partitions.
func DefaultOpts() Opts {
	return Opts{Producers: 3, Consumers: 2, Topics: []string{"t0", "t1"}, Partitions: 3}
}

func (o Opts) maxAttempts() int {
	if o.MaxAttempts > 0 {
		return o.MaxAttempts
	}
	return maxAttempts
}

func (o Opts) requestTimeout() actor.Duration {
	if o.ReqTimeout > 0 {
		return o.ReqTimeout
	}
	return requestTimeout
}

func (o Opts) topicPartitions() []core.TopicPartition {
	var out []core.TopicPartition
	for _, t := range o.Topics {
		for p := int32(0); p < o.Partitions; p++ {
			out = append(out, contract.NewTopicPartition(t, p))
		}
	}
	return out
}

const (
	requestTimeout = 1500 * actor.Millisecond // a request with no answer: drop the connection (Kafka does)
	fetchWait      = 10 * actor.Second        // a fetch reads a batch at a time from S3: it is slow on a long log
	maxAttempts    = 3                        // sends of one produce batch before it is abandoned
	maxFrame       = 8 << 20
)

type stage uint8

const (
	stateAPIVersions stage = iota
	stateMetadata
	stateListOffsets // consumers: ListOffsets earliest
	stateRunning
)

type requestKind uint8

const (
	requestAPIVersions requestKind = iota
	requestMetadata
	requestListOffsets
	requestProduce
	requestFetch
	kindCount
)

type waitingRequests struct {
	kind requestKind
	h    core.RequestHeader
}

// clientMsg is what a client receives.
//
//sumtype:decl
//dst:sum
type clientMsg interface {
	actor.Payload
	isClientMsg()
}

//dst:cases clientMsg
type (
	// clientStart is the first message of the client.
	clientStart struct{}
	// phaseChanged says that the run entered Phase.
	phaseChanged struct{ Phase uint8 }
	// fromSession is a message of the client's session: its timers, its
	// dial answer and its connection events.
	fromSession struct{ Msg session.Msg }
)

// outbox is the session's outbox: everything a client does goes through its
// session's scope.
type outbox = session.Outbox

// client is one Kafka client actor: a producer or a consumer. It has one
// request outstanding at a time, which also gives max.in.flight=1 per
// (producer, partition): a batch is resolved (acked or abandoned) before the
// next one for that partition is built, so the oracle can check that acked
// offsets follow send order.
type client struct {
	env      actor.Env
	id       int
	producer bool
	opts     Opts
	tps      []core.TopicPartition
	history  *History
	codec    kafkawire.Codec

	sess  session.Session
	stage stage
	fr    *kafkawire.Framer
	corr  int32
	ver   [kindCount]int16
	out   *waitingRequests
	p     producer
	c     consumer
}

// newClient builds client id. It records its operations in h. A nil h records
// nothing.
func newClient(env actor.Env, id int, brokers []string, h *History, o Opts) *client {
	c := &client{env: env, id: id, producer: id < o.Producers, opts: o, tps: o.topicPartitions(), history: h,
		sess: session.Session{ID: id, Brokers: brokers}}
	if !c.producer {
		c.c.pos = make([]int64, len(c.tps))
	}
	return c
}

// clientSpec is how the cluster runs client id.
func clientSpec(id int, h *History, o Opts) cluster.ClientSpec[clientMsg] {
	return cluster.ClientSpec[clientMsg]{
		New: func(env actor.Env, brokers []string) cluster.Client[clientMsg] {
			return newClient(env, id, brokers, h, o)
		},
		Boot:    func(actor.Incarnation) clientMsg { return clientStart{} },
		OnPhase: func(p sim.Phase) (clientMsg, bool) { return phaseChanged{Phase: uint8(p)}, true },
		FromNet: func(ev core.EventData) clientMsg { return fromSession{Msg: session.Net{Event: ev}} },
	}
}

// Quiescent reports that no produce batch is unresolved. Consumers poll
// forever and do not count.
func (c *client) Quiescent() bool { return len(c.p.cur) == 0 }

// Receive takes one message. Entering Heal the producers start no new batch
// (in-flight ones finish); the consumers keep reading. Entering Drain each
// consumer stops after a fetch that finds the end of every partition, so no
// read is outstanding at the end.
func (c *client) Receive(_ actor.Now, msg clientMsg, o *actor.Outbox[clientMsg, core.IO]) {
	so := actor.Sub(o, "session", func(m session.Msg) clientMsg { return fromSession{Msg: m} })
	switch m := msg.(type) {
	case clientStart:
		c.sess.Dial(so)
	case phaseChanged:
		switch sim.Phase(m.Phase) {
		case sim.PhaseHeal:
			c.p.noNew = true
		case sim.PhaseDrain:
			c.c.drain = true
		case sim.PhaseFaults:
		}
	case fromSession:
		ev := c.sess.Receive(m.Msg, so)
		switch ev.Kind {
		case session.Ready:
			c.onReady(so)
		case session.Idle:
			c.advance(so)
		case session.Bytes:
			c.onBytes(ev.Data, so)
		case session.Lost:
			c.lost(ev.Why)
		case session.Nothing:
		}
	}
}

func (c *client) onReady(o *outbox) {
	c.fr, c.stage, c.out = kafkawire.NewFramer(maxFrame), stateAPIVersions, nil
	r, _ := kafkawire.Range(kafkawire.KeyApiVersions)
	c.ver[requestAPIVersions] = r.Min + int16(c.env.Intn(int(r.Max-r.Min)+1))
	c.advance(o)
}

// advance sends the request of the client's current stage.
func (c *client) advance(o *outbox) {
	switch c.stage {
	case stateAPIVersions:
		r := kmsg.NewPtrApiVersionsRequest()
		r.ClientSoftwareName, r.ClientSoftwareVersion = "mdssimulator", "1"
		c.send(requestAPIVersions, r, o)
		return
	case stateMetadata:
		r := kmsg.NewPtrMetadataRequest()
		for _, t := range c.opts.Topics {
			r.Topics = append(r.Topics, kmsg.MetadataRequestTopic{Topic: &t})
		}
		c.send(requestMetadata, r, o)
		return
	case stateListOffsets:
		c.send(requestListOffsets, c.listReq(), o)
		return
	case stateRunning:
	}
	if c.producer {
		c.produceNext(o)
		return
	}
	c.fetchNext(o)
}

// send encodes req at the client's version for kind and starts the timeout.
func (c *client) send(kind requestKind, request kmsg.Request, o *outbox) {
	c.corr++
	h := core.RequestHeader{APIKey: request.Key(), APIVersion: c.ver[kind], CorrelationID: c.corr, ClientID: fmt.Sprintf("c%d", c.id)}
	data, err := c.codec.EncodeRequest(h, request)
	if err != nil {
		c.env.Unreachable(clientRequestEncodes, assert.Pair{Key: "err", Value: err.Error()})
		return
	}
	timeout := c.opts.requestTimeout()
	if kind == requestFetch {
		timeout = fetchWait
	}
	c.out = &waitingRequests{kind: kind, h: h}
	c.sess.Request(o, data, timeout)
}

// abort drops the connection as unusable: whatever was outstanding has an
// unknown fate, and the session reconnects.
func (c *client) abort(why string, o *outbox) {
	c.lost(why)
	c.sess.Abort(o)
}

func (c *client) onBytes(data []byte, o *outbox) {
	frames, err := c.fr.Push(data)
	for _, f := range frames {
		if c.sess.State != session.Connected { // lost while handling an earlier frame
			return
		}
		if c.out == nil { // unsolicited reply
			c.abort("bad_stream", o)
			return
		}
		in := c.out
		response, derr := c.codec.DecodeResponse(in.h, f)
		if derr != nil {
			c.abort("bad_response", o)
			return
		}
		c.out = nil
		c.sess.Answered(o)
		c.handle(response, o)
	}
	if err != nil && c.sess.State == session.Connected {
		c.abort("bad_stream", o)
	}
}

func (c *client) handle(response core.Response, o *outbox) {
	switch r := response.(type) {
	case *kmsg.ApiVersionsResponse:
		c.onAPI(r, o)
	case *kmsg.MetadataResponse:
		c.onMeta(r, o)
	case *kmsg.ListOffsetsResponse:
		c.onList(r, o)
	case *kmsg.ProduceResponse:
		c.onProduce(r, o)
	case *kmsg.FetchResponse:
		c.onFetch(r, o)
	default:
		c.abort("bad_response", o)
	}
}

// onAPI picks, per API, a version inside both our supported range and the
// broker's.
func (c *client) onAPI(r *kmsg.ApiVersionsResponse, o *outbox) {
	if r.ErrorCode != 0 {
		c.retryStage(o)
		return
	}
	pick := func(k requestKind, key int16) bool {
		lo, hi := int16(0), int16(-1)
		if s, ok := kafkawire.Range(key); ok {
			lo, hi = s.Min, s.Max
		}
		found := false
		for _, a := range r.ApiKeys {
			if a.ApiKey == key {
				lo, hi, found = max(lo, a.MinVersion), min(hi, a.MaxVersion), true
			}
		}
		if !found || lo > hi {
			return false
		}
		c.ver[k] = lo + int16(c.env.Intn(int(hi-lo)+1))
		return true
	}
	if !pick(requestMetadata, kafkawire.KeyMetadata) || !pick(requestListOffsets, kafkawire.KeyListOffsets) ||
		!pick(requestProduce, kafkawire.KeyProduce) || !pick(requestFetch, kafkawire.KeyFetch) {
		c.retryStage(o)
		return
	}
	c.stage = stateMetadata
	c.advance(o)
}

// onMeta checks that every topic exists with the expected partition count.
func (c *client) onMeta(r *kmsg.MetadataResponse, o *outbox) {
	ok := len(r.Topics) == len(c.opts.Topics)
	for _, t := range r.Topics {
		ok = ok && t.ErrorCode == 0 && int32(len(t.Partitions)) == c.opts.Partitions
	}
	if !ok {
		c.retryStage(o)
		return
	}
	switch {
	case !c.producer && !c.c.listed:
		c.stage = stateListOffsets
	default:
		c.stage = stateRunning
	}
	c.advance(o)
}

// retryStage repeats the current stage's request after a backoff (a refused
// but harmless answer: the connection stays).
func (c *client) retryStage(o *outbox) {
	c.sess.Fails++
	c.sess.Wake(o, session.RetryWaitTime(c.sess.Fails))
}

// lost settles whatever was outstanding, whose fate is unknown.
func (c *client) lost(why string) {
	if c.out != nil {
		switch c.out.kind {
		case requestProduce:
			for _, b := range c.p.cur {
				b.ambiguous = true
			}
		case requestFetch:
			c.abandonFetch(why)
		case requestAPIVersions, requestMetadata, requestListOffsets, kindCount:
		}
	}
	c.out, c.fr, c.stage = nil, nil, stateAPIVersions
}

// StateHash folds all state.
func (c *client) StateHash() uint64 {
	h := trace.New()
	h.Uint64(uint64(c.id))
	c.sess.Hash(&h)
	h.Uint64(uint64(c.stage))
	h.Uint64(uint64(c.corr))
	for _, v := range c.ver {
		h.Uint64(uint64(v))
	}
	if c.fr != nil {
		h.Uint64(uint64(c.fr.Buffered()))
	}
	h.Bool(c.out != nil)
	if c.out != nil {
		h.Uint64(uint64(c.out.kind))
		h.Uint64(uint64(c.out.h.CorrelationID))
	}
	c.p.hash(&h)
	c.c.hash(&h)
	return uint64(h)
}

var _ cluster.Client[clientMsg] = (*client)(nil)
