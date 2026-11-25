package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/dst/verify/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/session"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/assert"
	"github.com/manifesterio/mds/dst/core/trace"
)

// batch is one produce batch for one partition, retried unchanged until
// resolved. Producers are not idempotent. A retry after an ambiguous answer may
// commit the batch twice. idem marks the rare batch that deliberately claims a
// producer id. The broker must reject it with INVALID_REQUEST, per partition,
// and never commit it.
type batch struct {
	id        actor.ID
	tpIdx     int
	vals      []string
	data      []byte
	attempts  int
	ambiguous bool // some attempt ended without a definite answer
	lastCode  int16
	dig       uint64 // hash of data, so StateHash does not rescan it
	idem      bool   // carries producer id duplicateCheckProducerID: must be rejected
	corrupt   bool   // payload damaged after the CRC was computed: must be rejected
}

// producer is the at-least-once producer state of one client. cur holds the
// unresolved batches of the current request (distinct partitions).
type producer struct {
	serial int // batches built by this client (values stay unique)
	cur    []*batch
	noNew  bool
}

// duplicateCheckProducerID is the producer id of a deliberately idempotent batch.
const duplicateCheckProducerID = 7

// codeInvalidRequest is INVALID_REQUEST, the broker's answer to a batch that
// carries a producer id. codeCorruptMessage is CORRUPT_MESSAGE, its answer to a
// batch whose CRC does not match.
const (
	codeInvalidRequest int16 = 42
	codeCorruptMessage int16 = 2
)

// expect is the code the broker must answer a batch that is never admitted
// (0: the batch is good).
func (b *batch) expect() int16 {
	switch {
	case b.idem:
		return codeInvalidRequest
	case b.corrupt:
		return codeCorruptMessage
	}
	return 0
}

func (p *producer) hash(h *trace.Hash) {
	h.Uint64(uint64(p.serial))
	h.Bool(p.noNew)
	for _, b := range p.cur {
		h.Bytes(b.id[:])
		h.Uint64(uint64(b.tpIdx))
		h.Uint64(uint64(b.attempts))
		h.Bool(b.ambiguous)
		h.Bool(b.idem)
		h.Bool(b.corrupt)
		h.Uint64(uint64(b.lastCode))
		h.Uint64(b.dig)
	}
}

// Kafka codes the producer reacts to.
const (
	codeTimedOut               int16 = 7
	codeStorage                int16 = 56
	codeUnknown                int16 = -1
	codeCoordinatorUnavailable int16 = 15
	codeUnknownTopic           int16 = 3
	codeLeaderUnavailable      int16 = 5
	codeNotLeader              int16 = 6
)

// unclearResultCode: the broker may still have committed the batch (it gave up
// waiting). clearlyRetryable: the batch was not committed; try again.
func unclearResultCode(c int16) bool {
	return c == codeTimedOut || c == codeStorage || c == codeUnknown || c == codeCoordinatorUnavailable
}
func clearlyRetryable(c int16) bool {
	return c == codeUnknownTopic || c == codeLeaderUnavailable || c == codeNotLeader
}

func (c *client) produceNext(o *outbox) {
	p := &c.p
	if len(p.cur) == 0 {
		if p.noNew {
			return
		}
		c.newBatches()
	}
	for _, b := range p.cur {
		if b.attempts >= c.opts.maxAttempts() {
			c.giveUp("retries exhausted", o)
			return
		}
	}
	c.sendProduce(o)
}

// duplicateCheckEvery: one batch in duplicateCheckEvery claims a producer id and must be rejected.
// corruptEvery: one batch in corruptEvery is damaged in flight (CRC mismatch).
const (
	duplicateCheckEvery = 40
	corruptEvery        = 40
)

// pickCodec draws a codec id: none for a third of the batches, otherwise gzip,
// snappy, lz4 or zstd with equal weight.
func (c *client) pickCodec() int16 {
	if n := c.env.Intn(6); n >= 2 {
		return int16(n - 1)
	}
	return kafkawire.CodecNone
}

// newBatches builds the next request: 1 or 2 batches for distinct partitions.
func (c *client) newBatches() {
	p := &c.p
	k := 1 + c.env.Intn(2)
	first := c.env.Intn(len(c.tps))
	for j := range k {
		idx := first
		if j == 1 {
			idx = (first + 1 + c.env.Intn(len(c.tps)-1)) % len(c.tps)
		}
		n := 1 + c.env.Intn(5)
		pad := 0
		switch c.env.Intn(16) {
		case 0:
			pad = 100 + c.env.Intn(300)
			if c.env.Intn(2) == 0 { // a batch of several KiB: larger than a small segment cap
				pad = 2000 + c.env.Intn(2000)
			}
		default:
			pad = c.env.Intn(24)
		}
		p.serial++
		b := &batch{id: c.env.NewID(), tpIdx: idx}
		recs := make([]kmsg.Record, n)
		for i := range recs {
			v := fmt.Sprintf("c%d.b%d.r%d|%s", c.id, p.serial, i, strings.Repeat("x", pad))
			b.vals = append(b.vals, v)
			recs[i] = kmsg.Record{Value: []byte(v)}
		}
		b.idem = c.env.Intn(duplicateCheckEvery) == 0
		pp := kafkawire.BatchParams{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1, FirstTimestamp: 1, MaxTimestamp: 1}
		if b.idem {
			pp.ProducerID, pp.ProducerEpoch, pp.BaseSequence = duplicateCheckProducerID, 0, 0
		}
		data, err := buildBatch(pp, recs, c.pickCodec())
		if err != nil {
			c.env.Unreachable(clientBatchBuilds, assert.Pair{Key: "err", Value: err.Error()})
			continue
		}
		b.corrupt = !b.idem && c.env.Intn(corruptEvery) == 0
		if b.corrupt {
			data[len(data)-1] ^= 0x40
		}
		b.data = data
		dh := trace.New()
		dh.Bytes(data)
		b.dig = uint64(dh)
		if c.history != nil {
			c.history.Invoke(c.env.Self(), b.id, b.id, ProduceOp{Client: c.id, Batch: p.serial, TopicPartition: c.tps[idx], Vals: b.vals, Idem: b.idem})
		}
		p.cur = append(p.cur, b)
	}
}

func (c *client) sendProduce(o *outbox) {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = -1, 1000
	for _, b := range c.p.cur {
		b.attempts++
		topicPartition := c.tps[b.tpIdx]
		part := kmsg.ProduceRequestTopicPartition{Partition: topicPartition.Partition, Records: b.data}
		added := false
		for i := range r.Topics {
			if r.Topics[i].Topic == topicPartition.Topic {
				r.Topics[i].Partitions = append(r.Topics[i].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.ProduceRequestTopic{Topic: topicPartition.Topic, Partitions: []kmsg.ProduceRequestTopicPartition{part}})
		}
	}
	c.send(requestProduce, r, o)
}

func (c *client) think(o *outbox) {
	c.sess.Wake(o, actor.Duration(20+c.env.Intn(60))*actor.Millisecond)
}

// onProduce resolves each batch of the request from its partition's answer.
func (c *client) onProduce(r *kmsg.ProduceResponse, o *outbox) {
	p := &c.p
	var keep []*batch
	fatal := false
	for _, b := range p.cur {
		topicPartition := c.tps[b.tpIdx]
		code, base, found := codeUnknown, int64(-1), false
		for _, t := range r.Topics {
			for _, pr := range t.Partitions {
				if t.Topic == topicPartition.Topic && pr.Partition == topicPartition.Partition {
					code, base, found = pr.ErrorCode, pr.BaseOffset, true
				}
			}
		}
		b.lastCode = code
		switch {
		case !found:
			b.ambiguous = true
			keep = append(keep, b)
		case b.expect() != 0 && code == b.expect():
			// Rejected at admission, however many earlier attempts were lost.
			c.resolve(b, history.GivenUp, ProduceRes{Base: -1, Code: code, Why: "rejected", Attempts: b.attempts})
		case b.idem && code == 0:
			c.env.Unreachable(batchWithProducerIDAccepted)
			c.resolve(b, history.ResultKnown, ProduceRes{Base: base, Attempts: b.attempts})
		case b.corrupt && code == 0:
			c.env.Unreachable(batchWithBadCRCAccepted)
			c.resolve(b, history.ResultKnown, ProduceRes{Base: base, Attempts: b.attempts})
		case code == 0:
			c.resolve(b, history.ResultKnown, ProduceRes{Base: base, Attempts: b.attempts})
		case unclearResultCode(code):
			b.ambiguous = true
			keep = append(keep, b)
		case clearlyRetryable(code):
			keep = append(keep, b)
		default: // corrupt, invalid, ...: not retriable
			fatal = true
			keep = append(keep, b)
		}
	}
	p.cur = keep
	switch {
	case fatal:
		c.giveUp("fatal produce error", o)
	case len(keep) > 0:
		c.sess.Wake(o, session.RetryWaitTime(keep[0].attempts))
	default:
		c.sess.Fails = 0
		c.think(o)
	}
}

func (c *client) resolve(b *batch, out history.Outcome, res ProduceRes) {
	if c.history != nil {
		c.history.Return(b.id, res, out)
	}
}

// giveUp abandons every unresolved batch (Indeterminate if any attempt was
// ambiguous: it may still land at any later time).
func (c *client) giveUp(why string, o *outbox) {
	p := &c.p
	for _, b := range p.cur {
		out := history.GivenUp
		if b.ambiguous {
			out = history.ResultUnknown
		}
		c.resolve(b, out, ProduceRes{Base: -1, Code: b.lastCode, Why: why, Attempts: b.attempts})
	}
	p.cur = nil
	c.think(o)
}
