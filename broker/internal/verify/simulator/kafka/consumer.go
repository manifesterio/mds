package kafka

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/trace"
	"github.com/manifesterio/mds/dst/verify/history"
)

// consumer reads every partition from offset 0 with one multi-partition Fetch
// at a time. pos is the next offset wanted per partition.
type consumer struct {
	pos       []int64
	listed    bool
	operation actor.ID // outstanding ReadOp, zero when none
	drain     bool     // PhaseDrain began: stop once a fetch finds the end of every partition
	done      bool
}

func (c *consumer) hash(h *trace.Hash) {
	for _, p := range c.pos {
		h.Int64(p)
	}
	h.Bool(c.listed)
	h.Bytes(c.operation[:])
	h.Bool(c.drain)
	h.Bool(c.done)
}

func (c *client) listReq() *kmsg.ListOffsetsRequest {
	r := kmsg.NewPtrListOffsetsRequest()
	r.ReplicaID = -1
	for _, topicPartition := range c.tps {
		part := kmsg.ListOffsetsRequestTopicPartition{Partition: topicPartition.Partition, CurrentLeaderEpoch: -1, Timestamp: -2}
		added := false
		for i := range r.Topics {
			if r.Topics[i].Topic == topicPartition.Topic {
				r.Topics[i].Partitions = append(r.Topics[i].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.ListOffsetsRequestTopic{Topic: topicPartition.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{part}})
		}
	}
	return r
}

// onList starts each partition at its earliest offset.
func (c *client) onList(r *kmsg.ListOffsetsResponse, o *outbox) {
	n := 0
	pos := make([]int64, len(c.tps))
	for i, topicPartition := range c.tps {
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				if t.Topic == topicPartition.Topic && p.Partition == topicPartition.Partition && p.ErrorCode == 0 && p.Offset >= 0 {
					pos[i] = p.Offset
					n++
				}
			}
		}
	}
	if n != len(c.tps) {
		c.retryStage(o)
		return
	}
	c.c.pos, c.c.listed, c.stage, c.sess.Fails = pos, true, stateRunning, 0
	c.advance(o)
}

func (c *client) fetchNext(o *outbox) {
	cs := &c.c
	if cs.done {
		return
	}
	r := kmsg.NewPtrFetchRequest()
	r.MaxWaitMillis, r.MinBytes, r.MaxBytes, r.SessionEpoch = 100, 1, 1<<20, -1
	operation := ReadOp{Client: c.id}
	perPart := int32(1024) << c.env.Intn(6)
	for i, topicPartition := range c.tps {
		operation.Parts = append(operation.Parts, ReadFrom{TopicPartition: topicPartition, Off: cs.pos[i]})
		part := kmsg.FetchRequestTopicPartition{Partition: topicPartition.Partition, CurrentLeaderEpoch: -1, FetchOffset: cs.pos[i],
			LastFetchedEpoch: -1, LogStartOffset: -1, PartitionMaxBytes: perPart}
		added := false
		for j := range r.Topics {
			if r.Topics[j].Topic == topicPartition.Topic {
				r.Topics[j].Partitions = append(r.Topics[j].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.FetchRequestTopic{Topic: topicPartition.Topic, Partitions: []kmsg.FetchRequestTopicPartition{part}})
		}
	}
	cs.operation = c.env.NewID()
	if c.history != nil {
		c.history.Invoke(c.env.Self(), cs.operation, cs.operation, operation)
	}
	c.send(requestFetch, r, o)
}

// onFetch parses the answer of each partition (CRC-verified, whole batches,
// records below the fetch offset dropped) and advances the positions.
func (c *client) onFetch(r *kmsg.FetchResponse, o *outbox) {
	cs := &c.c
	res := ReadRes{Parts: make([]ReadPart, len(c.tps))}
	got, atEnd := false, true
	for i, topicPartition := range c.tps {
		part := &res.Parts[i]
		part.Code = codeUnknown
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				if t.Topic == topicPartition.Topic && p.Partition == topicPartition.Partition {
					*part = readPart(p, cs.pos[i])
				}
			}
		}
		if n := len(part.Recs); n > 0 {
			cs.pos[i] = part.Recs[n-1].Off + 1
			got = true
		}
		atEnd = atEnd && part.Code == 0 && !part.Corrupt && cs.pos[i] >= part.HWM
	}
	operation := cs.operation
	cs.operation = actor.ID{}
	if c.history != nil {
		c.history.Return(operation, res, history.ResultKnown)
	}
	d := 100 + c.env.Intn(200)
	if got {
		d = 30
	}
	c.sess.Fails = 0
	if cs.drain && atEnd {
		cs.done = true
		return
	}
	c.sess.Wake(o, actor.Duration(d)*actor.Millisecond)
}

func readPart(p kmsg.FetchResponseTopicPartition, pos int64) ReadPart {
	out := ReadPart{Code: p.ErrorCode, HWM: p.HighWatermark}
	if p.ErrorCode != 0 {
		return out
	}
	bs, err := kafkawire.SplitBatches(p.RecordBatches)
	if err != nil {
		out.Corrupt = true
	}
	for _, raw := range bs {
		bt, err := kafkawire.ParseBatch(raw)
		var recs []kmsg.Record
		if err == nil {
			recs, err = batchRecords(bt)
		}
		if err != nil {
			out.Corrupt = true
			break
		}
		for _, rec := range recs {
			if off := bt.BaseOffset + int64(rec.OffsetDelta); off >= pos {
				out.Recs = append(out.Recs, Rec{Off: off, Val: string(rec.Value)})
			}
		}
	}
	return out
}

// abandonFetch closes the outstanding read: it had no effect.
func (c *client) abandonFetch(why string) {
	if c.c.operation != (actor.ID{}) && c.history != nil {
		c.history.Return(c.c.operation, ReadRes{Why: why}, history.GivenUp)
	}
	c.c.operation = actor.ID{}
}
