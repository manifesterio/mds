package broker

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/segmentformat"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/shared/objectstore"
)

// partitionBatch is one admitted partition batch on its way to a segment.
type partitionBatch struct {
	s              *slot
	part           *kmsg.ProduceResponseTopicPartition
	topicPartition core.TopicPartition
	data           []byte
	records        int64
	loc            core.SegmentRef
	answered       bool // answerPart already ran (early under InjectedBugEarlyAck)
}

// openSegment is the buffer that the next flush seals.
type openSegment struct {
	batches []*partitionBatch
	bytes   int
	parts   []core.TopicPartition // distinct partitions buffered
	// Time of the first batch, for the segment_sealed age fact and
	// SegmentCaps. Not used to schedule. Does not move when
	// InjectedBugTimerFromLastBatch re-arms the flush timer.
	since actor.Mono
}

// Why a segment was sealed (the "reason" of the segment_sealed fact).
const (
	closeOnSize       = "size"
	closeOnTimer      = "timer"
	closeOnBatches    = "batches"
	closeOnPartitions = "partitions"
	closeOnStop       = "stop"
)

type segmentState uint8

const (
	segmentUpload segmentState = iota // ObjectPut in flight
	segmentUploadRetry
	segmentUploadDone // uploaded, waiting for its turn to commit
	segmentCommit     // CommitSegment in flight
	segmentCommitRetry
)

// segment is a sealed upload: one object, one CommitSegment.
type segment struct {
	id       uint64
	batches  []*partitionBatch
	key      string
	data     []byte
	state    segmentState
	attempts int

	// sealedAt and putDoneAt are facts only, and StateHash omits them.
	// The PUT starts at the seal. commitAt is the time of the first
	// CommitSegment, and commitStarted keeps that time fixed.
	// retryCommit reads commitAt for the deadline. StateHash omits both.
	sealedAt      actor.Mono
	putDoneAt     actor.Mono
	commitAt      actor.Mono
	commitStarted bool
}

// dataBytes is the produce data of the segment (what bufBytes counts).
func (s *segment) dataBytes() int {
	n := 0
	for _, pb := range s.batches {
		n += len(pb.data)
	}
	return n
}

// addBatch appends an admitted batch. The first batch arms a fixed
// BatchWindow timer, and a cap may seal earlier.
func (b *Broker) addBatch(pb *partitionBatch, outbox *brokerOutbox) {
	before := b.open.bytes
	b.open.batches = append(b.open.batches, pb)
	b.open.bytes += len(pb.data)
	b.bufBytes += len(pb.data)
	if !slices.Contains(b.open.parts, pb.topicPartition) {
		b.open.parts = append(b.open.parts, pb.topicPartition)
	}
	first := len(b.open.batches) == 1
	// since must not move when the mutant re-arms the timer, or the age
	// invariant cannot catch it.
	if first {
		b.open.since = b.now
	}
	if first || b.config.InjectedBug == InjectedBugTimerFromLastBatch {
		outbox.StartTimer(timerFlush, b.config.BatchWindow, core.FlushDue{})
	}
	sizeBytes := b.open.bytes
	if b.config.InjectedBug == InjectedBugSealOffByOne {
		sizeBytes = before
	}
	switch {
	case sizeBytes >= b.config.MaxSegmentBytes:
		b.seal(closeOnSize, outbox)
	case len(b.open.parts) >= b.config.MaxSegmentPartitions && b.config.InjectedBug != InjectedBugNoPartitionCap:
		b.seal(closeOnPartitions, outbox)
	case len(b.open.batches) >= b.config.MaxSegmentBatches && b.config.InjectedBug != InjectedBugNoBatchCap:
		b.seal(closeOnBatches, outbox)
	}
}

// seal turns the open buffer into a segment and starts its upload.
func (b *Broker) seal(why string, outbox *brokerOutbox) {
	if len(b.open.batches) == 0 {
		return
	}
	bs := b.open.batches
	switch why {
	case closeOnSize:
		b.env.Reachable(segmentSealedSize)
		if len(bs) == 1 {
			b.env.Reachable(oversizeBatchSealedAlone)
		}
	case closeOnTimer:
		b.env.Reachable(segmentSealedTimer)
	case closeOnBatches:
		b.env.Reachable(segmentSealedBatchCap)
	case closeOnPartitions:
		b.env.Reachable(segmentSealedPartitionCap)
	}
	outbox.CancelTimer(timerFlush)
	recordFact(outbox, "segment_sealed",
		actor.Field{Key: "reason", Value: actor.StringValue(why)},
		actor.Field{Key: "bytes", Value: actor.Int64Value(b.open.bytes)},
		actor.Field{Key: "batches", Value: actor.Int64Value(len(bs))},
		actor.Field{Key: "parts", Value: actor.Int64Value(len(b.open.parts))},
		actor.Field{Key: "last", Value: actor.Int64Value(len(bs[len(bs)-1].data))},
		actor.Field{Key: "age", Value: actor.Int64Value(b.now - b.open.since)},
		actor.Field{Key: "inflight", Value: actor.Int64Value(len(b.segs) + 1)},
		actor.Field{Key: "buffered", Value: actor.Int64Value(b.bufBytes)})
	b.open = openSegment{}
	b.sealBatches(bs, outbox)
}

func (b *Broker) sealBatches(bs []*partitionBatch, outbox *brokerOutbox) {
	// Sort by topic and partition, so each partition's batches sit next to
	// each other in the object. The sort is stable: batches of one partition
	// keep their arrival order, and so do their offsets.
	slices.SortStableFunc(bs, comparePartitionBatch)
	fb := make([]segmentformat.Batch, len(bs))
	for i, pb := range bs {
		fb[i] = segmentformat.Batch{TopicPartition: pb.topicPartition, Records: uint32(pb.records), Data: pb.data}
	}
	data, entries, err := segmentformat.Encode(fb)
	if err != nil {
		b.env.Unreachable(admittedBatchCannotEncode)
		for _, pb := range bs {
			b.answerPart(pb, errUnknownServer, contract.BatchRange{}, "")
		}
		return
	}
	s := &segment{id: b.id(), batches: bs, data: data,
		key: b.newSegmentKey(), sealedAt: b.now}
	for i, pb := range bs {
		pb.loc = entries[i].Ref(s.key)
	}
	b.segs = append(b.segs, s)
	b.put(s, outbox)
}

// newSegmentKey draws a fresh segment key from the Environment.
func (b *Broker) newSegmentKey() string {
	return segmentformat.SegmentKey([16]byte(b.env.NewID()))
}

// segmentTimer is the key of the retry timer of s.
func segmentTimer(s *segment) actor.TimerKey {
	return actor.TimerKey("segment/" + strconv.FormatUint(s.id, 10))
}

// comparePartitionBatch orders batches by topic, then partition.
func comparePartitionBatch(x, y *partitionBatch) int {
	if c := strings.Compare(x.topicPartition.Topic, y.topicPartition.Topic); c != 0 {
		return c
	}
	return cmp.Compare(x.topicPartition.Partition, y.topicPartition.Partition)
}

func (b *Broker) put(s *segment, outbox *brokerOutbox) {
	s.state = segmentUpload
	id := s.id
	actor.Perform(outbox, core.ObjectCall{Call: objectstore.Put{Key: s.key, Data: s.data}},
		func(d core.ObjectDone) core.EventData { return core.SegmentPutDone{Seg: id, Done: d} })
}

// onPutDone takes the answer of the PUT of s. Any other kind of answer is
// ignored.
func (b *Broker) onPutDone(s *segment, d core.ObjectDone, outbox *brokerOutbox) {
	e, ok := d.Result.(objectstore.PutDone)
	if !ok {
		return
	}
	if e.Err == core.ErrInvalid {
		b.failSegment(s, errUnknownServer, outbox)
		return
	}
	if e.Err != core.OK {
		b.retryPut(s, outbox)
		return
	}
	attempts := s.attempts + 1
	s.state, s.attempts, s.putDoneAt = segmentUploadDone, 0, b.now
	recordFact(outbox, "segment_put_done",
		actor.Field{Key: "segment", Value: actor.StringValue(s.key)},
		actor.Field{Key: "bytes", Value: actor.Int64Value(len(s.data))},
		actor.Field{Key: "put_ns", Value: actor.Int64Value(b.now - s.sealedAt)},
		actor.Field{Key: "attempts", Value: actor.Int64Value(attempts)})
	if b.config.InjectedBug == InjectedBugEarlyAck {
		for _, pb := range s.batches {
			b.guess += uint64(pb.records)
			b.answerPart(pb, errNone, contract.BatchRange{TopicPartition: pb.topicPartition, Base: core.Offset(b.guess) - core.Offset(pb.records), Count: pb.records}, s.key)
		}
	}
	b.pump(outbox)
}

// pump starts the commit of the head segment once it is uploaded. Only the
// head commits, so segments reach the control plane in seal order.
func (b *Broker) pump(outbox *brokerOutbox) {
	if len(b.segs) == 0 || b.segs[0].state != segmentUploadDone {
		return
	}
	b.commit(b.segs[0], outbox)
}

func (b *Broker) commit(s *segment, outbox *brokerOutbox) {
	s.state = segmentCommit
	if !s.commitStarted {
		s.commitStarted, s.commitAt = true, b.now
	}
	pend := make([]contract.PendingBatch, len(s.batches))
	for i, pb := range s.batches {
		pend[i] = contract.PendingBatch{TopicPartition: pb.topicPartition, Records: pb.records, Loc: pb.loc}
	}
	key := s.key
	if b.config.InjectedBug == InjectedBugFreshKeyOnRetry && s.attempts > 0 {
		key = b.newSegmentKey()
	}
	id := s.id
	actor.Perform(outbox, core.ControlPlaneCall{Op: contract.CommitSegment{SegmentKey: key, Batches: pend}},
		func(d core.ControlPlaneDone) core.EventData { return core.SegmentCommitDone{Seg: id, Done: d} })
}

func (b *Broker) onCommit(s *segment, e core.ControlPlaneDone, outbox *brokerOutbox) {
	switch e.Err {
	case core.OK:
		response, ok := e.Resp.(contract.CommitSegmentResp)
		shape := ok && len(response.Assigned) == len(s.batches)
		for i := 0; shape && i < len(s.batches); i++ {
			shape = response.Assigned[i].TopicPartition == s.batches[i].topicPartition
		}
		b.env.AlwaysOrUnreachable(shape, commitAnswersEveryBatchOrder)
		if !shape {
			b.failSegment(s, errUnknownServer, outbox)
			return
		}
		if s.attempts >= b.config.MaxAttempts {
			// Past the old six-attempt budget (audit item 8): the outage was
			// ridden out instead of failing the segment.
			b.env.Reachable(commitSucceededAfterOldRetryBudget)
		}
		b.committed(s, response.Assigned, outbox)
	case core.ErrInvalid:
		b.failSegment(s, errUnknownServer, outbox)
	case core.ErrConflict:
		// The same key with a different request: a broker bug, not a race.
		b.env.Unreachable(commitKeyReusedDifferentRequest)
		b.failSegment(s, errUnknownServer, outbox)
	case core.ErrTooLarge:
		// The segment exceeds the CP's commit limit; retrying cannot help.
		b.env.Unreachable(segmentExceedsCommitSize)
		b.failSegment(s, errUnknownServer, outbox)
	default:
		// MaybeApplied or a transient error: resend the identical request under
		// the same key until definite; a replay returns the same answer.
		b.retryCommit(s, outbox)
	}
}

// committed answers every batch of s from the control plane's verdicts.
// Timing is in nanoseconds. put plus wait plus commit equals total. wait is
// the time the uploaded segment queued behind earlier segments' commits.
func (b *Broker) committed(s *segment, rs []contract.BatchRange, outbox *brokerOutbox) {
	recordFact(outbox, "segment_committed",
		actor.Field{Key: "segment", Value: actor.StringValue(s.key)},
		actor.Field{Key: "batches", Value: actor.Int64Value(len(s.batches))},
		actor.Field{Key: "bytes", Value: actor.Int64Value(len(s.data))},
		actor.Field{Key: "put_ns", Value: actor.Int64Value(s.putDoneAt - s.sealedAt)},
		actor.Field{Key: "wait_ns", Value: actor.Int64Value(s.commitAt - s.putDoneAt)},
		actor.Field{Key: "commit_ns", Value: actor.Int64Value(b.now - s.commitAt)},
		actor.Field{Key: "total_ns", Value: actor.Int64Value(b.now - s.sealedAt)},
		actor.Field{Key: "inflight", Value: actor.Int64Value(len(b.segs) - 1)},
		actor.Field{Key: "buffered", Value: actor.Int64Value(b.bufBytes - s.dataBytes())})
	for i, pb := range s.batches {
		b.answerPart(pb, errNone, rs[i], s.key)
	}
	b.endSegment(s, outbox)
}

func (b *Broker) remove(s *segment) {
	for i, x := range b.segs {
		if x == s {
			b.segs = append(b.segs[:i:i], b.segs[i+1:]...)
			return
		}
	}
}

// endSegment retires s and starts the next commit.
func (b *Broker) endSegment(s *segment, outbox *brokerOutbox) {
	b.remove(s)
	for _, pb := range s.batches {
		b.bufBytes -= len(pb.data)
	}
	b.pump(outbox)
}

// failSegment answers every unanswered batch of s with code.
func (b *Broker) failSegment(s *segment, code kafkaErrorCode, outbox *brokerOutbox) {
	recordFact(outbox, "segment_failed",
		actor.Field{Key: "segment", Value: actor.StringValue(s.key)},
		actor.Field{Key: "code", Value: actor.Int64Value(int64(code))},
		actor.Field{Key: "total_ns", Value: actor.Int64Value(b.now - s.sealedAt)},
		actor.Field{Key: "inflight", Value: actor.Int64Value(len(b.segs) - 1)},
		actor.Field{Key: "buffered", Value: actor.Int64Value(b.bufBytes - s.dataBytes())})
	for _, pb := range s.batches {
		b.answerPart(pb, code, contract.BatchRange{}, "")
	}
	b.endSegment(s, outbox)
}

// retryPut backs off and repeats the PUT under the same key.
func (b *Broker) retryPut(s *segment, outbox *brokerOutbox) {
	if s.attempts++; s.attempts >= b.config.MaxAttempts {
		// The write may still land later. MaybeApplied is unbounded.
		b.env.Reachable(segmentRetriesExhausted)
		b.failSegment(s, errRequestTimedOut, outbox)
		return
	}
	s.state = segmentUploadRetry
	outbox.StartTimer(segmentTimer(s), b.backoff(s.attempts), core.SegmentRetryDue{Seg: s.id})
}

// retryCommit repeats a commit under the same key until CommitDeadline has
// passed since the first CommitSegment. Backoff is capped exponential with
// jitter (drawn from the Environment, so deterministic in simulation), never a
// count limit. Past the deadline the segment fails. The write may still land
// later.
func (b *Broker) retryCommit(s *segment, outbox *brokerOutbox) {
	left := b.config.CommitDeadline - actor.Duration(b.now-s.commitAt)
	if left <= 0 {
		b.env.Reachable(commitDeadlineExceeded)
		b.failSegment(s, errRequestTimedOut, outbox)
		return
	}
	s.attempts++
	d := b.config.RetryBase
	for n := s.attempts; n > 1 && d < b.config.CommitRetryMax; n-- {
		d *= 2
	}
	d = min(d, b.config.CommitRetryMax)
	d = d/2 + actor.Duration(b.env.Intn(int(d/2)+1)) // jitter in [d/2, d]
	d = min(d, left)
	s.state = segmentCommitRetry
	outbox.StartTimer(segmentTimer(s), d, core.SegmentRetryDue{Seg: s.id})
}

// onSegmentRetry repeats the PUT or the commit of s once its backoff has
// passed.
func (b *Broker) onSegmentRetry(s *segment, outbox *brokerOutbox) {
	switch s.state {
	case segmentUploadRetry:
		b.put(s, outbox)
	case segmentCommitRetry:
		b.env.Reachable(commitRetried)
		b.commit(s, outbox)
	case segmentUpload, segmentUploadDone, segmentCommit:
	}
}

// answerPart answers pb once, with code or, on errNone, with success at range
// r of segment key. A success also records the "produce_acked" fact that
// drain emits right before the Send.
func (b *Broker) answerPart(pb *partitionBatch, code kafkaErrorCode, r contract.BatchRange, key string) {
	if pb.answered {
		return
	}
	pb.answered = true
	pb.part.ErrorCode, pb.part.BaseOffset = int16(code), -1
	if code == errNone {
		pb.part.BaseOffset = int64(r.Base)
		pb.s.facts = append(pb.s.facts, actor.Fact{Name: "produce_acked", Fields: []actor.Field{
			{Key: "topic", Value: actor.StringValue(pb.topicPartition.Topic)},
			{Key: "partition", Value: actor.Int64Value(pb.topicPartition.Partition)},
			{Key: "base_offset", Value: actor.Int64Value(r.Base)},
			{Key: "count", Value: actor.Int64Value(pb.records)},
			{Key: "segment", Value: actor.StringValue(key)},
		}})
	}
	b.finishPart(pb.s)
}
