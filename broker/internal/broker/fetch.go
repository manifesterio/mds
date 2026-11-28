package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/shared/objectstore"
)

// maxFetchWait caps MaxWait, so one Fetch cannot hold a connection for long.
const maxFetchWait = 30 * actor.Second

// fetch serves each partition independently. It runs FindSegments, then ranged
// GETs (one per run of adjacent batches in a segment), verified and patched to
// each batch's committed offset.
// A part that finds no data is parked and polled again (long poll). The Fetch
// answers once it has MinBytes, or when MaxWait ends.
func (b *Broker) fetch(s *slot, r *kmsg.FetchRequest, outbox *brokerOutbox) {
	response := kmsg.NewPtrFetchResponse()
	s.response = response
	if r.SessionEpoch > 0 { // incremental sessions are never granted
		response.ErrorCode = int16(errFetchSessionNotFound)
		return
	}
	s.fetchMax = r.MaxBytes
	s.fetchMin = r.MinBytes
	s.fetchUntil = b.now + actor.Mono(fetchWait(r.MaxWaitMillis))
	fillFetchSkeleton(response, r)
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &response.Topics[i].Partitions[j]
			topicPartition := contract.NewTopicPartition(t.Topic, p.Partition)
			switch {
			case !b.knows(topicPartition):
				rp.ErrorCode = int16(errUnknownTopicOrPart)
			case p.FetchOffset < 0:
				rp.ErrorCode = int16(errOffsetOutOfRange)
			default:
				lim := p.PartitionMaxBytes
				if lim <= 0 {
					lim = b.config.DefaultFetchBytes
				}
				b.startOp(&operation{kind: opFetch, s: s, topicPartition: topicPartition, from: core.Offset(p.FetchOffset), maxBytes: lim, fpart: rp}, outbox)
			}
		}
	}
}

func fillFetchSkeleton(response *kmsg.FetchResponse, r *kmsg.FetchRequest) {
	response.Topics = make([]kmsg.FetchResponseTopic, len(r.Topics))
	for i, t := range r.Topics {
		rt := &response.Topics[i]
		rt.Topic = t.Topic
		rt.Partitions = make([]kmsg.FetchResponseTopicPartition, len(t.Partitions))
		for j := range t.Partitions {
			rp := &rt.Partitions[j]
			rp.Partition = t.Partitions[j].Partition
			rp.HighWatermark, rp.LastStableOffset, rp.LogStartOffset, rp.PreferredReadReplica = -1, -1, 0, -1
		}
	}
}

// onLookup picks the ranges that fit the partition limit (at least one) and
// starts reading them.
func (b *Broker) onLookup(o *operation, r contract.FindSegmentsResp, outbox *brokerOutbox) {
	o.fpart.HighWatermark, o.fpart.LastStableOffset = int64(r.End), int64(r.End)
	if o.from > r.End {
		o.fpart.ErrorCode = int16(errOffsetOutOfRange)
		b.endOp(o)
		return
	}
	var total int64
	for _, rg := range r.Ranges {
		if len(o.ranges) > 0 && total+rg.Loc.Len > int64(o.maxBytes) {
			break
		}
		o.ranges = append(o.ranges, rg)
		total += rg.Loc.Len
	}
	if o.from == r.End || len(o.ranges) == 0 {
		b.waitOrEnd(o, outbox)
		return
	}
	o.state, o.attempts, o.next = opGet, 0, 0
	b.issue(o, outbox)
}

// fetchWait is how long a Fetch may wait: MaxWait, capped. Zero means answer at once.
func fetchWait(maxWaitMillis int32) actor.Duration {
	if maxWaitMillis <= 0 {
		return 0
	}
	return min(actor.Duration(maxWaitMillis)*actor.Millisecond, maxFetchWait)
}

// fetchOver reports whether a Fetch must answer now: it has MinBytes, its
// MaxWait is spent, the client is gone, or the broker is stopping.
func (b *Broker) fetchOver(s *slot) bool {
	return s.fetchMin <= 0 || s.fetchGot >= int(s.fetchMin) || b.now >= s.fetchUntil ||
		s.conn.closed || b.stopping
}

// waitOrEnd parks a part that found no data, or ends it if the Fetch is over.
func (b *Broker) waitOrEnd(o *operation, outbox *brokerOutbox) {
	if b.fetchOver(o.s) {
		b.endOp(o)
		return
	}
	b.env.Reachable(fetchParkedForPoll)
	b.park(o, outbox)
}

// fetchPartData runs when a part has its batches. The bytes count toward
// MinBytes. If that meets MinBytes, the Fetch answers now: parked parts end
// too. If not, this part waits with its data (held), and the Fetch answers
// once MinBytes is reached or MaxWait ends.
func (b *Broker) fetchPartData(o *operation, outbox *brokerOutbox) {
	s := o.s
	s.fetchGot += len(o.fpart.RecordBatches)
	if s.fetchGot >= int(s.fetchMin) {
		b.endParked(func(p *operation) bool { return p.s == s })
		b.endOp(o)
		return
	}
	if b.fetchOver(s) {
		b.endOp(o)
		return
	}
	o.held = true
	b.park(o, outbox)
}

// park waits until the next poll of o: FetchPollInterval, or the end of
// MaxWait if that comes first.
func (b *Broker) park(o *operation, outbox *brokerOutbox) {
	o.state, o.attempts = opWait, 0
	after := min(b.config.FetchPollInterval, actor.Duration(o.s.fetchUntil-b.now))
	outbox.StartTimer(opTimer(o), after, core.OpDue{Op: o.id})
}

// pollOp runs when a parked part's timer fires. A part that holds data only
// waits for the clock. A part with no data runs its next Lookup, so only the
// partitions still short are polled.
func (b *Broker) pollOp(o *operation, outbox *brokerOutbox) {
	if b.fetchOver(o.s) {
		b.endOp(o)
		return
	}
	if o.held {
		b.park(o, outbox)
		return
	}
	o.state = opControlPlane
	b.issue(o, outbox)
}

// endParked ends each parked part that match, with no further polls. A held
// part ends with the data it holds. Its bytes were counted when it arrived.
func (b *Broker) endParked(match func(*operation) bool) {
	var parked []*operation
	for _, o := range b.ops {
		if o.state == opWait && match(o) {
			parked = append(parked, o)
		}
	}
	for _, o := range parked {
		b.endOp(o)
	}
}

// readRanges returns the ranges that the next GET reads, from o.next on. They
// are the ranges that follow each other in one segment object, so one GET
// reads them all. The ranges were already cut to the partition limit.
func (o *operation) readRanges() []contract.BatchRange {
	end := o.next + 1
	for end < len(o.ranges) && adjacent(o.ranges[end-1], o.ranges[end]) {
		end++
	}
	return o.ranges[o.next:end]
}

// adjacent reports whether next starts where prev ends, in the same object.
// Lengths are positive: a length of 0 or less means "to the end" (SegmentRef).
func adjacent(prev, next contract.BatchRange) bool {
	return prev.Loc.Len > 0 && next.Loc.Len > 0 &&
		prev.Loc.Key == next.Loc.Key && prev.Loc.Off+prev.Loc.Len == next.Loc.Off
}

// readRef is one ranged GET that covers rgs. rgs must be adjacent.
func readRef(rgs []contract.BatchRange) contract.SegmentRef {
	ref := rgs[0].Loc
	for _, rg := range rgs[1:] {
		ref.Len += rg.Loc.Len
	}
	return ref
}

// onFetchGet verifies one read from the object store. A read holds one or
// more batches (see readRanges). A corrupt read is never returned. The whole
// read is retried. The object itself is intact. S3 checksums cover whole
// objects but not byte ranges. Any other kind of answer is ignored.
func (b *Broker) onFetchGet(o *operation, d core.ObjectDone, outbox *brokerOutbox) {
	e, ok := d.Result.(objectstore.GetDone)
	if !ok {
		return
	}
	rgs := o.readRanges()
	if e.Err == core.ErrNotFound { // committed segments are never deleted
		b.env.Unreachable(committedSegmentMissing)
		b.failOp(o, errKafkaStorage, outbox)
		return
	}
	if e.Err != core.OK {
		o.corrupt = false
		b.retryOp(o, outbox)
		return
	}
	parts := splitRead(e.Data, rgs)
	if b.config.InjectedBug != InjectedBugServeCorrupt && !allIntact(e.Data, parts, rgs) {
		b.retryCorrupt(o, rgs[0], outbox)
		return
	}
	b.acceptFetch(o, parts, rgs, outbox)
}

func (b *Broker) retryCorrupt(o *operation, rg contract.BatchRange, outbox *brokerOutbox) {
	b.env.Reachable(corruptGetDetected)
	o.corrupt = true
	recordFact(outbox, "corruption_detected", actor.Field{Key: "segment", Value: actor.StringValue(rg.Loc.Key)})
	b.retryOp(o, outbox)
}

// acceptFetch takes the batches of one read, patches each one to its
// committed offset, and moves on to the next read.
func (b *Broker) acceptFetch(o *operation, parts [][]byte, rgs []contract.BatchRange, outbox *brokerOutbox) {
	for i, rg := range rgs {
		base := int64(rg.Base)
		if b.config.InjectedBug == InjectedBugWrongOffset {
			base++
		}
		o.out = append(o.out, kafkawire.ChangeBatchOffset(parts[i], base, 0)...)
	}
	o.next += len(rgs)
	o.attempts, o.corrupt = 0, false
	if o.next < len(o.ranges) {
		b.issue(o, outbox)
		return
	}
	o.fpart.RecordBatches = o.out
	b.fetchPartData(o, outbox)
}

// splitRead cuts the bytes of one read into one part per range, by Loc.Len.
// A short read gives short parts, so the checks below fail on it.
func splitRead(data []byte, rgs []contract.BatchRange) [][]byte {
	parts := make([][]byte, len(rgs))
	for i, rg := range rgs {
		n := min(rg.Loc.Len, int64(len(data)))
		parts[i], data = data[:n], data[n:]
	}
	return parts
}

// allIntact reports whether data is exactly the batches of rgs, and each part
// is the one valid batch that its range committed.
func allIntact(data []byte, parts [][]byte, rgs []contract.BatchRange) bool {
	var total int64
	for _, rg := range rgs {
		total += rg.Loc.Len
	}
	if int64(len(data)) != total {
		return false
	}
	for i, rg := range rgs {
		if !isIntact(parts[i], rg) {
			return false
		}
	}
	return true
}

// isIntact reports whether data is the one valid batch that rg committed.
func isIntact(data []byte, rg contract.BatchRange) bool {
	bt, err := kafkawire.ParseBatch(data)
	return err == nil && int64(len(data)) == rg.Loc.Len && int64(bt.NumRecords) == rg.Count
}
