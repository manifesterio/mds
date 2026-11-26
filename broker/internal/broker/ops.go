package broker

import (
	"slices"
	"strconv"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/shared/objectstore"
)

type operationKind uint8

const (
	opRefresh         operationKind = iota // DescribeTopics to resolve a topic-cache miss, then resume the request
	opMetadata                             // DescribeTopics answering Metadata
	opList                                 // FindSegments for one ListOffsets partition
	opFetch                                // FindSegments, then ranged GETs, for one Fetch partition
	opCreate                               // CreateTopic (exclusive), or DescribeTopics for validateOnly, for one CreateTopics topic
	opDescribeConfigs                      // DescribeTopics, answering DescribeConfigs for topics
	opGroup                                // one group call at the control plane (groups.go)
)

type operationState uint8

const (
	opControlPlane operationState = iota // a ControlPlaneCall is in flight
	opGet                                // an object store Get is in flight
	opBackoff                            // waiting for the retry timer
	opWait                               // a Fetch part parked until its next poll (fetch.go)
)

// operation is one control-plane / object-store interaction serving a request part.
// It retries with backoff; a part never fails silently.
type operation struct {
	id       uint64
	kind     operationKind
	state    operationState
	retry    operationState // state a backoff returns to
	attempts int
	s        *slot

	names []string // refresh, meta: topics asked
	creq  core.Request

	topicPartition core.TopicPartition
	from           core.Offset
	maxBytes       int32
	lpart          *kmsg.ListOffsetsResponseTopicPartition
	fpart          *kmsg.FetchResponseTopicPartition
	ctopic         *kmsg.CreateTopicsResponseTopic
	dresp          *kmsg.DescribeConfigsResponse

	create       contract.CreateTopic // opCreate: the topic to make
	validateOnly bool                 // opCreate: check, do not create
	// maybeApplied: opCreate: an earlier try may have taken effect.
	maybeApplied bool

	ranges  []contract.BatchRange
	next    int
	out     []byte
	corrupt bool // the last failure was a corrupt GET
	held    bool // Fetch part: has its data, waits for the Fetch to fill up (fetch.go)

	gop      contract.Operation          // opGroup: the group call to make
	gtimeout actor.Duration              // opGroup: the call's deadline (0: the default)
	gfill    func(core.ControlPlaneDone) // opGroup: writes the answer into the response
}

func (b *Broker) findOp(id uint64) *operation {
	for _, o := range b.ops {
		if o.id == id {
			return o
		}
	}
	return nil
}

func (b *Broker) dropOp(o *operation) {
	for i, x := range b.ops {
		if x == o {
			b.ops = append(b.ops[:i:i], b.ops[i+1:]...)
			return
		}
	}
}

func (b *Broker) addOp(o *operation) { b.ops = append(b.ops, o) }

// opTimer is the key of the retry or poll timer of o.
func opTimer(o *operation) actor.TimerKey {
	return actor.TimerKey("op/" + strconv.FormatUint(o.id, 10))
}

// startOp registers o as a pending part of its slot and issues its first call.
func (b *Broker) startOp(o *operation, outbox *brokerOutbox) {
	o.id = b.id()
	o.state = opControlPlane
	o.s.pending++
	b.addOp(o)
	b.issue(o, outbox)
}

// issue sends the call of o's state: an object store Get or a control plane
// call. The answer comes back with o's id.
func (b *Broker) issue(o *operation, outbox *brokerOutbox) {
	id := o.id
	if o.state == opGet {
		actor.Perform(outbox, core.ObjectCall{Call: objectstore.Get{Ref: readRef(o.readRanges())}},
			func(d core.ObjectDone) core.EventData { return core.OpReadDone{Op: id, Done: d} })
		return
	}
	actor.Perform(outbox, core.ControlPlaneCall{Op: o.controlPlaneOp(), Timeout: o.gtimeout},
		func(d core.ControlPlaneDone) core.EventData { return core.OpCallDone{Op: id, Done: d} })
}

// controlPlaneOp is the control plane call of o.
func (o *operation) controlPlaneOp() contract.Operation {
	switch o.kind {
	case opRefresh, opMetadata, opDescribeConfigs:
		return contract.DescribeTopics{Topics: o.names}
	case opList: // only the end offset is wanted: look past the end
		return contract.FindSegments{TopicPartition: o.topicPartition, From: core.Offset(1<<63 - 1), MaxBytes: 1}
	case opFetch:
		return contract.FindSegments{TopicPartition: o.topicPartition, From: o.from, MaxBytes: o.maxBytes}
	case opCreate:
		return o.createCP()
	case opGroup:
		return o.gop
	}
	return nil
}

// retryOp backs off and repeats the call, or gives up after MaxAttempts or
// when the client is gone.
func (b *Broker) retryOp(o *operation, outbox *brokerOutbox) {
	if o.s.conn.closed {
		b.endOp(o)
		return
	}
	if o.attempts++; o.attempts >= b.config.MaxAttempts {
		b.env.Reachable(requestRetriesExhausted)
		b.failOp(o, b.exhaustCode(o), outbox)
		return
	}
	o.retry, o.state = o.state, opBackoff
	outbox.StartTimer(opTimer(o), b.backoff(o.attempts), core.OpDue{Op: o.id})
}

func (b *Broker) exhaustCode(o *operation) kafkaErrorCode {
	switch o.kind {
	case opMetadata:
		return errLeaderNotAvailable
	case opFetch:
		if o.corrupt {
			return errKafkaStorage
		}
	}
	return errRequestTimedOut
}

// onOpTimer runs when o's timer fires: a backoff repeats the call, a parked
// Fetch part polls.
func (b *Broker) onOpTimer(o *operation, outbox *brokerOutbox) {
	switch o.state {
	case opBackoff:
		o.state = o.retry
		b.issue(o, outbox)
	case opWait:
		b.pollOp(o, outbox)
	case opControlPlane, opGet:
	}
}

// endOp retires o without touching its response.
func (b *Broker) endOp(o *operation) {
	b.dropOp(o)
	b.finishPart(o.s)
}

// failOp answers o's part with code and retires it.
func (b *Broker) failOp(o *operation, code kafkaErrorCode, outbox *brokerOutbox) {
	switch o.kind {
	case opRefresh:
		b.resume(o, outbox)
		return
	case opMetadata:
		b.metaResp(o, nil, code)
	case opList:
		o.lpart.ErrorCode = int16(code)
	case opFetch:
		o.fpart.ErrorCode = int16(code)
	case opCreate:
		o.ctopic.ErrorCode = int16(code)
	case opDescribeConfigs:
		o.failDescribeConfigs(code)
	case opGroup:
	}
	b.endOp(o)
}

func (b *Broker) onOpCP(o *operation, e core.ControlPlaneDone, outbox *brokerOutbox) {
	switch o.kind {
	case opGroup:
		b.onGroupControlPlane(o, e)
		return
	case opCreate:
		b.onCreateCP(o, e, outbox)
		return
	case opRefresh, opMetadata, opList, opFetch, opDescribeConfigs:
	}
	if r, ok := e.Resp.(contract.DescribeTopicsResp); ok && o.kind == opDescribeConfigs && e.Err == core.OK {
		b.onDescribeConfigsCP(o, r.Topics)
		return
	}
	if e.Err == core.ErrInvalid {
		b.failOp(o, errUnknownServer, outbox)
		return
	}
	if e.Err != core.OK {
		b.retryOp(o, outbox)
		return
	}
	switch r := e.Resp.(type) {
	case contract.DescribeTopicsResp:
		if o.kind == opRefresh || o.kind == opMetadata {
			b.merge(r.Topics)
			if o.kind == opRefresh {
				b.resume(o, outbox)
				return
			}
			b.metaResp(o, r.Topics, errNone)
			b.endOp(o)
			return
		}
	case contract.FindSegmentsResp:
		switch o.kind {
		case opList:
			o.lpart.Offset = int64(r.End)
			b.endOp(o)
			return
		case opFetch:
			b.onLookup(o, r, outbox)
			return
		}
	}
	b.retryOp(o, outbox)
}

// ---- dispatch ----

// handle runs a fresh request.
func (b *Broker) handle(s *slot, request core.Request, outbox *brokerOutbox) {
	b.dispatch(s, request, outbox)
	b.finishPart(s) // release the construction guard
}

// dispatch runs a request, refreshing the topic cache first when needed.
func (b *Broker) dispatch(s *slot, request core.Request, outbox *brokerOutbox) {
	if names := b.missing(request); len(names) > 0 {
		b.startOp(&operation{kind: opRefresh, s: s, names: names, creq: request}, outbox)
		return
	}
	b.dispatchAfterRefresh(s, request, outbox)
}

// dispatchAfterRefresh runs a request without trying another topic-cache refresh.
func (b *Broker) dispatchAfterRefresh(s *slot, request core.Request, outbox *brokerOutbox) {
	switch r := request.(type) {
	case *kmsg.MetadataRequest:
		b.metadata(s, r, outbox)
	case *kmsg.ProduceRequest:
		if r.Acks == 0 { // unsupported: no response, close
			s.silent, s.conn.closing = true, true
			return
		}
		b.produce(s, r, outbox)
	case *kmsg.FetchRequest:
		b.fetch(s, r, outbox)
	case *kmsg.ListOffsetsRequest:
		b.listOffsets(s, r, outbox)
	case *kmsg.CreateTopicsRequest:
		b.createTopics(s, r, outbox)
	case *kmsg.DescribeConfigsRequest:
		b.describeConfigs(s, r, outbox)
	default:
		if !b.dispatchGroup(s, request, outbox) {
			s.silent, s.conn.closing = true, true
		}
	}
}

// resume runs the request a refresh was holding, with whatever the cache now has.
func (b *Broker) resume(o *operation, outbox *brokerOutbox) {
	b.dropOp(o)
	if !o.s.conn.closed {
		b.dispatchAfterRefresh(o.s, o.creq, outbox)
	}
	b.finishPart(o.s)
}

// ---- topic cache ----

func (b *Broker) knows(topicPartition core.TopicPartition) bool {
	for _, t := range b.topics {
		if t.Name == topicPartition.Topic {
			return topicPartition.Partition >= 0 && topicPartition.Partition < t.Partitions
		}
	}
	return false
}

func (b *Broker) merge(infos []contract.TopicInfo) {
	for _, in := range infos {
		i := slices.IndexFunc(b.topics, func(t contract.TopicInfo) bool { return t.Name == in.Name })
		if i < 0 {
			b.topics = append(b.topics, in)
			continue
		}
		b.topics[i].Partitions = max(b.topics[i].Partitions, in.Partitions)
	}
}

// missing lists the topics of req (once each) that the cache cannot resolve.
func (b *Broker) missing(request core.Request) []string {
	var out []string
	add := func(topicPartition core.TopicPartition) {
		if b.knows(topicPartition) {
			return
		}
		if slices.Contains(out, topicPartition.Topic) {
			return
		}
		out = append(out, topicPartition.Topic)
	}
	switch r := request.(type) {
	case *kmsg.ProduceRequest:
		if r.Acks == 0 {
			return nil
		}
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(contract.NewTopicPartition(t.Topic, p.Partition))
			}
		}
	case *kmsg.FetchRequest:
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(contract.NewTopicPartition(t.Topic, p.Partition))
			}
		}
	case *kmsg.ListOffsetsRequest:
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				add(contract.NewTopicPartition(t.Topic, p.Partition))
			}
		}
	}
	return out
}
