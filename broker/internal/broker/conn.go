package broker

import (
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/dst/actor"
)

// connection is one client connection. Kafka answers a connection's requests in
// request order even when they are pipelined, so each request owns a slot in q
// and only the head is sent, once it is ready.
type connection struct {
	id      core.ConnID
	fr      *kafkawire.Framer
	rx      stateHasher // running hash of every byte received (a partial frame is state)
	q       []*slot
	paused  bool   // PauseReads is in force
	closing bool   // no more requests are read; graceful close once q drains
	abort   bool   // close with RST
	closed  bool   // CloseConn issued or peer gone; answers are dropped
	host    string // client host, from the peer address (JoinGroup's ClientHost)
	seq     uint64 // Seq of the last socket IO recorded for this connection
}

// nextSeq is the Seq of the next socket IO of c (core.IO).
func (c *connection) nextSeq() uint64 {
	c.seq++
	return c.seq
}

// slot is one request awaiting its response.
type slot struct {
	conn     *connection
	h        kafkawire.RequestHeader
	response core.Response
	pending  int  // parts still outstanding, plus 1 while the handler runs
	ready    bool // resp is complete
	silent   bool // no response is sent (acks=0)
	facts    []actor.Fact
	// fetchMax is the Fetch request's total byte limit (<= 0: none).
	fetchMax int32
	// Fetch long poll (fetch.go): the Fetch waits for fetchMin bytes until
	// fetchUntil. fetchGot counts the bytes its ended parts returned.
	fetchMin   int32
	fetchUntil actor.Mono
	fetchGot   int
}

func (b *Broker) findConn(id core.ConnID) *connection {
	for _, c := range b.conns {
		if c.id == id {
			return c
		}
	}
	return nil
}

func (b *Broker) dropConn(c *connection) {
	c.closed = true
	for i, x := range b.conns {
		if x == c {
			b.conns = append(b.conns[:i:i], b.conns[i+1:]...)
			return
		}
	}
}

func (b *Broker) addConn(c *connection) { b.conns = append(b.conns, c) }

func (b *Broker) onOpen(e core.ConnOpened, outbox *brokerOutbox) {
	if b.stopping || b.findConn(e.Conn) != nil {
		// A repeated open of a known connection closes it in its own Seq
		// order. Otherwise the broker has no state for the connection and
		// this close is the only socket IO it ever records for it: Seq 1.
		seq := uint64(1)
		if c := b.findConn(e.Conn); c != nil {
			// The broker forgets the connection: this close is its last IO.
			seq = c.nextSeq()
			b.dropConn(c)
			b.endParked(func(o *operation) bool { return o.s.conn == c })
		}
		outbox.Do(core.CloseConn{Conn: e.Conn, Abort: true, Seq: seq})
		return
	}
	b.addConn(&connection{id: e.Conn, fr: kafkawire.NewFramer(b.config.MaxFrame), host: peerHost(e.Peer)})
}

func (b *Broker) onClosed(e core.ConnClosed, outbox *brokerOutbox) {
	c := b.findConn(e.Conn)
	if c == nil {
		return
	}
	b.dropConn(c)
	b.endParked(func(o *operation) bool { return o.s.conn == c })
	// The close is recorded even when the peer reset: it is the last IO of
	// the connection and lets the executor forget it.
	outbox.Do(core.CloseConn{Conn: c.id, Abort: e.Abort, Seq: c.nextSeq()})
}

func (b *Broker) onBytes(e core.BytesReceived, outbox *brokerOutbox) {
	c := b.findConn(e.Conn)
	if c == nil || c.closing {
		return
	}
	c.rx.Bytes(e.Data)
	frames, err := c.fr.Push(e.Data)
	for _, f := range frames {
		if c.closing {
			break
		}
		b.request(c, f, outbox)
	}
	if err != nil {
		c.closing = true
	}
	b.dirty = append(b.dirty, c)
}

// peerHost is the host part of a peer address "host:port". Kafka shows a
// client host with a leading slash, so it is returned that way.
func peerHost(peer string) string {
	if i := strings.LastIndexByte(peer, ':'); i >= 0 {
		peer = peer[:i]
	}
	return "/" + peer
}

// request decodes one frame and routes it.
func (b *Broker) request(c *connection, frame []byte, outbox *brokerOutbox) {
	h, request, err := b.codec.DecodeRequest(frame)
	if response, ok := apiVersionsReply(h, request, err); ok {
		s := b.newSlot(c, h)
		s.response = response
		b.finishPart(s)
		return
	}
	if err != nil || b.stopping {
		c.closing = true
		return
	}
	s := b.newSlot(c, h)
	b.handle(s, request, outbox)
}

// apiVersionsReply is the one ApiVersions answer. An unsupported version of
// that key still gets the supported range. Every other decode error does not.
func apiVersionsReply(h kafkawire.RequestHeader, request core.Request, err error) (*kmsg.ApiVersionsResponse, bool) {
	if _, ok := errors.AsType[*kafkawire.UnsupportedError](err); ok && h.APIKey == kafkawire.KeyApiVersions {
		return kafkawire.ApiVersionsResponse(h), true
	}
	if err == nil && request.Key() == kafkawire.KeyApiVersions {
		return kafkawire.ApiVersionsResponse(h), true
	}
	return nil, false
}

// newSlot reserves the next response position on c. pending starts at 1, the
// guard released by handle (or finishPart) once the handler has queued all its
// parts.
func (b *Broker) newSlot(c *connection, h kafkawire.RequestHeader) *slot {
	s := &slot{conn: c, h: h, pending: 1}
	c.q = append(c.q, s)
	return s
}

// finishPart retires one pending part. The last part marks the slot ready.
func (b *Broker) finishPart(s *slot) {
	s.pending--
	if s.pending > 0 {
		return
	}
	b.readySlot(s)
}

func (b *Broker) readySlot(s *slot) {
	b.finalize(s)
	s.ready = true
	b.dirty = append(b.dirty, s.conn)
}

// finalize applies request-wide limits before the response is encoded.
func (b *Broker) finalize(s *slot) {
	r, ok := s.response.(*kmsg.FetchResponse)
	if !ok || s.fetchMax <= 0 {
		return
	}
	total := 0
	for i := range r.Topics {
		for j := range r.Topics[i].Partitions {
			p := &r.Topics[i].Partitions[j]
			if total > 0 && total+len(p.RecordBatches) > int(s.fetchMax) {
				p.RecordBatches = nil
				continue
			}
			total += len(p.RecordBatches)
		}
	}
}

// drainDirty sends every ready head slot, in order per connection, and closes
// connections that are done.
func (b *Broker) drainDirty(outbox *brokerOutbox) {
	for i := range len(b.dirty) {
		b.drain(b.dirty[i], outbox)
	}
	b.dirty = b.dirty[:0]
}

func (b *Broker) drain(c *connection, outbox *brokerOutbox) {
	for len(c.q) > 0 && c.q[0].ready {
		s := c.q[0]
		c.q = c.q[1:]
		if c.closed || s.silent {
			continue
		}
		data, err := b.codec.EncodeResponse(s.h, s.response)
		if err != nil {
			c.closing, c.abort = true, true
			continue
		}
		for _, f := range s.facts {
			outbox.Fact(f)
		}
		outbox.Do(core.Send{Conn: c.id, Data: data, Seq: c.nextSeq()})
	}
	if c.closing && !c.closed && (len(c.q) == 0 || c.abort) {
		b.dropConn(c)
		outbox.Do(core.CloseConn{Conn: c.id, Abort: c.abort, Seq: c.nextSeq()})
	}
}
