// Package session is the connection loop of one scenario client: dial the
// brokers in rotation, back off on failure, time out the one outstanding
// request, and reconnect when the connection is lost. The protocol spoken over
// the connection belongs to the client.
//
// A Session is a component: the client lends it an outbox with actor.Sub, so
// its messages (Msg) come back wrapped into the client's own, and its timers
// and its ForgetPending live in the scope of that Sub.
package session

//go:generate go run github.com/manifesterio/mds/ops/tools/dstgen

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/trace"
)

// Msg is what a session receives.
//
//sumtype:decl
//dst:sum
type Msg interface {
	actor.Payload
	isMsg()
}

//dst:cases Msg
type (
	// Dialed is the answer of the session's Connect.
	Dialed struct{ Done core.ConnectDone }
	// Net is an event of a connection: a core.BytesReceived (one whole
	// frame) or a core.ConnClosed.
	Net struct{ Event core.EventData }
	// WakeDue says that the wake timer fired.
	WakeDue struct{}
	// RequestDue says that the outstanding request timed out.
	RequestDue struct{}
)

// Outbox is the outbox a client lends its session.
type Outbox = actor.Outbox[Msg, core.IO]

// The timer keys of a session, inside its Sub scope.
const (
	keyWake    actor.TimerKey = "wake"
	keyRequest actor.TimerKey = "request"
)

// State is a client's connection state.
type State uint8

// The connection states the session drives.
const (
	WaitingToRetry State = iota // wake timer pending, then dial
	Connecting
	Connected
)

// Kind says what a session message means to the client.
type Kind uint8

// The kinds of Event.
const (
	Nothing Kind = iota // nothing for the client to do
	Ready               // the dial succeeded: Conn is set and State is Connected
	Idle                // the wake timer fired while connected and not busy
	Bytes               // a frame arrived on the connection: Event.Data
	Lost                // the connection is gone (Event.Why); the reconnect is scheduled
)

// Event is what Receive tells the client.
type Event struct {
	Kind Kind
	Data []byte
	Why  string
}

// Session is one client's connection state.
type Session struct {
	ID      int      // the client's number; it picks the first broker to dial
	Brokers []string // the broker actor names, dialed in rotation
	State   State
	Conn    core.ConnID
	Dials   int
	Fails   int  // consecutive dial/connection failures
	Busy    bool // a request is outstanding
}

// RetryWaitTime is the backoff after n consecutive failures.
func RetryWaitTime(n int) actor.Duration {
	n = min(n, 4)
	return 50 * actor.Millisecond << n
}

// Dial connects to the next broker in the rotation.
func (s *Session) Dial(o *Outbox) {
	s.State = Connecting
	addr := s.Brokers[(s.ID+s.Dials)%len(s.Brokers)]
	s.Dials++
	actor.Perform(o, core.Connect{Addr: addr}, func(d core.ConnectDone) Msg { return Dialed{Done: d} })
}

// Receive handles one session message and says what it means to the client.
func (s *Session) Receive(msg Msg, o *Outbox) Event {
	switch m := msg.(type) {
	case Dialed:
		if s.State != Connecting {
			return Event{}
		}
		if m.Done.Err != core.OK {
			s.backoff(o)
			return Event{}
		}
		s.Conn, s.State = m.Done.Conn, Connected
		return Event{Kind: Ready}
	case Net:
		return s.onNet(m.Event, o)
	case WakeDue:
		switch {
		case s.State == WaitingToRetry:
			s.Dial(o)
		case s.State == Connected && !s.Busy:
			return Event{Kind: Idle}
		}
	case RequestDue:
		if s.State == Connected && s.Busy {
			s.Abort(o)
			return Event{Kind: Lost, Why: "timeout"}
		}
	}
	return Event{}
}

// onNet handles an event of a connection. Events of an older connection
// are ignored.
func (s *Session) onNet(ev core.EventData, o *Outbox) Event {
	if b, ok := ev.(core.BytesReceived); ok && s.State == Connected && b.Conn == s.Conn {
		return Event{Kind: Bytes, Data: b.Data}
	}
	if c, ok := ev.(core.ConnClosed); ok && s.State == Connected && c.Conn == s.Conn {
		s.lost(o)
		return Event{Kind: Lost, Why: "conn_closed"}
	}
	return Event{}
}

// Wake arms the wake timer, replacing any pending one.
func (s *Session) Wake(o *Outbox, d actor.Duration) { o.StartTimer(keyWake, d, WakeDue{}) }

// Request sends data on the connection and starts its timeout.
func (s *Session) Request(o *Outbox, data []byte, timeout actor.Duration) {
	s.Busy = true
	o.Do(core.Send{Conn: s.Conn, Data: data})
	o.StartTimer(keyRequest, timeout, RequestDue{})
}

// Answered settles the outstanding request, so its timeout no longer fires.
func (s *Session) Answered(o *Outbox) {
	s.Busy = false
	o.CancelTimer(keyRequest)
}

// Abort drops the connection as unusable and schedules the reconnect. The
// client settles its own protocol state.
func (s *Session) Abort(o *Outbox) {
	o.Do(core.CloseConn{Conn: s.Conn, Abort: true})
	s.lost(o)
}

// lost handles a dead or unusable connection: whatever was outstanding is
// forgotten, and the client reconnects (to the next broker) with backoff.
func (s *Session) lost(o *Outbox) {
	s.Conn, s.Busy = 0, false
	o.ForgetPending()
	s.backoff(o)
}

func (s *Session) backoff(o *Outbox) {
	s.Fails++
	s.State = WaitingToRetry
	s.Wake(o, RetryWaitTime(s.Fails))
}

// Hash folds the session state.
func (s *Session) Hash(h *trace.Hash) {
	h.Uint64(uint64(s.State))
	h.Uint64(uint64(s.Conn))
	h.Uint64(uint64(s.Dials))
	h.Uint64(uint64(s.Fails))
	h.Bool(s.Busy)
}
