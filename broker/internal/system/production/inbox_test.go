package production

import (
	"errors"
	"sync"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
)

// fakeInbox is a system.Inbox that queues what it is told, for tests of the
// goroutines that feed the broker. After Close it refuses messages.
type fakeInbox struct {
	mu     sync.Mutex
	q      []core.EventData
	closed bool
}

var _ system.Inbox = (*fakeInbox)(nil)

var errInboxClosed = errors.New("inbox closed")

func (f *fakeInbox) Tell(msg core.EventData) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errInboxClosed
	}
	f.q = append(f.q, msg)
	return nil
}

func (f *fakeInbox) Pop() (core.EventData, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.q) == 0 {
		return nil, false
	}
	m := f.q[0]
	f.q = f.q[1:]
	return m, true
}

func (f *fakeInbox) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.q)
}

func (f *fakeInbox) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}
