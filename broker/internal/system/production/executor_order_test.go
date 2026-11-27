package production

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/live"
)

// Socket IO of one connection reaches the connections in record order, even
// when the live pool runs a later IO first.
//
// The race is forced, not waited for: the first IO of the script is held on
// its worker until the connections have seen some other IO (or holdFirst has
// passed). So without ordering in the executor the later IO always lands
// first, and the test fails every run. With ordering, the later IO is held
// back, the first one goes after holdFirst, and the order is kept.
func TestExecutorSocketIOKeepsRecordOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script []core.IO
		want   string
	}{
		{
			name:   "two responses on one connection",
			script: []core.IO{core.Send{Conn: 7, Data: []byte("corr41"), Seq: 1}, core.Send{Conn: 7, Data: []byte("corr42"), Seq: 2}},
			want:   "write corr41,write corr42",
		},
		{
			name:   "graceful close after a response",
			script: []core.IO{core.Send{Conn: 7, Data: []byte("corr9"), Seq: 1}, core.CloseConn{Conn: 7, Seq: 2}},
			want:   "write corr9,close",
		},
		{
			name:   "pause then resume",
			script: []core.IO{core.PauseReads{Conn: 7, Seq: 1}, core.ResumeReads{Conn: 7, Seq: 2}},
			want:   "pause,resume",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conns := &fakeConns{}
			e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
			first := describeIO(tc.script[0])
			exec := func(c *live.ExecCtx, io core.IO) error {
				if describeIO(io) == first {
					waitSeen(conns, 1, holdFirst)
				}
				return e.Exec(c, io)
			}
			runProbeExec(t, exec, tc.script...)
			waitSeen(conns, len(tc.script), 2*time.Second)
			if got := strings.Join(conns.seen(), ","); got != tc.want {
				t.Fatalf("connections saw %s, want %s", got, tc.want)
			}
		})
	}
}

// holdFirst is how long the first IO waits for a later one to overtake it.
const holdFirst = 300 * time.Millisecond

// waitSeen waits until conns has seen n calls, or d has passed.
func waitSeen(conns *fakeConns, n int, d time.Duration) {
	deadline := time.Now().Add(d)
	for len(conns.seen()) < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

func describeIO(io core.IO) string {
	if s, ok := io.(core.Send); ok {
		return "send " + string(s.Data)
	}
	return fmt.Sprintf("%T%+v", io, io)
}

// A connection the broker closes after the peer reset it (a lone abort
// CloseConn) leaves nothing behind in the executor.
func TestExecutorForgetsAbortedConnection(t *testing.T) {
	conns := &fakeConns{}
	e := NewExecutor(t.Context(), ExecConfig{}, nil, nil, conns, Classifier{})
	runProbe(t, e,
		core.Send{Conn: 7, Data: []byte("a"), Seq: 1}, core.CloseConn{Conn: 7, Abort: true, Seq: 2},
		core.CloseConn{Conn: 8, Abort: true, Seq: 1})
	waitSeen(conns, 3, 2*time.Second)
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		e.orderMu.Lock()
		n := len(e.order)
		e.orderMu.Unlock()
		if n == 0 {
			return
		}
	}
	t.Fatal("the executor still holds an entry for a closed connection")
}
