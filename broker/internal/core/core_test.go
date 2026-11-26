package core

import (
	"testing"

	"github.com/manifesterio/mds/dst/actor"
)

// Connection events are Data; everything the node caused itself is Control.
func TestPriorityOfCoversEveryEvent(t *testing.T) {
	data := map[string]bool{"ConnOpened": true, "BytesReceived": true, "ConnClosed": true}
	for name, msg := range sampleEvents() {
		want := actor.Control
		if data[name] {
			want = actor.Data
		}
		if got := PriorityOf(msg); got != want {
			t.Errorf("%s: priority %d, want %d", name, got, want)
		}
	}
}

func TestErrCodeString(t *testing.T) {
	want := map[ErrCode]string{
		OK: "OK", ErrTimeout: "ErrTimeout", ErrUnavailable: "ErrUnavailable",
		ErrNotFound: "ErrNotFound", ErrConflict: "ErrConflict",
		ErrConnClosed: "ErrConnClosed", ErrTooLarge: "ErrTooLarge",
		ErrCorrupt: "ErrCorrupt", ErrInvalid: "ErrInvalid", ErrIO: "ErrIO",
		ErrUnknownMember: "ErrUnknownMember", ErrIllegalGeneration: "ErrIllegalGeneration",
		ErrUnknownPartition: "ErrUnknownPartition", ErrTopicExists: "ErrTopicExists",
		ErrCode(999): "ErrCode(999)",
	}
	for c, s := range want {
		if c.String() != s {
			t.Errorf("%d: got %q want %q", uint16(c), c.String(), s)
		}
	}
}
