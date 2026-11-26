package sim

import "github.com/manifesterio/mds/dst/actor"

// entry is one item of the event queue. The closure run was made by a generic
// function that knew the types. body is kept only to fold it into the trace
// hash and to print it in a log line; it may be nil.
type entry struct {
	at   Time
	tie  uint64 // drawn when the entry is added
	seq  uint64 // the order of adding
	lane laneID
	kind entryKind

	// The target, for the trace hash and for dropping the entries of an
	// incarnation that ended. node is -1 when the target is not a node. inc
	// is zero when the entry goes to whatever incarnation is up.
	node    int
	inc     actor.ID
	service string

	body actor.Payload
	run  func()
	dead bool // cancelled or dropped; thrown away when it reaches the top
}
