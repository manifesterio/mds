package core

import "github.com/manifesterio/mds/dst/core/actor"

// PriorityOf picks the inbox of a message (actor.Props.Priority).
//
// Control is the class of things the node caused itself (answers of its own
// IO, its own timers, boot and stop) and of the live broker set. The runtime
// takes Control first; it stays bounded because the broker caps its
// in-flight work.
//
// Data is the class of everything a connection sends, one ordered stream per
// connection. Its real backpressure is PauseReads. Messages that must stay
// ordered relative to each other share an inbox: ConnClosed is Data, so a
// client's EOF can never overtake its last request.
func PriorityOf(msg EventData) actor.Priority {
	switch msg.(type) {
	case ConnOpened, BytesReceived, ConnClosed:
		return actor.Data
	case NodeStarted, StopRequest, ConnectDone, ObjectDone, ControlPlaneDone,
		SegmentPutDone, SegmentCommitDone, OpReadDone, OpCallDone,
		FlushDue, SegmentRetryDue, OpDue, ClientDue, LiveBrokers:
		return actor.Control
	}
	return actor.Control
}
