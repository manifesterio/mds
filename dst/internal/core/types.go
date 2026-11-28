package core

// Duration is a length of time in nanoseconds.
type Duration int64

// ID is a 16-byte id drawn from the seeded random stream (Rand.NewID).
type ID [16]byte

// End says how an incarnation ended.
type End uint8

// The ways an incarnation can end.
const (
	Crash End = iota // killed from outside (nemesis, attrition, stop deadline, panic)
	Trip             // the actor asked for a restart: its memory is not trusted
	Exit             // the actor finished a graceful stop
)

// String returns the plain name of the end.
func (e End) String() string {
	switch e {
	case Crash:
		return "crash"
	case Trip:
		return "trip"
	case Exit:
		return "exit"
	}
	return "unknown"
}

// TimerKey names a timer of one actor. Products build keys from their own
// data, e.g. TimerKey("session/" + group + "/" + member).
type TimerKey string
