// Package live runs actors in production: a Host runs several actors of
// package actor in one process, one goroutine each, with real timers, an exec
// function per actor for product IO (answers come back through Answer), and
// outside callers through Ask, Tell and OnLoop. The same actors run in tests
// on package sim. When one actor ends, the Host stops the others; Run returns
// nil (Exit), ErrTripped (Trip), ErrKilled (a stop missed StopGrace),
// ErrStopped (its context ended), or the crash or protocol error.
//
// NewEnv is the one production Env. RealClock is the process clock and
// FakeClock is the test clock that moves only on Advance.
package live
