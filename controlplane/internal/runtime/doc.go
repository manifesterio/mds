// Package runtime holds the plain-data vocabulary of the control-plane replica:
// the messages it takes (EventData), the IO it performs on the log and the object
// store (IO, each case with its answer type), and the Environment through which
// the only permitted nondeterminism (randomness, buggify) and assertion recording
// enter. The replica is an actor (package dst/actor).
//
// Time and identity types come from dst/actor.
//
// Rules (enforced by depguard/forbidigo in CI):
//   - no imports of time, math/rand, crypto/rand, os, net, sync, or any SDK;
//   - no goroutines, channels or select;
//   - never let iteration over a map decide behavior;
//   - messages and IO values are plain data; byte slices are immutable once sent.
//
// The control plane's own request and response types are not here. They are in
// contract (a public package that the broker also imports). This package imports
// contract, never the reverse.
//
// See docs/contribute/determinism.md and docs/architecture.md.
package runtime
