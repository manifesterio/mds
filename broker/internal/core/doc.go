// Package core holds the plain-data vocabulary of the broker: the messages it
// takes (EventData), the IO it hands to its executor (IO, the requests each
// with their answer type), and the Environment through which the only
// permitted nondeterminism (randomness, buggify) and assertion recording
// enter. The broker is an actor (package dst/actor).
//
// Time, identity and fact types come from dst/actor. A fact name and its
// field encoding are trace-hash inputs: frozen, past-tense constants, never
// derived from Go type names. An ack fact is recorded in the same Receive,
// immediately before the Send that acks, so invariants can check "acked
// implies committed" on that Receive.
//
// The trace-hash tags of the messages are in dst.tags, written by dstgen and
// append-only. The control plane call and reply types are not here: they are
// in controlplane/contract, which folds them itself.
//
// Rules (enforced by depguard/forbidigo in CI):
//   - no imports of time, math/rand, crypto/rand, os, net, sync, or any SDK;
//   - no goroutines, channels or select;
//   - never let iteration over a map decide behavior;
//   - messages and IO values are plain data; byte slices are immutable once sent.
//
// See docs/architecture.md.
package core
