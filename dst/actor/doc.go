// Package actor is what product code imports to write a deterministic state
// machine: an Actor receives one message at a time and records what it wants
// done on an Outbox. A runtime carries the effects out after Receive returns:
// package sim (dst/sim) in tests, package live (dst/live) in
// production. The same actor code runs in both.
//
// Wiring is typed: an EntityRef[P] sends P to an actor, a ReplyTo[R] answers
// one ask with R, a ServiceRef[Req, Rep] calls a simulated service. Strings
// are only identity labels (names, timer keys).
//
// Glossary:
//
//	Actor        a state machine: Receive(now, msg, outbox) and StateHash.
//	Props        how to run an actor: Name, New, Boot, StopMsg, Priority, Restart.
//	Incarnation  one boot of an actor; everything it owned dies with it.
//	Outbox       the effects of one Receive, carried out in record order after it.
//	Effect       one recorded action: a send, a timer, a stop, a fact, IO.
//	EntityRef    the stable handle of an actor; it survives restarts.
//	ReplyTo      the handle of one ask; it dies with the asking incarnation.
//	ServiceRef   the handle of a simulated outside service.
//	TimerKey     the name of a timer; starting a key again replaces the timer.
//	Ask, Call    a request whose answer comes back as my message (adapt).
//	Perform      product IO with one typed answer (//dst:answer); Do has none.
//	Watch        tells me when an asker is gone before I replied.
//	Sub          lends a component its own outbox with its own protocol and scope.
//	ForgetPending drops my pending timers and answers, without a restart.
//	Always       (and Sometimes, Reachable, Unreachable) a typed assertion handle.
//	Site         a named buggify site (NewSite, NewFaultSite) that Env may fire.
//	Payload      every message and IO value; dstgen writes its Fold.
package actor
