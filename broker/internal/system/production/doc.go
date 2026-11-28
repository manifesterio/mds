// Package production is the production runtime of the broker: one
// dst/live Host running the broker actor, its exec function
// (Executor), the TCP connections, the heartbeat, the env, and the wiring
// shared by cmd/mdsbroker and the integration tests. The clock is
// live.RealClock. It is nondeterministic by design and is not in the
// determinism gate.
package production
