// Package simulator is the broker simulation on dst/sim.
//
// It holds the fake services (fakeservices), the cluster host with its
// connection table (cluster), the history and the truth views, and the
// scenario: kafka. The command cmd/mdssimulator runs it through
// dst/explore/campaign and imports only this tree.
package simulator
