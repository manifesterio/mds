// Package simulator is the broker simulation on dst/kernel (dst v2).
//
// The old tree with this name was deleted in plan task T1.12d. This tree has the
// name "simulator" again (it was named "simulation" during T1.12).
//
// It holds the fake services (fakeservices), the cluster host (cluster), the history
// and the truth views, and the scenario: kafka. The command
// cmd/mdssimulator runs it through dst/campaign and imports only this tree.
package simulator
