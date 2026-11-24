package kafka

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/truth"
	"github.com/manifesterio/mds/dst/sim"
)

// Test-only hooks for the C4 conformance check (c4_test.go, package
// kafka_test, which also imports speccheck; this package never does).

// NewOracle returns a fresh oracle invariant for o.
func NewOracle(o Opts, h history.Reader[Op, Res], objs truth.ObjectStore, cp truth.ControlPlane) sim.Rule {
	return newOracle(o, h, objs, cp)
}

// OracleLog is the log inv (from NewOracle) reconstructed for tp.
func OracleLog(inv sim.Rule, topicPartition core.TopicPartition) []string {
	o := inv.(*expectedLogChecker)
	return o.logs[o.idx[topicPartition]].vals
}
