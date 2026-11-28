// Package specbridge lets the broker talk to the one control plane spec
// (controlplane/spec). The broker performs core.ControlPlaneCall and waits for
// core.ControlPlaneDone; the spec takes a contract.Operation and gives a
// contract.Done. This package only converts between the two. It has no rule of
// its own: every answer comes from the spec.
package specbridge

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/spec"
)

// ControlPlane is the control plane spec with an Exec for core calls. The
// spec methods StateHash, End and Apply are available on it too.
type ControlPlane struct{ *spec.ControlPlane }

// New returns an empty control plane spec.
func New() *ControlPlane { return &ControlPlane{spec.New()} }

// Exec runs one core.ControlPlaneCall and returns its core.ControlPlaneDone.
func (c *ControlPlane) Exec(call core.ControlPlaneCall) core.ControlPlaneDone {
	done := c.Apply(call.Op)
	return core.ControlPlaneDone{Resp: done.Resp, Err: done.Err, Outcome: done.Outcome}
}

// CreateTopic makes a topic (or grows it) and returns the error code.
func (c *ControlPlane) CreateTopic(name string, partitions int32) core.ErrCode {
	return c.Apply(contract.CreateTopic{Name: name, Partitions: partitions}).Err
}
