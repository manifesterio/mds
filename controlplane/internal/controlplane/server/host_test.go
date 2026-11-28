package server

import "testing"

// The role bit changes on an edge only, and each change is one RoleChanged.
func TestRoleChangedOnEdgesOnly(t *testing.T) {
	n := New(Config{})
	var got []bool
	for _, active := range []bool{false, true, true, false, false, true} {
		if n.roleEdge(active) {
			got = append(got, active)
		}
	}
	if len(got) != 3 || !got[0] || got[1] || !got[2] {
		t.Fatalf("RoleChanged %v, want [true false true]", got)
	}
}
