// Package core is the control-plane Core: the SMR entrypoint used by server,
// cmd and the simulator. Outer packages import core for replica configuration
// and log-entry helpers; they do not import metadb, its engine, or the
// topic/group schemas.
package core
