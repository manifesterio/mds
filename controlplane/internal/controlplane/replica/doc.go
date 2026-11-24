// Package replica is one control-plane node's copy of the replicated log.
// It keeps the node in step with the log. Roles are leader or standby. It does
// catch-up, checkpoint, and restore. Each log entry is applied to the metadb.
//
// It is an actor (actor.Actor with runtime.EventData as input and runtime.IO as
// its product IO), so one Receive runs under the simulator and under the
// production host. This package is an extractable unit under
// controlplane/: it may import github.com/manifesterio/mds/controlplane/internal/runtime and
// other controlplane packages, but not broker, simulator, system, cmd, spec, kafkawire,
// format, net/http, Pebble, or the Dynamo SDK.
//
// Log I/O goes out only as Log* requests (actor.Perform). Catch-up reads the log, applies
// entries in slot order through an in-memory metadb fold. The log has no
// empty slot below its tail: a node that finds its next slot missing while a
// later one exists fell behind a trim and restores from the checkpoint. Client
// commits enter via Offer into a bounded queue; flush packs up to MaxCmdsPerSlot
// commands into one entry and keeps one LogAppend in flight. Answers leave as a
// Reply to the caller once the winning slot applies.
//
// One node writes at a time. A node becomes the writer by claiming a new epoch
// in the log (LogClaimEpoch), then writes a fence entry at the tail; every entry
// it writes carries the epoch. A node that sees a newer epoch, or whose put is
// refused below the floor of the log, stops writing and stays a standby that
// still reads. Checkpoints publish metadb snapshots via ObjectPut and
// trim via LogDeleteOld; NodeStarted restores then catch-up before Ready, and a
// node that is restoring never claims and never writes.
package replica
