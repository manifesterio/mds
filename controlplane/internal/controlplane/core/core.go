package core

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// Config is the Core replica configuration.
type Config = replica.Config

// ConfigBuilder builds Config without outside packages writing fields.
type ConfigBuilder = replica.ConfigBuilder

// Replica is one control-plane node's copy of the replicated log.
type Replica = replica.Replica

// Factory builds a Replica for one incarnation of a node.
type Factory = replica.Factory

// CheckpointDecision reports what the checkpoint timer did and why.
type CheckpointDecision = replica.CheckpointDecision

// Engine is the ordered key-value store the fold keeps materialized state in.
type Engine = replica.Engine

// Entry is one log slot's command payload after decoding.
type Entry = metadb.Entry

// DB is the materialized control-plane state over an Engine.
type DB = metadb.DB

// NewConfig starts a ConfigBuilder with a zero Config.
func NewConfig() *ConfigBuilder { return replica.NewConfig() }

// New returns the Factory of the control-plane actor.
func New(config Config) Factory { return replica.New(config) }

// Props says how to run the replica.
func Props(config Config, keep func(*Replica)) actor.Props[runtime.EventData, runtime.IO] {
	return replica.Props(config, keep)
}

// EntryCommands decodes the commands of one log entry.
var EntryCommands = replica.EntryCommands

// EntryWriter reads the epoch and writer of an enveloped log entry.
var EntryWriter = replica.EntryWriter

// EntryOf is the log entry of a logged operation. It returns false for a read
// or an unknown operation.
func EntryOf(operation runtime.ControlPlaneOperation) (Entry, bool) {
	return metadb.EntryOf(operation)
}

// NewDB returns a fold over keyValue (empty, or restored from a checkpoint).
func NewDB(keyValue Engine) *DB { return metadb.New(keyValue) }
