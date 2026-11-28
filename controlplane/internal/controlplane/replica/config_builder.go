package replica

import (
	"github.com/manifesterio/mds/dst/actor"
)

// ConfigBuilder builds Config without outside packages writing fields.
type ConfigBuilder struct {
	cfg Config
}

// NewConfig starts a ConfigBuilder with a zero Config.
func NewConfig() *ConfigBuilder {
	return &ConfigBuilder{}
}

// Build returns the configured Config.
func (b *ConfigBuilder) Build() Config {
	return b.cfg
}

// WithCatchUpEvery sets CatchUpEvery.
func (b *ConfigBuilder) WithCatchUpEvery(d actor.Duration) *ConfigBuilder {
	b.cfg.CatchUpEvery = d
	return b
}

// WithReadLimit sets ReadLimit.
func (b *ConfigBuilder) WithReadLimit(n int32) *ConfigBuilder {
	b.cfg.ReadLimit = n
	return b
}

// WithWriter sets Writer.
func (b *ConfigBuilder) WithWriter(name string) *ConfigBuilder {
	b.cfg.Writer = name
	return b
}

// WithOnCheckpointDecision sets OnCheckpointDecision.
func (b *ConfigBuilder) WithOnCheckpointDecision(fn func(CheckpointDecision)) *ConfigBuilder {
	b.cfg.OnCheckpointDecision = fn
	return b
}

// WithMaxCmdsPerSlot sets MaxCmdsPerSlot.
func (b *ConfigBuilder) WithMaxCmdsPerSlot(n int) *ConfigBuilder {
	b.cfg.MaxCmdsPerSlot = n
	return b
}

// WithQueueMax sets QueueMax.
func (b *ConfigBuilder) WithQueueMax(n int) *ConfigBuilder {
	b.cfg.QueueMax = n
	return b
}

// WithMaxEntryBytes sets MaxEntryBytes.
func (b *ConfigBuilder) WithMaxEntryBytes(n int) *ConfigBuilder {
	b.cfg.MaxEntryBytes = n
	return b
}

// WithRank sets Rank.
func (b *ConfigBuilder) WithRank(n int) *ConfigBuilder {
	b.cfg.Rank = n
	return b
}

// WithHeartbeat sets Heartbeat.
func (b *ConfigBuilder) WithHeartbeat(d actor.Duration) *ConfigBuilder {
	b.cfg.Heartbeat = d
	return b
}

// WithTakeoverAfter sets TakeoverAfter.
func (b *ConfigBuilder) WithTakeoverAfter(d actor.Duration) *ConfigBuilder {
	b.cfg.TakeoverAfter = d
	return b
}

// WithTakeoverRankStep sets TakeoverRankStep.
func (b *ConfigBuilder) WithTakeoverRankStep(d actor.Duration) *ConfigBuilder {
	b.cfg.TakeoverRankStep = d
	return b
}

// WithCheckpointEvery sets CheckpointEvery.
func (b *ConfigBuilder) WithCheckpointEvery(d actor.Duration) *ConfigBuilder {
	b.cfg.CheckpointEvery = d
	return b
}

// WithCheckpointMinSlots sets CheckpointMinSlots.
func (b *ConfigBuilder) WithCheckpointMinSlots(n int64) *ConfigBuilder {
	b.cfg.CheckpointMinSlots = n
	return b
}

// WithCheckpointPrefix sets CheckpointPrefix.
func (b *ConfigBuilder) WithCheckpointPrefix(prefix string) *ConfigBuilder {
	b.cfg.CheckpointPrefix = prefix
	return b
}

// WithKeepSlots sets KeepSlots.
func (b *ConfigBuilder) WithKeepSlots(n int64) *ConfigBuilder {
	b.cfg.KeepSlots = n
	return b
}

// WithOpenStore sets OpenStore.
func (b *ConfigBuilder) WithOpenStore(fn func() (Engine, error)) *ConfigBuilder {
	b.cfg.OpenStore = fn
	return b
}

// WithWindowSlots sets WindowSlots.
func (b *ConfigBuilder) WithWindowSlots(n int64) *ConfigBuilder {
	b.cfg.WindowSlots = n
	return b
}

// WithTrimStride sets TrimStride.
func (b *ConfigBuilder) WithTrimStride(n int64) *ConfigBuilder {
	b.cfg.TrimStride = n
	return b
}

// WithStallProb sets StallProb.
func (b *ConfigBuilder) WithStallProb(p float64) *ConfigBuilder {
	b.cfg.StallProb = p
	return b
}

// WithCrashProb sets CrashProb.
func (b *ConfigBuilder) WithCrashProb(p float64) *ConfigBuilder {
	b.cfg.CrashProb = p
	return b
}

// WithStallDur sets StallDur.
func (b *ConfigBuilder) WithStallDur(d [2]actor.Duration) *ConfigBuilder {
	b.cfg.StallDur = d
	return b
}

// WithDownDur sets DownDur.
func (b *ConfigBuilder) WithDownDur(d [2]actor.Duration) *ConfigBuilder {
	b.cfg.DownDur = d
	return b
}
