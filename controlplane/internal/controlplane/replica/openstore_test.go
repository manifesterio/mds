package replica_test

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/actor"
)

// trackedStore is an in-memory store that remembers whether it was closed.
type trackedStore struct {
	*memory.Engine
	closed bool
}

func (s *trackedStore) Close() error {
	s.closed = true
	return nil
}

// storeSource is a Config.OpenStore that counts its calls and can fail on one.
type storeSource struct {
	opened []*trackedStore
	failOn int // 1-based call number that fails; 0 never fails
}

func (s *storeSource) open() (engine.Engine, error) {
	if s.failOn == len(s.opened)+1 {
		return nil, errors.New("disk is full")
	}
	st := &trackedStore{Engine: memory.New()}
	s.opened = append(s.opened, st)
	return st, nil
}

func tombConfig(src *storeSource) replica.Config {
	return replica.Config{
		CatchUpEvery: actor.Millisecond, Writer: "w",
		CheckpointPrefix: "checkpoint/t/", CheckpointEvery: actor.Second, Rank: 1,
		OpenStore: src.open,
	}
}

func TestOpenStoreFailureAtBuildStopsOnStart(t *testing.T) {
	h := newHarness(t, tombConfig(&storeSource{failOn: 1}))
	effects := h.recv(runtime.NodeStarted{})
	if len(effects) != 1 || effects[0].Kind != actor.EffectStop || effects[0].How != actor.Trip || effects[0].Cause != "disk is full" {
		t.Fatalf("effects = %v, want only a Stop (Trip) with the open error", describe(effects))
	}
	if again := h.recv(runtime.CatchUpDue{}); len(again) != 0 {
		t.Fatalf("a stopped replica recorded %v", describe(again))
	}
}
