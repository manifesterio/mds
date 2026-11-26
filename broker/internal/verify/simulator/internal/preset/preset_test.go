package preset

import (
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/actor/actortest"
)

func TestNewAndPhases(t *testing.T) {
	t.Parallel()
	p := New()
	if p.Phases != Phases() {
		t.Fatalf("New phases = %+v, want %+v", p.Phases, Phases())
	}
	if p.QuiesceEvery != actor.Second {
		t.Fatalf("New defaults QuiesceEvery=%d", p.QuiesceEvery)
	}
	if ProgressGraceTime != 6*actor.Second {
		t.Fatalf("ProgressGraceTime = %d", ProgressGraceTime)
	}
	if KafkaFaultCoverage.HardCrash.Name() == "" || KafkaFaultCoverage.GracefulStop.Name() == "" {
		t.Fatal("KafkaFaultCoverage handles missing names")
	}
}

func TestSwarm(t *testing.T) {
	t.Parallel()
	p := New()
	s := p.Settings
	Swarm(actortest.NewEnv(1).Rand, &s)
	Swarm(actortest.NewEnv(2).Rand, &s)
}
