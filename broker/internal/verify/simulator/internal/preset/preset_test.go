package preset

import (
	"testing"

	"github.com/manifesterio/mds/dst/core/actor"
	"github.com/manifesterio/mds/dst/core/rng"
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
	Swarm(rng.New(1), &s)
	Swarm(rng.New(2), &s)
}
