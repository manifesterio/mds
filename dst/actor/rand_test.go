package actor_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/rng"
)

func TestNoDrawEdges(t *testing.T) {
	src := rng.New(42)
	r := actor.NewRand(src)
	_ = r.Float64()
	_ = r.Intn(10)
	before := src.Calls()
	if r.Chance(0) {
		t.Fatal("Chance(0) true")
	}
	if r.Intn(0) != 0 || src.Calls() != before {
		t.Fatal("Intn(0) drew or wrong")
	}
	if got := r.Between(5, 5); got != 5 || src.Calls() != before {
		t.Fatal("Between(5,5) drew or wrong")
	}
	if r.Between(9, 3) != 9 || src.Calls() != before {
		t.Fatal("Between with hi < lo must return lo and draw nothing")
	}
	if !r.Chance(1) || src.Calls() != before+1 {
		t.Fatal("Chance(1) must be true and draw once")
	}
}

func TestRangesAndIntnSpread(t *testing.T) {
	r := actor.NewRand(rng.New(42))
	for range 2000 {
		if v := r.Intn(7); v < 0 || v >= 7 {
			t.Fatalf("Intn out of range: %d", v)
		}
		if f := r.Float64(); f < 0 || f >= 1 {
			t.Fatalf("Float64 out of range: %v", f)
		}
		if b := r.Between(-5, 5); b < -5 || b > 5 {
			t.Fatalf("Between out of range: %d", b)
		}
	}
	var buckets [3]int
	for range 30000 {
		buckets[r.Intn(3)]++
	}
	for _, b := range buckets {
		if b < 9000 || b > 11000 {
			t.Fatalf("Intn is skewed: %v", buckets)
		}
	}
}

func TestNewIDIsTwoLittleEndianDraws(t *testing.T) {
	id := actor.NewRand(rng.New(7)).NewID()
	b := rng.New(7)
	u0, u1 := b.Uint64(), b.Uint64()
	for i := range 8 {
		if id[i] != byte(u0>>(8*i)) || id[8+i] != byte(u1>>(8*i)) {
			t.Fatalf("byte %d", i)
		}
	}
}
