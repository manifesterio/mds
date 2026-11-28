package rng_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/internal/rng"
)

func TestSeed42Stream(t *testing.T) {
	want := []uint64{
		0xbdd732262feb6e95,
		0x28efe333b266f103,
		0x47526757130f9f52,
		0x581ce1ff0e4ae394,
		0x09bc585a244823f2,
		0xde4431fa3c80db06,
		0x37e9671c45376d5d,
		0xccf635ee9e9e2fa4,
	}
	r := rng.New(42)
	for i, w := range want {
		if got := r.Uint64(); got != w {
			t.Fatalf("draw %d: got %016x want %016x", i, got, w)
		}
	}
	if r.Calls() != 8 {
		t.Fatalf("Calls = %d, want 8", r.Calls())
	}
}

// The values below were made by the old Derive of the broker kernel
// (CountedRand.Derive). The formula must not change.
func TestDeriveMatchesTheOldFormula(t *testing.T) {
	cases := []struct {
		seed  uint64
		label string
		want  uint64
	}{
		{99, "swarm", 0xf59e9e20f6e82e0b},
		{5, "x", 0x8b0268362b66c315},
		{0, "", 0x7261e9031e12a9d7},
	}
	for _, c := range cases {
		if got := rng.New(c.seed).Derive(c.label).Uint64(); got != c.want {
			t.Fatalf("seed %d label %q: got %#x want %#x", c.seed, c.label, got, c.want)
		}
	}
}

func TestDeriveIndependence(t *testing.T) {
	r := rng.New(99)
	before := r.Calls()
	a, b := r.Derive("swarm"), r.Derive("buggify")
	if r.Calls() != before {
		t.Fatal("Derive must not draw from the parent")
	}
	if a.Uint64() == b.Uint64() {
		t.Fatal("different labels must give different streams")
	}
	// Drawing from the parent never moves a derived stream.
	p := rng.New(5)
	d1 := p.Derive("x").Uint64()
	for range 10 {
		p.Uint64()
	}
	if p.Derive("x").Uint64() != d1 {
		t.Fatal("the derived stream depends on the position of the parent")
	}
	if rng.New(5).Derive("x").Uint64() == rng.New(6).Derive("x").Uint64() {
		t.Fatal("the derived stream ignores the seed")
	}
}

func TestSeedZeroReference(t *testing.T) {
	r := rng.New(0)
	for i, want := range []uint64{0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4, 0x06c45d188009454f} {
		if got := r.Uint64(); got != want {
			t.Fatalf("draw %d: got %#x want %#x", i, got, want)
		}
	}
}
