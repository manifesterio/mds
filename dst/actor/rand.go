package actor

import "math/bits"

// Source is a stream of uniform 64-bit values. Rand turns it into every other
// draw, so one algorithm serves the simulator, the live runtime and tests.
type Source interface {
	Uint64() uint64
}

// Rand is the seeded random stream of the run, as an actor sees it. The same
// Source and the same call sequence give the same values. It is not safe for
// concurrent use.
type Rand struct{ src Source }

// NewRand returns a Rand that draws from src.
func NewRand(src Source) *Rand { return &Rand{src: src} }

// Uint64 returns the next value of the Source.
func (r *Rand) Uint64() uint64 { return r.src.Uint64() }

// Intn returns a value in [0, n). It returns 0, and draws nothing, when n <= 0.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	bound := uint64(n)
	hi, lo := bits.Mul64(r.Uint64(), bound)
	if lo < bound {
		threshold := -bound % bound
		for lo < threshold {
			hi, lo = bits.Mul64(r.Uint64(), bound)
		}
	}
	return int(hi)
}

// Between returns a value in [lo, hi], both ends included. It returns lo, and
// draws nothing, when hi <= lo.
func (r *Rand) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo+1)))
}

// Float64 returns a value in [0, 1).
func (r *Rand) Float64() float64 { return float64(r.Uint64()>>11) / (1 << 53) }

// Chance is true with probability p. It draws nothing when p <= 0.
func (r *Rand) Chance(p float64) bool { return p > 0 && r.Float64() < p }

// Duration is Between for durations.
func (r *Rand) Duration(lo, hi Duration) Duration {
	return Duration(r.Between(int64(lo), int64(hi)))
}

// NewID draws two Uint64 values and puts them in the id, little-endian.
func (r *Rand) NewID() ID {
	var id ID
	for i := range 2 {
		v := r.Uint64()
		for j := range 8 {
			id[8*i+j] = byte(v >> (8 * j))
		}
	}
	return id
}
