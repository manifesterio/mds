// Package rng is the SplitMix64 source of the seeded random stream of one
// run. The draws (Intn, Chance, ...) are actor.Rand over this source.
package rng

// Rand is the single seeded source of one run (SplitMix64).
// Invariant: same seed and same call sequence produce the same values.
// Not safe for concurrent use. One Rand per run, owned by the caller.
type Rand struct {
	seed  uint64 // the seed given to New; Derive reads it
	state uint64
	calls uint64
}

// New returns a stream for seed. Seed 0 is a valid seed.
func New(seed uint64) *Rand { return &Rand{seed: seed, state: seed} }

// Derive returns an independent stream for (seed, label). It reads only the
// seed of r and draws nothing, so a new label never moves the stream of r.
// The same seed and label always give the same stream.
// Formula: New(mix(seed ^ mix(fnv1a(label)))). fnv1a has no length prefix.
func (r *Rand) Derive(label string) *Rand {
	return New(mix64(r.seed ^ mix64(fnv1a(label))))
}

// fnv1a is the 64-bit FNV-1a hash of the bytes of s.
func fnv1a(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := range len(s) {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}

func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Uint64 returns the next value.
func (r *Rand) Uint64() uint64 {
	r.calls++
	r.state += 0x9e3779b97f4a7c15
	return mix64(r.state)
}

// Calls is the number of draws so far.
func (r *Rand) Calls() uint64 { return r.calls }
