package sim

// ring keeps the last N opaque step records for failure reports.
// T is caller-defined. dst does not format domain events.
type ring[T any] struct {
	buf []T
	pos int
	n   int
}

// newRing returns a ring that retains at most n records. n <= 0 yields an empty ring.
func newRing[T any](n int) *ring[T] {
	if n <= 0 {
		return &ring[T]{}
	}
	return &ring[T]{buf: make([]T, n)}
}

// Add appends v, overwriting the oldest entry when full.
func (r *ring[T]) Add(v T) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.pos%len(r.buf)] = v
	r.pos++
	if r.n < len(r.buf) {
		r.n++
	}
}

// Tail returns the retained records oldest first.
func (r *ring[T]) Tail() []T {
	if r.n == 0 {
		return nil
	}
	out := make([]T, 0, r.n)
	first := max(0, r.pos-len(r.buf))
	for i := first; i < r.pos; i++ {
		out = append(out, r.buf[i%len(r.buf)])
	}
	return out
}
