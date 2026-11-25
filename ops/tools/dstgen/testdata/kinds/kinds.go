// Package kinds is dstgen test input: every annotation and every field kind.
package kinds

import "github.com/manifesterio/mds/dst/core/trace"

// Message is what every case has.
type Message interface{ Fold(h *trace.Hash) }

// in is a sum.
//
//sumtype:decl
//dst:sum
type in interface {
	Message
	isIn()
}

// io is a second sum; get is in both.
//
//sumtype:decl
//dst:sum
type io interface {
	Message
	isIo()
}

type name string

type count int32

type id [4]byte

type point struct {
	X, Y int64
}

//dst:cases in
type (
	empty  struct{}
	scalar struct {
		B  bool
		I  int
		I8 int8
		N  count
		U  uint
		U8 uint8
		F  float64
		F2 float32
		S  string
		SN name
	}
	bytesAndArrays struct {
		Raw   []byte
		ID    id
		Pairs [2]int16
	}
	collections struct {
		Names  []string
		Points []point
		Counts map[string]int
		ByNum  map[uint32][]byte
		Nested map[name]map[int]bool
	}
	withMessages struct {
		Answer answer   // a generated payload, folded through its Fold
		Inner  in       // an interface with Fold: nil folds as 0
		List   []answer // a slice of payloads
		At     point    // a plain struct, folded inline
		Opt    map[string]answer
	}
)

// get is a request of io answered by answer.
//
//dst:cases io
//dst:answer answer
type get struct{ Key string }

// shared is a case of two sums.
//
//dst:cases io in
type shared struct{ N uint8 }

//dst:payload
type answer struct{ Key, Value string }

// legacy keeps its hand-written Fold.
//
//dst:cases io
//dst:fold manual
type legacy struct{ N int }

func (x legacy) Fold(h *trace.Hash) {
	h.Uint64(x.Tag())
	h.Int64(int64(x.N))
}

// actor state.
//
//dst:state
type actor struct {
	seen   map[string]count
	last   *answer `dst:"skip"`
	points []point
	name   name
}
