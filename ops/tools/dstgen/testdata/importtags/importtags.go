// Package importtags is dstgen test input for -import.
package importtags

import "github.com/manifesterio/mds/dst/trace"

type a struct{}
type b struct{ N int }
type c struct{}
type notATag struct{}

func (a) Tag() uint64          { return 101 }
func (x a) Fold(h *trace.Hash) { h.Uint64(x.Tag()) }

// b has no Tag: its Fold starts with the literal.
func (x b) Fold(h *trace.Hash) {
	h.Uint64(7)
	h.Int64(int64(x.N))
}

func (c) Tag() uint64 { return 0x20 }

// notATag computes its tag: nothing to import.
func (notATag) Tag() uint64 { return uint64(len("x")) }
