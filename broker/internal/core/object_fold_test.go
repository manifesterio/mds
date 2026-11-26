package core

import (
	"testing"

	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// The object store wrappers have a hand-written Fold: the tag of the wrapper, the
// number of the case, then the fields of the case. Each case below writes that layout
// by hand, so a change of Tag or Fold shows up as a different hash.

func hashOf(write func(h *trace.Hash)) uint64 {
	h := trace.New()
	write(&h)
	return uint64(h)
}

func TestObjectCallFoldLayout(t *testing.T) {
	ref := SegmentRef{Key: "seg/1", Off: 10, Len: -1}
	cases := []struct {
		name string
		call objectstore.Call
		want func(h *trace.Hash)
	}{
		{"put", objectstore.Put{Key: "k", Data: []byte("v")}, func(h *trace.Hash) {
			h.Uint64(108)
			h.Uint64(1)
			h.String("k")
			h.Bytes([]byte("v"))
		}},
		{"get", objectstore.Get{Ref: ref}, func(h *trace.Hash) {
			h.Uint64(108)
			h.Uint64(2)
			foldSegmentRef(h, ref)
		}},
		{"delete", objectstore.Delete{Key: "k"}, func(h *trace.Hash) {
			h.Uint64(108)
			h.Uint64(3)
			h.String("k")
		}},
		{"list", objectstore.List{Prefix: "p/"}, func(h *trace.Hash) {
			h.Uint64(108)
			h.Uint64(4)
			h.String("p/")
		}},
	}
	for _, c := range cases {
		a := ObjectCall{Call: c.call}
		if a.Tag() != 108 {
			t.Errorf("%s: tag %d, want 108", c.name, a.Tag())
		}
		if got, want := hashOf(a.Fold), hashOf(c.want); got != want {
			t.Errorf("%s: the hash differs from the layout", c.name)
		}
	}
}

func TestObjectDoneFoldLayout(t *testing.T) {
	cases := []struct {
		name   string
		result objectstore.Result
		want   func(h *trace.Hash)
	}{
		{"put", objectstore.PutDone{Key: "k", Err: ErrTimeout, Outcome: MaybeApplied}, func(h *trace.Hash) {
			h.Uint64(9)
			h.Uint64(1)
			h.String("k")
			h.Uint64(uint64(ErrTimeout))
			h.Uint64(uint64(MaybeApplied))
		}},
		{"get", objectstore.GetDone{Data: []byte("d"), Err: ErrNotFound}, func(h *trace.Hash) {
			h.Uint64(9)
			h.Uint64(2)
			h.Bytes([]byte("d"))
			h.Uint64(uint64(ErrNotFound))
		}},
		{"delete", objectstore.DeleteDone{Err: ErrIO, Outcome: Applied}, func(h *trace.Hash) {
			h.Uint64(9)
			h.Uint64(3)
			h.Uint64(uint64(ErrIO))
			h.Uint64(uint64(Applied))
		}},
		{"list", objectstore.ListDone{Keys: []string{"a", "b"}, Err: ErrUnavailable}, func(h *trace.Hash) {
			h.Uint64(9)
			h.Uint64(4)
			h.Uint64(uint64(ErrUnavailable))
			h.Uint64(2)
			h.String("a")
			h.String("b")
		}},
	}
	for _, c := range cases {
		e := ObjectDone{Result: c.result}
		if e.Tag() != 9 {
			t.Errorf("%s: tag %d, want 9", c.name, e.Tag())
		}
		if got, want := hashOf(e.Fold), hashOf(c.want); got != want {
			t.Errorf("%s: the hash differs from the layout", c.name)
		}
	}
}

// A wrapper with no call or result folds as its tag and a zero.
func TestObjectFoldOfNothing(t *testing.T) {
	if got, want := hashOf(ObjectCall{}.Fold), hashOf(func(h *trace.Hash) { h.Uint64(108); h.Uint64(0) }); got != want {
		t.Error("empty ObjectCall")
	}
	if got, want := hashOf(ObjectDone{}.Fold), hashOf(func(h *trace.Hash) { h.Uint64(9); h.Uint64(0) }); got != want {
		t.Error("empty ObjectDone")
	}
}
