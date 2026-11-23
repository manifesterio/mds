package conformance

import (
	"fmt"

	"github.com/manifesterio/mds/shared/objectstore"
)

// The Format functions turn one operation into a Go expression, for the
// failure report of a lockstep run. A failing run prints its shrunk operations
// with them, so you can paste the list into a regression test.
//
// The text has no request id. The run numbers the calls itself, from 1, in
// order, so a pasted list must go through the same run code.

// FormatObjectOp returns op as a Go expression.
func FormatObjectOp(operation objectstore.Call) string {
	switch operation := operation.(type) {
	case objectstore.Put:
		return fmt.Sprintf("objectstore.Put{Key: %q%s}", operation.Key, formatDataField(operation.Data))
	case objectstore.Get:
		return fmt.Sprintf("objectstore.Get{Ref: %s}", formatRef(operation.Ref))
	case objectstore.Delete:
		return fmt.Sprintf("objectstore.Delete{Key: %q}", operation.Key)
	case objectstore.List:
		return fmt.Sprintf("objectstore.List{Prefix: %q}", operation.Prefix)
	}
	return fmt.Sprintf("%#v", operation)
}

// formatDataField writes the Data field with a leading comma, or nothing when
// Data is nil. It keeps nil and empty data apart.
func formatDataField(data []byte) string {
	switch {
	case data == nil:
		return ""
	case len(data) == 0:
		return ", Data: []byte{}"
	}
	return fmt.Sprintf(", Data: []byte(%q)", string(data))
}

func formatRef(ref objectstore.SegmentRef) string {
	return fmt.Sprintf("objectstore.SegmentRef{Key: %q, Off: %d, Len: %d}", ref.Key, ref.Off, ref.Len)
}
