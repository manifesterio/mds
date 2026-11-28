package actor

import "github.com/manifesterio/mds/dst/internal/core"

// Fact is a named, structured observation for the timeline. Rules (the
// checks of a run) read facts by name.
type Fact = core.Fact

// Field is one key and value of a Fact.
type Field = core.Field

// FieldValue is the value of one Field. The cases are Int64Value,
// Uint64Value, StringValue, Bool and Bytes.
type FieldValue = core.FieldValue

// The field value cases.
type (
	// Int64Value is a signed integer field value.
	Int64Value = core.Int64Value
	// Uint64Value is an unsigned integer field value.
	Uint64Value = core.Uint64Value
	// StringValue is a text field value.
	StringValue = core.StringValue
	// Bool is a true or false field value.
	Bool = core.Bool
	// Bytes is a byte slice field value.
	Bytes = core.Bytes
)
