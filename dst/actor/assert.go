package actor

import "github.com/manifesterio/mds/dst/internal/catalog"

// AssertKind classifies how a property was recorded.
type AssertKind = catalog.Kind

// The kinds of property a run records.
const (
	KindAlways              = catalog.KindAlways
	KindAlwaysOrUnreachable = catalog.KindAlwaysOrUnreachable
	KindSometimes           = catalog.KindSometimes
	KindReachable           = catalog.KindReachable
	KindUnreachable         = catalog.KindUnreachable
)

// Pair is one ordered context field for a failure detail.
type Pair struct {
	Key   string
	Value any
}

// Typed handles. Each kind of assertion has its own type, so a Sometimes
// property cannot be used where an Always is needed. Make them only with the
// New* functions, in package-level var blocks. A declaration puts the name in
// the catalog (dst/internal/catalog), which a campaign reads; code records a
// handle through Env.

// Always names a property that must hold every time it is checked.
type Always struct{ name string }

// AlwaysOrUnreachable names a property that must hold every time it is
// checked; it is fine if the code never checks it.
type AlwaysOrUnreachable struct{ name string }

// Sometimes names a property that must hold at least once in a campaign.
type Sometimes struct{ name string }

// Reachable names a place in the code that a campaign must reach.
type Reachable struct{ name string }

// Unreachable names a place in the code that must never be reached.
type Unreachable struct{ name string }

// Name returns the name given to NewAlways.
func (p Always) Name() string { return p.name }

// Name returns the name given to NewAlwaysOrUnreachable.
func (p AlwaysOrUnreachable) Name() string { return p.name }

// Name returns the name given to NewSometimes.
func (p Sometimes) Name() string { return p.name }

// Name returns the name given to NewReachable.
func (p Reachable) Name() string { return p.name }

// Name returns the name given to NewUnreachable.
func (p Unreachable) Name() string { return p.name }

// NewAlways declares an Always property. Call it only in a package-level var
// block. A name declared twice panics at program start.
func NewAlways(name string) Always {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindAlways})
	return Always{name: name}
}

// NewAlwaysOnFailure declares an Always property that the code records only
// when it fails: a violation check, such as "run ends". It behaves as an
// Always, but a campaign does not list it as "never checked". Call it only in
// a package-level var block.
func NewAlwaysOnFailure(name string) Always {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindAlways, FailureOnly: true})
	return Always{name: name}
}

// NewAlwaysOrUnreachable declares an AlwaysOrUnreachable property. Call it
// only in a package-level var block.
func NewAlwaysOrUnreachable(name string) AlwaysOrUnreachable {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindAlwaysOrUnreachable})
	return AlwaysOrUnreachable{name: name}
}

// NewSometimes declares a Sometimes property. Call it only in a package-level
// var block.
func NewSometimes(name string) Sometimes {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindSometimes})
	return Sometimes{name: name}
}

// NewReachable declares a Reachable property. Call it only in a package-level
// var block.
func NewReachable(name string) Reachable {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindReachable})
	return Reachable{name: name}
}

// NewReachableNotExpected declares a Reachable property that a campaign may
// miss (a rare path that no scenario aims at). It behaves as a Reachable, but
// a campaign does not expect it: it is not listed as "never reached". Call it
// only in a package-level var block.
func NewReachableNotExpected(name string) Reachable {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindReachable, NotExpected: true})
	return Reachable{name: name}
}

// NewUnreachable declares an Unreachable property. Call it only in a
// package-level var block.
func NewUnreachable(name string) Unreachable {
	catalog.Declare(catalog.Declared{Name: name, Kind: catalog.KindUnreachable})
	return Unreachable{name: name}
}
