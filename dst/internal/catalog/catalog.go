// Package catalog is the registry of declared properties and buggify sites.
// The New* constructors in package actor add to it while packages
// initialize; a campaign reads it to list what never fired or never held.
package catalog

import (
	"slices"
	"strings"
)

// Kind classifies how a property was recorded.
type Kind string

// The kinds of property a run records.
const (
	KindAlways              Kind = "always"
	KindAlwaysOrUnreachable Kind = "always_or_unreachable"
	KindSometimes           Kind = "sometimes"
	KindReachable           Kind = "reachable"
	KindUnreachable         Kind = "unreachable"
)

// Declared is one entry of the property catalog.
type Declared struct {
	Name string
	Kind Kind
	// FailureOnly is set for an Always that the code records only when it
	// fails (a violation check). A campaign never checks it on a good run, so
	// the report does not list it as "never checked".
	FailureOnly bool
	// NotExpected is set for a Reachable that a campaign may miss. The report
	// does not list it as "never reached" and does not count it as expected.
	NotExpected bool
}

// declared is the catalog: every name made by a New* function, with its kind.
// The New* functions are called only from package-level var blocks, so the
// catalog is complete before main or a test starts, and it never changes after
// that.
//
//determinismcheck:ok written only while packages initialize; read-only after that
var declared = map[string]Declared{}

// sites is the catalog of every site name. The value is true for a site that
// the injector counts. Same rules as declared.
//
//determinismcheck:ok written only while packages initialize; read-only after that
var sites = map[string]bool{}

// Declare adds a property to the catalog. A name declared twice is a
// programming error, so it panics at program start.
func Declare(d Declared) {
	name, k := d.Name, d.Kind
	if strings.TrimSpace(name) == "" {
		panic("assert: an assertion name must not be empty") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	if old, dup := declared[name]; dup {
		panic("assert: the name " + name + " is declared twice (" + string(old.Kind) + " and " + string(k) + ")") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	declared[name] = d
}

// DeclareSite adds a site to the catalog. counted is true for a site that the
// injector counts. An empty or repeated name panics at program start.
func DeclareSite(name string, counted bool) {
	if strings.TrimSpace(name) == "" {
		panic("fault: a site name must not be empty") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	if _, dup := sites[name]; dup {
		panic("fault: the site " + name + " is declared twice") //nolint:forbidigo // a bad declaration must stop the program at start
	}
	sites[name] = counted
}

// Asserts returns every declared property, sorted by name. A campaign uses it
// to list properties that were never reached or never checked.
func Asserts() []Declared {
	names := make([]string, 0, len(declared))
	//determinismcheck:ok the names are sorted below
	for n := range declared {
		names = append(names, n)
	}
	slices.Sort(names)
	out := make([]Declared, 0, len(names))
	for _, n := range names {
		out = append(out, declared[n])
	}
	return out
}

// Sites returns the name of every declared site that the injector counts,
// sorted. A site made by NewFaultSite is not in the list.
func Sites() []string {
	out := make([]string, 0, len(sites))
	//determinismcheck:ok the names are sorted below
	for n, counted := range sites {
		if counted {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}
