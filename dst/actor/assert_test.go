package actor_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/internal/catalog"
)

var (
	catAlways      = actor.NewAlways("assert test: always")
	catOrUnreached = actor.NewAlwaysOrUnreachable("assert test: always or unreachable")
	catSometimes   = actor.NewSometimes("assert test: sometimes")
	catReachable   = actor.NewReachable("assert test: reachable")
	catUnreachable = actor.NewUnreachable("assert test: unreachable")
	catOnFailure   = actor.NewAlwaysOnFailure("assert test: on failure")
	catNotExpected = actor.NewReachableNotExpected("assert test: not expected")
)

func TestCatalogFlags(t *testing.T) {
	flags := map[string]catalog.Declared{}
	for _, d := range catalog.Asserts() {
		flags[d.Name] = d
	}
	if d := flags[catOnFailure.Name()]; d.Kind != actor.KindAlways || !d.FailureOnly || d.NotExpected {
		t.Fatalf("on failure: %+v", d)
	}
	if d := flags[catNotExpected.Name()]; d.Kind != actor.KindReachable || !d.NotExpected || d.FailureOnly {
		t.Fatalf("not expected: %+v", d)
	}
	if d := flags[catAlways.Name()]; d.FailureOnly || d.NotExpected {
		t.Fatalf("plain always: %+v", d)
	}
}

func TestCatalogListsEveryDeclaration(t *testing.T) {
	want := map[string]actor.AssertKind{
		catAlways.Name():      actor.KindAlways,
		catOrUnreached.Name(): actor.KindAlwaysOrUnreachable,
		catSometimes.Name():   actor.KindSometimes,
		catReachable.Name():   actor.KindReachable,
		catUnreachable.Name(): actor.KindUnreachable,
	}
	got := catalog.Asserts()
	names := make([]string, 0, len(got))
	for _, d := range got {
		names = append(names, d.Name)
		if k, ok := want[d.Name]; ok && k != d.Kind {
			t.Fatalf("%s: kind %s, want %s", d.Name, d.Kind, k)
		}
	}
	if !slices.IsSorted(names) {
		t.Fatalf("catalog is not sorted: %v", names)
	}
	for n := range want {
		if !slices.Contains(names, n) {
			t.Fatalf("%s is missing from the catalog", n)
		}
	}
}

func TestDeclaringANameTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a second declaration of one name must panic")
		}
	}()
	actor.NewSometimes(catAlways.Name())
}

func TestEmptyNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an empty name must panic")
		}
	}()
	actor.NewReachable(" ")
}
