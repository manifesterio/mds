package actor_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/actor"
)

var (
	siteA = actor.NewSite("fault test: a")
	siteB = actor.NewSite("fault test: b")
	siteF = actor.NewFaultSite("fault test: fault only")
)

func TestSitesLeavesOutFaultSites(t *testing.T) {
	if slices.Contains(actor.Sites(), siteF.Name()) {
		t.Fatal("a Fault site must not be in Sites")
	}
}

func TestFaultSiteNameIsUnique(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a second declaration of a Fault site name must panic")
		}
	}()
	actor.NewSite(siteF.Name())
}

func TestSitesListsEveryDeclaration(t *testing.T) {
	got := actor.Sites()
	if !slices.IsSorted(got) {
		t.Fatalf("not sorted: %v", got)
	}
	if !slices.Contains(got, siteA.Name()) || !slices.Contains(got, siteB.Name()) {
		t.Fatalf("missing sites: %v", got)
	}
}

func TestDeclaringASiteTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a second declaration of one site must panic")
		}
	}()
	actor.NewSite(siteA.Name())
}

func TestEmptySiteNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an empty site name must panic")
		}
	}()
	actor.NewSite("")
}
