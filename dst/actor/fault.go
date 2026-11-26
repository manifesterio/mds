package actor

import "github.com/manifesterio/mds/dst/internal/catalog"

// Site is a declared buggify site: a named place in the code where a
// fault may be injected on purpose. Make it only with NewSite, in a
// package-level var block. The declaration puts the name in the catalog (see
// Sites), so a campaign can list the sites that never fired.
type Site struct{ name string }

// Name returns the name given to NewSite.
func (s Site) Name() string { return s.name }

// NewSite declares a buggify site. Call it only in a package-level var block.
// A name declared twice, or an empty name, panics at program start.
func NewSite(name string) Site {
	return declareSite(name, true)
}

// NewFaultSite declares a site that the code asks with Env.Fault. Sites
// leaves it out, and a campaign does not list it as "never fired". Call it
// only in a package-level var block.
func NewFaultSite(name string) Site {
	return declareSite(name, false)
}

func declareSite(name string, counted bool) Site {
	catalog.DeclareSite(name, counted)
	return Site{name: name}
}

// Sites returns the name of every declared site that the injector counts,
// sorted. A site made by NewFaultSite is not in the list.
func Sites() []string { return catalog.Sites() }
