// Package localstore opens on-disk Engines for the control-plane binary.
// The simulator does not import this package.
package localstore

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/pebble"
)

// OpenPebble opens an empty Pebble Engine in dir.
func OpenPebble(dir string) (engine.Engine, error) {
	return pebble.Open(dir, pebble.Options{})
}
