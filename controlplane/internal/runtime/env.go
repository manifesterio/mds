package runtime

import "github.com/manifesterio/mds/dst/actor"

// Environment is the Env of a control-plane actor: random numbers, buggify
// sites and assertions. In production no site fires.
type Environment = actor.Env
