package core

import "github.com/manifesterio/mds/dst/actor"

// Environment is the Env of a broker or simulated client: random numbers,
// buggify sites and assertions. It is the only way nondeterminism enters the
// core. Time is not here: it comes with each Receive. In production no site
// fires.
type Environment = actor.Env
