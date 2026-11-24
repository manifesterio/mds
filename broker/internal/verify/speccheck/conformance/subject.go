package conformance

import (
	"github.com/manifesterio/mds/dst/verify/check/spectest"
	"github.com/manifesterio/mds/shared/objectstore"
)

// EnvPrefix is the prefix of the environment variables of the lockstep
// checks: MDS_SEED (run one seed), MDS_SEEDS (the number of checks) and
// MDS_CAMPAIGN (run until a time has passed). The parser is the one in
// dst/explore/campaign; see spectest.Lockstep.Check.
const EnvPrefix = "MDS"

// ObjectSubject is an object store under test. It is a spectest.Subject: Apply runs one operation and
// returns its result. A completion for a Put or a Delete must carry the same
// Outcome as the spec (Applied or NotApplied). The error is for broken test
// plumbing, such as a missing completion, and never for a wrong answer.
//
// A subject that can give the same state hash as objectstore.Spec also has the
// method StateHash() uint64 (spectest.HasStateHash). The run then compares it
// with the spec hash after every step. A subject with no spec hash, for
// example real S3, has no such method.
type ObjectSubject = spectest.Subject[objectstore.Call, ObjectResult]

// hasStateHash is spectest.HasStateHash, to ask a subject for its hash.
type hasStateHash = spectest.HasStateHash

// withoutHashObject hides the state hash of a subject. It embeds the interface
// type, so only Apply is promoted.
type withoutHashObject struct{ ObjectSubject }

// WithoutStateHashObject returns s with its state hash hidden, as if s were
// real S3.
func WithoutStateHashObject(s ObjectSubject) ObjectSubject { return withoutHashObject{s} }

// specObjectSubject is the object store spec as a subject.
type specObjectSubject struct{ store *objectstore.Spec }

// NewSpecObjectSubject returns a fresh objectstore.Spec as a subject. Use it
// to run a suite spec against spec, and as the correct subject that a planted
// bug wraps.
func NewSpecObjectSubject() ObjectSubject {
	return &specObjectSubject{store: objectstore.NewSpec()}
}

func (s *specObjectSubject) Apply(operation objectstore.Call) (ObjectResult, error) {
	return FromObjectResult(s.store.Exec(operation)), nil
}

func (s *specObjectSubject) StateHash() uint64 { return s.store.StateHash() }
