package conformance

import (
	"fmt"
	"go/parser"
	"reflect"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/core/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

const (
	coverageSeeds = 500
	coverageOps   = 100
)

// minimumHits is the least number of times every profile must reach a case
// over all seeds and operations. A case that a profile reaches fewer times
// is a blind spot, so the test fails.
const minimumHits = 25

// onlyProfile lists the cases that one profile is built to reach. The other
// profiles may reach them, but the test does not require it.
var onlyProfile = map[string]string{"commit many batches (40 or more)": "many-batches"}

var objectCases = []string{
	"put accepted", "put empty key", "put empty data",
	"get accepted", "get to end of object (Len <= 0)", "get missing key", "get out of range", "get empty key",
	"delete accepted", "delete missing key",
	"list empty prefix", "list prefix with slash", "list prefix without slash", "list with keys", "list without keys",
}

func TestObjectCoverage(t *testing.T) {
	table := map[string]map[string]int{}
	for _, p := range InputProfiles() {
		table[p.Name] = objectCoverage(p)
	}
	reportCoverage(t, "object store", objectCases, table)
}

// reportCoverage prints one table and fails for every case that a profile
// reaches too few times.
func reportCoverage(t *testing.T, title string, cases []string, table map[string]map[string]int) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "coverage of the %s generator, %d seeds of %d operations\n%-38s", title, coverageSeeds, coverageOps, "case")
	for _, p := range InputProfiles() {
		fmt.Fprintf(&b, "%14s", p.Name)
	}
	b.WriteString("\n")
	for _, c := range cases {
		fmt.Fprintf(&b, "%-38s", c)
		for _, p := range InputProfiles() {
			fmt.Fprintf(&b, "%14d", table[p.Name][c])
		}
		b.WriteString("\n")
	}
	t.Log("\n" + b.String())
	for _, p := range InputProfiles() {
		for _, c := range cases {
			if only, ok := onlyProfile[c]; ok && only != p.Name {
				continue
			}
			if n := table[p.Name][c]; n < minimumHits {
				t.Errorf("profile %s reached the case %q only %d times, the minimum is %d.", p.Name, c, n, minimumHits)
			}
		}
	}
}

// sequence returns the coverageOps operations that a generator draws for seed.
// The same seed always gives the same list, so the coverage numbers are the
// same on every run.
func sequence[V any](gen *rapid.Generator[V], seed int) []V {
	return rapid.SliceOfN(gen, coverageOps, coverageOps).Example(seed)
}

func objectCoverage(p InputProfile) map[string]int {
	hits := map[string]int{}
	gen := ObjectOps(p)
	for seed := 1; seed <= coverageSeeds; seed++ {
		spec := objectstore.NewSpec()
		for _, operation := range sequence(gen, seed) {
			for _, c := range classifyObject(spec, operation) {
				hits[c]++
			}
		}
	}
	return hits
}

// classifyObject runs op on the spec and names the cases that it reached.
func classifyObject(spec *objectstore.Spec, operation objectstore.Call) []string {
	done := spec.Exec(operation)
	switch operation := operation.(type) {
	case objectstore.Put:
		if done.(objectstore.PutDone).Err == core.ErrInvalid {
			return []string{"put empty key"}
		}
		if len(operation.Data) == 0 {
			return []string{"put accepted", "put empty data"}
		}
		return []string{"put accepted"}
	case objectstore.Get:
		return classifyGet(operation, done.(objectstore.GetDone))
	case objectstore.Delete:
		if done.(objectstore.DeleteDone).Err == core.ErrNotFound {
			return []string{"delete missing key"}
		}
		return []string{"delete accepted"}
	case objectstore.List:
		return classifyList(operation, done.(objectstore.ListDone))
	}
	return nil
}

func classifyGet(operation objectstore.Get, done objectstore.GetDone) []string {
	var out []string
	if operation.Ref.Key == "" {
		out = append(out, "get empty key")
	}
	switch done.Err {
	case core.ErrNotFound:
		return append(out, "get missing key")
	case core.ErrInvalid:
		return append(out, "get out of range")
	}
	out = append(out, "get accepted")
	if operation.Ref.Len <= 0 {
		out = append(out, "get to end of object (Len <= 0)")
	}
	return out
}

func classifyList(operation objectstore.List, done objectstore.ListDone) []string {
	var out []string
	switch {
	case operation.Prefix == "":
		out = append(out, "list empty prefix")
	case strings.HasSuffix(operation.Prefix, "/"):
		out = append(out, "list prefix with slash")
	default:
		out = append(out, "list prefix without slash")
	}
	if len(done.Keys) > 0 {
		return append(out, "list with keys")
	}
	return append(out, "list without keys")
}

// The generators take all their numbers from rapid, so one seed must always
// give one list. A hidden source of chance (a map order, the clock) would break
// this and make a failure impossible to replay.
func TestSameSeedGivesSameSequence(t *testing.T) {
	for _, p := range InputProfiles() {
		objA, objB := objectSequence(p, 7), objectSequence(p, 7)
		if !reflect.DeepEqual(objA, objB) {
			t.Errorf("profile %s: two runs of the object generator with seed 7 gave different operations.", p.Name)
		}
	}
}

func objectSequence(p InputProfile, seed int) []objectstore.Call {
	return sequence(ObjectOps(p), seed)
}

// recordingObject keeps the request number of every call it gets.
type recordingObject struct{ reqs []machine.RequestID }

func (r *recordingObject) Apply(operation objectstore.Call) (ObjectResult, error) {
	r.reqs = append(r.reqs, operation.RequestID())
	return ObjectResult{}, nil
}

// The generators make operations with no request id. The pair of the suite
// must give the call at position i the id i+1, and no other id.
func TestRequestIDsFollowPosition(t *testing.T) {
	objects := &recordingObject{}
	pair := &ObjectPair{mirror: objectstore.NewSpec(), subject: objects}
	for _, operation := range objectSequence(Default(), 3)[:50] {
		if _, err := pair.Apply(operation); err != nil {
			t.Fatal(err)
		}
	}
	for i, got := range objects.reqs {
		if got != machine.RequestID(i+1) {
			t.Fatalf("object operation %d has request id %d, want %d.", i, got, i+1)
		}
	}
}

func TestFormatIsAGoExpression(t *testing.T) {
	for _, p := range InputProfiles() {
		objects := objectSequence(p, 9)
		for i := range coverageOps {
			text := FormatObjectOp(objects[i])
			if _, err := parser.ParseExpr(text); err != nil {
				t.Fatalf("the text %q is not a Go expression: %v", text, err)
			}
		}
	}
}

func TestFormatExamples(t *testing.T) {
	cases := []struct{ got, want string }{
		{FormatObjectOp(objectstore.Put{Key: "a", Data: []byte("hi")}), `objectstore.Put{Key: "a", Data: []byte("hi")}`},
		{FormatObjectOp(objectstore.Put{Key: "", Data: []byte{}}), `objectstore.Put{Key: "", Data: []byte{}}`},
		{FormatObjectOp(objectstore.List{Prefix: "a/"}), `objectstore.List{Prefix: "a/"}`},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("the format gave %s, want %s.", c.got, c.want)
		}
	}
}

// The profile weights are chances. The integer generators of rapid favor small
// values on purpose, so the generators here draw fair bits. This test draws
// many numbers and checks that each value comes up as often as its weight says.
func TestDrawsFollowTheWeights(t *testing.T) {
	const draws = 20000
	weights := []int{3, 2, 1, 1}
	gen := Custom(func(r Rand) int { return chooseByWeight(r, weights...) })
	counts := make([]int, len(weights))
	for seed := 1; seed <= draws; seed++ {
		counts[gen.Example(seed)]++
	}
	for i, w := range weights {
		want := float64(draws) * float64(w) / 7
		if got := float64(counts[i]); got < want*0.93 || got > want*1.07 {
			t.Errorf("kind %d (weight %d) came up %d times in %d draws, want about %.0f.", i, w, counts[i], draws, want)
		}
	}
	percentGen := Custom(func(r Rand) bool { return percent(r, 12) })
	hits := 0
	for seed := 1; seed <= draws; seed++ {
		if percentGen.Example(seed) {
			hits++
		}
	}
	if want := float64(draws) * 0.12; float64(hits) < want*0.93 || float64(hits) > want*1.07 {
		t.Errorf("a chance of 12 percent came up %d times in %d draws, want about %.0f.", hits, draws, want)
	}
}
