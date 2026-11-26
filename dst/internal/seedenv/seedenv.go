// Package seedenv reads the environment variables that choose seeds: <prefix>_SEEDS,
// <prefix>_SEED and <prefix>_CAMPAIGN. campaign and spectest share it, so the
// two parse them the same way.
package seedenv

import (
	"fmt"
	"strconv"
	"time"
)

// Settings are the values of the three variables <prefix>_SEEDS,
// <prefix>_SEED and <prefix>_CAMPAIGN. A variable that is not set (or is
// empty) leaves its field at the zero value.
type Settings struct {
	// Seeds is the value of <prefix>_SEEDS: the number of seeds (1 or more).
	// 0: not set.
	Seeds int
	// HasSeed is true when <prefix>_SEED is set. Seed is its value (0 or more).
	HasSeed bool
	Seed    uint64
	// Duration is the value of <prefix>_CAMPAIGN, for example 30s. 0: not set.
	Duration time.Duration
}

// Read reads the variables of prefix with getenv. A bad value is an error,
// because a silent fallback would hide a typing mistake. An empty value counts
// as not set, so that "VAR=" in a shell is not a bad value. This is the one
// place that parses these variables: campaign.ParseOptions uses it, and so does
// the spec toolkit (dst/spectest) for its tests.
func Read(prefix string, getenv func(string) string) (Settings, error) {
	e := envReader{prefix: prefix, getenv: getenv}
	var out Settings
	var err error
	if out.Seeds, err = e.seeds(0); err != nil {
		return Settings{}, err
	}
	if out.Duration, err = e.campaign(0); err != nil {
		return Settings{}, err
	}
	seed, err := e.seed(-1)
	if err != nil {
		return Settings{}, err
	}
	if seed >= 0 {
		out.HasSeed, out.Seed = true, uint64(seed)
	}
	return out, nil
}

// envReader reads one variable of a prefix.
type envReader struct {
	prefix string
	getenv func(string) string
}

func (e envReader) value(suffix string) (string, string) {
	name := e.prefix + "_" + suffix
	if e.getenv == nil {
		return name, ""
	}
	return name, e.getenv(name)
}

func (e envReader) seeds(def int) (int, error) {
	name, v := e.value("SEEDS")
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, Error{fmt.Sprintf("%s has the value %q: it must be a whole number of 1 or more, for example 200", name, v)}
	}
	return n, nil
}

func (e envReader) seed(def int64) (int64, error) {
	name, v := e.value("SEED")
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, Error{fmt.Sprintf("%s has the value %q: it must be a whole number of 0 or more, for example 17", name, v)}
	}
	return n, nil
}

func (e envReader) campaign(def time.Duration) (time.Duration, error) {
	name, v := e.value("CAMPAIGN")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, Error{fmt.Sprintf("%s has the value %q: it must be a duration longer than zero, for example 30s or 5m", name, v)}
	}
	return d, nil
}

// Describe returns a short help text for the three variables of
// prefix. Docs can use it, so the text and the code do not drift apart.
func Describe(prefix string) string {
	return prefix + "_SEED=<n>        run only seed n (n is 0 or more); the strongest setting\n" +
		prefix + "_CAMPAIGN=<time> run seeds until the time ends, for example 30s\n" +
		prefix + "_SEEDS=<n>       run seeds 1 to n (n is 1 or more)\n"
}

// Error is a bad value in an environment variable. The flag package prints
// its own errors; campaign.Main prints this one.
type Error struct{ msg string }

func (e Error) Error() string { return e.msg }
