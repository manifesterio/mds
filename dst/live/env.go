package live

import (
	"cmp"
	"crypto/rand"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"time"

	"github.com/manifesterio/mds/dst/actor"
)

// EnvOption changes what NewEnv does with a broken assertion.
type EnvOption func(*envConfig)

// envConfig is the options of NewEnv and, once built, its Assertions: it
// reports broken safety properties to onViolation.
type envConfig struct {
	onViolation func(kind, property string, details []actor.Pair)
}

// OnViolation calls f with the property name and the details when an Always,
// AlwaysOrUnreachable or Unreachable assertion breaks. f runs on the goroutine
// of the actor. The default, and a nil f, ignore violations.
func OnViolation(f func(property, detail string)) EnvOption {
	return func(c *envConfig) {
		c.onViolation = nil
		if f != nil {
			c.onViolation = func(_, property string, details []actor.Pair) {
				f(property, fmt.Sprint(details))
			}
		}
	}
}

// LogViolations logs every broken assertion with log (kind, prop, and the
// detail key/value pairs) and continues. A nil log is slog.Default.
func LogViolations(log *slog.Logger) EnvOption {
	return func(c *envConfig) {
		c.onViolation = func(kind, property string, details []actor.Pair) {
			log := cmp.Or(log, slog.Default())
			args := []any{"kind", kind, "prop", property}
			for _, d := range details {
				args = append(args, d.Key, d.Value)
			}
			log.Error("assertion violated", args...)
		}
	}
}

// NewEnv makes a production Env: a random stream seeded from crypto/rand,
// buggify sites that never fire, Sometimes and Reachable that record nothing,
// and Always, AlwaysOrUnreachable and Unreachable that report a violation to
// the option (none: ignored). An actor uses its Env only on its own goroutine.
func NewEnv(self actor.Incarnation, opts ...EnvOption) actor.Env {
	var c envConfig
	for _, o := range opts {
		o(&c)
	}
	var seed [32]byte
	_, _ = rand.Read(seed[:])
	r := actor.NewRand(mrand.NewChaCha8(seed))
	return actor.NewEnv(self, r, noFaults{}, c)
}

// noFaults is Faults that never fire.
type noFaults struct{}

func (noFaults) Maybe(actor.Site) bool           { return false }
func (noFaults) MaybeP(actor.Site, float64) bool { return false }
func (noFaults) Fault(actor.Site, float64) bool  { return false }

func (envConfig) Sometimes(bool, actor.Sometimes) {}
func (envConfig) Reachable(actor.Reachable)       {}

func (a envConfig) Always(c bool, p actor.Always, d ...actor.Pair) {
	if !c {
		a.violated(actor.KindAlways, p.Name(), d)
	}
}

func (a envConfig) AlwaysOrUnreachable(c bool, p actor.AlwaysOrUnreachable, d ...actor.Pair) {
	if !c {
		a.violated(actor.KindAlwaysOrUnreachable, p.Name(), d)
	}
}

func (a envConfig) Unreachable(p actor.Unreachable, d ...actor.Pair) {
	a.violated(actor.KindUnreachable, p.Name(), d)
}

func (a envConfig) violated(kind actor.AssertKind, name string, d []actor.Pair) {
	if a.onViolation != nil {
		a.onViolation(string(kind), name, d)
	}
}

// RealClock is the Clock of the process: time.Now and time.AfterFunc.
func RealClock() Clock { return realClock{start: time.Now()} }

type realClock struct{ start time.Time }

func (c realClock) Mono() actor.Mono { return actor.Mono(time.Since(c.start)) }
func (realClock) Wall() actor.Wall   { return actor.Wall(time.Now().UnixNano()) }

func (realClock) AfterFunc(d actor.Duration, f func()) Timer {
	return time.AfterFunc(time.Duration(d), f)
}
