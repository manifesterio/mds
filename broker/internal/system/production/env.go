package production

import (
	"log/slog"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/actor"
	"github.com/manifesterio/mds/dst/live"
)

// NewEnv returns the production Env of incarnation self that logs assertion
// violations to log and continues. If log is nil, slog.Default is used.
func NewEnv(self actor.Incarnation, log *slog.Logger) core.Environment {
	return live.NewEnv(self, live.LogViolations(log))
}
